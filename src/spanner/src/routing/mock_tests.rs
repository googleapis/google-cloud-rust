// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Mock server integration tests for Spanner location-aware routing and multi-server topologies.
//!
//! This test suite exercises:
//! - Inline `CacheUpdate` observation and cache population across single-use queries/reads, read-write
//!   transactions (commits, batch DML), write-only mutations, and partitioned DML.
//! - Direct multi-server routing, connection pre-warming, and gateway fallback on cache misses.
//! - Replica selection (leader vs. read-only replicas, candidate pool distribution, distance-tier
//!   prioritization, and directed read filtering).
//! - Cooldown and failure failover (leader cooldown fallback, replica skip/cooldown failover, and recovery).
//! - Dynamic range updates (split updates, generation ordering, and boundary lookups).
//! - Transaction affinity isolation across concurrent transactions and independence for read-only transactions.
//! - Proactive background cache synchronization via `CacheSubscriber`.

use crate::client::{Spanner, SpannerBuilderExt};
use crate::database_client::DatabaseClient;
use crate::key::KeySet;
use crate::model::directed_read_options::replica_selection::Type as ReplicaType;
use crate::model::directed_read_options::{
    ExcludeReplicas, IncludeReplicas, ReplicaSelection, Replicas,
};
use crate::model::tablet::Role;
use crate::model::{
    CacheUpdate as ModelCacheUpdate, DirectedReadOptions, Group as ModelGroup, Range as ModelRange,
    Tablet as ModelTablet,
};
use crate::mutation::Mutation;
use crate::omni::InstanceType;
use crate::read::ReadRequest;
use crate::read_write_transaction::ReadWriteTransaction;
use crate::routing::cache_subscriber::CacheSubscriber;
use crate::routing::directed_read::select_eligible_tablets_for_directed_read;
use crate::routing::key_range_cache::RangeMode;
use crate::routing::latency_registry::LatencyRegistry;
use crate::routing::location_router::RoutingContext;
use crate::statement::Statement;
use bytes::Bytes;
use gaxi::grpc::tonic::Response;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_test_macros::tokio_test_no_panics;
use prost_types::{Timestamp, Value};
use spanner_grpc_mock::MockSpanner;
use spanner_grpc_mock::google::rpc::Status;
use spanner_grpc_mock::google::spanner::v1 as mock_v1;
use spanner_grpc_mock::start;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio_test_no_panics]
async fn single_use_query_ingests_inline_cache_update_and_populates_caches() -> anyhow::Result<()> {
    let mut mock = create_base_mock();
    mock.expect_execute_streaming_sql().returning(|_| {
        let partial_result_set = sample_int64_partial_result_set(
            "SingerId",
            "42",
            Some(sample_mock_cache_update(
                123456789,
                5001,
                "tablet-5001-leader.spanner.internal:15000",
                "tablet-5001-follower.spanner.internal:15000",
            )),
        );
        let (sender, receiver) = mpsc::channel(1);
        sender
            .try_send(Ok(partial_result_set))
            .expect("should send partial result set");
        Ok(Response::from(receiver))
    });

    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    assert_eq!(
        database_client.database_id(),
        Some(0),
        "database ID should initially be 0 before any cache update"
    );

    let transaction = database_client.single_use().build();
    let statement = Statement::builder("SELECT SingerId FROM Singers WHERE SingerId = 42").build();
    let mut result_set = transaction.execute_query(statement).await?;

    let row = result_set.next().await;
    assert!(row.is_some(), "result set should yield at least one row");
    let row = row.expect("row must exist")?;
    assert_eq!(row.raw_values().len(), 1, "row should have 1 column");

    assert_eq!(
        database_client.database_id(),
        Some(123456789),
        "database ID should match the ingested cache update"
    );

    let recipe_cache = database_client
        .key_recipe_cache()
        .expect("key recipe cache must be present");
    assert!(
        recipe_cache.get_table_recipe("Singers").is_some(),
        "Singers table recipe should be cached"
    );

    let router = database_client
        .location_router()
        .expect("location router must be present");
    let range_lookup =
        router
            .key_range_cache()
            .find_range(b"singer_500", &[], RangeMode::CoveringSplit);
    assert!(
        range_lookup.is_some(),
        "key range covering singer_500 must be cached"
    );
    let cached_range = range_lookup.expect("cached range must exist");
    assert_eq!(
        cached_range.group_uid, 5001,
        "cached range must map to group 5001"
    );

    let group = router
        .key_range_cache()
        .get_group(5001)
        .expect("group 5001 must exist");
    assert_eq!(
        group.tablets.len(),
        2,
        "group should have leader and follower tablets"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn single_use_read_ingests_inline_cache_update() -> anyhow::Result<()> {
    let mut mock = create_base_mock();
    mock.expect_streaming_read().returning(|_| {
        let partial_result_set = sample_int64_partial_result_set(
            "SingerId",
            "100",
            Some(sample_mock_cache_update(
                987654321,
                7001,
                "tablet-7001-leader.spanner.internal:15000",
                "tablet-7001-follower.spanner.internal:15000",
            )),
        );
        let (sender, receiver) = mpsc::channel(1);
        sender
            .try_send(Ok(partial_result_set))
            .expect("should send streaming read partial result set");
        Ok(Response::from(receiver))
    });

    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    let transaction = database_client.single_use().build();
    let read_request = ReadRequest::builder("Singers", vec!["SingerId"])
        .with_keys(KeySet::all())
        .build();
    let mut result_set = transaction.execute_read(read_request).await?;

    let row = result_set.next().await;
    assert!(row.is_some(), "read should yield at least one row");

    assert_eq!(
        database_client.database_id(),
        Some(987654321),
        "database ID should update after streaming read"
    );

    let router = database_client
        .location_router()
        .expect("location router must be present");
    let cached_range =
        router
            .key_range_cache()
            .find_range(b"singer_200", &[], RangeMode::CoveringSplit);
    assert!(
        cached_range.is_some(),
        "range covering singer_200 should be cached"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn read_write_transaction_commit_ingests_cache_update_and_sends_route_to_leader()
-> anyhow::Result<()> {
    let mut mock = create_base_mock();
    mock.expect_begin_transaction().returning(|_| {
        Ok(Response::new(mock_v1::Transaction {
            id: vec![1, 2, 3, 4],
            ..Default::default()
        }))
    });

    mock.expect_commit().returning(|request| {
        let headers = request.metadata();
        assert_eq!(
            headers
                .get("x-goog-spanner-route-to-leader")
                .and_then(|header_value| header_value.to_str().ok()),
            Some("true"),
            "commit request must include x-goog-spanner-route-to-leader header"
        );

        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(Timestamp {
                seconds: 1700000000,
                nanos: 0,
            }),
            cache_update: Some(sample_mock_cache_update(
                555555,
                9001,
                "tablet-9001-leader.spanner.internal:15000",
                "tablet-9001-follower.spanner.internal:15000",
            )),
            ..Default::default()
        }))
    });

    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    let runner = database_client.read_write_transaction().build().await?;
    runner
        .run(|transaction: ReadWriteTransaction| async move {
            transaction.buffer(vec![
                Mutation::new_insert_builder("Singers")
                    .set("SingerId")
                    .to(101i64)
                    .set("Name")
                    .to("Alice")
                    .build(),
            ])?;
            Ok(())
        })
        .await?;

    assert_eq!(
        database_client.database_id(),
        Some(555555),
        "database ID should update after commit"
    );

    let router = database_client
        .location_router()
        .expect("location router must be present");
    let cached_range =
        router
            .key_range_cache()
            .find_range(b"singer_100", &[], RangeMode::CoveringSplit);
    assert!(
        cached_range.is_some(),
        "range covering singer_100 should be cached from commit"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn read_write_transaction_batch_dml_ingests_cache_update() -> anyhow::Result<()> {
    let mut mock = create_base_mock();
    mock.expect_begin_transaction().returning(|_| {
        Ok(Response::new(mock_v1::Transaction {
            id: vec![1, 2, 3, 4],
            ..Default::default()
        }))
    });

    mock.expect_execute_batch_dml().returning(|_| {
        Ok(Response::new(mock_v1::ExecuteBatchDmlResponse {
            result_sets: vec![mock_v1::ResultSet {
                metadata: Some(mock_v1::ResultSetMetadata {
                    transaction: Some(mock_v1::Transaction {
                        id: vec![1, 2, 3, 4],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                cache_update: Some(sample_mock_cache_update(
                    333333,
                    4001,
                    "tablet-4001-leader.spanner.internal:15000",
                    "tablet-4001-follower.spanner.internal:15000",
                )),
                ..Default::default()
            }],
            status: Some(Status {
                code: 0,
                ..Default::default()
            }),
            ..Default::default()
        }))
    });

    mock.expect_commit().returning(|_| {
        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(Timestamp {
                seconds: 1700000001,
                nanos: 0,
            }),
            ..Default::default()
        }))
    });

    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    let runner = database_client.read_write_transaction().build().await?;
    runner
        .run(|transaction: ReadWriteTransaction| async move {
            transaction
                .execute_batch_update(vec![Statement::from("UPDATE Singers SET Active = true")])
                .await?;
            Ok(())
        })
        .await?;

    assert_eq!(
        database_client.database_id(),
        Some(333333),
        "database ID should update after batch DML"
    );

    let router = database_client
        .location_router()
        .expect("location router must be present");
    let cached_range =
        router
            .key_range_cache()
            .find_range(b"singer_300", &[], RangeMode::CoveringSplit);
    assert!(
        cached_range.is_some(),
        "range covering singer_300 should be cached from batch DML"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn write_only_transaction_ingests_cache_update() -> anyhow::Result<()> {
    let mut mock = create_base_mock();
    mock.expect_commit().returning(|_| {
        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(Timestamp {
                seconds: 1700000002,
                nanos: 0,
            }),
            cache_update: Some(sample_mock_cache_update(
                222222,
                3001,
                "tablet-3001-leader.spanner.internal:15000",
                "tablet-3001-follower.spanner.internal:15000",
            )),
            ..Default::default()
        }))
    });

    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    let mutation = Mutation::new_insert_builder("Singers")
        .set("SingerId")
        .to(202i64)
        .set("Name")
        .to("Bob")
        .build();

    let transaction = database_client.write_only_transaction().build();
    transaction.write_at_least_once(vec![mutation]).await?;

    assert_eq!(
        database_client.database_id(),
        Some(222222),
        "database ID should update after write_at_least_once"
    );

    let router = database_client
        .location_router()
        .expect("location router must be present");
    let cached_range =
        router
            .key_range_cache()
            .find_range(b"singer_400", &[], RangeMode::CoveringSplit);
    assert!(
        cached_range.is_some(),
        "range covering singer_400 should be cached from write_at_least_once"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn partitioned_dml_ingests_cache_update() -> anyhow::Result<()> {
    let mut mock = create_base_mock();
    mock.expect_begin_transaction().returning(|_| {
        Ok(Response::new(mock_v1::Transaction {
            id: vec![1, 2, 3, 4],
            ..Default::default()
        }))
    });
    mock.expect_execute_streaming_sql().returning(|_| {
        let partial_result_set = mock_v1::PartialResultSet {
            stats: Some(mock_v1::ResultSetStats {
                row_count: Some(mock_v1::result_set_stats::RowCount::RowCountLowerBound(50)),
                ..Default::default()
            }),
            cache_update: Some(sample_mock_cache_update(
                444444,
                6001,
                "tablet-6001-leader.spanner.internal:15000",
                "tablet-6001-follower.spanner.internal:15000",
            )),
            last: true,
            ..Default::default()
        };
        let (sender, receiver) = mpsc::channel(1);
        sender
            .try_send(Ok(partial_result_set))
            .expect("should send partial result set");
        Ok(Response::from(receiver))
    });

    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    let affected_rows = database_client
        .partitioned_dml_transaction()
        .build()
        .await?
        .execute_update(Statement::from(
            "UPDATE Singers SET Active = true WHERE true",
        ))
        .await?;

    assert_eq!(affected_rows, 50, "partitioned DML should affect 50 rows");

    assert_eq!(
        database_client.database_id(),
        Some(444444),
        "database ID should update after partitioned DML"
    );

    let router = database_client
        .location_router()
        .expect("location router must be present");
    let cached_range =
        router
            .key_range_cache()
            .find_range(b"singer_500", &[], RangeMode::CoveringSplit);
    assert!(
        cached_range.is_some(),
        "range covering singer_500 should be cached from partitioned DML"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn database_id_switch_clears_stale_caches_and_rejects_older_generations() -> anyhow::Result<()>
{
    let mock = create_base_mock();
    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    // Ingest initial generation (database_id = 100)
    let initial_update = sample_model_cache_update(
        100,
        1001,
        "tablet-1001-leader.spanner.internal:15000",
        "tablet-1001-follower.spanner.internal:15000",
    );
    database_client.observe_cache_update(Some(initial_update));

    assert_eq!(
        database_client.database_id(),
        Some(100),
        "initial database ID should be 100"
    );
    assert!(
        router
            .key_range_cache()
            .find_range(b"singer_100", &[], RangeMode::CoveringSplit)
            .is_some(),
        "initial range should be present"
    );

    // Ingest newer generation (database_id = 200)
    let mut newer_update = sample_model_cache_update(
        200,
        2001,
        "tablet-2001-leader.spanner.internal:15000",
        "tablet-2001-follower.spanner.internal:15000",
    );
    newer_update.range[0].start_key = Bytes::from_static(b"newer_001");
    newer_update.range[0].limit_key = Bytes::from_static(b"newer_999");
    database_client.observe_cache_update(Some(newer_update));

    assert_eq!(
        database_client.database_id(),
        Some(200),
        "database ID should advance to 200"
    );
    assert!(
        router
            .key_range_cache()
            .find_range(b"singer_100", &[], RangeMode::CoveringSplit)
            .is_none(),
        "older generation ranges must be cleared on database ID transition"
    );
    assert!(
        router
            .key_range_cache()
            .find_range(b"newer_100", &[], RangeMode::CoveringSplit)
            .is_some(),
        "newer generation range must be present"
    );

    // Ingest stale generation (database_id = 150 < 200)
    let stale_update = sample_model_cache_update(
        150,
        3001,
        "tablet-3001-leader.spanner.internal:15000",
        "tablet-3001-follower.spanner.internal:15000",
    );
    database_client.observe_cache_update(Some(stale_update));

    assert_eq!(
        database_client.database_id(),
        Some(200),
        "database ID must not regress on stale update"
    );
    assert!(
        router
            .key_range_cache()
            .find_range(b"newer_100", &[], RangeMode::CoveringSplit)
            .is_some(),
        "valid generation ranges must remain intact"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn multi_server_routing_resolves_and_prewarms_tablet_endpoint() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_tablet = create_base_mock();
    let (tablet_address, _tablet_server) = start("127.0.0.1:0", mock_tablet).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address.clone())
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    // Ingest a cache update advertising the tablet mock server address
    let update = sample_model_cache_update(
        1001,
        8001,
        &tablet_address,
        "tablet-8001-follower.spanner.internal:15000",
    );
    database_client.observe_cache_update(Some(update));

    // Ensure connection cache has pre-warmed / connected to the tablet address
    let connection_cache = router.connection_cache();
    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _prewarmed = connection_cache.get(&tablet_address, client_config).await?;

    // Route request targeting key inside the cached range
    let context = RoutingContext {
        routing_key: Some(b"singer_500"),
        prefer_leader: true,
        ..Default::default()
    };
    let resolved = router.resolve_connection(&context);
    assert_eq!(
        resolved.address(),
        tablet_address,
        "routing must resolve directly to the tablet mock server"
    );

    // Route request targeting key outside any cached range -> falls back to gateway
    let unmapped_context = RoutingContext {
        routing_key: Some(b"unknown_key_999"),
        prefer_leader: true,
        ..Default::default()
    };
    let fallback = router.resolve_connection(&unmapped_context);
    assert_eq!(
        fallback.address(),
        gateway_address,
        "routing must fall back to the default gateway on cache miss"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn multi_replica_power_of_two_selection_distributes_requests() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_replica1 = create_base_mock();
    let (replica1_address, _replica1_server) = start("127.0.0.1:0", mock_replica1).await?;

    let mock_replica2 = create_base_mock();
    let (replica2_address, _replica2_server) = start("127.0.0.1:0", mock_replica2).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    // Ingest cache update with two follower replicas
    let update = sample_model_cache_update(2002, 9001, &replica1_address, &replica2_address);
    database_client.observe_cache_update(Some(update));

    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _ = router
        .connection_cache()
        .get(&replica1_address, client_config)
        .await?;
    let _ = router
        .connection_cache()
        .get(&replica2_address, client_config)
        .await?;

    let group = router
        .key_range_cache()
        .get_group(9001)
        .expect("group 9001 must exist");

    // 1. Deterministically verify that both follower replicas are in the eligible candidate pool
    assert_eq!(
        group.eligible_replica_indices.len(),
        2,
        "group should have exactly 2 eligible follower replicas for load balancing"
    );
    let candidate_addresses: Vec<&str> = group
        .eligible_replica_indices
        .iter()
        .map(|&index| group.tablets[index].server_address.as_str())
        .collect();
    assert!(
        candidate_addresses.contains(&replica1_address.as_str()),
        "candidate pool must contain replica 1"
    );
    assert!(
        candidate_addresses.contains(&replica2_address.as_str()),
        "candidate pool must contain replica 2"
    );

    // 2. Deterministically verify that resolution returns one of the eligible candidate replicas
    let context = RoutingContext {
        routing_key: Some(b"singer_500"),
        prefer_leader: false,
        ..Default::default()
    };
    let connection = router.resolve_connection(&context);
    let address = connection.address();
    assert!(
        address == replica1_address || address == replica2_address,
        "routing resolution must select an eligible candidate replica"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn non_leader_failover_when_candidate_replica_skipped() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_replica1 = create_base_mock();
    let (replica1_address, _replica1_server) = start("127.0.0.1:0", mock_replica1).await?;

    let mock_replica2 = create_base_mock();
    let (replica2_address, _replica2_server) = start("127.0.0.1:0", mock_replica2).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    // Ingest topology where replica 1 is skipped, leaving only replica 2 eligible
    let update_skip1 = ModelCacheUpdate {
        database_id: 2003,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"singer_001"),
            limit_key: Bytes::from_static(b"singer_999"),
            group_uid: 9002,
            split_id: 9002,
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 9002,
            leader_index: -1,
            tablets: vec![
                ModelTablet {
                    tablet_uid: 9001,
                    server_address: replica1_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: true,
                    _unknown_fields: Default::default(),
                },
                ModelTablet {
                    tablet_uid: 9002,
                    server_address: replica2_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
            ],
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update_skip1));

    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _ = router
        .connection_cache()
        .get(&replica1_address, client_config)
        .await?;
    let _ = router
        .connection_cache()
        .get(&replica2_address, client_config)
        .await?;

    let context = RoutingContext {
        routing_key: Some(b"singer_500"),
        prefer_leader: false,
        ..Default::default()
    };

    assert_eq!(
        router.resolve_connection(&context).address(),
        replica2_address,
        "when replica 1 is skipped, resolution must deterministically route to replica 2"
    );

    // Ingest update where replica 2 is skipped, leaving replica 1 eligible
    let update_skip2 = ModelCacheUpdate {
        database_id: 2003,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"singer_001"),
            limit_key: Bytes::from_static(b"singer_999"),
            group_uid: 9002,
            split_id: 9002,
            generation: Bytes::from_static(b"gen_2"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 9002,
            leader_index: -1,
            tablets: vec![
                ModelTablet {
                    tablet_uid: 9001,
                    server_address: replica1_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_2"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
                ModelTablet {
                    tablet_uid: 9002,
                    server_address: replica2_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_2"),
                    distance: 1,
                    skip: true,
                    _unknown_fields: Default::default(),
                },
            ],
            generation: Bytes::from_static(b"gen_2"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update_skip2));

    assert_eq!(
        router.resolve_connection(&context).address(),
        replica1_address,
        "when replica 2 is skipped, resolution must deterministically route to replica 1"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn transaction_affinity_lifecycle_binds_and_clears_affinity() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_tablet = create_base_mock();
    let (tablet_address, _tablet_server) = start("127.0.0.1:0", mock_tablet).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address.clone())
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    let update = sample_model_cache_update(
        3003,
        7001,
        &tablet_address,
        "tablet-7001-follower.spanner.internal:15000",
    );
    database_client.observe_cache_update(Some(update));

    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _ = router
        .connection_cache()
        .get(&tablet_address, client_config)
        .await?;

    let transaction_id = b"transaction-affinity-test-123";

    // First request in transaction: has routing key, establishes affinity to tablet
    let initial_context = RoutingContext {
        transaction_id: Some(transaction_id),
        use_transaction_affinity: true,
        routing_key: Some(b"singer_500"),
        prefer_leader: true,
    };
    let initial_connection = router.resolve_connection(&initial_context);
    assert_eq!(
        initial_connection.address(),
        tablet_address,
        "first transaction request should resolve to tablet replica"
    );

    // Second request in transaction: no routing key provided, but uses transaction affinity
    let subsequent_context = RoutingContext {
        transaction_id: Some(transaction_id),
        use_transaction_affinity: true,
        routing_key: None,
        prefer_leader: true,
    };
    let pinned_connection = router.resolve_connection(&subsequent_context);
    assert_eq!(
        pinned_connection.address(),
        tablet_address,
        "subsequent request in same transaction must be pinned to the affinity address"
    );

    // Clear transaction affinity (simulating commit or rollback)
    router.clear_transaction_affinity(transaction_id);

    // Subsequent request after affinity cleared: falls back to default gateway
    let post_commit_context = RoutingContext {
        transaction_id: Some(transaction_id),
        use_transaction_affinity: true,
        routing_key: None,
        prefer_leader: true,
    };
    let fallback_connection = router.resolve_connection(&post_commit_context);
    assert_eq!(
        fallback_connection.address(),
        gateway_address,
        "clearing transaction affinity must restore default fallback routing"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn read_only_transaction_does_not_bind_affinity_and_routes_independently()
-> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_tablet1 = create_base_mock();
    let (tablet1_address, _tablet1_server) = start("127.0.0.1:0", mock_tablet1).await?;

    let mock_tablet2 = create_base_mock();
    let (tablet2_address, _tablet2_server) = start("127.0.0.1:0", mock_tablet2).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    // Ingest two distinct ranges mapping to tablet 1 and tablet 2
    let update = ModelCacheUpdate {
        database_id: 3004,
        range: vec![
            ModelRange {
                start_key: Bytes::from_static(b"a"),
                limit_key: Bytes::from_static(b"m"),
                group_uid: 1001,
                split_id: 1001,
                generation: Bytes::from_static(b"gen_1"),
                _unknown_fields: Default::default(),
            },
            ModelRange {
                start_key: Bytes::from_static(b"m"),
                limit_key: Bytes::from_static(b"z"),
                group_uid: 1002,
                split_id: 1002,
                generation: Bytes::from_static(b"gen_1"),
                _unknown_fields: Default::default(),
            },
        ],
        group: vec![
            ModelGroup {
                group_uid: 1001,
                leader_index: 0,
                tablets: vec![ModelTablet {
                    tablet_uid: 1001,
                    server_address: tablet1_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                }],
                generation: Bytes::from_static(b"gen_1"),
                _unknown_fields: Default::default(),
            },
            ModelGroup {
                group_uid: 1002,
                leader_index: 0,
                tablets: vec![ModelTablet {
                    tablet_uid: 1002,
                    server_address: tablet2_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                }],
                generation: Bytes::from_static(b"gen_1"),
                _unknown_fields: Default::default(),
            },
        ],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update));

    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _ = router
        .connection_cache()
        .get(&tablet1_address, client_config)
        .await?;
    let _ = router
        .connection_cache()
        .get(&tablet2_address, client_config)
        .await?;

    let read_only_txn_id = b"read-only-txn-xyz";

    // Request 1 targeting range [a, m) with use_transaction_affinity: false
    let context_range1 = RoutingContext {
        transaction_id: Some(read_only_txn_id),
        use_transaction_affinity: false,
        routing_key: Some(b"f"),
        prefer_leader: false,
    };
    let connection1 = router.resolve_connection(&context_range1);
    assert_eq!(
        connection1.address(),
        tablet1_address,
        "first read-only query must route to tablet 1"
    );

    // Request 2 targeting range [m, z) with same transaction_id should route independently to tablet 2
    let context_range2 = RoutingContext {
        transaction_id: Some(read_only_txn_id),
        use_transaction_affinity: false,
        routing_key: Some(b"p"),
        prefer_leader: false,
    };
    let connection2 = router.resolve_connection(&context_range2);
    assert_eq!(
        connection2.address(),
        tablet2_address,
        "second read-only query in same transaction must route independently to tablet 2"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn concurrent_transactions_bind_independent_affinities() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_tablet1 = create_base_mock();
    let (tablet1_address, _tablet1_server) = start("127.0.0.1:0", mock_tablet1).await?;

    let mock_tablet2 = create_base_mock();
    let (tablet2_address, _tablet2_server) = start("127.0.0.1:0", mock_tablet2).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    let update = ModelCacheUpdate {
        database_id: 3005,
        range: vec![
            ModelRange {
                start_key: Bytes::from_static(b"a"),
                limit_key: Bytes::from_static(b"m"),
                group_uid: 1001,
                split_id: 1001,
                generation: Bytes::from_static(b"gen_1"),
                _unknown_fields: Default::default(),
            },
            ModelRange {
                start_key: Bytes::from_static(b"m"),
                limit_key: Bytes::from_static(b"z"),
                group_uid: 1002,
                split_id: 1002,
                generation: Bytes::from_static(b"gen_1"),
                _unknown_fields: Default::default(),
            },
        ],
        group: vec![
            ModelGroup {
                group_uid: 1001,
                leader_index: 0,
                tablets: vec![ModelTablet {
                    tablet_uid: 1001,
                    server_address: tablet1_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadWrite,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                }],
                generation: Bytes::from_static(b"gen_1"),
                _unknown_fields: Default::default(),
            },
            ModelGroup {
                group_uid: 1002,
                leader_index: 0,
                tablets: vec![ModelTablet {
                    tablet_uid: 1002,
                    server_address: tablet2_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadWrite,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                }],
                generation: Bytes::from_static(b"gen_1"),
                _unknown_fields: Default::default(),
            },
        ],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update));

    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _ = router
        .connection_cache()
        .get(&tablet1_address, client_config)
        .await?;
    let _ = router
        .connection_cache()
        .get(&tablet2_address, client_config)
        .await?;

    let txn_1 = b"transaction-1";
    let txn_2 = b"transaction-2";

    // Bind Txn 1 to tablet 1
    let context_txn1 = RoutingContext {
        transaction_id: Some(txn_1),
        use_transaction_affinity: true,
        routing_key: Some(b"f"),
        prefer_leader: true,
    };
    let connection1 = router.resolve_connection(&context_txn1);
    assert_eq!(
        connection1.address(),
        tablet1_address,
        "txn 1 binds to tablet 1"
    );

    // Bind Txn 2 to tablet 2
    let context_txn2 = RoutingContext {
        transaction_id: Some(txn_2),
        use_transaction_affinity: true,
        routing_key: Some(b"p"),
        prefer_leader: true,
    };
    let connection2 = router.resolve_connection(&context_txn2);
    assert_eq!(
        connection2.address(),
        tablet2_address,
        "txn 2 binds to tablet 2"
    );

    // Subsequent calls with no routing key verify isolation
    let pinned_txn1 = RoutingContext {
        transaction_id: Some(txn_1),
        use_transaction_affinity: true,
        routing_key: None,
        prefer_leader: true,
    };
    let pinned_txn2 = RoutingContext {
        transaction_id: Some(txn_2),
        use_transaction_affinity: true,
        routing_key: None,
        prefer_leader: true,
    };

    assert_eq!(
        router.resolve_connection(&pinned_txn1).address(),
        tablet1_address,
        "txn 1 remains pinned to tablet 1"
    );
    assert_eq!(
        router.resolve_connection(&pinned_txn2).address(),
        tablet2_address,
        "txn 2 remains pinned to tablet 2"
    );

    // Clear Txn 1 -> Txn 2 remains untouched
    router.clear_transaction_affinity(txn_1);
    assert_ne!(
        router.resolve_connection(&pinned_txn1).address(),
        tablet1_address,
        "clearing txn 1 resets its affinity"
    );
    assert_eq!(
        router.resolve_connection(&pinned_txn2).address(),
        tablet2_address,
        "txn 2 affinity remains valid after txn 1 is cleared"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn directed_read_routes_to_matching_location_and_role() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_central = create_base_mock();
    let (central_address, _central_server) = start("127.0.0.1:0", mock_central).await?;

    let mock_east = create_base_mock();
    let (east_address, _east_server) = start("127.0.0.1:0", mock_east).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    // Ingest custom topology with central and east replicas
    let update = ModelCacheUpdate {
        database_id: 4004,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"singer_001"),
            limit_key: Bytes::from_static(b"singer_999"),
            group_uid: 4001,
            split_id: 4001,
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 4001,
            leader_index: 0,
            tablets: vec![
                ModelTablet {
                    tablet_uid: 4001,
                    server_address: central_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadWrite,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
                ModelTablet {
                    tablet_uid: 4002,
                    server_address: east_address.clone(),
                    location: "us-east1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 2,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
            ],
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update));

    let directed_options = DirectedReadOptions {
        replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
            replica_selections: vec![ReplicaSelection {
                location: "us-east1".to_string(),
                r#type: ReplicaType::ReadOnly,
                _unknown_fields: Default::default(),
            }],
            auto_failover_disabled: false,
            _unknown_fields: Default::default(),
        }))),
        _unknown_fields: Default::default(),
    };

    let router = database_client
        .location_router()
        .expect("location router present");
    let group = router
        .key_range_cache()
        .get_group(4001)
        .expect("group 4001 must exist");

    let eligible = select_eligible_tablets_for_directed_read(
        &group.tablets,
        Some(0),
        false,
        Some(&directed_options),
    );

    assert_eq!(
        eligible.len(),
        1,
        "directed read must match exactly 1 replica"
    );
    assert_eq!(
        eligible[0].server_address, east_address,
        "directed read targeting us-east1 must select the east replica"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn directed_read_exclude_replicas_filters_out_excluded_locations() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let update = ModelCacheUpdate {
        database_id: 4005,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"singer_001"),
            limit_key: Bytes::from_static(b"singer_999"),
            group_uid: 4005,
            split_id: 4005,
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 4005,
            leader_index: 0,
            tablets: vec![
                ModelTablet {
                    tablet_uid: 5001,
                    server_address: "central-node:15000".to_string(),
                    location: "us-central1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
                ModelTablet {
                    tablet_uid: 5002,
                    server_address: "east-node:15000".to_string(),
                    location: "us-east1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
            ],
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update));

    let exclude_options = DirectedReadOptions {
        replicas: Some(Replicas::ExcludeReplicas(Box::new(ExcludeReplicas {
            replica_selections: vec![ReplicaSelection {
                location: "us-central1".to_string(),
                r#type: ReplicaType::ReadOnly,
                _unknown_fields: Default::default(),
            }],
            _unknown_fields: Default::default(),
        }))),
        _unknown_fields: Default::default(),
    };

    let router = database_client
        .location_router()
        .expect("location router present");
    let group = router
        .key_range_cache()
        .get_group(4005)
        .expect("group 4005 must exist");

    let eligible = select_eligible_tablets_for_directed_read(
        &group.tablets,
        None,
        false,
        Some(&exclude_options),
    );

    assert_eq!(
        eligible.len(),
        1,
        "excluding us-central1 must leave only the east replica"
    );
    assert_eq!(
        eligible[0].server_address, "east-node:15000",
        "eligible replica must be east-node"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn multi_region_distance_tier_prioritizes_local_replicas() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_local = create_base_mock();
    let (local_address, _local_server) = start("127.0.0.1:0", mock_local).await?;

    let mock_remote = create_base_mock();
    let (remote_address, _remote_server) = start("127.0.0.1:0", mock_remote).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    let update = ModelCacheUpdate {
        database_id: 6001,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"singer_001"),
            limit_key: Bytes::from_static(b"singer_999"),
            group_uid: 6001,
            split_id: 6001,
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 6001,
            leader_index: -1,
            tablets: vec![
                ModelTablet {
                    tablet_uid: 6001,
                    server_address: local_address.clone(),
                    location: "us-central1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
                ModelTablet {
                    tablet_uid: 6002,
                    server_address: remote_address.clone(),
                    location: "europe-west1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 10,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
            ],
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update));

    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _ = router
        .connection_cache()
        .get(&local_address, client_config)
        .await?;
    let _ = router
        .connection_cache()
        .get(&remote_address, client_config)
        .await?;

    let context = RoutingContext {
        routing_key: Some(b"singer_500"),
        prefer_leader: false,
        ..Default::default()
    };

    // When both local (dist=1) and remote (dist=10) are present, resolution strictly selects the local replica
    for _ in 0..10 {
        let connection = router.resolve_connection(&context);
        assert_eq!(
            connection.address(),
            local_address,
            "routing resolution must prioritize local distance tier replicas"
        );
    }

    Ok(())
}

#[tokio_test_no_panics]
async fn endpoint_cooldown_leader_on_cooldown_falls_back_to_gateway() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_leader = create_base_mock();
    let (leader_address, _leader_server) = start("127.0.0.1:0", mock_leader).await?;

    let mock_follower = create_base_mock();
    let (follower_address, _follower_server) = start("127.0.0.1:0", mock_follower).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address.clone())
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    let update = sample_model_cache_update(777, 8001, &leader_address, &follower_address);
    database_client.observe_cache_update(Some(update));

    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _ = router
        .connection_cache()
        .get(&leader_address, client_config)
        .await?;
    let _ = router
        .connection_cache()
        .get(&follower_address, client_config)
        .await?;

    let context = RoutingContext {
        routing_key: Some(b"singer_500"),
        prefer_leader: true,
        ..Default::default()
    };

    let resolved_connection = router.resolve_connection(&context);
    assert_eq!(
        resolved_connection.address(),
        leader_address,
        "should resolve to leader address before cooldown"
    );

    // Place the leader on cooldown
    router.cooldown_tracker().record_failure(&leader_address);

    // For prefer_leader: true requests, when the leader is on cooldown, router falls back to default gateway
    let resolved_fallback = router.resolve_connection(&context);
    assert_eq!(
        resolved_fallback.address(),
        gateway_address,
        "request requiring leader must fall back to default gateway when leader is on cooldown"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn endpoint_cooldown_recovers_after_clear() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_leader = create_base_mock();
    let (leader_address, _leader_server) = start("127.0.0.1:0", mock_leader).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address.clone())
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    let update = sample_model_cache_update(
        888,
        8002,
        &leader_address,
        "tablet-8002-follower.spanner.internal:15000",
    );
    database_client.observe_cache_update(Some(update));

    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _ = router
        .connection_cache()
        .get(&leader_address, client_config)
        .await?;

    let context = RoutingContext {
        routing_key: Some(b"singer_500"),
        prefer_leader: true,
        ..Default::default()
    };

    router.cooldown_tracker().record_failure(&leader_address);
    assert_eq!(
        router.resolve_connection(&context).address(),
        gateway_address,
        "single replica on cooldown falls back to default gateway"
    );

    // Clear cooldown state
    router.cooldown_tracker().clear();

    assert_eq!(
        router.resolve_connection(&context).address(),
        leader_address,
        "clearing cooldown restores routing eligibility for the tablet"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn all_replicas_on_cooldown_falls_back_to_gateway() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let mock_rep1 = create_base_mock();
    let (rep1_address, _rep1_server) = start("127.0.0.1:0", mock_rep1).await?;

    let mock_rep2 = create_base_mock();
    let (rep2_address, _rep2_server) = start("127.0.0.1:0", mock_rep2).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address.clone())
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    let update = sample_model_cache_update(999, 9001, &rep1_address, &rep2_address);
    database_client.observe_cache_update(Some(update));

    let client_config = database_client
        .cache_updater()
        .expect("cache updater present")
        .client_config();
    let _ = router
        .connection_cache()
        .get(&rep1_address, client_config)
        .await?;
    let _ = router
        .connection_cache()
        .get(&rep2_address, client_config)
        .await?;

    // Mark both replicas on cooldown
    router.cooldown_tracker().record_failure(&rep1_address);
    router.cooldown_tracker().record_failure(&rep2_address);

    let context = RoutingContext {
        routing_key: Some(b"singer_500"),
        prefer_leader: false,
        ..Default::default()
    };

    let resolved = router.resolve_connection(&context);
    assert_eq!(
        resolved.address(),
        gateway_address,
        "when all replicas are on cooldown, routing must gracefully fall back to gateway"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn skipped_tablets_fall_back_to_gateway() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address.clone())
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    let update = ModelCacheUpdate {
        database_id: 111,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"singer_001"),
            limit_key: Bytes::from_static(b"singer_999"),
            group_uid: 1111,
            split_id: 1111,
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 1111,
            leader_index: 0,
            tablets: vec![ModelTablet {
                tablet_uid: 1111,
                server_address: "skipped-leader:15000".to_string(),
                location: "us-central1".to_string(),
                role: Role::ReadWrite,
                incarnation: Bytes::from_static(b"inc_1"),
                distance: 1,
                skip: true,
                _unknown_fields: Default::default(),
            }],
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update));

    let router = database_client
        .location_router()
        .expect("location router present");
    let context = RoutingContext {
        routing_key: Some(b"singer_500"),
        prefer_leader: true,
        ..Default::default()
    };

    let resolved = router.resolve_connection(&context);
    assert_eq!(
        resolved.address(),
        gateway_address,
        "tablets marked skip: true must be bypassed, falling back to gateway"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn split_updates_replaces_parent_ranges_and_routes_to_new_groups() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    // Ingest initial wide range [a, z)
    let initial_update = ModelCacheUpdate {
        database_id: 500,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"a"),
            limit_key: Bytes::from_static(b"z"),
            group_uid: 5001,
            split_id: 5001,
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 5001,
            leader_index: 0,
            tablets: vec![ModelTablet {
                tablet_uid: 5001,
                server_address: "tablet-5001:15000".to_string(),
                location: "us-central1".to_string(),
                role: Role::ReadWrite,
                incarnation: Bytes::from_static(b"inc_1"),
                distance: 1,
                skip: false,
                _unknown_fields: Default::default(),
            }],
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(initial_update));

    let router = database_client
        .location_router()
        .expect("location router present");
    assert_eq!(
        router
            .key_range_cache()
            .find_range(b"f", &[], RangeMode::CoveringSplit)
            .expect("range for f must exist")
            .group_uid,
        5001
    );

    // Ingest split: [a, m) -> group 6001, [m, z) -> group 6002 with gen_2
    let split_update = ModelCacheUpdate {
        database_id: 500,
        range: vec![
            ModelRange {
                start_key: Bytes::from_static(b"a"),
                limit_key: Bytes::from_static(b"m"),
                group_uid: 6001,
                split_id: 6001,
                generation: Bytes::from_static(b"gen_2"),
                _unknown_fields: Default::default(),
            },
            ModelRange {
                start_key: Bytes::from_static(b"m"),
                limit_key: Bytes::from_static(b"z"),
                group_uid: 6002,
                split_id: 6002,
                generation: Bytes::from_static(b"gen_2"),
                _unknown_fields: Default::default(),
            },
        ],
        group: vec![
            ModelGroup {
                group_uid: 6001,
                leader_index: 0,
                tablets: vec![ModelTablet {
                    tablet_uid: 6001,
                    server_address: "tablet-6001:15000".to_string(),
                    location: "us-central1".to_string(),
                    role: Role::ReadWrite,
                    incarnation: Bytes::from_static(b"inc_2"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                }],
                generation: Bytes::from_static(b"gen_2"),
                _unknown_fields: Default::default(),
            },
            ModelGroup {
                group_uid: 6002,
                leader_index: 0,
                tablets: vec![ModelTablet {
                    tablet_uid: 6002,
                    server_address: "tablet-6002:15000".to_string(),
                    location: "us-central1".to_string(),
                    role: Role::ReadWrite,
                    incarnation: Bytes::from_static(b"inc_2"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                }],
                generation: Bytes::from_static(b"gen_2"),
                _unknown_fields: Default::default(),
            },
        ],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(split_update));

    assert_eq!(
        router
            .key_range_cache()
            .find_range(b"f", &[], RangeMode::CoveringSplit)
            .expect("range for f must exist")
            .group_uid,
        6001,
        "key f in [a, m) must route to split group 6001"
    );
    assert_eq!(
        router
            .key_range_cache()
            .find_range(b"r", &[], RangeMode::CoveringSplit)
            .expect("range for r must exist")
            .group_uid,
        6002,
        "key r in [m, z) must route to split group 6002"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn overlapping_key_range_stale_generation_rejected() -> anyhow::Result<()> {
    let mock = create_base_mock();
    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    // Ingest gen_2 range
    let update_gen2 = ModelCacheUpdate {
        database_id: 100,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"a"),
            limit_key: Bytes::from_static(b"z"),
            group_uid: 2000,
            split_id: 2000,
            generation: Bytes::from_static(b"gen_2"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 2000,
            leader_index: 0,
            tablets: vec![ModelTablet {
                tablet_uid: 2000,
                server_address: "node-gen2:15000".to_string(),
                location: "us-central1".to_string(),
                role: Role::ReadWrite,
                incarnation: Bytes::from_static(b"inc_2"),
                distance: 1,
                skip: false,
                _unknown_fields: Default::default(),
            }],
            generation: Bytes::from_static(b"gen_2"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update_gen2));

    let range = router
        .key_range_cache()
        .find_range(b"f", &[], RangeMode::CoveringSplit)
        .expect("range for f must exist");
    assert_eq!(range.group_uid, 2000);
    assert_eq!(range.generation.as_ref(), b"gen_2");

    // Ingest stale gen_1 update covering the same range
    let update_gen1 = ModelCacheUpdate {
        database_id: 100,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"a"),
            limit_key: Bytes::from_static(b"z"),
            group_uid: 1000,
            split_id: 1000,
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 1000,
            leader_index: 0,
            tablets: vec![ModelTablet {
                tablet_uid: 1000,
                server_address: "node-gen1:15000".to_string(),
                location: "us-central1".to_string(),
                role: Role::ReadWrite,
                incarnation: Bytes::from_static(b"inc_1"),
                distance: 1,
                skip: false,
                _unknown_fields: Default::default(),
            }],
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update_gen1));

    // Range must remain gen_2 and group 2000
    let range_after = router
        .key_range_cache()
        .find_range(b"f", &[], RangeMode::CoveringSplit)
        .expect("range for f must still exist");
    assert_eq!(
        range_after.group_uid, 2000,
        "stale gen_1 update must not overwrite active gen_2 range"
    );
    assert_eq!(
        range_after.generation.as_ref(),
        b"gen_2",
        "cached generation must remain gen_2"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn key_range_boundary_lookup_covering_split() -> anyhow::Result<()> {
    let mock = create_base_mock();
    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    let router = database_client
        .location_router()
        .expect("location router must be present");

    // Ingest range [singer_100, singer_200)
    let update = ModelCacheUpdate {
        database_id: 50,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"singer_100"),
            limit_key: Bytes::from_static(b"singer_200"),
            group_uid: 7777,
            split_id: 7777,
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid: 7777,
            leader_index: 0,
            tablets: vec![ModelTablet {
                tablet_uid: 7777,
                server_address: "node-7777:15000".to_string(),
                location: "us-central1".to_string(),
                role: Role::ReadWrite,
                incarnation: Bytes::from_static(b"inc_1"),
                distance: 1,
                skip: false,
                _unknown_fields: Default::default(),
            }],
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    };
    database_client.observe_cache_update(Some(update));

    let cache = router.key_range_cache();

    // 1. Exact start_key (inclusive) -> Found
    let start_match = cache.find_range(b"singer_100", &[], RangeMode::CoveringSplit);
    assert!(
        start_match.is_some(),
        "start_key is inclusive and must match"
    );
    assert_eq!(start_match.expect("start_key match").group_uid, 7777);

    // 2. Middle of range -> Found
    let middle_match = cache.find_range(b"singer_150", &[], RangeMode::CoveringSplit);
    assert!(middle_match.is_some(), "middle key must match");

    // 3. Last internal key -> Found
    let last_internal_match = cache.find_range(b"singer_199", &[], RangeMode::CoveringSplit);
    assert!(
        last_internal_match.is_some(),
        "key before limit_key must match"
    );

    // 4. Exact limit_key (exclusive) -> None
    let limit_match = cache.find_range(b"singer_200", &[], RangeMode::CoveringSplit);
    assert!(
        limit_match.is_none(),
        "limit_key is exclusive and must not match"
    );

    // 5. Out of range keys -> None
    assert!(
        cache
            .find_range(b"singer_050", &[], RangeMode::CoveringSplit)
            .is_none()
    );
    assert!(
        cache
            .find_range(b"singer_250", &[], RangeMode::CoveringSplit)
            .is_none()
    );

    // 6. Empty key -> None
    assert!(
        cache
            .find_range(b"", &[], RangeMode::CoveringSplit)
            .is_none()
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn composite_key_recipe_ingestion_and_extraction() -> anyhow::Result<()> {
    let mut mock = create_base_mock();
    mock.expect_execute_streaming_sql().returning(|_| {
        let partial_result_set = mock_v1::PartialResultSet {
            metadata: Some(mock_v1::ResultSetMetadata {
                row_type: Some(mock_v1::StructType {
                    fields: vec![mock_v1::struct_type::Field {
                        name: "AlbumTitle".to_string(),
                        r#type: Some(mock_v1::Type {
                            code: mock_v1::TypeCode::String as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            }),
            values: vec![Value {
                kind: Some(prost_types::value::Kind::StringValue(
                    "Total Rust".to_string(),
                )),
            }],
            cache_update: Some(mock_v1::CacheUpdate {
                database_id: 8888,
                range: vec![],
                group: vec![],
                key_recipes: Some(mock_v1::RecipeList {
                    schema_generation: b"schema_v2".to_vec(),
                    recipe: vec![mock_v1::KeyRecipe {
                        target: Some(mock_v1::key_recipe::Target::TableName("Albums".to_string())),
                        part: vec![
                            mock_v1::key_recipe::Part {
                                tag: 200,
                                order: 0,
                                null_order: 0,
                                r#type: None,
                                struct_identifiers: vec![],
                                value_type: None,
                            },
                            mock_v1::key_recipe::Part {
                                tag: 0,
                                order: mock_v1::key_recipe::part::Order::Ascending as i32,
                                null_order: mock_v1::key_recipe::part::NullOrder::NullsFirst as i32,
                                r#type: Some(mock_v1::Type {
                                    code: mock_v1::TypeCode::Int64 as i32,
                                    ..Default::default()
                                }),
                                struct_identifiers: vec![],
                                value_type: Some(mock_v1::key_recipe::part::ValueType::Identifier(
                                    "SingerId".to_string(),
                                )),
                            },
                            mock_v1::key_recipe::Part {
                                tag: 0,
                                order: mock_v1::key_recipe::part::Order::Descending as i32,
                                null_order: mock_v1::key_recipe::part::NullOrder::NullsLast as i32,
                                r#type: Some(mock_v1::Type {
                                    code: mock_v1::TypeCode::Int64 as i32,
                                    ..Default::default()
                                }),
                                struct_identifiers: vec![],
                                value_type: Some(mock_v1::key_recipe::part::ValueType::Identifier(
                                    "AlbumId".to_string(),
                                )),
                            },
                        ],
                    }],
                }),
            }),
            last: true,
            ..Default::default()
        };
        let (sender, receiver) = mpsc::channel(1);
        sender
            .try_send(Ok(partial_result_set))
            .expect("should send composite recipe partial result set");
        Ok(Response::from(receiver))
    });

    let (database_client, _spanner, _server) = setup_mock_database_client(mock).await?;

    let transaction = database_client.single_use().build();
    let statement = Statement::builder("SELECT AlbumTitle FROM Albums WHERE SingerId = 1").build();
    let mut result_set = transaction.execute_query(statement).await?;
    let _ = result_set.next().await;

    let recipe_cache = database_client
        .key_recipe_cache()
        .expect("key recipe cache must be present");
    let album_recipe = recipe_cache
        .get_table_recipe("Albums")
        .expect("Albums table recipe must be cached");

    assert_eq!(
        album_recipe.part.len(),
        3,
        "composite recipe should contain 3 parts (table tag, SingerId, AlbumId)"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn concurrent_cache_updates_and_lookups_are_thread_safe() -> anyhow::Result<()> {
    let mock_gateway = create_base_mock();
    let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway).await?;

    let spanner = Spanner::builder()
        .with_endpoint(gateway_address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = Arc::new(
        spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .with_location_aware_routing(true)
            .build()
            .await?,
    );

    let client_clone1 = Arc::clone(&database_client);
    let client_clone2 = Arc::clone(&database_client);

    // Task 1: Repeatedly ingests cache updates
    let updater_handle = tokio::spawn(async move {
        for index in 0..100 {
            let update = sample_model_cache_update(
                index + 1,
                1000 + index,
                &format!("node-{index}:15000"),
                &format!("node-follower-{index}:15000"),
            );
            client_clone1.observe_cache_update(Some(update));
            tokio::task::yield_now().await;
        }
    });

    // Task 2: Concurrently resolves connections
    let reader_handle = tokio::spawn(async move {
        for _ in 0..200 {
            if let Some(router) = client_clone2.location_router() {
                let context = RoutingContext {
                    routing_key: Some(b"singer_500"),
                    prefer_leader: true,
                    ..Default::default()
                };
                let _connection = router.resolve_connection(&context);
            }
            tokio::task::yield_now().await;
        }
    });

    let (updater_result, reader_result) = tokio::join!(updater_handle, reader_handle);
    updater_result.expect("updater task must complete cleanly");
    reader_result.expect("reader task must complete cleanly");

    Ok(())
}

#[tokio_test_no_panics]
async fn latency_aware_selection_prefers_lower_latency_replica() -> anyhow::Result<()> {
    let latency_registry = LatencyRegistry::new();
    latency_registry.record_latency(None, 4001, "fast-node:15000", Duration::from_millis(5));
    latency_registry.record_latency(None, 4001, "slow-node:15000", Duration::from_millis(500));

    let fast_score = latency_registry.get_selection_cost(None, 4001, 0, "fast-node:15000");
    let slow_score = latency_registry.get_selection_cost(None, 4001, 0, "slow-node:15000");

    assert!(
        fast_score < slow_score,
        "fast node must exhibit strictly lower effective latency than slow node"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn proactive_cache_subscriber_streams_update_to_router() -> anyhow::Result<()> {
    let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);
    let mut mock = create_base_mock();
    let update = sample_mock_cache_update(
        888888,
        9999,
        "tablet-9999-leader.spanner.internal:15000",
        "tablet-9999-follower.spanner.internal:15000",
    );

    mock.expect_fetch_cache_update().returning(move |_| {
        let _ = attempt_sender.try_send(());
        let (stream_sender, stream_receiver) = mpsc::channel(4);
        let _ = stream_sender.try_send(Ok(update.clone()));
        Ok(Response::from(stream_receiver))
    });

    let (database_client, spanner, _server) = setup_mock_database_client(mock).await?;

    let cache_updater = database_client
        .cache_updater()
        .expect("cache updater must be present");

    let subscriber = CacheSubscriber::start(
        "projects/test-project/instances/test-instance/databases/test-db".to_string(),
        spanner.clone(),
        Arc::clone(cache_updater),
    );

    // Deterministically wait for the initial connection and subsequent reconnection attempt,
    // which guarantees that the first stream's CacheUpdate was completely ingested into KeyRangeCache.
    attempt_receiver
        .recv()
        .await
        .expect("initial connection attempt should arrive");
    attempt_receiver
        .recv()
        .await
        .expect("reconnection attempt should arrive after first stream finishes");

    let router = database_client
        .location_router()
        .expect("location router present");
    let found_range = router
        .key_range_cache()
        .find_range(b"singer_500", &[], RangeMode::CoveringSplit)
        .expect("KeyRangeCache should receive streamed range from CacheSubscriber");
    assert_eq!(
        found_range.group_uid, 9999,
        "cached range must map to streamed group 9999"
    );

    subscriber.wait_for_shutdown().await;

    Ok(())
}

fn create_base_mock() -> MockSpanner {
    let mut mock = MockSpanner::new();
    mock.expect_create_session().returning(|_| {
        Ok(Response::new(mock_v1::Session {
            name:
                "projects/test-project/instances/test-instance/databases/test-db/sessions/session-1"
                    .to_string(),
            ..Default::default()
        }))
    });
    mock
}

async fn setup_mock_database_client(
    mock: MockSpanner,
) -> anyhow::Result<(DatabaseClient, Spanner, tokio::task::JoinHandle<()>)> {
    let (address, server) = start("127.0.0.1:0", mock).await?;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_instance_type(InstanceType::Omni)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .with_location_aware_routing(true)
        .build()
        .await?;

    Ok((database_client, spanner, server))
}

fn sample_int64_partial_result_set(
    column_name: &str,
    value: &str,
    cache_update: Option<mock_v1::CacheUpdate>,
) -> mock_v1::PartialResultSet {
    mock_v1::PartialResultSet {
        metadata: Some(mock_v1::ResultSetMetadata {
            row_type: Some(mock_v1::StructType {
                fields: vec![mock_v1::struct_type::Field {
                    name: column_name.to_string(),
                    r#type: Some(mock_v1::Type {
                        code: mock_v1::TypeCode::Int64 as i32,
                        ..Default::default()
                    }),
                }],
            }),
            ..Default::default()
        }),
        values: vec![Value {
            kind: Some(prost_types::value::Kind::StringValue(value.to_string())),
        }],
        cache_update,
        last: true,
        ..Default::default()
    }
}

fn sample_mock_cache_update(
    database_id: u64,
    group_uid: u64,
    leader_address: &str,
    follower_address: &str,
) -> mock_v1::CacheUpdate {
    mock_v1::CacheUpdate {
        database_id,
        range: vec![mock_v1::Range {
            start_key: b"singer_001".to_vec(),
            limit_key: b"singer_999".to_vec(),
            group_uid,
            split_id: group_uid,
            generation: b"gen_1".to_vec(),
        }],
        group: vec![mock_v1::Group {
            group_uid,
            leader_index: 0,
            tablets: vec![
                mock_v1::Tablet {
                    tablet_uid: group_uid,
                    server_address: leader_address.to_string(),
                    location: "us-central1".to_string(),
                    role: mock_v1::tablet::Role::ReadWrite as i32,
                    incarnation: b"inc_1".to_vec(),
                    distance: 1,
                    skip: false,
                },
                mock_v1::Tablet {
                    tablet_uid: group_uid + 1000,
                    server_address: follower_address.to_string(),
                    location: "us-central1".to_string(),
                    role: mock_v1::tablet::Role::ReadOnly as i32,
                    incarnation: b"inc_1".to_vec(),
                    distance: 1,
                    skip: false,
                },
            ],
            generation: b"gen_1".to_vec(),
        }],
        key_recipes: Some(mock_v1::RecipeList {
            schema_generation: b"schema_v1".to_vec(),
            recipe: vec![mock_v1::KeyRecipe {
                target: Some(mock_v1::key_recipe::Target::TableName(
                    "Singers".to_string(),
                )),
                part: vec![
                    mock_v1::key_recipe::Part {
                        tag: 100,
                        order: 0,
                        null_order: 0,
                        r#type: None,
                        struct_identifiers: vec![],
                        value_type: None,
                    },
                    mock_v1::key_recipe::Part {
                        tag: 0,
                        order: mock_v1::key_recipe::part::Order::Ascending as i32,
                        null_order: mock_v1::key_recipe::part::NullOrder::NullsFirst as i32,
                        r#type: Some(mock_v1::Type {
                            code: mock_v1::TypeCode::Int64 as i32,
                            ..Default::default()
                        }),
                        struct_identifiers: vec![],
                        value_type: Some(mock_v1::key_recipe::part::ValueType::Identifier(
                            "SingerId".to_string(),
                        )),
                    },
                ],
            }],
        }),
    }
}

fn sample_model_cache_update(
    database_id: u64,
    group_uid: u64,
    leader_address: &str,
    follower_address: &str,
) -> ModelCacheUpdate {
    ModelCacheUpdate {
        database_id,
        range: vec![ModelRange {
            start_key: Bytes::from_static(b"singer_001"),
            limit_key: Bytes::from_static(b"singer_999"),
            group_uid,
            split_id: group_uid,
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        group: vec![ModelGroup {
            group_uid,
            leader_index: 0,
            tablets: vec![
                ModelTablet {
                    tablet_uid: group_uid,
                    server_address: leader_address.to_string(),
                    location: "us-central1".to_string(),
                    role: Role::ReadWrite,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
                ModelTablet {
                    tablet_uid: group_uid + 1000,
                    server_address: follower_address.to_string(),
                    location: "us-central1".to_string(),
                    role: Role::ReadOnly,
                    incarnation: Bytes::from_static(b"inc_1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
            ],
            generation: Bytes::from_static(b"gen_1"),
            _unknown_fields: Default::default(),
        }],
        key_recipes: None,
        _unknown_fields: Default::default(),
    }
}
