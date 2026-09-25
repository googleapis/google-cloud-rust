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

//! Semantic integration verification for Spanner channel pools.
//!
//! Validates that both dynamic and static channel pools are fully wired into
//! [`DatabaseClient`] and all transaction types, verifying:
//! 1. In-flight RPC permits increment during active execution and decrement to zero on completion.
//! 2. Early stream drops and errors release permits immediately without leaks.
//! 3. Read/Write transaction affinity hard-pins all operations to the exact same channel entry.
//! 4. Multi-use Read-Only transaction affinity pins queries across statements.
//! 5. Write-only and Partitioned DML transactions preserve channel affinity.
//! 6. Dynamic channel pools scale up under concurrent load from `DatabaseClient`.
//! 7. Static channel pools maintain fixed channel capacity under concurrent load.
//! 8. Transport error penalties deprioritize degraded channels in P2C selection.
//! 9. Mixed workloads leave zero leaked permits or active Read/Write transaction guards.

use crate::channel_pool::entry::ChannelEntry;
use crate::channel_pool::{DynamicChannelPoolConfig, StaticChannelPoolConfig};
use crate::client::{Spanner, SpannerBuilderExt};
use crate::database_client::DatabaseClient;
use crate::key::KeySet;
use crate::model::PartitionOptions;
use crate::mutation::Mutation;
use crate::read::ReadRequest;
use crate::read_only_transaction::BeginTransactionOption;
use crate::read_only_transaction::tests::{create_session_mock, setup_select1};
use crate::read_write_transaction::ReadWriteTransactionBuilder;
use crate::result_set::tests::adapt;
use crate::statement::Statement;
use gaxi::grpc::tonic::{Response, Status};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_test_macros::tokio_test_no_panics;
use spanner_grpc_mock::google::spanner::v1 as mock_v1;
use spanner_grpc_mock::{MockSpanner, start};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::sleep;

async fn setup_client_with_static_pool(
    mock: MockSpanner,
    channel_count: usize,
) -> (DatabaseClient, Spanner, tokio::task::JoinHandle<()>) {
    let (address, server) = start("127.0.0.1:0", mock)
        .await
        .expect("mock server should start");

    let static_config = StaticChannelPoolConfig::new(channel_count);
    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(static_config)
        .build()
        .await
        .expect("spanner client should build");

    let database_client = spanner
        .database_client("projects/p/instances/i/databases/d")
        .build()
        .await
        .expect("database client should build");

    (database_client, spanner, server)
}

async fn setup_client_with_dynamic_pool(
    mock: MockSpanner,
    config: DynamicChannelPoolConfig,
) -> (DatabaseClient, Spanner, tokio::task::JoinHandle<()>) {
    let (address, server) = start("127.0.0.1:0", mock)
        .await
        .expect("mock server should start");

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(config)
        .build()
        .await
        .expect("spanner client should build");

    let database_client = spanner
        .database_client("projects/p/instances/i/databases/d")
        .build()
        .await
        .expect("database client should build");

    (database_client, spanner, server)
}

fn total_in_flight(spanner: &Spanner) -> u32 {
    let active_guard = spanner
        .channel_pool
        .inner
        .active_entries
        .read()
        .expect("lock poisoned");
    active_guard.iter().map(|entry| entry.in_flight()).sum()
}

fn total_active_rw(spanner: &Spanner) -> u32 {
    let active_guard = spanner
        .channel_pool
        .inner
        .active_entries
        .read()
        .expect("lock poisoned");
    active_guard
        .iter()
        .map(|entry| entry.active_rw_count())
        .sum()
}

fn active_channel_entries(spanner: &Spanner) -> Vec<Arc<ChannelEntry>> {
    let active_guard = spanner
        .channel_pool
        .inner
        .active_entries
        .read()
        .expect("lock poisoned");
    active_guard.clone()
}

async fn wait_for_in_flight(spanner: &Spanner, expected: u32, timeout_duration: Duration) {
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout_duration {
        if total_in_flight(spanner) == expected {
            return;
        }
        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        total_in_flight(spanner),
        expected,
        "Timed out waiting for in-flight permits to reach {expected}"
    );
}

async fn wait_for_channels_greater_than(
    spanner: &Spanner,
    threshold: usize,
    timeout_duration: Duration,
) {
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout_duration {
        if spanner.channel_pool.active_channel_count() > threshold {
            return;
        }
        sleep(Duration::from_millis(5)).await;
    }
    let count = spanner.channel_pool.active_channel_count();
    assert!(
        count > threshold,
        "Timed out waiting for channel count ({count}) to exceed {threshold}"
    );
}

fn setup_chunk(last: bool) -> mock_v1::PartialResultSet {
    let mut chunk = setup_select1();
    chunk.last = last;
    chunk
}

#[tokio_test_no_panics]
async fn single_use_query_in_flight_lifecycle() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    mock.expect_execute_streaming_sql()
        .once()
        .returning(|_| Ok(Response::from(adapt([Ok(setup_select1())]))));

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 2).await;

    assert_eq!(
        total_in_flight(&spanner),
        0,
        "Pool must have 0 in-flight before query"
    );

    let mut result_set = database_client
        .single_use()
        .build()
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;

    assert_eq!(
        total_in_flight(&spanner),
        1,
        "Active result set stream must hold exactly 1 in-flight permit"
    );

    let row = result_set.next().await;
    assert!(row.is_some(), "Stream must return first row");

    let end_of_stream = result_set.next().await;
    assert!(end_of_stream.is_none(), "Stream must reach EOF");

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn single_use_read_in_flight_lifecycle() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    mock.expect_streaming_read()
        .once()
        .returning(|_| Ok(Response::from(adapt([Ok(setup_select1())]))));

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 2).await;

    assert_eq!(
        total_in_flight(&spanner),
        0,
        "Pool must have 0 in-flight before read"
    );

    let read_request = ReadRequest::builder("Users", vec!["id".to_string()])
        .with_keys(KeySet::all())
        .build();

    let mut result_set = database_client
        .single_use()
        .build()
        .execute_read(read_request)
        .await?;

    assert_eq!(
        total_in_flight(&spanner),
        1,
        "Active read stream must hold exactly 1 in-flight permit"
    );

    let row = result_set.next().await;
    assert!(row.is_some(), "Read stream must return first row");

    let end_of_stream = result_set.next().await;
    assert!(end_of_stream.is_none(), "Read stream must reach EOF");

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn streaming_query_early_drop_releases_in_flight_immediately() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    mock.expect_execute_streaming_sql().once().returning(|_| {
        Ok(Response::from(adapt([
            Ok(setup_chunk(false)),
            Ok(setup_chunk(false)),
            Ok(setup_chunk(true)),
        ])))
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 2).await;

    let mut result_set = database_client
        .single_use()
        .build()
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;

    assert_eq!(
        total_in_flight(&spanner),
        1,
        "In-flight permit must be held by open stream"
    );

    let first_row = result_set.next().await;
    assert!(first_row.is_some(), "First row should be available");

    // Drop the ResultSet before stream finishes
    drop(result_set);

    wait_for_in_flight(&spanner, 0, Duration::from_millis(100)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn streaming_query_initial_error_releases_in_flight_and_applies_penalty() -> anyhow::Result<()>
{
    let mut mock = create_session_mock();
    mock.expect_execute_streaming_sql()
        .once()
        .returning(|_| Err(Status::unavailable("simulated network partition")));

    let config = DynamicChannelPoolConfig::new()
        .with_initial_channels(2)
        .with_min_channels(2)
        .with_max_channels(4)
        .with_min_rpc_per_channel(2.0)
        .with_max_rpc_per_channel(10.0)
        .with_error_penalty_step(3)
        .with_scale_up_cooldown(Duration::from_millis(500));
    let (database_client, spanner, _server) = setup_client_with_dynamic_pool(mock, config).await;

    let query_result = database_client
        .single_use()
        .build()
        .execute_query(Statement::builder("SELECT 1").build())
        .await;

    assert!(
        query_result.is_err(),
        "Query must propagate unavailable error"
    );

    assert_eq!(
        total_in_flight(&spanner),
        0,
        "Error on stream creation must decrement in-flight permit immediately"
    );

    let entries = active_channel_entries(&spanner);
    let penalized_channel_count = entries
        .iter()
        .filter(|entry| entry.current_penalty() > 0)
        .count();
    assert_eq!(
        penalized_channel_count, 1,
        "The failed channel entry must record a synthetic error penalty"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn multi_use_read_only_transaction_pins_same_channel_inline_begin() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    for _ in 0..2 {
        let addresses_clone = Arc::clone(&captured_addresses);
        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |request| {
                addresses_clone
                    .lock()
                    .expect("lock")
                    .push(request.remote_addr().expect("remote_addr"));
                let mut partial_result_set = setup_select1();
                let metadata = partial_result_set
                    .metadata
                    .as_mut()
                    .expect("metadata present");
                metadata.transaction = Some(mock_v1::Transaction {
                    id: vec![10, 20, 30],
                    ..Default::default()
                });
                Ok(Response::from(adapt([Ok(partial_result_set)])))
            });
    }

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = database_client
        .read_only_transaction()
        .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
        .build()
        .await?;

    let mut result_set1 = transaction
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    while (result_set1.next().await).is_some() {}

    let mut result_set2 = transaction
        .execute_query(Statement::builder("SELECT 2").build())
        .await?;
    while (result_set2.next().await).is_some() {}

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(addresses.len(), 2, "Both queries must execute");
    assert_eq!(
        addresses[0], addresses[1],
        "Both queries in multi-use read-only transaction must pin to the exact same channel"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn multi_use_read_only_transaction_pins_same_channel_explicit_begin() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_begin_transaction()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            Ok(Response::new(mock_v1::Transaction {
                id: vec![10, 20, 30],
                ..Default::default()
            }))
        });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_streaming_sql()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            Ok(Response::from(adapt([Ok(setup_select1())])))
        });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = database_client
        .read_only_transaction()
        .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
        .build()
        .await?;

    let mut result_set = transaction
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    while (result_set.next().await).is_some() {}

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(
        addresses.len(),
        2,
        "BeginTransaction and query must execute"
    );
    assert_eq!(
        addresses[0], addresses[1],
        "BeginTransaction and query in multi-use transaction must pin to the exact same channel"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn multi_use_read_only_transaction_parallel_initial_queries_inline_begin()
-> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    // One query will be leader (has begin), the second will wait for transaction ID
    mock.expect_execute_streaming_sql().times(2).returning({
        let captured_addresses = Arc::clone(&captured_addresses);
        move |request| {
            let address = request.remote_addr().expect("remote_addr");
            captured_addresses.lock().expect("lock").push(address);

            let message = request.into_inner();
            let has_begin = message
                .transaction
                .as_ref()
                .and_then(|selector| selector.selector.as_ref())
                .is_some_and(|mode| {
                    matches!(mode, mock_v1::transaction_selector::Selector::Begin(_))
                });

            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move {
                if has_begin {
                    // Leader simulates small network delay then returns transaction ID
                    sleep(Duration::from_millis(50)).await;
                    let mut partial_result_set = setup_select1();
                    let metadata = partial_result_set
                        .metadata
                        .as_mut()
                        .expect("metadata present");
                    metadata.transaction = Some(mock_v1::Transaction {
                        id: vec![77, 88, 99],
                        ..Default::default()
                    });
                    let _ = sender.send(Ok(partial_result_set)).await;
                } else {
                    // Follower query runs after receiving transaction ID
                    let _ = sender.send(Ok(setup_select1())).await;
                }
            });

            Ok(Response::from(receiver))
        }
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = database_client
        .read_only_transaction()
        .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
        .build()
        .await?;

    let query1_future = async {
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        while (result_set.next().await).is_some() {}
        Ok::<(), anyhow::Error>(())
    };

    let query2_future = async {
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 2").build())
            .await?;
        while (result_set.next().await).is_some() {}
        Ok::<(), anyhow::Error>(())
    };

    let (result1, result2) = tokio::join!(query1_future, query2_future);
    result1?;
    result2?;

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(addresses.len(), 2, "Both parallel queries must execute");
    assert_eq!(
        addresses[0], addresses[1],
        "Parallel initial queries in inline-begin read-only transaction must pin to identical channel"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn multi_use_read_only_transaction_parallel_initial_queries_explicit_begin()
-> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    // 1. BeginTransaction called during build()
    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_begin_transaction()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            Ok(Response::new(mock_v1::Transaction {
                id: vec![77, 88, 99],
                ..Default::default()
            }))
        });

    // 2. Both parallel queries execute concurrently on the wire
    mock.expect_execute_streaming_sql().times(2).returning({
        let captured_addresses = Arc::clone(&captured_addresses);
        move |request| {
            let address = request.remote_addr().expect("remote_addr");
            captured_addresses.lock().expect("lock").push(address);

            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move {
                // Hold stream open to allow concurrent in-flight overlap
                sleep(Duration::from_millis(60)).await;
                let _ = sender.send(Ok(setup_select1())).await;
            });

            Ok(Response::from(receiver))
        }
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = database_client
        .read_only_transaction()
        .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
        .build()
        .await?;

    let query1_future = async {
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        while (result_set.next().await).is_some() {}
        Ok::<(), anyhow::Error>(())
    };

    let query2_future = async {
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 2").build())
            .await?;
        while (result_set.next().await).is_some() {}
        Ok::<(), anyhow::Error>(())
    };

    let (result1, result2) = tokio::join!(query1_future, query2_future);
    result1?;
    result2?;

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(
        addresses.len(),
        3,
        "BeginTransaction and both queries must execute"
    );
    assert_eq!(
        addresses[0], addresses[1],
        "Query 1 must route to the same channel as BeginTransaction"
    );
    assert_eq!(
        addresses[1], addresses[2],
        "Query 2 must route to the same channel as Query 1"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn read_write_transaction_full_lifecycle_hard_affinity() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    // 1. Query: execute_streaming_sql (starts transaction via inline begin)
    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_streaming_sql()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            let mut partial_result_set = setup_select1();
            let metadata = partial_result_set
                .metadata
                .as_mut()
                .expect("metadata present");
            metadata.transaction = Some(mock_v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            });
            Ok(Response::from(adapt([Ok(partial_result_set)])))
        });

    // 2. Update: execute_sql
    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_sql().once().returning(move |request| {
        addresses_clone
            .lock()
            .expect("lock")
            .push(request.remote_addr().expect("remote_addr"));
        Ok(Response::new(mock_v1::ResultSet {
            metadata: Some(mock_v1::ResultSetMetadata {
                row_type: Some(mock_v1::StructType { fields: vec![] }),
                ..Default::default()
            }),
            stats: Some(mock_v1::ResultSetStats {
                row_count: Some(mock_v1::result_set_stats::RowCount::RowCountExact(1)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    });

    // 3. Batch DML: execute_batch_dml
    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_batch_dml()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            Ok(Response::new(mock_v1::ExecuteBatchDmlResponse {
                result_sets: vec![mock_v1::ResultSet {
                    stats: Some(mock_v1::ResultSetStats {
                        row_count: Some(mock_v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }))
        });

    // 4. Commit: commit
    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_commit().once().returning(move |request| {
        addresses_clone
            .lock()
            .expect("lock")
            .push(request.remote_addr().expect("remote_addr"));
        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(prost_types::Timestamp {
                seconds: 5000,
                nanos: 0,
            }),
            ..Default::default()
        }))
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "Active RW count must start at 0"
    );

    let transaction = ReadWriteTransactionBuilder::new(database_client)
        .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
        .build(None)
        .await?;

    let mut result_set = transaction
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    while (result_set.next().await).is_some() {}

    assert_eq!(
        total_active_rw(&spanner),
        1,
        "Active RW guard must be attached to pinned channel during transaction"
    );

    let updated_rows = transaction
        .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
        .await?;
    assert_eq!(updated_rows, 1, "Must update 1 row");

    let batch_result = transaction
        .execute_batch_update(vec![Statement::from(
            "UPDATE Users SET Name = 'Bob' WHERE Id = 2",
        )])
        .await?;
    assert_eq!(batch_result.len(), 1);

    transaction.commit().await?;

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "Active RW guard must be released immediately upon transaction commit"
    );

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(
        addresses.len(),
        4,
        "Query, Update, Batch DML, and Commit must all be captured"
    );
    assert_eq!(
        addresses[0], addresses[1],
        "Update must route to identical channel as initial query"
    );
    assert_eq!(
        addresses[1], addresses[2],
        "Batch DML must route to identical channel as update"
    );
    assert_eq!(
        addresses[2], addresses[3],
        "Commit must route to identical channel as prior operations"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn read_write_transaction_parallel_initial_queries_inline_begin() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    // One statement initiates the inline begin, second waits for transaction ID
    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_streaming_sql()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));

            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move {
                sleep(Duration::from_millis(50)).await;
                let mut partial_result_set = setup_select1();
                let metadata = partial_result_set
                    .metadata
                    .as_mut()
                    .expect("metadata present");
                metadata.transaction = Some(mock_v1::Transaction {
                    id: vec![55, 66, 77],
                    ..Default::default()
                });
                let _ = sender.send(Ok(partial_result_set)).await;
            });
            Ok(Response::from(receiver))
        });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_sql().once().returning(move |request| {
        addresses_clone
            .lock()
            .expect("lock")
            .push(request.remote_addr().expect("remote_addr"));
        Ok(Response::new(mock_v1::ResultSet {
            metadata: Some(mock_v1::ResultSetMetadata {
                row_type: Some(mock_v1::StructType { fields: vec![] }),
                ..Default::default()
            }),
            stats: Some(mock_v1::ResultSetStats {
                row_count: Some(mock_v1::result_set_stats::RowCount::RowCountExact(1)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_commit().once().returning(move |request| {
        addresses_clone
            .lock()
            .expect("lock")
            .push(request.remote_addr().expect("remote_addr"));
        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(prost_types::Timestamp {
                seconds: 5000,
                nanos: 0,
            }),
            ..Default::default()
        }))
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = ReadWriteTransactionBuilder::new(database_client)
        .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
        .build(None)
        .await?;

    let op1 = async {
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        while (result_set.next().await).is_some() {}
        Ok::<(), anyhow::Error>(())
    };

    let op2 = async {
        let _ = transaction
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        Ok::<(), anyhow::Error>(())
    };

    let (res1, res2) = tokio::join!(op1, op2);
    res1?;
    res2?;

    assert_eq!(
        total_active_rw(&spanner),
        1,
        "Active RW guard must be held during transaction"
    );

    transaction.commit().await?;

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "Active RW guard released after commit"
    );

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(addresses.len(), 3, "Query, Update, and Commit executed");
    assert_eq!(
        addresses[0], addresses[1],
        "Query and Update must route to identical channel"
    );
    assert_eq!(
        addresses[1], addresses[2],
        "Commit must route to identical channel"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn read_write_transaction_parallel_initial_queries_explicit_begin() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_begin_transaction()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            Ok(Response::new(mock_v1::Transaction {
                id: vec![55, 66, 77],
                ..Default::default()
            }))
        });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_streaming_sql()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));

            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move {
                sleep(Duration::from_millis(50)).await;
                let _ = sender.send(Ok(setup_select1())).await;
            });
            Ok(Response::from(receiver))
        });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_sql().once().returning(move |request| {
        addresses_clone
            .lock()
            .expect("lock")
            .push(request.remote_addr().expect("remote_addr"));
        Ok(Response::new(mock_v1::ResultSet {
            metadata: Some(mock_v1::ResultSetMetadata {
                row_type: Some(mock_v1::StructType { fields: vec![] }),
                ..Default::default()
            }),
            stats: Some(mock_v1::ResultSetStats {
                row_count: Some(mock_v1::result_set_stats::RowCount::RowCountExact(1)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_commit().once().returning(move |request| {
        addresses_clone
            .lock()
            .expect("lock")
            .push(request.remote_addr().expect("remote_addr"));
        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(prost_types::Timestamp {
                seconds: 5000,
                nanos: 0,
            }),
            ..Default::default()
        }))
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = ReadWriteTransactionBuilder::new(database_client)
        .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
        .build(None)
        .await?;

    assert_eq!(
        total_active_rw(&spanner),
        1,
        "Active RW guard attached upon explicit begin"
    );

    let op1 = async {
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        while (result_set.next().await).is_some() {}
        Ok::<(), anyhow::Error>(())
    };

    let op2 = async {
        let _ = transaction
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        Ok::<(), anyhow::Error>(())
    };

    let (res1, res2) = tokio::join!(op1, op2);
    res1?;
    res2?;

    transaction.commit().await?;

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "Active RW guard released after commit"
    );

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(
        addresses.len(),
        4,
        "BeginTransaction, Query, Update, and Commit executed"
    );
    assert_eq!(
        addresses[0], addresses[1],
        "Query must route to identical channel as BeginTransaction"
    );
    assert_eq!(
        addresses[1], addresses[2],
        "Update must route to identical channel as Query"
    );
    assert_eq!(
        addresses[2], addresses[3],
        "Commit must route to identical channel as Update"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn read_write_transaction_rollback_releases_rw_guard() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_streaming_sql()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            let mut partial_result_set = setup_select1();
            let metadata = partial_result_set
                .metadata
                .as_mut()
                .expect("metadata present");
            metadata.transaction = Some(mock_v1::Transaction {
                id: vec![7, 8, 9],
                ..Default::default()
            });
            Ok(Response::from(adapt([Ok(partial_result_set)])))
        });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_rollback().once().returning(move |request| {
        addresses_clone
            .lock()
            .expect("lock")
            .push(request.remote_addr().expect("remote_addr"));
        Ok(Response::new(()))
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = ReadWriteTransactionBuilder::new(database_client)
        .build(None)
        .await?;

    let mut result_set = transaction
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    while (result_set.next().await).is_some() {}

    assert_eq!(
        total_active_rw(&spanner),
        1,
        "Active RW guard held during active transaction"
    );

    transaction.rollback().await?;

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "Active RW guard released upon rollback"
    );

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(addresses.len(), 2, "Query and Rollback executed");
    assert_eq!(
        addresses[0], addresses[1],
        "Rollback must route to the identical pinned channel"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn read_write_transaction_drop_releases_rw_guard() -> anyhow::Result<()> {
    let mut mock = create_session_mock();

    mock.expect_execute_streaming_sql().once().returning(|_| {
        let mut partial_result_set = setup_select1();
        let metadata = partial_result_set
            .metadata
            .as_mut()
            .expect("metadata present");
        metadata.transaction = Some(mock_v1::Transaction {
            id: vec![99],
            ..Default::default()
        });
        Ok(Response::from(adapt([Ok(partial_result_set)])))
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = ReadWriteTransactionBuilder::new(database_client)
        .build(None)
        .await?;

    let mut result_set = transaction
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    while (result_set.next().await).is_some() {}

    assert_eq!(
        total_active_rw(&spanner),
        1,
        "Active RW guard held by live transaction"
    );

    drop(result_set);
    drop(transaction);

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "Dropping active transaction and its result sets must decrement active RW guard immediately"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn write_only_transaction_pins_begin_and_commit_and_releases_guards() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_begin_transaction()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            Ok(Response::new(mock_v1::Transaction {
                id: vec![55, 66],
                ..Default::default()
            }))
        });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_commit().once().returning(move |request| {
        addresses_clone
            .lock()
            .expect("lock")
            .push(request.remote_addr().expect("remote_addr"));
        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(prost_types::Timestamp {
                seconds: 100,
                nanos: 0,
            }),
            ..Default::default()
        }))
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = database_client.write_only_transaction().build();
    let mutations = vec![Mutation::delete("Users", KeySet::all())];
    let response = transaction.write(mutations).await?;

    assert!(
        response.commit_timestamp.is_some(),
        "Write transaction must return commit timestamp"
    );

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(addresses.len(), 2, "Begin and Commit executed");
    assert_eq!(
        addresses[0], addresses[1],
        "Write-only transaction Begin and Commit must pin to identical channel"
    );

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "Write-only transaction must not leak RW guards"
    );
    assert_eq!(
        total_in_flight(&spanner),
        0,
        "Write-only transaction must leave 0 in-flight permits"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn write_only_transaction_write_at_least_once_lifecycle() -> anyhow::Result<()> {
    let mut mock = create_session_mock();

    mock.expect_commit().once().returning(|_| {
        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(prost_types::Timestamp {
                seconds: 200,
                nanos: 0,
            }),
            ..Default::default()
        }))
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = database_client.write_only_transaction().build();
    let mutations = vec![Mutation::delete("Users", KeySet::all())];
    let response = transaction.write_at_least_once(mutations).await?;

    assert!(response.commit_timestamp.is_some());

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "write_at_least_once must have 0 active RW guards"
    );
    assert_eq!(
        total_in_flight(&spanner),
        0,
        "write_at_least_once must leave 0 in-flight permits"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn partitioned_dml_pins_begin_and_execute_and_releases() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_begin_transaction()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            Ok(Response::new(mock_v1::Transaction {
                id: vec![77, 88],
                ..Default::default()
            }))
        });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_execute_streaming_sql()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            let mut partial_result_set = setup_select1();
            partial_result_set.stats = Some(mock_v1::ResultSetStats {
                row_count: Some(mock_v1::result_set_stats::RowCount::RowCountLowerBound(100)),
                ..Default::default()
            });
            Ok(Response::from(adapt([Ok(partial_result_set)])))
        });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = database_client
        .partitioned_dml_transaction()
        .build()
        .await?;

    let modified = transaction
        .execute_update("UPDATE Users SET active = true WHERE true")
        .await?;
    assert_eq!(modified, 100, "Must return modified row count");

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(addresses.len(), 2, "Begin and Streaming SQL executed");
    assert_eq!(
        addresses[0], addresses[1],
        "Partitioned DML Begin and Execute must route to the same pinned channel"
    );

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "Active RW guard must be released after Partitioned DML"
    );
    assert_eq!(
        total_in_flight(&spanner),
        0,
        "In-flight permits must be 0 after Partitioned DML"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn batch_read_only_transaction_routes_through_pool() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_begin_transaction()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            Ok(Response::new(mock_v1::Transaction {
                id: vec![44, 55, 66],
                ..Default::default()
            }))
        });

    let addresses_clone = Arc::clone(&captured_addresses);
    mock.expect_partition_query()
        .once()
        .returning(move |request| {
            addresses_clone
                .lock()
                .expect("lock")
                .push(request.remote_addr().expect("remote_addr"));
            Ok(Response::new(mock_v1::PartitionResponse {
                partitions: vec![mock_v1::Partition {
                    partition_token: vec![1, 2, 3],
                }],
                transaction: Some(mock_v1::Transaction {
                    id: vec![44, 55, 66],
                    ..Default::default()
                }),
            }))
        });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    let transaction = database_client
        .batch_read_only_transaction()
        .build()
        .await?;

    let partitions = transaction
        .partition_query(
            Statement::builder("SELECT * FROM Users").build(),
            PartitionOptions::default(),
        )
        .await?;

    assert_eq!(partitions.len(), 1, "Must return 1 partition");

    let addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(
        addresses.len(),
        2,
        "BeginTransaction and partition_query must execute"
    );
    assert_eq!(
        addresses[0], addresses[1],
        "BeginTransaction and partition_query must route to the same pinned channel"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn dynamic_channel_pool_scales_up_under_concurrent_queries() -> anyhow::Result<()> {
    let mut mock = create_session_mock();

    // Scale-up worker primes newly opened channels using unary execute_sql("SELECT 1")
    mock.expect_execute_sql().returning(|_| {
        Ok(Response::new(mock_v1::ResultSet {
            metadata: Some(mock_v1::ResultSetMetadata {
                row_type: Some(mock_v1::StructType { fields: vec![] }),
                ..Default::default()
            }),
            ..Default::default()
        }))
    });

    // 8 concurrent queries that hold their stream open for 100ms to create sustained concurrency
    mock.expect_execute_streaming_sql().returning(|_| {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            let _ = sender.send(Ok(setup_select1())).await;
        });
        Ok(Response::from(receiver))
    });

    // Configure dynamic pool with initial=2, min=2, max=6, min_rpc=0.5, max_rpc=1.5, penalty_step=1, cooldown=30ms
    let config = DynamicChannelPoolConfig::new()
        .with_initial_channels(2)
        .with_min_channels(2)
        .with_max_channels(6)
        .with_min_rpc_per_channel(0.5)
        .with_max_rpc_per_channel(1.5)
        .with_error_penalty_step(1)
        .with_scale_up_cooldown(Duration::from_millis(30));
    let (database_client, spanner, _server) = setup_client_with_dynamic_pool(mock, config).await;

    assert_eq!(
        spanner.channel_pool.active_channel_count(),
        2,
        "Initial dynamic channel pool size must be 2"
    );

    // Launch 8 concurrent queries
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let client_clone = database_client.clone();
        tasks.push(tokio::spawn(async move {
            let mut result_set = client_clone
                .single_use()
                .build()
                .execute_query(Statement::builder("SELECT 1").build())
                .await
                .expect("query should succeed");
            while let Some(row) = result_set.next().await {
                let _ = row.expect("row should succeed");
            }
        }));
    }

    for task in tasks {
        task.await.expect("task join should succeed");
    }

    wait_for_channels_greater_than(&spanner, 2, Duration::from_millis(500)).await;
    wait_for_in_flight(&spanner, 0, Duration::from_millis(500)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn static_channel_pool_remains_fixed_under_concurrent_queries() -> anyhow::Result<()> {
    let mut mock = create_session_mock();

    mock.expect_execute_streaming_sql().returning(|_| {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            sleep(Duration::from_millis(60)).await;
            let _ = sender.send(Ok(setup_select1())).await;
        });
        Ok(Response::from(receiver))
    });

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 2).await;

    assert_eq!(
        spanner.channel_pool.active_channel_count(),
        2,
        "Static pool must have exactly 2 channels"
    );

    let mut tasks = Vec::new();
    for _ in 0..6 {
        let client_clone = database_client.clone();
        tasks.push(tokio::spawn(async move {
            let mut result_set = client_clone
                .single_use()
                .build()
                .execute_query(Statement::builder("SELECT 1").build())
                .await
                .expect("query succeeds");
            while let Some(row) = result_set.next().await {
                let _ = row.expect("row succeeds");
            }
        }));
    }

    for task in tasks {
        task.await.expect("task join succeeds");
    }

    assert_eq!(
        spanner.channel_pool.active_channel_count(),
        2,
        "Static channel pool must remain strictly at 2 channels under load"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn p2c_avoids_channel_with_error_penalty() -> anyhow::Result<()> {
    let mut mock = create_session_mock();
    let call_count = Arc::new(AtomicUsize::new(0));
    let captured_addresses = Arc::new(Mutex::new(Vec::new()));

    let call_count_clone = Arc::clone(&call_count);
    let captured_addresses_clone = Arc::clone(&captured_addresses);

    mock.expect_execute_streaming_sql()
        .returning(move |request| {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let address = request.remote_addr().expect("remote_addr");
            captured_addresses_clone.lock().expect("lock").push(address);

            if count == 0 {
                // First RPC fails with UNAVAILABLE to apply error penalty
                Err(Status::unavailable("simulated failure for error penalty"))
            } else {
                Ok(Response::from(adapt([Ok(setup_select1())])))
            }
        });

    let config = DynamicChannelPoolConfig::new()
        .with_initial_channels(2)
        .with_min_channels(2)
        .with_max_channels(4)
        .with_min_rpc_per_channel(2.0)
        .with_max_rpc_per_channel(10.0)
        .with_error_penalty_step(5)
        .with_scale_up_cooldown(Duration::from_secs(5));
    let (database_client, spanner, _server) = setup_client_with_dynamic_pool(mock, config).await;

    // First query fails and penalizes its channel
    let _ = database_client
        .single_use()
        .build()
        .execute_query(Statement::builder("SELECT 1").build())
        .await;

    let penalized_address = {
        let addrs = captured_addresses.lock().expect("lock");
        assert_eq!(addrs.len(), 1, "First query executed");
        addrs[0]
    };

    // Execute 6 subsequent queries
    for _ in 0..6 {
        let mut result_set = database_client
            .single_use()
            .build()
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        while (result_set.next().await).is_some() {}
    }

    let all_addresses = captured_addresses.lock().expect("lock").clone();
    assert_eq!(all_addresses.len(), 7, "All 7 queries executed");

    // The subsequent 6 queries must avoid the penalized channel whenever P2C compares it
    let subsequent_on_penalized = all_addresses[1..]
        .iter()
        .filter(|addr| **addr == penalized_address)
        .count();

    assert!(
        subsequent_on_penalized < 6,
        "P2C must deprioritize the penalized channel (penalized channel received {subsequent_on_penalized} out of 6 queries)"
    );

    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    Ok(())
}

#[tokio_test_no_panics]
async fn zero_permit_leak_mixed_workload_battery() -> anyhow::Result<()> {
    let mut mock = create_session_mock();

    mock.expect_execute_streaming_sql().returning(|_| {
        Ok(Response::from(adapt([
            Ok(setup_chunk(false)),
            Ok(setup_chunk(true)),
        ])))
    });

    mock.expect_streaming_read()
        .returning(|_| Ok(Response::from(adapt([Ok(setup_select1())]))));

    mock.expect_execute_sql().returning(|_| {
        Ok(Response::new(mock_v1::ResultSet {
            metadata: Some(mock_v1::ResultSetMetadata {
                row_type: Some(mock_v1::StructType { fields: vec![] }),
                transaction: Some(mock_v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            stats: Some(mock_v1::ResultSetStats {
                row_count: Some(mock_v1::result_set_stats::RowCount::RowCountExact(1)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    });

    mock.expect_commit().returning(|_| {
        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(prost_types::Timestamp {
                seconds: 1234,
                nanos: 0,
            }),
            ..Default::default()
        }))
    });

    mock.expect_rollback().returning(|_| Ok(Response::new(())));

    let (database_client, spanner, _server) = setup_client_with_static_pool(mock, 4).await;

    // 1. Single-use query full read
    let mut rs = database_client
        .single_use()
        .build()
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    while (rs.next().await).is_some() {}

    // 2. Single-use read full read
    let mut rs = database_client
        .single_use()
        .build()
        .execute_read(
            ReadRequest::builder("Users", vec!["id".to_string()])
                .with_keys(KeySet::all())
                .build(),
        )
        .await?;
    while (rs.next().await).is_some() {}

    // 3. Early dropped query stream
    let mut rs = database_client
        .single_use()
        .build()
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    let _ = rs.next().await;
    drop(rs);

    // 4. ReadWriteTransaction commit
    let rw_tx = ReadWriteTransactionBuilder::new(database_client.clone())
        .build(None)
        .await?;
    let _ = rw_tx.execute_update("UPDATE Users SET x = 1").await?;
    rw_tx.commit().await?;

    // 5. ReadWriteTransaction rollback
    let rw_tx = ReadWriteTransactionBuilder::new(database_client.clone())
        .build(None)
        .await?;
    let _ = rw_tx.execute_update("UPDATE Users SET x = 2").await?;
    rw_tx.rollback().await?;

    // 6. ReadWriteTransaction dropped without commit/rollback
    let rw_tx = ReadWriteTransactionBuilder::new(database_client.clone())
        .build(None)
        .await?;
    let _ = rw_tx.execute_update("UPDATE Users SET x = 3").await?;
    drop(rw_tx);

    // 7. Write-only transaction write_at_least_once
    let _ = database_client
        .write_only_transaction()
        .build()
        .write_at_least_once(vec![Mutation::delete("Users", KeySet::all())])
        .await?;

    // Final Assertion: ZERO leaked permits or guards across entire pool
    wait_for_in_flight(&spanner, 0, Duration::from_millis(200)).await;

    assert_eq!(
        total_active_rw(&spanner),
        0,
        "Total active Read/Write guards across all channels must be exactly 0"
    );

    Ok(())
}
