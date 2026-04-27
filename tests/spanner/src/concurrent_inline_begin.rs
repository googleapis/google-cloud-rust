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

use crate::client::{get_database_id, get_emulator_host, provision_emulator, update_database_ddl};
use crate::test_proxy::{InterceptedSpanner, SpannerInterceptor};
use futures::stream::StreamExt;
use google_cloud_spanner::client::{ResultSet, Row, Spanner, TimestampBound};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use spanner_grpc_mock::google::spanner::v1 as spanner_v1;
use spanner_grpc_mock::google::spanner::v1::spanner_client::SpannerClient;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::sync::{Barrier, Mutex};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

/// An interceptor that injects transient (Unavailable) and permanent (Internal) failures
/// into streaming SQL responses when the query matches specific test strings
/// (e.g., starting with "SELECT 'Transient-" or equal to "SELECT 'Permanent'").
pub struct ConcurrentFaultInterceptor {
    emulator_client: SpannerClient<Channel>,
    /// Tracks failure counts to allow transient recovery.
    failure_counts: Arc<Mutex<HashMap<String, u32>>>,
}

impl ConcurrentFaultInterceptor {
    pub fn new(emulator_client: SpannerClient<Channel>) -> Self {
        Self {
            emulator_client,
            failure_counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl SpannerInterceptor for ConcurrentFaultInterceptor {
    fn emulator_client(&self) -> SpannerClient<Channel> {
        self.emulator_client.clone()
    }

    async fn execute_streaming_sql(
        &self,
        request: tonic::Request<spanner_v1::ExecuteSqlRequest>,
    ) -> std::result::Result<
        tonic::Response<crate::test_proxy::ExecuteStreamingSqlStream>,
        tonic::Status,
    > {
        let sql = request.get_ref().sql.clone();

        // Emulates a transient stream failure.
        if sql.starts_with("SELECT 'Transient-") {
            let mut counts = self.failure_counts.lock().await;
            let count = counts.entry(sql.clone()).or_insert(0);
            if *count == 0 {
                *count += 1;
                // Return a stream that fails immediately with Unavailable.
                let stream = futures::stream::once(async {
                    Err(tonic::Status::unavailable("Transient stream failure"))
                });
                return Ok(tonic::Response::new(stream.boxed()));
            }
            // Second attempt succeeds (fall through to emulator).
        }

        // Emulates a permanent stream failure.
        if sql == "SELECT 'Permanent'" {
            // Returns a stream that always fails with an Internal error.
            let stream = futures::stream::once(async {
                Err(tonic::Status::internal("Permanent stream failure"))
            });
            return Ok(tonic::Response::new(stream.boxed()));
        }

        // Forward other queries to the emulator.
        let res = self
            .emulator_client()
            .execute_streaming_sql(request)
            .await?;
        let (metadata, stream, extensions) = res.into_parts();
        Ok(tonic::Response::from_parts(
            metadata,
            stream.boxed(),
            extensions,
        ))
    }
}

/// Verifies that concurrent queries using "inline begin" (lazy transaction initialization)
/// maintain snapshot consistency and handle stream failures correctly.
///
/// This test:
/// 1. Captures a snapshot timestamp.
/// 2. Creates a table after that timestamp (so it doesn't exist at the snapshot time).
/// 3. Spawns 20 concurrent tasks with mixed workloads:
///    - Queries against a table existing at snapshot time (should succeed).
///    - Queries against a table NOT existing at snapshot time (should fail with NotFound).
///    - Queries that trigger transient stream failures (should be retried and succeed).
///    - Queries that trigger permanent failures (should fail as expected).
///
/// This ensures that even though the transaction ID is acquired lazily by whatever query
/// happens to win the race, all concurrent queries share that same transaction ID and
/// see the database state as of the original snapshot timestamp.
pub async fn test_concurrent_inline_begin_with_snapshot_consistency() -> anyhow::Result<()> {
    let emulator_host = match get_emulator_host() {
        Some(host) => host,
        None => return Ok(()),
    };
    provision_emulator(&emulator_host).await;
    let db_id = get_database_id().await;
    let db_path = format!(
        "projects/test-project/instances/test-instance/databases/{}",
        db_id
    );

    // 1. Setup Table 1 (Exists at snapshot time)
    let suffix = LowercaseAlphanumeric.random_string(6);
    let table_success = format!("TableSuccess_{}", suffix);
    let table_not_found = format!("TableNotFound_{}", suffix);

    let statement = format!("CREATE TABLE {} (Id INT64) PRIMARY KEY (Id)", table_success);
    update_database_ddl(statement).await?;

    // 2. Capture snapshot time.
    let spanner = Spanner::builder()
        .with_endpoint(format!("http://{}", emulator_host))
        .build()
        .await?;
    let db_client = spanner.database_client(&db_path).build().await?;

    let mut rs: ResultSet = db_client
        .single_use()
        .build()
        .execute_query("SELECT CURRENT_TIMESTAMP")
        .await?;
    let row: Row = rs.next().await.unwrap().unwrap();
    let snapshot_time: OffsetDateTime = row.try_get(0)?;

    // 3. Setup Table 2 (Does NOT exist at snapshot time)
    let statement = format!(
        "CREATE TABLE {} (Id INT64) PRIMARY KEY (Id)",
        table_not_found
    );
    update_database_ddl(statement).await?;

    // 4. Start the Intercepted Server
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;
    let emulator_channel = Channel::from_shared(format!("http://{}", emulator_host))?
        .connect()
        .await?;
    let interceptor = ConcurrentFaultInterceptor::new(SpannerClient::new(emulator_channel));
    let service = InterceptedSpanner(interceptor);

    tokio::spawn(async move {
        Server::builder()
            .add_service(spanner_v1::spanner_server::SpannerServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("Server failed");
    });

    // 5. Build Client pointing to Interceptor
    let intercepted_spanner = Spanner::builder()
        .with_endpoint(format!("http://{}", local_addr))
        .build()
        .await?;
    let intercepted_db = intercepted_spanner
        .database_client(&db_path)
        .build()
        .await?;

    // 6. Spawn 20 tasks with random workloads
    let tx = intercepted_db
        .read_only_transaction()
        .with_timestamp_bound(TimestampBound::read_timestamp(snapshot_time))
        .with_explicit_begin_transaction(false)
        .build()
        .await?;
    let tx = Arc::new(tx);
    let barrier = Arc::new(Barrier::new(20));
    let mut handles = Vec::new();

    for i in 0..20 {
        // Each thread is assigned a random workload.
        let role = rand::random_range(0..4);
        let tx = Arc::clone(&tx);
        let barrier = Arc::clone(&barrier);
        let table_success = table_success.clone();
        let table_not_found = table_not_found.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            match role {
                0 => {
                    // Success
                    let mut result_set: ResultSet = tx
                        .execute_query(format!("SELECT * FROM {}", table_success))
                        .await?;
                    while let Some(row) = result_set.next().await {
                        row?;
                    }
                    Ok::<_, anyhow::Error>(format!("Task {} Success: OK", i))
                }
                1 => {
                    // Table not found
                    let res: Result<ResultSet, _> = tx
                        .execute_query(format!("SELECT * FROM {}", table_not_found))
                        .await;
                    match res {
                        Err(e)
                            if e.to_string().contains("not found")
                                || e.to_string().contains("NotFound") =>
                        {
                            Ok(format!("Task {} NotFound: OK", i))
                        }
                        Ok(_) => anyhow::bail!("Task {} expected NotFound but got Success", i),
                        Err(e) => anyhow::bail!("Task {} expected NotFound but got: {:?}", i, e),
                    }
                }
                2 => {
                    // Transient stream error. This will trigger a retry of the stream.
                    let sql = format!("SELECT 'Transient-{}'", i);
                    let mut result_set: ResultSet = tx.execute_query(sql).await?;
                    while let Some(row) = result_set.next().await {
                        row?;
                    }
                    Ok(format!("Task {} Transient: OK", i))
                }
                3 => {
                    // Permanent stream error.
                    let result_set_res: Result<ResultSet, _> =
                        tx.execute_query("SELECT 'Permanent'").await;
                    let mut result_set = match result_set_res {
                        Ok(rs) => rs,
                        Err(e) => anyhow::bail!(
                            "Task {} expected successful RPC initiation but got: {:?}",
                            i,
                            e
                        ),
                    };

                    let next = result_set.next().await;
                    match next {
                        Some(Err(e))
                            if e.to_string().contains("Permanent")
                                || e.to_string().contains("Internal") =>
                        {
                            Ok(format!("Task {} Permanent: OK", i))
                        }
                        Some(Ok(_)) => {
                            anyhow::bail!("Task {} expected Permanent error but got a valid row", i)
                        }
                        _ => anyhow::bail!(
                            "Task {} expected Permanent error but succeeded or got empty results",
                            i
                        ),
                    }
                }
                _ => unreachable!(),
            }
        }));
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}
