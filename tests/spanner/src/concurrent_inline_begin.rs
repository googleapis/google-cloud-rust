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
use crate::test_proxy::{InterceptionResult, PassThroughProxy};
use futures::future::BoxFuture;
use google_cloud_spanner::client::{
    BeginTransactionOption, ResultSet, Row, Spanner, TimestampBound,
};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use http::{Request, Response, StatusCode, Uri};
use http_body::Frame;
use http_body_util::Full;
use http_body_util::StreamBody;
use prost::Message;
use spanner_grpc_mock::google::spanner::v1 as spanner_v1;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::{Barrier, Mutex};
use tonic::Status;
use tonic::body::Body;
use tonic::transport::Channel;

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
    let emulator_channel = Channel::from_shared(format!("http://{}", emulator_host))?
        .connect()
        .await?;

    let failure_counts = Arc::new(Mutex::new(HashMap::new()));

    let interceptor = move |req: http::Request<tonic::body::Body>| {
        let counts = failure_counts.clone();
        Box::pin(async move {
            // Only intercept ExecuteStreamingSql calls.
            if req.uri().path() == "/google.spanner.v1.Spanner/ExecuteStreamingSql" {
                let (parts, body) = req.into_parts();
                // Read the body to inspect the SQL query. This consumes the body, so we must reconstruct it later.
                let bytes = match http_body_util::BodyExt::collect(body).await {
                    Ok(c) => c.to_bytes(),
                    Err(e) => {
                        let status = Status::internal(format!("Failed to read body: {}", e));
                        // Return a trailers-only response with error status. In gRPC, errors are returned as
                        // successful HTTP responses (200 OK) with the error status in the grpc-status header/trailer.
                        let mut resp = Response::new(Body::empty());
                        *resp.status_mut() = StatusCode::OK;
                        status.add_header(resp.headers_mut()).unwrap();
                        return InterceptionResult::Complete(resp);
                    }
                };

                // gRPC over HTTP/2 uses a 5-byte framing header (1 byte compression, 4 bytes length).
                // Skip it to get the actual Protobuf encoded message.
                let grpc_data = if bytes.len() >= 5 {
                    bytes.slice(5..)
                } else {
                    bytes.clone()
                };

                // Decode the Protobuf message to inspect the SQL query.
                match spanner_v1::ExecuteSqlRequest::decode(grpc_data) {
                    Ok(request) => {
                        let sql = request.sql;

                        if sql.starts_with("SELECT 'Transient-") {
                            let mut counts = counts.lock().await;
                            let count = counts.entry(sql.clone()).or_insert(0);
                            if *count == 0 {
                                *count += 1;
                                // Return a stream that yields only trailers with Unavailable status.
                                // This simulates a stream failure that occurs after successful initiation,
                                // which triggers the client's retry logic.
                                let stream = futures::stream::once(async {
                                    let mut headers = http::HeaderMap::new();
                                    Status::unavailable("Transient stream failure")
                                        .add_header(&mut headers)
                                        .unwrap();
                                    Ok::<_, Status>(Frame::<prost::bytes::Bytes>::trailers(headers))
                                });
                                let new_body = Body::new(StreamBody::new(stream));
                                let mut resp = Response::new(new_body);
                                *resp.status_mut() = StatusCode::OK;
                                return InterceptionResult::Complete(resp);
                            }
                        }

                        if sql == "SELECT 'Permanent'" {
                            // Return a stream that yields only trailers with Internal status.
                            let stream = futures::stream::once(async {
                                let mut headers = http::HeaderMap::new();
                                Status::internal("Permanent stream failure")
                                    .add_header(&mut headers)
                                    .unwrap();
                                Ok::<_, Status>(Frame::<prost::bytes::Bytes>::trailers(headers))
                            });
                            let new_body = Body::new(StreamBody::new(stream));
                            let mut resp = Response::new(new_body);
                            *resp.status_mut() = StatusCode::OK;
                            return InterceptionResult::Complete(resp);
                        }
                    }
                    Err(e) => {
                        let status =
                            Status::internal(format!("Failed to decode ExecuteSqlRequest: {}", e));
                        let mut resp = Response::new(Body::empty());
                        *resp.status_mut() = StatusCode::OK;
                        status.add_header(resp.headers_mut()).unwrap();
                        return InterceptionResult::Complete(resp);
                    }
                }

                // Reconstruct the body for requests that are not intercepted or need to be forwarded.
                let new_body = Body::new(Full::new(bytes));
                let req = Request::from_parts(parts, new_body);
                return InterceptionResult::Continue(req);
            }

            InterceptionResult::Continue(req)
        }) as BoxFuture<'static, InterceptionResult>
    };

    let endpoint_str = format!("http://{}", emulator_host);
    let uri = endpoint_str.parse::<Uri>()?;
    let scheme = uri
        .scheme()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing scheme"))?;
    let authority = uri
        .authority()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing authority"))?;
    let proxy = PassThroughProxy::new(emulator_channel, scheme, authority, interceptor);
    let proxy_server = proxy.start("127.0.0.1:0").await?;

    // 5. Build Client pointing to Interceptor
    let intercepted_spanner = Spanner::builder()
        .with_endpoint(proxy_server.uri())
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
        .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
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
                    match result_set_res {
                        Err(e)
                            if e.to_string().contains("Permanent")
                                || e.to_string().contains("Internal") =>
                        {
                            Ok(format!("Task {} Permanent: OK", i))
                        }
                        Ok(mut rs) => match rs.next().await {
                            Some(Err(e))
                                if e.to_string().contains("Permanent")
                                    || e.to_string().contains("Internal") =>
                            {
                                Ok(format!("Task {} Permanent: OK", i))
                            }
                            _ => anyhow::bail!("Task {} expected Permanent error but succeeded", i),
                        },
                        Err(e) => {
                            anyhow::bail!("Task {} expected Permanent error but got: {:?}", i, e)
                        }
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
