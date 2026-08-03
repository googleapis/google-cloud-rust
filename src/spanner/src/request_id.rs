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

//! Types and utilities for generating Spanner Request IDs (`x-goog-spanner-request-id`).
//!
//! Spanner Request IDs are structured strings sent in gRPC headers to uniquely identify
//! client instances, channels, requests, and retry attempts:
//! `<VERSION>.<RAND_PROCESS_ID>.<nthClientId>.<nthChannelId>.<nthRequest>.<attempt>`

use std::fmt::Write as _;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// The Spanner Request ID protocol version.
const VERSION: &str = "1";

/// A 64-bit random value formatted as 16 lowercase hexadecimal characters (`"%016x"`),
/// generated once per process lifetime.
pub(crate) static RAND_PROCESS_ID: LazyLock<String> =
    LazyLock::new(|| format!("{:016x}", rand::random::<u64>()));

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// A generator for Spanner Request IDs (`x-goog-spanner-request-id`).
///
/// Each `RequestIdCreator` receives a unique `client_id` upon creation and pre-computes
/// the static prefix `<VERSION>.<RAND_PROCESS_ID>.<client_id>.` to minimize string formatting
/// overhead on RPC invocations.
#[derive(Debug)]
pub(crate) struct RequestIdCreator {
    client_prefix: String,
    next_request_id: AtomicU64,
}

impl RequestIdCreator {
    /// Constructs a new `RequestIdCreator` with an atomically incremented client ID.
    ///
    /// We use `Ordering::Relaxed` because client and request IDs only require atomic uniqueness
    /// and monotonic increments across threads without synchronizing other memory accesses.
    pub(crate) fn new() -> Self {
        let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        let client_prefix = format!("{VERSION}.{}.{}.", *RAND_PROCESS_ID, client_id);
        Self {
            client_prefix,
            next_request_id: AtomicU64::new(1),
        }
    }
}

impl Default for RequestIdCreator {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestIdCreator {
    /// Returns a Request ID prefix string for a new RPC, excluding the attempt suffix:
    /// `"1.<RAND_PROCESS_ID>.<client_id>.<channel_id>.<request_id>."`
    ///
    /// The returned string ends with a dot (`'.'`), ready for the attempt interceptor
    /// (`SpannerRequestIdInterceptor`) to append the 1-based attempt number on each retry.
    ///
    /// # Arguments
    /// * `channel_id` - The 1-based channel identifier (`1, 2, ...`), or `0` for an unknown channel.
    pub(crate) fn next_id_prefix(&self, channel_id: usize) -> String {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let mut result = String::with_capacity(self.client_prefix.len() + 48);
        result.push_str(&self.client_prefix);
        let _ = write!(result, "{channel_id}.{request_id}.");
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;
    use gaxi::grpc::tonic::{Response, Status};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_test_macros::tokio_test_no_panics;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use spanner_grpc_mock::{MockSpanner, start};
    use std::sync::Mutex;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(
            RequestIdCreator: Send,
            Sync,
            std::fmt::Debug,
            Default
        );
    }

    #[test]
    fn rand_process_id_format() {
        let process_id = &*RAND_PROCESS_ID;
        assert_eq!(process_id.len(), 16);
        assert!(
            process_id
                .chars()
                .all(|char| char.is_ascii_digit() || ('a'..='f').contains(&char)),
            "RAND_PROCESS_ID must be 16 lowercase hex characters, got {process_id}"
        );
    }

    #[test]
    fn request_id_creator_sequence() {
        let creator = RequestIdCreator::new();

        let base_id1 = creator.next_id_prefix(1);
        assert!(
            base_id1.starts_with(&format!("{VERSION}.{}.", *RAND_PROCESS_ID)),
            "prefix should start with VERSION and RAND_PROCESS_ID, got {base_id1}"
        );
        assert!(
            base_id1.ends_with(".1.1."),
            "first call on channel 1 should end with .1.1., got {base_id1}"
        );

        let base_id2 = creator.next_id_prefix(1);
        assert!(
            base_id2.ends_with(".1.2."),
            "second call on channel 1 should end with .1.2., got {base_id2}"
        );

        let base_id3 = creator.next_id_prefix(2);
        assert!(
            base_id3.ends_with(".2.3."),
            "third call on channel 2 should end with .2.3., got {base_id3}"
        );
    }

    #[test]
    fn multiple_creators_unique_client_ids() {
        let creator1 = RequestIdCreator::new();
        let creator2 = RequestIdCreator::new();
        assert_ne!(creator1.next_id_prefix(1), creator2.next_id_prefix(1));
    }

    #[tokio_test_no_panics]
    async fn request_id_header_sent_unary_rpc() {
        let captured = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // 1. Initial attempt of first RPC -> records header and returns UNAVAILABLE to force retry
        let captured_clone = captured.clone();
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let request_id = req
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id should be sent for unary RPC")
                    .to_str()
                    .expect("should be valid ASCII")
                    .to_string();
                captured_clone.lock().unwrap().push(request_id);
                Err(Status::unavailable("server is unavailable"))
            });

        // 2. Retry attempt of first RPC -> records header and returns Ok(Session)
        let captured_clone = captured.clone();
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let request_id = req
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id should be sent for unary RPC")
                    .to_str()
                    .expect("should be valid ASCII")
                    .to_string();
                captured_clone.lock().unwrap().push(request_id);
                Ok(Response::new(mock_v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    ..Default::default()
                }))
            });

        // 3. Second RPC -> records header and returns Ok(Session)
        let captured_clone = captured.clone();
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let request_id = req
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id should be sent for unary RPC")
                    .to_str()
                    .expect("should be valid ASCII")
                    .to_string();
                captured_clone.lock().unwrap().push(request_id);
                Ok(Response::new(mock_v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s2".to_string(),
                    ..Default::default()
                }))
            });

        let (address, _server) = start("127.0.0.1:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build Spanner client");

        let request = crate::model::CreateSessionRequest {
            database: "projects/p/instances/i/databases/d".to_string(),
            ..Default::default()
        };

        // Execute first RPC (attempt 1 -> UNAVAILABLE, attempt 2 -> success)
        let _session1 = client
            .create_session(
                request.clone(),
                crate::RequestOptions::default(),
                0,
                &crate::observability::Observability::disabled(),
            )
            .await
            .expect("first create_session should succeed after retry");

        // Execute second RPC (attempt 1 -> success)
        let _session2 = client
            .create_session(
                request,
                crate::RequestOptions::default(),
                0,
                &crate::observability::Observability::disabled(),
            )
            .await
            .expect("second create_session should succeed");

        let ids = captured.lock().unwrap();
        assert_eq!(ids.len(), 3, "should have captured 3 RPC attempt headers");

        let id_rpc1_attempt1 = &ids[0];
        let id_rpc1_attempt2 = &ids[1];
        let id_rpc2_attempt1 = &ids[2];

        assert!(
            id_rpc1_attempt1.starts_with("1."),
            "Request ID should start with version 1, got {id_rpc1_attempt1}"
        );

        let prefix1_attempt1 = id_rpc1_attempt1.rsplit_once('.').unwrap().0;
        let prefix1_attempt2 = id_rpc1_attempt2.rsplit_once('.').unwrap().0;
        assert_eq!(
            prefix1_attempt2, prefix1_attempt1,
            "Retry attempt should have exactly the same values as initial attempt"
        );
        assert!(
            id_rpc1_attempt1.ends_with(".1"),
            "Initial attempt should end with .1, got {id_rpc1_attempt1}"
        );
        assert!(
            id_rpc1_attempt2.ends_with(".2"),
            "Retry attempt should end with .2, got {id_rpc1_attempt2}"
        );

        let req_num_1: u64 = id_rpc1_attempt1
            .split('.')
            .nth(4)
            .expect("request_id field")
            .parse()
            .expect("numeric request_id");
        let req_num_2: u64 = id_rpc2_attempt1
            .split('.')
            .nth(4)
            .expect("request_id field")
            .parse()
            .expect("numeric request_id");
        assert_eq!(
            req_num_2,
            req_num_1 + 1,
            "Following request should use a Request ID that is 1 higher ({id_rpc2_attempt1} vs {id_rpc1_attempt1})"
        );
        assert!(
            id_rpc2_attempt1.ends_with(".1"),
            "Following request initial attempt should end with .1, got {id_rpc2_attempt1}"
        );
    }

    #[tokio_test_no_panics]
    async fn request_id_header_sent_streaming_rpc() {
        use crate::result_set::tests::adapt;

        let captured = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // 1. Initial attempt of first streaming SQL -> records header and returns stream with 1 row + UNAVAILABLE
        let captured_clone = captured.clone();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let request_id = req
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id should be sent for streaming RPC")
                    .to_str()
                    .expect("should be valid ASCII")
                    .to_string();
                captured_clone.lock().unwrap().push(request_id);

                let prs1 = mock_v1::PartialResultSet {
                    metadata: Some(mock_v1::ResultSetMetadata {
                        row_type: Some(mock_v1::StructType {
                            fields: vec![mock_v1::struct_type::Field {
                                name: "col0".to_string(),
                                ..Default::default()
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("row1".to_string())),
                    }],
                    resume_token: b"token1".to_vec(),
                    ..Default::default()
                };
                let stream = adapt([Ok(prs1), Err(Status::unavailable("server is unavailable"))]);
                Ok(Response::from(stream))
            });

        // 2. Retry attempt of first streaming SQL -> records header and returns stream with row2 (last)
        let captured_clone = captured.clone();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let request_id = req
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id should be sent for streaming RPC")
                    .to_str()
                    .expect("should be valid ASCII")
                    .to_string();
                captured_clone.lock().unwrap().push(request_id);

                let prs2 = mock_v1::PartialResultSet {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("row2".to_string())),
                    }],
                    resume_token: b"token2".to_vec(),
                    last: true,
                    ..Default::default()
                };
                let stream = adapt([Ok(prs2)]);
                Ok(Response::from(stream))
            });

        // 3. Second streaming query -> records header and returns stream with row3 (last)
        let captured_clone = captured.clone();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let request_id = req
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id should be sent for streaming RPC")
                    .to_str()
                    .expect("should be valid ASCII")
                    .to_string();
                captured_clone.lock().unwrap().push(request_id);

                let prs3 = mock_v1::PartialResultSet {
                    metadata: Some(mock_v1::ResultSetMetadata {
                        row_type: Some(mock_v1::StructType {
                            fields: vec![mock_v1::struct_type::Field {
                                name: "col0".to_string(),
                                ..Default::default()
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("row3".to_string())),
                    }],
                    resume_token: b"token3".to_vec(),
                    last: true,
                    ..Default::default()
                };
                let stream = adapt([Ok(prs3)]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build Spanner client");

        let db_client = client
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .unwrap();

        // Execute first streaming query via single_use().execute_query (attempt 1 -> row1 + UNAVAILABLE, attempt 2 -> row2)
        let mut rs1 = db_client
            .single_use()
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("first execute_query should succeed");
        while let Some(row) = rs1.next().await {
            row.expect("row should succeed after stream retry");
        }

        // Execute second streaming query via single_use().execute_query (attempt 1 -> row3)
        let mut rs2 = db_client
            .single_use()
            .build()
            .execute_query("SELECT 2")
            .await
            .expect("second execute_query should succeed");
        while let Some(row) = rs2.next().await {
            row.expect("row should succeed");
        }

        let ids = captured.lock().unwrap();
        assert_eq!(ids.len(), 3, "should have captured 3 RPC attempt headers");

        let id_rpc1_attempt1 = &ids[0];
        let id_rpc1_attempt2 = &ids[1];
        let id_rpc2_attempt1 = &ids[2];

        assert!(
            id_rpc1_attempt1.starts_with("1."),
            "Request ID should start with version 1, got {id_rpc1_attempt1}"
        );

        let prefix1_attempt1 = id_rpc1_attempt1.rsplit_once('.').unwrap().0;
        let prefix1_attempt2 = id_rpc1_attempt2.rsplit_once('.').unwrap().0;
        assert_eq!(
            prefix1_attempt2, prefix1_attempt1,
            "Retry attempt should have exactly the same values as initial attempt"
        );
        assert!(
            id_rpc1_attempt1.ends_with(".1"),
            "Initial attempt should end with .1, got {id_rpc1_attempt1}"
        );
        assert!(
            id_rpc1_attempt2.ends_with(".2"),
            "Retry attempt should end with .2, got {id_rpc1_attempt2}"
        );

        let req_num_1: u64 = id_rpc1_attempt1
            .split('.')
            .nth(4)
            .expect("request_id field")
            .parse()
            .expect("numeric request_id");
        let req_num_2: u64 = id_rpc2_attempt1
            .split('.')
            .nth(4)
            .expect("request_id field")
            .parse()
            .expect("numeric request_id");
        assert_eq!(
            req_num_2,
            req_num_1 + 1,
            "Following request should use a Request ID that is 1 higher ({id_rpc2_attempt1} vs {id_rpc1_attempt1})"
        );
        assert!(
            id_rpc2_attempt1.ends_with(".1"),
            "Following request initial attempt should end with .1, got {id_rpc2_attempt1}"
        );
    }

    #[tokio_test_no_panics]
    async fn request_id_header_sent_read_write_transaction_aborted_retry() {
        use crate::transaction_retry_policy::tests::create_aborted_status;

        let captured = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(mock_v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: true,
                    ..Default::default()
                }))
            });

        // 1. First transaction attempt -> execute_sql fails with ABORTED
        let captured_clone = captured.clone();
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let request_id = req
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id should be sent for execute_sql")
                    .to_str()
                    .expect("should be valid ASCII")
                    .to_string();
                captured_clone.lock().unwrap().push(request_id);
                Err(create_aborted_status(std::time::Duration::from_nanos(1)))
            });

        // 2. Second transaction attempt (after ABORTED retry) -> execute_sql succeeds
        let captured_clone = captured.clone();
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let request_id = req
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id should be sent for execute_sql")
                    .to_str()
                    .expect("should be valid ASCII")
                    .to_string();
                captured_clone.lock().unwrap().push(request_id);
                Ok(Response::new(mock_v1::ResultSet {
                    metadata: Some(mock_v1::ResultSetMetadata {
                        transaction: Some(mock_v1::Transaction {
                            id: vec![8, 8, 8],
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

        // 3. Second transaction attempt -> commit succeeds
        let captured_clone = captured.clone();
        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let request_id = req
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id should be sent for commit")
                    .to_str()
                    .expect("should be valid ASCII")
                    .to_string();
                captured_clone.lock().unwrap().push(request_id);
                Ok(Response::new(mock_v1::CommitResponse::default()))
            });

        let (address, _server) = start("127.0.0.1:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build Spanner client");

        let db_client = client
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .unwrap();

        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_clone = count.clone();

        let runner = db_client
            .read_write_transaction()
            .build()
            .await
            .expect("runner build");

        runner
            .run(async |tx| {
                count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tx.execute_update("UPDATE foo SET bar = 1").await?;
                Ok(())
            })
            .await
            .expect("transaction should succeed after aborted retry");

        assert_eq!(
            count.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "transaction closure should have run twice"
        );

        let ids = captured.lock().unwrap();
        assert_eq!(
            ids.len(),
            3,
            "should have captured 3 RPC attempt headers (execute_sql attempt 1, execute_sql attempt 2, commit)"
        );

        let id_exec1 = &ids[0];
        let id_exec2 = &ids[1];
        let id_commit = &ids[2];

        // 1. Verify that all 3 RPCs have attempt number .1 (retrying an aborted transaction does NOT bump the attempt number)
        assert!(
            id_exec1.ends_with(".1"),
            "First transaction execute_sql should end with .1, got {id_exec1}"
        );
        assert!(
            id_exec2.ends_with(".1"),
            "Retried transaction execute_sql should end with .1, got {id_exec2}"
        );
        assert!(
            id_commit.ends_with(".1"),
            "Commit should end with .1, got {id_commit}"
        );

        // 2. Verify that RequestIds are monotonically increasing across the aborted transaction retry
        let req_num_exec1: u64 = id_exec1
            .split('.')
            .nth(4)
            .expect("request_id field")
            .parse()
            .expect("numeric request_id");
        let req_num_exec2: u64 = id_exec2
            .split('.')
            .nth(4)
            .expect("request_id field")
            .parse()
            .expect("numeric request_id");
        let req_num_commit: u64 = id_commit
            .split('.')
            .nth(4)
            .expect("request_id field")
            .parse()
            .expect("numeric request_id");

        assert_eq!(
            req_num_exec2,
            req_num_exec1 + 1,
            "Retried transaction execute_sql should increment Request ID by 1 ({req_num_exec2} vs {req_num_exec1})"
        );
        assert_eq!(
            req_num_commit,
            req_num_exec2 + 1,
            "Commit should increment Request ID by 1 ({req_num_commit} vs {req_num_exec2})"
        );
    }
}
