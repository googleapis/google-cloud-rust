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

use crate::client::Spanner;
use crate::model::{CreateSessionRequest, Session};
use crate::{RequestOptions, Result};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;
use tokio::time::{Instant, sleep};

/// The interval at which the background task checks if the session needs replacement.
pub(crate) const SESSION_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(3600);

/// The maximum age of a session before it is considered for replacement.
/// Multiplexed sessions are designed to live for 28 days, but we rotate them
/// every 7 days to be safe.
pub(crate) const SESSION_MAINTENANCE_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

#[derive(Debug)]
pub(crate) struct ManagedSessionMaintainer {
    pub(crate) spanner: Spanner,
    pub(crate) session: RwLock<ManagedSession>,
    pub(crate) database_name: String,
    pub(crate) database_role: String,
    pub(crate) options: RequestOptions,
}

#[derive(Debug)]
pub(crate) struct ManagedSession {
    pub(crate) session: Arc<Session>,
    pub(crate) created_at: Instant,
}

impl ManagedSessionMaintainer {
    pub(crate) fn session_name(&self) -> String {
        self.session
            .read()
            .expect("failed to read session")
            .session
            .name
            .clone()
    }

    /// Creates a new `ManagedSessionMaintainer` with an initial session,
    /// and spawns a background task to periodically check and rotate the session.
    pub(crate) async fn create_and_start_maintenance(
        spanner: Spanner,
        database_name: String,
        database_role: String,
        options: RequestOptions,
    ) -> Result<Arc<Self>> {
        let session =
            Self::create_session(&spanner, &database_name, &database_role, &options).await?;

        let maintainer = Arc::new(ManagedSessionMaintainer {
            spanner,
            session: RwLock::new(ManagedSession {
                session: Arc::new(session),
                created_at: Instant::now(),
            }),
            database_name,
            database_role,
            options,
        });

        let weak_maintainer = Arc::downgrade(&maintainer);
        tokio::spawn(async move {
            Self::maintenance_loop(
                weak_maintainer,
                SESSION_MAINTENANCE_INTERVAL,
                SESSION_MAINTENANCE_AGE,
            )
            .await;
        });

        Ok(maintainer)
    }

    async fn check_and_replace_session(&self, age: Duration) -> Result<()> {
        let should_replace = {
            let guard = self.session.read().expect("failed to read session");
            guard.created_at.elapsed() >= age
        };

        if should_replace {
            self.replace_session().await?;
        }
        Ok(())
    }

    async fn replace_session(&self) -> Result<()> {
        let new_session = Self::create_session(
            &self.spanner,
            &self.database_name,
            &self.database_role,
            &self.options,
        )
        .await?;

        let mut guard = self.session.write().expect("failed to write session");
        *guard = ManagedSession {
            session: Arc::new(new_session),
            created_at: Instant::now(),
        };
        tracing::info!(
            "Successfully replaced multiplexed session for {}",
            self.database_name
        );
        Ok(())
    }

    async fn create_session(
        spanner: &Spanner,
        database_name: &str,
        database_role: &str,
        options: &RequestOptions,
    ) -> Result<Session> {
        let request = CreateSessionRequest::new()
            .set_database(database_name)
            .set_session(
                Session::new()
                    .set_multiplexed(true)
                    .set_creator_role(database_role),
            );

        spanner
            .create_session(request, options.clone(), spanner.next_channel_hint())
            .await
    }

    async fn maintenance_loop(
        maintainer: Weak<ManagedSessionMaintainer>,
        interval: Duration,
        age: Duration,
    ) {
        sleep(interval).await;
        while let Some(m) = maintainer.upgrade() {
            Self::maintain(m, age).await;
            sleep(interval).await;
        }
    }

    /// Performs a single maintenance iteration.
    async fn maintain(maintainer: Arc<ManagedSessionMaintainer>, age: Duration) {
        if let Err(e) = maintainer.check_and_replace_session(age).await {
            tracing::warn!(
                "Failed to check and replace session for {}: {}. Retrying in 1 hour.",
                maintainer.database_name,
                e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaxi::grpc::tonic::Response;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use spanner_grpc_mock::google::spanner::v1::Session as GrpcSession;
    use spanner_grpc_mock::{MockSpanner, start};

    #[tokio::test]
    async fn session_maintenance() {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(GrpcSession {
                    name:
                        "projects/test-project/instances/test-instance/databases/test-db/sessions/1"
                            .to_string(),
                    multiplexed: true,
                    ..Default::default()
                }))
            });

        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(GrpcSession {
                    name:
                        "projects/test-project/instances/test-instance/databases/test-db/sessions/2"
                            .to_string(),
                    multiplexed: true,
                    ..Default::default()
                }))
            });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let maintainer = ManagedSessionMaintainer::create_and_start_maintenance(
            spanner,
            "projects/test-project/instances/test-instance/databases/test-db".to_string(),
            "test-role".to_string(),
            RequestOptions::default(),
        )
        .await
        .expect("Failed to create ManagedSessionMaintainer");

        {
            let session = maintainer
                .session
                .read()
                .expect("failed to read session")
                .session
                .clone();
            assert_eq!(
                session.name,
                "projects/test-project/instances/test-instance/databases/test-db/sessions/1"
            );
        }

        // Modify created_at to be in the past (older than 7 days)
        {
            let mut guard = maintainer.session.write().expect("failed to write session");
            guard.created_at = Instant::now() - Duration::from_secs(7 * 24 * 3600 + 3600);
        }

        // Manually trigger maintenance check
        maintainer
            .check_and_replace_session(SESSION_MAINTENANCE_AGE)
            .await
            .expect("Failed to check and replace session");

        {
            let session = maintainer
                .session
                .read()
                .expect("failed to read session")
                .session
                .clone();
            assert_eq!(
                session.name,
                "projects/test-project/instances/test-instance/databases/test-db/sessions/2"
            );
        }
    }

    #[tokio::test]
    async fn maintain_success() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(Response::new(GrpcSession {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/1"
                    .to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let maintainer = ManagedSessionMaintainer::create_and_start_maintenance(
            spanner,
            "projects/test-project/instances/test-instance/databases/test-db".to_string(),
            "test-role".to_string(),
            RequestOptions::default(),
        )
        .await
        .expect("Failed to create ManagedSessionMaintainer");

        let weak = Arc::downgrade(&maintainer);
        let m = weak.upgrade().expect("should be alive");
        ManagedSessionMaintainer::maintain(m, SESSION_MAINTENANCE_AGE).await;
    }

    #[tokio::test]
    async fn maintain_dropped() {
        let weak = Weak::<ManagedSessionMaintainer>::new();
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn check_and_replace_session_no_op() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(Response::new(GrpcSession {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/1"
                    .to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let maintainer = ManagedSessionMaintainer::create_and_start_maintenance(
            spanner,
            "projects/test-project/instances/test-instance/databases/test-db".to_string(),
            "test-role".to_string(),
            RequestOptions::default(),
        )
        .await
        .expect("Failed to create ManagedSessionMaintainer");

        // Call check_and_replace_session with full age (7 days).
        // The session is still valid, and should not be replaced.
        maintainer
            .check_and_replace_session(SESSION_MAINTENANCE_AGE)
            .await
            .expect("Failed to check and replace session");

        // Verify session is still the same.
        let session = maintainer
            .session
            .read()
            .expect("failed to read session")
            .session
            .clone();
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/1"
        );
    }

    #[tokio::test]
    async fn maintain_creation_fails() {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // 1st call succeeds (initialization)
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(GrpcSession {
                    name:
                        "projects/test-project/instances/test-instance/databases/test-db/sessions/1"
                            .to_string(),
                    multiplexed: true,
                    ..Default::default()
                }))
            });

        // 2nd call fails (maintenance)
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Err(gaxi::grpc::tonic::Status::internal("mock failure")));

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let maintainer = ManagedSessionMaintainer::create_and_start_maintenance(
            spanner,
            "projects/test-project/instances/test-instance/databases/test-db".to_string(),
            "test-role".to_string(),
            RequestOptions::default(),
        )
        .await
        .expect("Failed to create ManagedSessionMaintainer");

        // Age the session
        {
            let mut guard = maintainer.session.write().expect("failed to write session");
            guard.created_at = Instant::now() - Duration::from_secs(7 * 24 * 3600 + 3600);
        }

        let weak = Arc::downgrade(&maintainer);
        let m = weak.upgrade().expect("should be alive");
        ManagedSessionMaintainer::maintain(m, SESSION_MAINTENANCE_AGE).await;

        // Verify session is still the old one!
        let session = maintainer
            .session
            .read()
            .expect("failed to read session")
            .session
            .clone();
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/1"
        );
    }

    #[tokio::test]
    async fn transaction_session_consistency_across_retries() {
        use crate::database_client::DatabaseClient;
        use crate::transaction_retry_policy::tests::create_aborted_status;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;
        use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;

        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // 1st call succeeds (initialization)
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(mock_v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/1".to_string(),
                    multiplexed: true,
                    ..Default::default()
                }))
            });

        // 2nd call succeeds (forced replacement)
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(mock_v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/2".to_string(),
                    multiplexed: true,
                    ..Default::default()
                }))
            });

        // Mock begin_transaction (twice)
        mock.expect_begin_transaction().times(2).returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/1");
            Ok(Response::new(mock_v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });

        // Mock execute_sql for update (attempt 1)
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/1");

            Ok(Response::new(mock_v1::ResultSet {
                stats: Some(mock_v1::ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        // Mock commit returning Aborted
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/1");
            Err(create_aborted_status(std::time::Duration::from_nanos(1)))
        });

        // Mock execute_sql for update (retry attempt)
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/1");

            Ok(Response::new(mock_v1::ResultSet {
                stats: Some(mock_v1::ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        // Mock commit returning success
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/1");
            Ok(Response::new(mock_v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let maintainer = ManagedSessionMaintainer::create_and_start_maintenance(
            spanner.clone(),
            "projects/p/instances/i/databases/d".to_string(),
            "test-role".to_string(),
            RequestOptions::default(),
        )
        .await
        .expect("Failed to create ManagedSessionMaintainer");

        let db_client = DatabaseClient {
            spanner,
            session_maintainer: maintainer.clone(),
        };

        // 1. Create builder (captures session 1)
        let runner = db_client
            .read_write_transaction()
            .build()
            .await
            .expect("Failed to build runner");

        // 2. Force rotation
        maintainer
            .replace_session()
            .await
            .expect("Failed to replace session");

        // Verify that the maintainer now has session 2
        assert_eq!(
            maintainer.session_name(),
            "projects/p/instances/i/databases/d/sessions/2"
        );

        // 3. Run transaction
        let result = runner
            .run(
                |tx: crate::read_write_transaction::ReadWriteTransaction| async move {
                    let count = tx.execute_update("UPDATE Users SET active = true").await?;
                    assert_eq!(count, 1);
                    Ok(())
                },
            )
            .await;

        result.expect("Transaction failed");
    }
}
