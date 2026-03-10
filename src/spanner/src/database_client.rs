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
use crate::model::Session;
use crate::read_only_transaction::SingleUseReadOnlyTransactionBuilder;
use std::sync::Arc;

/// A client for interacting with a specific Spanner database.
///
/// `DatabaseClient` provides methods to execute transactions and queries.
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # async fn sample() -> anyhow::Result<()> {
///     let spanner = Spanner::builder().build().await?;
///     let database_client = spanner
///         .database_client("projects/my-project/instances/my-instance/databases/my-db")
///         .build()
///         .await?;
///     # Ok(())
/// # }
/// ```
///
/// `DatabaseClient` provides methods to execute transactions and queries.
/// It holds a single multiplexed session for the database.
///
/// A `DatabaseClient` is intended to be a long-lived object, and normally an
/// application will have a single `DatabaseClient` per database. The client is
/// thread-safe and should be reused for all operations on the database.
///
/// Cloning a `DatabaseClient` is cheap, as it shares the underlying session and channel.
#[derive(Clone, Debug)]
pub struct DatabaseClient {
    #[allow(dead_code)]
    pub(crate) spanner: Spanner,
    #[allow(dead_code)]
    pub(crate) session: Arc<Session>,
}

impl DatabaseClient {
    /// Returns a builder for a single-use read-only transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.single_use().build();
    /// let mut rs = tx.execute_query(Statement::new("SELECT 1")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn single_use(&self) -> SingleUseReadOnlyTransactionBuilder {
        SingleUseReadOnlyTransactionBuilder::new(self.clone())
    }
}

/// A builder for [DatabaseClient].
pub struct DatabaseClientBuilder {
    spanner: Spanner,
    database_name: String,
    database_role: Option<String>,
    options: Option<crate::RequestOptions>,
}

impl DatabaseClientBuilder {
    pub(crate) fn new(spanner: Spanner, database_name: String) -> Self {
        Self {
            spanner,
            database_name,
            database_role: None,
            options: None,
        }
    }

    /// Sets the database role for the client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    ///     let spanner = Spanner::builder().build().await?;
    ///     let database_client = spanner
    ///         .database_client("projects/my-project/instances/my-instance/databases/my-db")
    ///         .with_database_role("my-role")
    ///         .build()
    ///         .await?;
    ///     # Ok(())
    /// # }
    /// ```
    ///
    /// Database roles are used for Fine-Grained Access Control (FGAC).
    /// You can assign a database role to a session, and that role determines the permissions for that session.
    /// For more information, see [Access with FGAC](https://docs.cloud.google.com/spanner/docs/access-with-fgac).
    pub fn with_database_role(mut self, role: impl Into<String>) -> Self {
        self.database_role = Some(role.into());
        self
    }

    /// Sets the request options that will be used when creating the multiplexed
    /// session for the client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::options::RequestOptions;
    /// # use std::time::Duration;
    /// # async fn sample() -> anyhow::Result<()> {
    ///     let spanner = Spanner::builder().build().await?;
    ///     let mut options = RequestOptions::default();
    ///     options.set_attempt_timeout(Duration::from_secs(60));
    ///     let database_client = spanner
    ///         .database_client("projects/my-project/instances/my-instance/databases/my-db")
    ///         .with_request_options(options)
    ///         .build()
    ///         .await?;
    ///     # Ok(())
    /// # }
    /// ```
    pub fn with_request_options(mut self, options: crate::RequestOptions) -> Self {
        self.options = Some(options);
        self
    }

    /// Builds the [DatabaseClient] and creates a single multiplexed session that
    /// will be used for all operations on the database.
    pub async fn build(self) -> crate::Result<DatabaseClient> {
        let request = crate::model::CreateSessionRequest::new()
            .set_database(self.database_name)
            .set_session(
                Session::new()
                    .set_multiplexed(true)
                    .set_creator_role(self.database_role.unwrap_or_default()),
            );

        let session = self
            .spanner
            .create_session(request, self.options.unwrap_or_default())
            .await?;

        Ok(DatabaseClient {
            spanner: self.spanner,
            session: Arc::new(session),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use spanner_grpc_mock::{MockSpanner, start};

    #[test]
    fn test_auto_traits() {
        use static_assertions::assert_impl_all;
        assert_impl_all!(DatabaseClient: Send, Sync, Clone, std::fmt::Debug);
    }

    #[tokio::test]
    async fn test_database_client_builder() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|req| {
            let req = req.into_inner();
            let session = req.session.unwrap();
            assert!(session.multiplexed);
            assert_eq!(session.creator_role, "test-role");

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                    multiplexed: true,
                    creator_role: "test-role".to_string(),
                    ..Default::default()
                },
            ))
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

        let db_client = spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .with_database_role("test-role")
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        assert_eq!(
            db_client.session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
        );
        assert!(db_client.session.multiplexed);
        assert_eq!(db_client.session.creator_role, "test-role");
    }

    #[tokio::test]
    async fn test_database_client_builder_with_options() {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Err(gaxi::grpc::tonic::Status::unavailable("unavailable")));
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                let session = req.session.unwrap();
                assert!(session.multiplexed);
                Ok(gaxi::grpc::tonic::Response::new(
                    spanner_grpc_mock::google::spanner::v1::Session {
                        name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                        multiplexed: true,
                        ..Default::default()
                    },
                ))
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

        let mut options = crate::RequestOptions::default();
        options.set_retry_policy(google_cloud_gax::retry_policy::Aip194Strict);
        options.set_idempotency(true);

        let db_client = spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .with_request_options(options)
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        assert_eq!(
            db_client.session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
        );
    }

    #[tokio::test]
    async fn test_database_client_builder_error() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Err(gaxi::grpc::tonic::Status::permission_denied(
                "permission denied",
            ))
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

        let result = spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .build()
            .await;

        match result {
            Ok(_) => panic!("Client creation should have failed"),
            Err(e) => assert_eq!(
                e.status().map(|s| s.code),
                Some(google_cloud_gax::error::rpc::Code::PermissionDenied)
            ),
        }
    }
}
