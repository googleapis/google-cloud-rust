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

/// A client for interacting with a specific Spanner database.
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
    pub(crate) session: std::sync::Arc<Session>,
}

/// A builder for [DatabaseClient].
pub struct DatabaseClientBuilder {
    spanner: Spanner,
    database_name: String,
    database_role: Option<String>,
}

impl DatabaseClientBuilder {
    pub(crate) fn new(spanner: Spanner, database_name: String) -> Self {
        Self {
            spanner,
            database_name,
            database_role: None,
        }
    }

    /// Sets the database role for the client.
    pub fn database_role(mut self, role: impl Into<String>) -> Self {
        self.database_role = Some(role.into());
        self
    }

    /// Builds the [DatabaseClient] and creates a multiplexed session on Spanner.
    pub async fn build(self) -> crate::Result<DatabaseClient> {
        let mut request = crate::model::CreateSessionRequest::new();
        request.database = self.database_name;

        let mut session_template = crate::model::Session::new();
        session_template.multiplexed = true;
        if let Some(role) = &self.database_role {
            session_template.creator_role = role.clone();
        }
        request.session = Some(session_template);

        let session = self
            .spanner
            .create_session(request, crate::RequestOptions::default())
            .await?;

        Ok(DatabaseClient {
            spanner: self.spanner,
            session: std::sync::Arc::new(session),
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
            .database_role("test-role")
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
