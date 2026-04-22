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

use crate::client::DatabaseClient;
use crate::model::BatchWriteRequest;
use crate::mutation::MutationGroup;
use crate::server_streaming::stream::BatchWriteStream;

/// A builder for [BatchWriteTransaction].
#[allow(dead_code)]
pub struct BatchWriteTransactionBuilder {
    client: DatabaseClient,
}

impl BatchWriteTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self { client }
    }

    /// Builds the [BatchWriteTransaction].
    #[allow(dead_code)]
    pub fn build(self) -> BatchWriteTransaction {
        let session_name = self.client.session_name();
        BatchWriteTransaction {
            session_name,
            client: self.client,
        }
    }
}

/// A transaction for executing batch writes.
///
/// Batch writes are not guaranteed to be atomic across mutation groups.
/// All mutations within a group are applied atomically.
#[allow(dead_code)]
pub struct BatchWriteTransaction {
    session_name: String,
    client: DatabaseClient,
}

impl BatchWriteTransaction {
    /// Executes the batch write and returns a stream of responses.
    #[allow(dead_code)]
    pub(crate) async fn execute_streaming<I>(self, groups: I) -> crate::Result<BatchWriteStream>
    where
        I: IntoIterator<Item = MutationGroup>,
    {
        let req = BatchWriteRequest::new()
            .set_session(self.session_name.clone())
            .set_mutation_groups(groups.into_iter().map(|g| g.build_proto()));

        self.client
            .spanner
            .batch_write(req, crate::RequestOptions::default())
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Mutation, Spanner};
    use crate::result_set::tests::adapt;
    use gaxi::grpc::tonic::Response;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;

    pub(crate) async fn setup_db_client(
        mock: MockSpanner,
    ) -> (DatabaseClient, tokio::task::JoinHandle<()>) {
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        let (address, server) = spanner_grpc_mock::start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        (db_client, server)
    }

    #[tokio::test]
    async fn test_execute_streaming() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.mutation_groups.len(), 1);

            let response = mock_v1::BatchWriteResponse {
                indexes: vec![0],
                status: None,
                commit_timestamp: None,
            };

            Ok(Response::from(adapt([Ok(response)])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .build();
        let group = MutationGroup::new(vec![mutation]);

        let tx = db_client.batch_write_transaction().build();
        let mut stream = tx.execute_streaming(vec![group]).await.unwrap();

        let result = stream.next_message().await;
        assert!(result.is_some());
        let result = result.unwrap().unwrap();
        assert_eq!(result.indexes, vec![0]);
    }
}
