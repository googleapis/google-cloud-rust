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
use crate::model::BatchWriteResponse;
use crate::mutation::MutationGroup;
use crate::server_streaming::stream::BatchWriteStream;
use gaxi::prost::FromProto;

/// A builder for [BatchWriteTransaction].
pub struct BatchWriteTransactionBuilder {
    client: DatabaseClient,
}

impl BatchWriteTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self { client }
    }

    /// Builds the [BatchWriteTransaction].
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
pub struct BatchWriteTransaction {
    session_name: String,
    client: DatabaseClient,
}

impl BatchWriteTransaction {
    /// Executes the batch write and returns a stream of responses.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Mutation, Spanner, MutationGroup};
    /// # use google_cloud_gax::error::rpc::Code;
    /// # async fn sample() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Spanner::builder().build().await?;
    /// let db = client.database_client("projects/p/instances/i/databases/d").build().await?;
    ///
    /// let mutation = Mutation::new_insert_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .build();
    /// let group = MutationGroup::new(vec![mutation]);
    ///
    /// let tx = db.batch_write_transaction().build();
    /// let mut stream = tx.execute_streaming(vec![group]).await?;
    ///
    /// while let Some(response) = stream.next_message().await {
    ///     let response = response?;
    ///     if let Some(status) = response.status.as_ref().filter(|s| s.code != Code::Ok as i32) {
    ///         eprintln!("Error applying groups {:?}: {}", response.indexes, status.message);
    ///     } else {
    ///         println!("Applied groups: {:?}", response.indexes);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This method sends the mutation groups to Spanner and returns the responses as a stream.
    /// Each response includes a status code that indicates whether the mutation groups that
    /// it references were applied successfully.
    /// The method does not handle any errors, including retryable errors like Aborted.
    /// The caller is responsible for handling any errors and for retrying the transaction in
    /// case it is aborted by Spanner.
    pub async fn execute_streaming<I>(self, groups: I) -> crate::Result<BatchWriteResponseStream>
    where
        I: IntoIterator<Item = MutationGroup>,
    {
        let req = BatchWriteRequest::new()
            .set_session(self.session_name.clone())
            .set_mutation_groups(groups.into_iter().map(|g| g.build_proto()));

        let stream = self
            .client
            .spanner
            .batch_write(req, crate::RequestOptions::default())
            .send()
            .await?;
        Ok(BatchWriteResponseStream { inner: stream })
    }
}

/// A stream of [BatchWriteResponse] messages.
pub struct BatchWriteResponseStream {
    pub(crate) inner: BatchWriteStream,
}

impl BatchWriteResponseStream {
    /// Fetches the next [BatchWriteResponse] from the stream.
    ///
    /// Returns `Some(Ok(BatchWriteResponse))` when a message is successfully received,
    /// `None` when the stream concludes naturally, or `Some(Err(_))` on RPC errors.
    pub async fn next_message(&mut self) -> Option<crate::Result<BatchWriteResponse>> {
        let proto_opt = self.inner.next_message().await?;
        match proto_opt {
            Ok(proto) => match proto.cnv() {
                Ok(model) => Some(Ok(model)),
                Err(e) => Some(Err(crate::Error::deser(e))),
            },
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Mutation, Spanner};
    use crate::result_set::tests::adapt;
    use anyhow::Result;
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
    async fn execute_streaming() -> Result<()> {
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
        let mut stream = tx.execute_streaming(vec![group]).await?;

        let result = stream
            .next_message()
            .await
            .expect("stream should have yielded a message")?;
        assert_eq!(result.indexes, vec![0]);

        Ok(())
    }
}
