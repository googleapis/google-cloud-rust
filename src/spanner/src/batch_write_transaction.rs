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
use crate::model::RequestOptions;
use crate::model::request_options::Priority;
use crate::mutation::MutationGroup;
use crate::server_streaming::stream::BatchWriteStream;
use gaxi::prost::FromProto;

#[cfg(feature = "unstable-stream")]
use futures::Stream;

/// A builder for [BatchWriteTransaction].
///
/// Note that the `request_tag` field of [RequestOptions] is not exposed here:
/// per-request tags apply only to queries and reads, and are ignored by the
/// `BatchWrite` RPC. Use [set_transaction_tag][BatchWriteTransactionBuilder::set_transaction_tag]
/// to tag the transactions of a batch write.
pub struct BatchWriteTransactionBuilder {
    client: DatabaseClient,
    transaction_tag: Option<String>,
    priority: Priority,
    exclude_txn_from_change_streams: bool,
}

impl BatchWriteTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            transaction_tag: None,
            priority: Priority::Unspecified,
            exclude_txn_from_change_streams: false,
        }
    }

    /// Sets a transaction tag to be used for the batch write.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_write_transaction()
    ///     .set_transaction_tag("my-tag")
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The tag applies to all of the transactions created to apply the mutation
    /// groups of the batch write.
    ///
    /// See also: [Troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
    pub fn set_transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.transaction_tag = Some(tag.into());
        self
    }

    /// Sets the RPC priority to use for the batch write request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::model::request_options::Priority;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_write_transaction()
    ///     .set_priority(Priority::Low)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Sets whether to exclude the batch write from change streams.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_write_transaction()
    ///     .set_exclude_txn_from_change_streams(true)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// When set to `true`, it prevents modifications from all transactions in this batch write
    /// operation from being tracked in change streams.
    /// Note that this only affects change streams that have been created with the DDL option `allow_txn_exclusion = true`.
    /// If `allow_txn_exclusion` is not set or set to `false` for a change stream, updates made within this batch write
    /// are recorded in that change stream regardless of this setting.
    ///
    /// When set to `false` or not specified, modifications from this batch write are recorded in all change streams
    /// tracking columns modified by these transactions.
    pub fn set_exclude_txn_from_change_streams(mut self, exclude: bool) -> Self {
        self.exclude_txn_from_change_streams = exclude;
        self
    }

    /// Builds the [BatchWriteTransaction].
    pub fn build(self) -> BatchWriteTransaction {
        let session_name = self.client.session_name();
        let channel_hint = self.client.spanner.next_channel_hint();
        BatchWriteTransaction {
            session_name,
            client: self.client,
            channel_hint,
            transaction_tag: self.transaction_tag,
            priority: self.priority,
            exclude_txn_from_change_streams: self.exclude_txn_from_change_streams,
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
    channel_hint: usize,
    transaction_tag: Option<String>,
    priority: Priority,
    exclude_txn_from_change_streams: bool,
}

impl BatchWriteTransaction {
    /// Executes the batch write and returns a stream of responses.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::mutation::Mutation;
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::mutation::MutationGroup;
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
    /// while let Some(response) = stream.next().await {
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
    ///
    /// The method does not handle any errors, including retryable errors like Aborted.
    /// The caller is responsible for handling any errors and for retrying the transaction in
    /// case it is aborted by Spanner.
    pub async fn execute_streaming<I>(self, groups: I) -> crate::Result<BatchWriteResponseStream>
    where
        I: IntoIterator<Item = MutationGroup>,
    {
        let req_options = RequestOptions::default()
            .set_transaction_tag(self.transaction_tag.unwrap_or_default())
            .set_priority(self.priority);
        let req = BatchWriteRequest::new()
            .set_session(self.session_name)
            .set_mutation_groups(groups.into_iter().map(|g| g.build_proto()))
            .set_request_options(req_options)
            .set_exclude_txn_from_change_streams(self.exclude_txn_from_change_streams);

        let stream = self
            .client
            .spanner
            .batch_write(req, crate::RequestOptions::default(), self.channel_hint)
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
    pub async fn next(&mut self) -> Option<crate::Result<BatchWriteResponse>> {
        let proto_opt = self.inner.next_message().await?;
        match proto_opt {
            Ok(proto) => match proto.cnv() {
                Ok(model) => Some(Ok(model)),
                Err(e) => Some(Err(crate::Error::deser(e))),
            },
            Err(e) => Some(Err(e)),
        }
    }

    /// Converts the [`BatchWriteResponseStream`] into a [`Stream`].
    ///
    /// This consumes the [`BatchWriteResponseStream`] and returns a stream of responses.
    #[cfg(feature = "unstable-stream")]
    pub fn into_stream(self) -> impl Stream<Item = crate::Result<BatchWriteResponse>> + Unpin {
        use futures::stream::unfold;
        Box::pin(unfold(self, |mut stream| async move {
            stream.next().await.map(|res| (res, stream))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;
    use crate::mutation::Mutation;
    use crate::result_set::tests::adapt;
    use anyhow::Result;
    use gaxi::grpc::tonic::Response;
    use google_cloud_test_macros::tokio_test_no_panics;
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

    #[tokio_test_no_panics]
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
            .next()
            .await
            .expect("stream should have yielded a message")?;
        assert_eq!(
            result.indexes,
            vec![0],
            "indexes should match the mocked response"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_with_request_options() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().once().returning(|req| {
            let req = req.into_inner();

            // Validate the custom request options contain the transaction tag and priority
            let req_opts = req
                .request_options
                .as_ref()
                .expect("request_options should be present");
            assert_eq!(req_opts.transaction_tag, "my_tag");
            assert_eq!(Priority::from(req_opts.priority), Priority::High);
            assert_eq!(req_opts.request_tag, "");

            assert!(req.exclude_txn_from_change_streams);

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

        let tx = db_client
            .batch_write_transaction()
            .set_transaction_tag("my_tag")
            .set_priority(Priority::High)
            .set_exclude_txn_from_change_streams(true)
            .build();
        let mut stream = tx.execute_streaming(vec![group]).await?;

        let result = stream
            .next()
            .await
            .expect("stream should have yielded a message")?;
        assert_eq!(
            result.indexes,
            vec![0],
            "indexes should match the mocked response"
        );

        Ok(())
    }

    #[cfg(feature = "unstable-stream")]
    #[tokio_test_no_panics]
    async fn execute_streaming_into_stream() -> Result<()> {
        use futures::StreamExt;

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
                req.session, "projects/p/instances/i/databases/d/sessions/123",
                "session name should match"
            );
            assert_eq!(
                req.mutation_groups.len(),
                1,
                "should contain precisely 1 mutation group"
            );

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

        let transaction = db_client.batch_write_transaction().build();
        let stream = transaction.execute_streaming(vec![group]).await?;
        let mut stream = stream.into_stream();

        let result = stream
            .next()
            .await
            .expect("stream should have yielded a message")?;
        assert_eq!(
            result.indexes,
            vec![0],
            "indexes should match the mocked response"
        );

        Ok(())
    }
}
