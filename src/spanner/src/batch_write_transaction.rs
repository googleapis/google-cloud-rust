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

use crate::Error;
use crate::Result;
use crate::client::DatabaseClient;
use crate::error::internal_error;
use crate::google::spanner::v1::BatchWriteResponse as ProtoBatchWriteResponse;
use crate::model::BatchWriteRequest;
use crate::model::BatchWriteResponse;
use crate::model::RequestOptions;
use crate::model::batch_write_request::MutationGroup as ProtoMutationGroup;
use crate::model::request_options::Priority;
use crate::mutation::MutationGroup;
use crate::retry_policy::SpannerRetryPolicy;
use crate::server_streaming::stream::BatchWriteStream;
use gaxi::prost::FromProto;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::error::rpc::Status as RpcStatus;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use http::HeaderMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::time::sleep;

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
    gax_options: GaxRequestOptions,
}

impl BatchWriteTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            transaction_tag: None,
            priority: Priority::Unspecified,
            exclude_txn_from_change_streams: false,
            gax_options: GaxRequestOptions::default(),
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

    /// Sets the per-attempt timeout for this batch write request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use std::time::Duration;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_write_transaction()
    ///     .with_attempt_timeout(Duration::from_secs(30))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.gax_options.set_attempt_timeout(timeout);
        self
    }

    /// Sets the retry policy for this batch write request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::retry_policy::SpannerRetryPolicy;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_write_transaction()
    ///     .with_retry_policy(SpannerRetryPolicy::new())
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.gax_options.set_retry_policy(policy);
        self
    }

    /// Sets the backoff policy for this batch write request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_write_transaction()
    ///     .with_backoff_policy(ExponentialBackoffBuilder::default().clamp())
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.gax_options.set_backoff_policy(policy);
        self
    }

    /// Builds the [BatchWriteTransaction].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_write_transaction().build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(self) -> BatchWriteTransaction {
        let session_name = self.client.session_name();
        let channel_hint = self.client.next_channel_hint();
        let gax_options = apply_defaults(self.gax_options);
        BatchWriteTransaction {
            session_name,
            client: self.client,
            channel_hint,
            transaction_tag: self.transaction_tag,
            priority: self.priority,
            exclude_txn_from_change_streams: self.exclude_txn_from_change_streams,
            gax_options,
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
    gax_options: GaxRequestOptions,
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
    /// let transaction = db.batch_write_transaction().build();
    /// let mut stream = transaction.execute_streaming(vec![group]).await?;
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
    /// Transient network or server errors during stream creation or iteration are automatically retried
    /// according to the configured retry and backoff policies. Upon retry, already-processed mutation groups
    /// are not resent, and returned response indices always refer to the caller's original input slice.
    pub async fn execute_streaming<I>(self, mutation_groups: I) -> Result<BatchWriteResponseStream>
    where
        I: IntoIterator<Item = MutationGroup>,
    {
        let original_groups = mutation_groups
            .into_iter()
            .map(|group| Some(group.build_proto()))
            .collect::<Vec<_>>();
        let total_count = original_groups.len();
        let now = Instant::now();

        Ok(BatchWriteResponseStream {
            client: self.client,
            session_name: self.session_name,
            channel_hint: self.channel_hint,
            transaction_tag: self.transaction_tag,
            priority: self.priority,
            exclude_txn_from_change_streams: self.exclude_txn_from_change_streams,
            gax_options: self.gax_options,
            stream: None,
            original_groups,
            total_count,
            completed_count: 0,
            current_stream_indices: Vec::new(),
            retry_count: 0,
            method_name: "BatchWrite",
            headers: HeaderMap::new(),
            attempt_start_time: now,
            operation_start_time: now,
            stream_started: false,
            attempt_recorded: false,
            operation_recorded: false,
        })
    }
}

/// A stream of [BatchWriteResponse] messages.
pub struct BatchWriteResponseStream {
    client: DatabaseClient,
    session_name: String,
    channel_hint: usize,
    transaction_tag: Option<String>,
    priority: Priority,
    exclude_txn_from_change_streams: bool,
    gax_options: GaxRequestOptions,
    stream: Option<BatchWriteStream>,

    // All original mutation groups and tracking of completed groups.
    // Confirmed completed mutation groups are taken (`None`), releasing their payload memory immediately.
    original_groups: Vec<Option<ProtoMutationGroup>>,
    total_count: usize,
    completed_count: usize,
    current_stream_indices: Vec<usize>,

    // Retry state
    retry_count: usize,

    // Observability metrics
    method_name: &'static str,
    headers: HeaderMap,
    attempt_start_time: Instant,
    operation_start_time: Instant,
    stream_started: bool,
    attempt_recorded: bool,
    operation_recorded: bool,
}

impl BatchWriteResponseStream {
    /// Fetches the next [BatchWriteResponse] from the stream.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::mutation::{Mutation, MutationGroup};
    /// # async fn run(spanner: Spanner) -> Result<(), Box<dyn std::error::Error>> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_write_transaction().build();
    /// let group = MutationGroup::new(vec![Mutation::new_insert_builder("Users").set("UserId").to(&1).build()]);
    /// let mut stream = transaction.execute_streaming(vec![group]).await?;
    /// while let Some(response) = stream.next().await {
    ///     let response = response?;
    ///     println!("Applied groups: {:?}", response.indexes);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Returns `Some(Ok(BatchWriteResponse))` when a message is successfully received,
    /// `None` when the stream concludes naturally and all mutation groups have been processed,
    /// or `Some(Err(_))` on non-retryable RPC errors or when retries are exhausted.
    pub async fn next(&mut self) -> Option<Result<BatchWriteResponse>> {
        loop {
            // Check if all mutation groups are already completed and no active stream is running.
            if self.is_complete() && self.stream.is_none() {
                self.record_operation_complete(None);
                return None;
            }

            // If the stream is not currently established, connect or reconnect it.
            if self.stream.is_none()
                && let Err(error) = self.ensure_stream_established().await
            {
                return Some(Err(error));
            }

            // Read the next response message from the active gRPC stream.
            let stream = self.stream.as_mut().expect("stream must be established");
            match stream.next_message().await {
                Some(Ok(proto_response)) => {
                    return Some(self.handle_response_message(proto_response));
                }
                Some(Err(stream_error)) => {
                    if let Err(terminal_error) = self.handle_stream_error(stream_error).await {
                        return Some(Err(terminal_error));
                    }
                }
                None => {
                    if let Err(terminal_error) = self.handle_stream_end().await {
                        return Some(Err(terminal_error));
                    }
                }
            }
        }
    }

    /// Returns `true` if all input mutation groups have been acknowledged by Spanner.
    fn is_complete(&self) -> bool {
        self.completed_count == self.total_count
    }

    /// Connects a new gRPC server stream with the remaining unprocessed mutation groups.
    async fn ensure_stream_established(&mut self) -> Result<()> {
        loop {
            self.stream_started = true;
            // Collect the original indices and clones of all mutation groups that have not yet completed.
            self.current_stream_indices.clear();
            let mut mutation_groups = Vec::new();
            for (index, group_opt) in self.original_groups.iter().enumerate() {
                if let Some(group) = group_opt {
                    self.current_stream_indices.push(index);
                    mutation_groups.push(group.clone());
                }
            }

            let request_options = RequestOptions::default()
                .set_transaction_tag(self.transaction_tag.clone().unwrap_or_default())
                .set_priority(self.priority.clone());
            let request = BatchWriteRequest::new()
                .set_session(self.session_name.clone())
                .set_mutation_groups(mutation_groups)
                .set_request_options(request_options)
                .set_exclude_txn_from_change_streams(self.exclude_txn_from_change_streams);

            self.attempt_start_time = Instant::now();
            self.attempt_recorded = false;
            self.headers.clear();

            let stream_result = self
                .client
                .batch_write(request, self.gax_options.clone(), self.channel_hint)
                .send()
                .await;

            match stream_result {
                Ok(stream) => {
                    self.headers = stream.headers().clone();
                    self.stream = Some(stream);
                    return Ok(());
                }
                Err(error) => {
                    self.record_current_attempt(Some(&error));
                    self.check_and_apply_retry(error).await?;
                }
            }
        }
    }

    /// Handles a protobuf response message by translating relative indices back to original indices
    /// and updating the internal completed state.
    fn handle_response_message(
        &mut self,
        proto_response: ProtoBatchWriteResponse,
    ) -> Result<BatchWriteResponse> {
        let mut translated_indexes = Vec::with_capacity(proto_response.indexes.len());

        // Each response message contains 0-based indices relative to the request payload
        // sent on the *active* stream attempt (which may only be a subset of the original groups).
        // Here, we validate bounds and translate each relative index back to the caller's original 0-indexed positions.
        for &relative_index in &proto_response.indexes {
            if relative_index < 0 {
                let error = internal_error(format!(
                    "Spanner returned negative mutation group index: {relative_index}"
                ));
                self.stream = None;
                self.record_current_attempt(Some(&error));
                self.record_operation_complete(Some(&error));
                return Err(error);
            }
            let unsigned_index = relative_index as usize;
            let Some(&original_index) = self.current_stream_indices.get(unsigned_index) else {
                let error = internal_error(format!(
                    "Spanner returned index {unsigned_index} out of bounds for active stream mutation groups (len {})",
                    self.current_stream_indices.len()
                ));
                self.stream = None;
                self.record_current_attempt(Some(&error));
                self.record_operation_complete(Some(&error));
                return Err(error);
            };

            // Mark the mutation group as processed and release its payload memory immediately.
            // Note: Even if the response carries an error status for this group (e.g. ABORTED or ALREADY_EXISTS),
            // the outcome has been acknowledged and delivered to the caller, so we consider it processed.
            if self.original_groups[original_index].take().is_some() {
                self.completed_count += 1;
            }
            translated_indexes.push(original_index as i32);
        }

        let mut model_response: BatchWriteResponse = proto_response.cnv().map_err(Error::deser)?;
        model_response.indexes = translated_indexes;
        Ok(model_response)
    }

    /// Handles a stream error by checking if it is retryable, applying backoff delay, or recording failure.
    async fn handle_stream_error(&mut self, stream_error: Error) -> Result<()> {
        self.record_current_attempt(Some(&stream_error));
        self.stream = None;
        self.current_stream_indices.clear();

        if self.is_complete() {
            return Ok(());
        }

        self.check_and_apply_retry(stream_error).await
    }

    /// Handles the end of a stream (EOF), verifying whether all mutation groups were processed.
    async fn handle_stream_end(&mut self) -> Result<()> {
        self.record_current_attempt(None);
        self.stream = None;
        self.current_stream_indices.clear();

        if self.is_complete() {
            return Ok(());
        }

        let remaining = self.total_count - self.completed_count;
        let error = Error::service(
            RpcStatus::default()
                .set_code(Code::Unavailable)
                .set_message(format!(
                    "BatchWrite stream closed prematurely with {remaining} unprocessed mutation groups"
                )),
        );
        self.check_and_apply_retry(error).await
    }

    /// Checks the retry policy and, if retryable, sleeps for the backoff duration.
    async fn check_and_apply_retry(&mut self, error: Error) -> Result<()> {
        match self.check_retry(error) {
            Ok(()) => {
                self.retry_count += 1;
                // Rotate channel hint only when a retry is confirmed to distribute load across healthy connections.
                self.channel_hint = self.client.next_channel_hint();
                if let Some(policy) = self.gax_options.backoff_policy() {
                    let state = RetryState::new(true).set_attempt_count(self.retry_count as u32);
                    let delay = policy.on_failure(&state);
                    sleep(delay).await;
                }
                Ok(())
            }
            Err(terminal_error) => {
                self.record_operation_complete(Some(&terminal_error));
                Err(terminal_error)
            }
        }
    }

    fn check_retry(&self, error: Error) -> Result<()> {
        let policy = self
            .gax_options
            .retry_policy()
            .as_ref()
            .expect("retry_policy is initialized by apply_defaults");
        let attempt_count = 1 + self.retry_count as u32;
        let state = RetryState::new(true).set_attempt_count(attempt_count);

        match policy.on_error(&state, error) {
            RetryResult::Continue(_) => Ok(()),
            RetryResult::Permanent(err) | RetryResult::Exhausted(err) => Err(err),
        }
    }

    fn record_current_attempt(&mut self, error: Option<&Error>) {
        if self.attempt_recorded {
            return;
        }
        self.attempt_recorded = true;
        let elapsed = self.attempt_start_time.elapsed();
        self.client
            .o11y
            .record_attempt(self.method_name, elapsed, error, Some(&self.headers));
    }

    fn record_operation_complete(&mut self, error: Option<&Error>) {
        if self.operation_recorded {
            return;
        }
        self.operation_recorded = true;
        let elapsed = self.operation_start_time.elapsed();
        self.client
            .o11y
            .record_operation(self.method_name, elapsed, error);
    }

    /// Converts the [`BatchWriteResponseStream`] into a [`Stream`].
    ///
    /// This consumes the [`BatchWriteResponseStream`] and returns a stream of responses.
    #[cfg(feature = "unstable-stream")]
    pub fn into_stream(self) -> impl Stream<Item = Result<BatchWriteResponse>> + Unpin {
        use futures::stream::unfold;
        Box::pin(unfold(self, |mut stream| async move {
            stream.next().await.map(|response| (response, stream))
        }))
    }
}

impl Drop for BatchWriteResponseStream {
    fn drop(&mut self) {
        if self.stream_started {
            let drop_error = (!self.is_complete()).then(|| {
                Error::service(RpcStatus::default().set_code(Code::Cancelled).set_message(
                    "BatchWrite stream dropped before all mutation groups were processed",
                ))
            });
            if !self.attempt_recorded {
                self.record_current_attempt(drop_error.as_ref());
            }
            if !self.operation_recorded {
                self.record_operation_complete(drop_error.as_ref());
            }
        }
    }
}

const DEFAULT_ATTEMPT_LIMIT: u32 = 10;

fn apply_defaults(mut gax_options: GaxRequestOptions) -> GaxRequestOptions {
    if gax_options.retry_policy().is_none() {
        gax_options
            .set_retry_policy(SpannerRetryPolicy::new().with_attempt_limit(DEFAULT_ATTEMPT_LIMIT));
    }
    if gax_options.backoff_policy().is_none() {
        gax_options.set_backoff_policy(default_backoff_policy());
    }
    gax_options
}

fn default_backoff_policy() -> Arc<dyn BackoffPolicy> {
    Arc::new(ExponentialBackoffBuilder::default().clamp())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;
    use crate::mutation::Mutation;
    use crate::result_set::tests::adapt;
    use anyhow::Result;
    use gaxi::grpc::tonic::Response;
    use gaxi::grpc::tonic::Status as TonicStatus;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_test_macros::tokio_test_no_panics;
    use mockall::Sequence;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::rpc::Status as ProtoStatus;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(BatchWriteTransactionBuilder: Send, Sync);
        static_assertions::assert_impl_all!(BatchWriteTransaction: Send, Sync);
        static_assertions::assert_impl_all!(BatchWriteResponseStream: Send, Sync);
    }

    pub(crate) async fn setup_db_client(
        mock: MockSpanner,
    ) -> (DatabaseClient, tokio::task::JoinHandle<()>) {
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

    fn create_test_mutation_group(table_name: &str, user_id: i64) -> MutationGroup {
        let mutation = Mutation::new_insert_builder(table_name)
            .set("UserId")
            .to(user_id)
            .build();
        MutationGroup::new(vec![mutation])
    }

    fn test_backoff_policy() -> Arc<dyn BackoffPolicy> {
        Arc::new(
            ExponentialBackoffBuilder::default()
                .with_initial_delay(Duration::from_nanos(1))
                .with_maximum_delay(Duration::from_nanos(1))
                .clamp(),
        )
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_happy_path() -> Result<()> {
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
            assert_eq!(req.mutation_groups.len(), 3);

            let response1 = mock_v1::BatchWriteResponse {
                indexes: vec![0, 1],
                status: None,
                commit_timestamp: None,
            };
            let response2 = mock_v1::BatchWriteResponse {
                indexes: vec![2],
                status: None,
                commit_timestamp: None,
            };

            Ok(Response::from(adapt([Ok(response1), Ok(response2)])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 1),
            create_test_mutation_group("Users", 2),
            create_test_mutation_group("Users", 3),
        ];

        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response1 = stream
            .next()
            .await
            .expect("stream should yield first message")?;
        assert_eq!(response1.indexes, vec![0, 1]);

        let response2 = stream
            .next()
            .await
            .expect("stream should yield second message")?;
        assert_eq!(response2.indexes, vec![2]);

        assert!(
            stream.next().await.is_none(),
            "stream must conclude after all messages"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_empty_input() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });
        mock.expect_batch_write().never();

        let (db_client, _server) = setup_db_client(mock).await;

        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction
            .execute_streaming(Vec::<MutationGroup>::new())
            .await?;

        assert!(
            stream.next().await.is_none(),
            "empty input must yield None immediately without sending RPC"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_retry_initial_connection() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut sequence = Sequence::new();
        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.mutation_groups.len(),
                    2,
                    "initial attempt must contain 2 mutation groups"
                );
                Err(TonicStatus::unavailable(
                    "Transient connection establishment failure",
                ))
            });

        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.mutation_groups.len(),
                    2,
                    "retry attempt must contain all 2 mutation groups"
                );
                let response = mock_v1::BatchWriteResponse {
                    indexes: vec![0, 1],
                    status: None,
                    commit_timestamp: None,
                };
                Ok(Response::from(adapt([Ok(response)])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 1),
            create_test_mutation_group("Users", 2),
        ];

        let transaction = db_client
            .batch_write_transaction()
            .with_backoff_policy(test_backoff_policy())
            .build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response = stream
            .next()
            .await
            .expect("stream should yield response after retry")?;
        assert_eq!(response.indexes, vec![0, 1]);
        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_retry_midway_stream() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut sequence = Sequence::new();
        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.mutation_groups.len(),
                    4,
                    "attempt 1 must contain all 4 original mutation groups"
                );
                let response = mock_v1::BatchWriteResponse {
                    indexes: vec![0, 2],
                    status: None,
                    commit_timestamp: None,
                };
                let stream = adapt([
                    Ok(response),
                    Err(TonicStatus::unavailable(
                        "Stream dropped after first response",
                    )),
                ]);
                Ok(Response::from(stream))
            });

        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.mutation_groups.len(),
                    2,
                    "attempt 2 must contain strictly the remaining 2 unprocessed mutation groups"
                );
                let response = mock_v1::BatchWriteResponse {
                    indexes: vec![0, 1],
                    status: None,
                    commit_timestamp: None,
                };
                Ok(Response::from(adapt([Ok(response)])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 10),
            create_test_mutation_group("Users", 20),
            create_test_mutation_group("Users", 30),
            create_test_mutation_group("Users", 40),
        ];

        let transaction = db_client
            .batch_write_transaction()
            .with_backoff_policy(test_backoff_policy())
            .build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response1 = stream
            .next()
            .await
            .expect("stream should yield message 1")?;
        assert_eq!(
            response1.indexes,
            vec![0, 2],
            "original indices 0 and 2 must match"
        );

        let response2 = stream
            .next()
            .await
            .expect("stream should yield message 2 after midway retry")?;
        assert_eq!(
            response2.indexes,
            vec![1, 3],
            "retry relative indices 0 and 1 must be translated back to original indices 1 and 3"
        );

        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_multiple_cascading_retries() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut sequence = Sequence::new();
        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(req.mutation_groups.len(), 6);
                let stream = adapt([
                    Ok(mock_v1::BatchWriteResponse {
                        indexes: vec![0],
                        status: None,
                        commit_timestamp: None,
                    }),
                    Err(TonicStatus::unavailable("Drop 1")),
                ]);
                Ok(Response::from(stream))
            });

        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                // Remaining: 1, 2, 3, 4, 5 (len 5)
                assert_eq!(req.mutation_groups.len(), 5);
                let stream = adapt([
                    Ok(mock_v1::BatchWriteResponse {
                        indexes: vec![0, 3], // Corresponds to original 1 and 4
                        status: None,
                        commit_timestamp: None,
                    }),
                    Err(TonicStatus::unavailable("Drop 2")),
                ]);
                Ok(Response::from(stream))
            });

        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                // Remaining: 2, 3, 5 (len 3)
                assert_eq!(req.mutation_groups.len(), 3);
                let stream = adapt([
                    Ok(mock_v1::BatchWriteResponse {
                        indexes: vec![0], // Corresponds to original 2
                        status: None,
                        commit_timestamp: None,
                    }),
                    Err(TonicStatus::unavailable("Drop 3")),
                ]);
                Ok(Response::from(stream))
            });

        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                // Remaining: 3, 5 (len 2)
                assert_eq!(req.mutation_groups.len(), 2);
                let stream = adapt([Ok(mock_v1::BatchWriteResponse {
                    indexes: vec![0, 1], // Corresponds to original 3 and 5
                    status: None,
                    commit_timestamp: None,
                })]);
                Ok(Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = (0..6)
            .map(|i| create_test_mutation_group("Users", i))
            .collect::<Vec<_>>();

        let transaction = db_client
            .batch_write_transaction()
            .with_backoff_policy(test_backoff_policy())
            .build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response1 = stream.next().await.expect("res 1")?;
        assert_eq!(response1.indexes, vec![0]);

        let response2 = stream.next().await.expect("res 2")?;
        assert_eq!(response2.indexes, vec![1, 4]);

        let response3 = stream.next().await.expect("res 3")?;
        assert_eq!(response3.indexes, vec![2]);

        let response4 = stream.next().await.expect("res 4")?;
        assert_eq!(response4.indexes, vec![3, 5]);

        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_non_retryable_initial_error() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().times(1).returning(|_| {
            Err(TonicStatus::permission_denied(
                "Caller does not have spanner.database.write permission",
            ))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![create_test_mutation_group("Users", 1)];
        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let result = stream.next().await;
        assert!(result.is_some(), "stream must yield error");
        let error = result.expect("some").expect_err("must be error");
        let error_message = error.to_string();
        assert!(
            error_message.contains("Caller does not have spanner.database.write permission"),
            "error should contain permission denied message, got: {error_message}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_non_retryable_midway_error() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().times(1).returning(|_| {
            let response = mock_v1::BatchWriteResponse {
                indexes: vec![0],
                status: None,
                commit_timestamp: None,
            };
            let stream = adapt([
                Ok(response),
                Err(TonicStatus::unauthenticated("OAuth access token expired")),
            ]);
            Ok(Response::from(stream))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 1),
            create_test_mutation_group("Users", 2),
        ];
        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response1 = stream
            .next()
            .await
            .expect("should yield message 1 successfully")?;
        assert_eq!(response1.indexes, vec![0]);

        let response2 = stream.next().await;
        assert!(
            response2.is_some(),
            "stream must yield error for second call"
        );
        let error = response2.expect("some").expect_err("must be error");
        assert!(error.to_string().contains("OAuth access token expired"));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_exhausted_retry_limit() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut sequence = Sequence::new();
        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(TonicStatus::unavailable("Unavailable attempt 1")));

        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(TonicStatus::unavailable("Unavailable attempt 2")));

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![create_test_mutation_group("Users", 1)];
        let transaction = db_client
            .batch_write_transaction()
            .with_retry_policy(SpannerRetryPolicy::new().with_attempt_limit(2))
            .with_backoff_policy(test_backoff_policy())
            .build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let result = stream.next().await;
        assert!(result.is_some(), "must yield error after retries exhausted");
        assert!(result.expect("some").is_err());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_handles_aborted_status_in_response() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut sequence = Sequence::new();
        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(req.mutation_groups.len(), 3);
                let aborted_response = mock_v1::BatchWriteResponse {
                    indexes: vec![1],
                    status: Some(ProtoStatus {
                        code: Code::Aborted as i32,
                        message: "Transaction was aborted due to concurrent modification"
                            .to_string(),
                        details: vec![],
                    }),
                    commit_timestamp: None,
                };
                let stream = adapt([
                    Ok(aborted_response),
                    Err(TonicStatus::unavailable("Stream dropped")),
                ]);
                Ok(Response::from(stream))
            });

        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                // The aborted mutation group 1 was reported to caller, so retry must ONLY contain 0 and 2
                assert_eq!(
                    req.mutation_groups.len(),
                    2,
                    "retry must exclude group 1 since its aborted status was already reported"
                );
                let ok_response = mock_v1::BatchWriteResponse {
                    indexes: vec![0, 1],
                    status: None,
                    commit_timestamp: None,
                };
                Ok(Response::from(adapt([Ok(ok_response)])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 10),
            create_test_mutation_group("Users", 20),
            create_test_mutation_group("Users", 30),
        ];

        let transaction = db_client
            .batch_write_transaction()
            .with_backoff_policy(test_backoff_policy())
            .build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response1 = stream.next().await.expect("res 1")?;
        assert_eq!(response1.indexes, vec![1]);
        let status = response1
            .status
            .as_ref()
            .expect("status must be present for aborted group");
        assert_eq!(status.code, Code::Aborted as i32);
        assert!(status.message.contains("Transaction was aborted"));

        let response2 = stream.next().await.expect("res 2")?;
        assert_eq!(response2.indexes, vec![0, 2]);
        assert!(
            response2.status.is_none(),
            "status should be None for successful group"
        );

        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_handles_rejected_mutation_status() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().once().returning(|_| {
            let response1 = mock_v1::BatchWriteResponse {
                indexes: vec![0],
                status: Some(ProtoStatus {
                    code: Code::AlreadyExists as i32,
                    message: "Row with key 1 already exists".to_string(),
                    details: vec![],
                }),
                commit_timestamp: None,
            };
            let response2 = mock_v1::BatchWriteResponse {
                indexes: vec![1],
                status: None,
                commit_timestamp: None,
            };
            Ok(Response::from(adapt([Ok(response1), Ok(response2)])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 1),
            create_test_mutation_group("Users", 2),
        ];

        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response1 = stream.next().await.expect("res 1")?;
        assert_eq!(response1.indexes, vec![0]);
        assert_eq!(
            response1.status.as_ref().expect("status").code,
            Code::AlreadyExists as i32
        );

        let response2 = stream.next().await.expect("res 2")?;
        assert_eq!(response2.indexes, vec![1]);

        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_stream_drop_after_all_processed() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().times(1).returning(|_| {
            let stream = adapt([
                Ok(mock_v1::BatchWriteResponse {
                    indexes: vec![0, 1],
                    status: None,
                    commit_timestamp: None,
                }),
                Err(TonicStatus::unavailable(
                    "Network drop after all responses delivered",
                )),
            ]);
            Ok(Response::from(stream))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 1),
            create_test_mutation_group("Users", 2),
        ];

        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response = stream.next().await.expect("res")?;
        assert_eq!(response.indexes, vec![0, 1]);

        // Since all mutation groups were already acknowledged, the stream concludes with None
        assert!(
            stream.next().await.is_none(),
            "stream must return None without extra retry when all groups are processed"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_premature_stream_eof() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut sequence = Sequence::new();
        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(req.mutation_groups.len(), 3);
                // Delivers group 0 then ends stream prematurely (EOF) without delivering 1 or 2
                Ok(Response::from(adapt([Ok(mock_v1::BatchWriteResponse {
                    indexes: vec![0],
                    status: None,
                    commit_timestamp: None,
                })])))
            });

        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.mutation_groups.len(),
                    2,
                    "retry after premature EOF must send remaining 2 mutation groups"
                );
                Ok(Response::from(adapt([Ok(mock_v1::BatchWriteResponse {
                    indexes: vec![0, 1],
                    status: None,
                    commit_timestamp: None,
                })])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 1),
            create_test_mutation_group("Users", 2),
            create_test_mutation_group("Users", 3),
        ];

        let transaction = db_client
            .batch_write_transaction()
            .with_backoff_policy(test_backoff_policy())
            .build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response1 = stream.next().await.expect("res 1")?;
        assert_eq!(response1.indexes, vec![0]);

        let response2 = stream.next().await.expect("res 2")?;
        assert_eq!(response2.indexes, vec![1, 2]);

        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_premature_stream_eof_exhausted_retries() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut sequence = Sequence::new();
        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(req.mutation_groups.len(), 2);
                Ok(Response::from(adapt([Ok(mock_v1::BatchWriteResponse {
                    indexes: vec![0],
                    status: None,
                    commit_timestamp: None,
                })])))
            });

        mock.expect_batch_write()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(req.mutation_groups.len(), 1);
                // Ends immediately with EOF without sending group 1
                Ok(Response::from(adapt([])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 1),
            create_test_mutation_group("Users", 2),
        ];

        let transaction = db_client
            .batch_write_transaction()
            .with_retry_policy(SpannerRetryPolicy::new().with_attempt_limit(2))
            .with_backoff_policy(test_backoff_policy())
            .build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response1 = stream.next().await.expect("res 1")?;
        assert_eq!(response1.indexes, vec![0]);

        let result = stream.next().await;
        assert!(
            result.is_some(),
            "stream must yield error when retries exhausted on premature EOF"
        );
        let error = result.expect("some").expect_err("must be error");
        assert!(
            error.to_string().contains("closed prematurely"),
            "error should indicate premature close, got: {error}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_drop_before_polling() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });
        mock.expect_batch_write().never();

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![create_test_mutation_group("Users", 1)];

        let transaction = db_client.batch_write_transaction().build();
        let stream = transaction.execute_streaming(groups).await?;

        // Drop stream immediately before any next() call.
        // Because no stream was ever established, no RPC is sent and no phantom attempt metric is recorded.
        drop(stream);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_corrupted_response_index_bounds() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().once().returning(|_| {
            Ok(Response::from(adapt([Ok(mock_v1::BatchWriteResponse {
                indexes: vec![99],
                status: None,
                commit_timestamp: None,
            })])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![create_test_mutation_group("Users", 1)];

        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let result = stream.next().await;
        assert!(result.is_some(), "must yield error for corrupted index");
        let error = result.expect("some").expect_err("must be error");
        assert!(
            error.to_string().contains("out of bounds"),
            "error should indicate index out of bounds, got: {error}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_corrupted_response_negative_index() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().once().returning(|_| {
            Ok(Response::from(adapt([Ok(mock_v1::BatchWriteResponse {
                indexes: vec![-1],
                status: None,
                commit_timestamp: None,
            })])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![create_test_mutation_group("Users", 1)];

        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let result = stream.next().await;
        assert!(result.is_some(), "must yield error for negative index");
        let error = result.expect("some").expect_err("must be error");
        assert!(
            error.to_string().contains("negative mutation group index"),
            "error should indicate negative index, got: {error}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_duplicate_response_indices() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().once().returning(|_| {
            let response1 = mock_v1::BatchWriteResponse {
                indexes: vec![0, 0],
                status: None,
                commit_timestamp: None,
            };
            let response2 = mock_v1::BatchWriteResponse {
                indexes: vec![1],
                status: None,
                commit_timestamp: None,
            };
            Ok(Response::from(adapt([Ok(response1), Ok(response2)])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 1),
            create_test_mutation_group("Users", 2),
        ];

        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response1 = stream.next().await.expect("res 1")?;
        assert_eq!(response1.indexes, vec![0, 0]);

        let response2 = stream.next().await.expect("res 2")?;
        assert_eq!(response2.indexes, vec![1]);

        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_with_request_options_and_gax_options() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().once().returning(|req| {
            let req = req.into_inner();

            let request_options = req
                .request_options
                .as_ref()
                .expect("request_options should be present");
            assert_eq!(request_options.transaction_tag, "my_tag");
            assert_eq!(Priority::from(request_options.priority), Priority::High);
            assert_eq!(request_options.request_tag, "");

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
            .to(1)
            .build();
        let group = MutationGroup::new(vec![mutation]);

        let transaction = db_client
            .batch_write_transaction()
            .set_transaction_tag("my_tag")
            .set_priority(Priority::High)
            .set_exclude_txn_from_change_streams(true)
            .with_attempt_timeout(Duration::from_secs(30))
            .with_retry_policy(SpannerRetryPolicy::new().with_attempt_limit(5))
            .with_backoff_policy(ExponentialBackoffBuilder::default().clamp())
            .build();
        let mut stream = transaction.execute_streaming(vec![group]).await?;

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
    async fn execute_streaming_drop_mid_stream() -> Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_batch_write().once().returning(|_| {
            let response = mock_v1::BatchWriteResponse {
                indexes: vec![0],
                status: None,
                commit_timestamp: None,
            };
            Ok(Response::from(adapt([Ok(response)])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let groups = vec![
            create_test_mutation_group("Users", 1),
            create_test_mutation_group("Users", 2),
        ];

        let transaction = db_client.batch_write_transaction().build();
        let mut stream = transaction.execute_streaming(groups).await?;

        let response = stream.next().await.expect("res")?;
        assert_eq!(response.indexes, vec![0]);

        // Explicitly drop stream before all mutation groups are read. Drop handler records metrics cleanly.
        drop(stream);

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
            .to(1)
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
