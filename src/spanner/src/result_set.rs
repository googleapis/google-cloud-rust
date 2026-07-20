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

use crate::database_client::DatabaseClient;
use crate::error::internal_error;
use crate::google::spanner::v1::{self, PartialResultSet};
use crate::model::ResultSetStats;
use crate::model::result_set_stats::RowCount;
use crate::precommit::PrecommitTokenTracker;
use crate::read_only_transaction::{ReadContextTransactionSelector, TransactionState};
use crate::result_set_metadata::ResultSetMetadata;
use crate::retry_policy::SpannerRetryPolicy;
use crate::row::Row;
use crate::server_streaming::stream::PartialResultSetStream;
use bytes::Bytes;
use gaxi::prost::FromProto;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use std::collections::VecDeque;
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::{sleep, timeout};

#[cfg(feature = "unstable-stream")]
use futures::Stream;

/// `ResultSet` contains the rows of a query result.
///
/// # Example
/// ```
/// # use google_cloud_spanner::result::ResultSet;
/// # use google_cloud_spanner::result::Row;
/// # async fn process_result_set(mut rs: ResultSet) -> Result<(), google_cloud_spanner::Error> {
/// while let Some(row) = rs.next().await {
///     let row: Row = row?;
///     // Process the row
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ResultSet {
    stream: Option<PartialResultSetStream>,
    buffered_values: VecDeque<prost_types::Value>,
    chunked: bool,
    seen_last: bool,
    ready_rows: VecDeque<Row>,
    local_metadata: Option<ResultSetMetadata>,
    stats: Option<ResultSetStats>,
    precommit_token_tracker: PrecommitTokenTracker,
    tokio_handle: Option<Handle>,

    // Fields for retries and buffering of a stream of PartialResultSets.
    client: DatabaseClient,
    session_name: String,
    transaction_tag: Option<String>,
    operation: StreamOperation,
    last_resume_token: Bytes,
    partial_result_sets_buffer: VecDeque<PartialResultSet>,
    safe_to_retry: bool,
    max_buffered_partial_result_sets: usize,
    retry_count: usize,
    transaction_selector: Option<ReadContextTransactionSelector>,
    channel_hint: usize,
    gax_options: GaxRequestOptions,
}

#[derive(Debug, Clone)]
pub(crate) enum StreamOperation {
    Query(crate::model::ExecuteSqlRequest),
    Read(crate::model::ReadRequest),
}

pub(crate) struct ResultSetParams {
    pub stream: PartialResultSetStream,
    pub transaction_selector: Option<ReadContextTransactionSelector>,
    pub precommit_token_tracker: PrecommitTokenTracker,
    pub client: DatabaseClient,
    pub session_name: String,
    pub transaction_tag: Option<String>,
    pub operation: StreamOperation,
    pub channel_hint: usize,
    pub gax_options: GaxRequestOptions,
}

// The maximum number of PartialResultSets to buffer without a resume token.
// Spanner will normally include a resume token with each PartialResultSet.
// This maximum is therefore primarily for safety.
const MAX_BUFFERED_PARTIAL_RESULT_SETS: usize = 10;

impl ResultSet {
    /// Creates a new result set asynchronously, waiting for the first chunk to arrive.
    pub(crate) async fn create(params: ResultSetParams) -> crate::Result<Self> {
        let mut result_set = Self::new(params);
        result_set.init_stream().await?;
        Ok(result_set)
    }

    /// Creates a new result set.
    fn new(params: ResultSetParams) -> Self {
        let ResultSetParams {
            stream,
            transaction_selector,
            precommit_token_tracker,
            client,
            session_name,
            transaction_tag,
            operation,
            channel_hint,
            gax_options,
        } = params;

        let gax_options = Self::apply_defaults(gax_options);

        Self {
            stream: Some(stream),
            buffered_values: VecDeque::new(),
            chunked: false,
            seen_last: false,
            ready_rows: VecDeque::new(),
            local_metadata: None,
            stats: None,
            precommit_token_tracker,
            client,
            session_name,
            transaction_tag,
            operation,
            last_resume_token: Bytes::new(),
            partial_result_sets_buffer: VecDeque::new(),
            safe_to_retry: true,
            max_buffered_partial_result_sets: MAX_BUFFERED_PARTIAL_RESULT_SETS,
            retry_count: 0,
            transaction_selector,
            channel_hint,
            gax_options,
            tokio_handle: Handle::try_current().ok(),
        }
    }

    fn apply_defaults(mut gax_options: GaxRequestOptions) -> GaxRequestOptions {
        if gax_options.retry_policy().is_none() {
            gax_options.set_retry_policy(SpannerRetryPolicy::new().with_attempt_limit(10));
        }
        if gax_options.backoff_policy().is_none() {
            gax_options.set_backoff_policy(Self::default_backoff_policy());
        }
        gax_options
    }

    fn default_backoff_policy() -> Arc<dyn BackoffPolicy> {
        Arc::new(ExponentialBackoffBuilder::default().clamp())
    }

    async fn init_stream(&mut self) -> crate::Result<()> {
        // We loop here because if an initial stream failure occurs and is retriable (e.g., UNAVAILABLE),
        // we restart the stream and retry fetching the initial chunk.
        loop {
            let stream_result = match &mut self.stream {
                Some(s) => s.next_message().await,
                None => {
                    return Err(internal_error(
                        "Query stream ended without metadata or error",
                    ));
                }
            };

            match stream_result {
                Some(Ok(partial_result_set)) => {
                    self.handle_partial_result_set(partial_result_set)?;
                    return Ok(());
                }
                Some(Err(e)) => {
                    self.handle_stream_error(e).await?;
                }
                None => {
                    return Err(internal_error(
                        "Query stream ended without metadata or error",
                    ));
                }
            }
        }
    }

    /// Returns the metadata of the result set.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::result::ResultSet;
    /// # use google_cloud_spanner::result::Row;
    /// # async fn fetch_metadata(mut rs: ResultSet) -> Result<(), Box<dyn std::error::Error>> {
    /// if let Some(metadata) = rs.metadata() {
    ///     for column in metadata.column_names() {
    ///         println!("Column name: {}", column);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn metadata(&self) -> Option<&ResultSetMetadata> {
        self.local_metadata.as_ref()
    }

    /// Returns the stats of the result set, if available.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::result::ResultSet;
    /// # use google_cloud_spanner::result::Row;
    /// # async fn process_stats(mut rs: ResultSet) -> Result<(), google_cloud_spanner::Error> {
    /// while let Some(row) = rs.next().await {
    ///     let row = row?;
    ///     // Process row
    /// }
    /// if let Some(stats) = rs.stats() {
    ///     println!("Query plan: {:?}", stats.query_plan);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Stats are only available after the results have been fully consumed
    /// and the query was run in PLAN or PROFILE mode.
    pub fn stats(&self) -> Option<&ResultSetStats> {
        self.stats.as_ref()
    }

    /// Returns the number of rows modified by the DML statement, if available.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::DatabaseClient;
    /// # use google_cloud_spanner::result::ResultSet;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn check_update_count(db_client: &DatabaseClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let runner = db_client.read_write_transaction().build().await?;
    /// runner.run(async |tx| {
    ///     let stmt = Statement::builder("UPDATE Singers SET LastName = 'Simpson' WHERE SingerId = @id THEN RETURN SingerId, LastName")
    ///         .add_param("id", &123_i64)
    ///         .build();
    ///     let mut rs = tx.execute_query(stmt).await?;
    ///     while let Some(row) = rs.next().await.transpose()? {
    ///         // Process returned rows
    ///     }
    ///     if let Some(count) = rs.update_count() {
    ///         println!("Rows modified: {}", count);
    ///     }
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Returns the number of rows modified when this [`ResultSet`] was produced from a
    /// DML statement with a `THEN RETURN` clause.
    pub fn update_count(&self) -> Option<i64> {
        self.stats.as_ref().and_then(|s| {
            s.row_count.as_ref().map(|rc| match rc {
                RowCount::RowCountExact(c) => *c,
                RowCount::RowCountLowerBound(c) => *c,
            })
        })
    }

    /// Fetches the next row from the result set.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::result::ResultSet;
    /// # use google_cloud_spanner::result::Row;
    /// # async fn fetch_next(mut rs: ResultSet) -> Result<(), google_cloud_spanner::Error> {
    /// if let Some(row) = rs.next().await.transpose()? {
    ///     // Process the row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Returns `None` when all rows have been retrieved.
    pub async fn next(&mut self) -> Option<crate::Result<Row>> {
        loop {
            if let Some(row) = self.ready_rows.pop_front() {
                return Some(Ok(row));
            }

            if self.seen_last {
                if let Some(handle) = &self.tokio_handle
                    && let Some(s) = self.stream.take()
                {
                    drain_stream_in_background(handle, s);
                }
                return None;
            }

            let stream_result = match &mut self.stream {
                Some(s) => s.next_message().await,
                None => return None,
            };

            match stream_result {
                Some(Ok(partial_result_set)) => {
                    if let Err(e) = self.handle_partial_result_set(partial_result_set) {
                        return Some(Err(e));
                    }
                }
                Some(Err(e)) => {
                    if let Err(err) = self.handle_stream_error(e).await {
                        return Some(Err(err));
                    }
                }
                None => match self.handle_stream_end() {
                    Ok(Some(row)) => return Some(Ok(row)),
                    Ok(None) => return None,
                    Err(e) => return Some(Err(e)),
                },
            }
        }
    }

    /// Converts the [`ResultSet`] into a [`Stream`].
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_spanner::result::ResultSet;
    /// # use futures::TryStreamExt;
    /// # use std::future::ready;
    /// # async fn example(result_set: ResultSet) -> Result<(), google_cloud_spanner::Error> {
    /// let rows: Vec<_> = result_set
    ///     .into_stream()
    ///     .try_filter(|row| {
    ///         let id = row.get::<String, _>("Id");
    ///         ready(id == "id1")
    ///     })
    ///     .try_collect()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This consumes the [`ResultSet`] and returns a stream of rows.
    #[cfg(feature = "unstable-stream")]
    pub fn into_stream(self) -> impl Stream<Item = crate::Result<Row>> + Unpin {
        use futures::stream::unfold;
        Box::pin(unfold(self, |mut result_set| async move {
            result_set.next().await.map(|row| (row, result_set))
        }))
    }
}

impl ResultSet {
    fn handle_partial_result_set(
        &mut self,
        mut partial_result_set: PartialResultSet,
    ) -> crate::Result<()> {
        self.precommit_token_tracker.update(
            partial_result_set
                .precommit_token
                .clone()
                .map(|t| t.cnv().expect("failed to convert precommit token")),
        );

        if partial_result_set.last {
            self.seen_last = true;
        }

        match (
            self.local_metadata.as_ref(),
            partial_result_set.metadata.take(),
        ) {
            (Some(_), None) => {}
            (None, None) => {
                return Err(internal_error(
                    "First PartialResultSet did not contain metadata",
                ));
            }
            (Some(_), Some(_)) => {
                return Err(internal_error("Additional metadata after first result set"));
            }
            (None, Some(m)) => {
                self.handle_metadata(m)?;
            }
        }

        // Keep track of the last resume_token that we see to be able to resume the stream
        // in case of a transient error. Most PartialResultSets will have a resume token,
        // but the API contract is not explicitly guaranteeing that each of them will have
        // one.
        if !partial_result_set.resume_token.is_empty() {
            self.last_resume_token = partial_result_set.resume_token.clone();
            self.safe_to_retry = true;
            self.partial_result_sets_buffer
                .push_back(partial_result_set);
            self.flush_buffer()?;
            return Ok(());
        }

        // The PartialResultSet did not have a resume_token. Buffer the result
        // and continue with the next PartialResultSet, unless the buffer is full.
        if self.partial_result_sets_buffer.len() >= self.max_buffered_partial_result_sets {
            // Mark this stream as 'unsafe to retry', meaning that any transient error
            // that we see will not be retried. We will instead propagate the error.
            self.safe_to_retry = false;
            if let Some(oldest) = self.partial_result_sets_buffer.pop_front() {
                self.process_partial_result_set(oldest)?;
            }
        }
        self.partial_result_sets_buffer
            .push_back(partial_result_set);

        if self.seen_last {
            self.flush_buffer()?;
            if self.chunked {
                return Err(crate::error::internal_error(
                    "Stream ended with chunked_value=true",
                ));
            }
        }

        Ok(())
    }

    fn handle_metadata(&mut self, mut m: v1::ResultSetMetadata) -> crate::Result<()> {
        let transaction = m.transaction.take();
        let meta = ResultSetMetadata::new(Some(m));
        if let Some(selector) = &self.transaction_selector {
            if let Some(transaction) = transaction {
                selector.update(
                    transaction.id,
                    transaction
                        .read_timestamp
                        .and_then(|t| wkt::Timestamp::new(t.seconds, t.nanos).ok()),
                )?;
            } else if let ReadContextTransactionSelector::Lazy(lazy) = selector {
                let is_started = matches!(
                    &*lazy.lock().expect("transaction state mutex poisoned"),
                    TransactionState::Started(_, _)
                );
                if !is_started {
                    return Err(internal_error(
                        "Spanner failed to return a transaction ID for a query that included a BeginTransaction option",
                    ));
                }
            }
        }
        self.local_metadata = Some(meta);
        Ok(())
    }
    async fn handle_stream_error(&mut self, e: crate::Error) -> crate::Result<()> {
        let mut e = e;
        if self.safe_to_retry {
            match self.check_retry(e) {
                Ok(()) => {
                    self.retry_count += 1;
                    // Clear the buffer and restart the stream using the last
                    // resume_token that we have seen.
                    self.partial_result_sets_buffer.clear();

                    // Apply backoff delay if policy is present
                    if let Some(policy) = self.gax_options.backoff_policy() {
                        let state = RetryState::new(self.safe_to_retry)
                            .set_attempt_count(self.retry_count as u32);
                        let delay = policy.on_failure(&state);
                        sleep(delay).await;
                    }

                    self.restart_stream().await?;
                    return Ok(());
                }
                Err(err) => {
                    e = err;
                }
            }
        }

        // Check if this stream included an inlined BeginTransaction option
        // and has not yet returned a transaction ID. If so, we explicitly
        // begin the transaction and restart the stream.
        let Some(ReadContextTransactionSelector::Lazy(lazy)) = &self.transaction_selector else {
            return Err(e);
        };
        let is_started = matches!(
            &*lazy.lock().unwrap(),
            crate::read_only_transaction::TransactionState::Started(_, _)
        );
        if is_started {
            return Err(e);
        }

        self.transaction_selector
            .as_ref()
            .unwrap()
            .begin_explicitly(crate::read_only_transaction::ExplicitBeginParams {
                client: self.client.clone(),
                session_name: self.session_name.clone(),
                transaction_tag: self.transaction_tag.clone(),
                channel_hint: self.channel_hint,
                request_options: self.gax_options.clone(),
                is_stream_fallback: true,
                precommit_token_tracker: self.precommit_token_tracker.clone(),
                mutation_key: None,
            })
            .await?;

        self.partial_result_sets_buffer.clear();
        self.restart_stream().await?;
        Ok(())
    }

    fn handle_stream_end(&mut self) -> crate::Result<Option<Row>> {
        // We are at the end of the stream. Return any buffered rows as long
        // as there are any. If there are no buffered rows, return None.

        // First flush any PartialResultSets that we had received without a resume_token.
        if !self.partial_result_sets_buffer.is_empty() {
            self.flush_buffer()?;
        }
        if self.chunked {
            // This should never happen.
            return Err(crate::error::internal_error(
                "Stream ended with chunked_value=true",
            ));
        }
        if let Some(row) = self.ready_rows.pop_front() {
            return Ok(Some(row));
        }
        Ok(None)
    }

    fn flush_buffer(&mut self) -> crate::Result<()> {
        let mut buffer_to_flush = take(&mut self.partial_result_sets_buffer);
        while let Some(partial_result_set) = buffer_to_flush.pop_front() {
            self.process_partial_result_set(partial_result_set)?;
        }
        Ok(())
    }

    fn process_partial_result_set(
        &mut self,
        partial_result_set: PartialResultSet,
    ) -> crate::Result<()> {
        let PartialResultSet {
            stats,
            values,
            chunked_value,
            ..
        } = partial_result_set;

        match (&self.stats, stats) {
            (Some(_), Some(_)) => {
                return Err(internal_error("Additional stats received after first"));
            }
            (None, Some(s)) => {
                let converted_stats = s
                    .cnv()
                    .map_err(|e| internal_error(format!("failed to convert stats: {}", e)))?;
                self.stats = Some(converted_stats);
            }
            _ => {}
        }

        if values.is_empty() {
            return Ok(());
        }
        let metadata = self.local_metadata.as_ref().ok_or_else(|| {
            internal_error("PartialResultSet contained values but no metadata was provided")
        })?;
        if metadata.column_types.is_empty() {
            return Err(internal_error(
                "PartialResultSet contained values but no column metadata was provided",
            ));
        }

        let mut values_iter = values.into_iter();
        if self.chunked
            && let Some(last_val) = self.buffered_values.back_mut()
            && let Some(first_new) = values_iter.next()
        {
            merge_values(last_val, first_new)?;
        }

        self.buffered_values.extend(values_iter);
        self.chunked = chunked_value;

        let column_count = metadata.column_types.len();
        self.ready_rows
            .reserve(self.buffered_values.len() / column_count);
        while self.buffered_values.len() >= column_count {
            if self.buffered_values.len() == column_count && self.chunked {
                break;
            }

            let row_values: Vec<crate::value::Value> = self
                .buffered_values
                .drain(..column_count)
                .map(crate::value::Value)
                .collect();
            self.ready_rows.push_back(Row {
                values: row_values,
                metadata: metadata.clone(),
            });
        }
        Ok(())
    }

    async fn restart_stream(&mut self) -> crate::Result<()> {
        // If we are restarting the stream (due to a failure), and the transaction
        // was in the process of starting (but failed before yielding an ID),
        // reset the state so the retry attempt can include the begin option again.
        if let Some(s) = &self.transaction_selector {
            s.maybe_reset_starting();
        }

        // Get the latest transaction selector for this transaction.
        let transaction_selector = if let Some(s) = &self.transaction_selector {
            Some(s.selector().await?)
        } else {
            None
        };

        // If we are restarting the stream from the beginning (because no resume token
        // was received prior to the transient failure), we clear our local metadata state.
        // This ensures that when Spanner transmits the initial metadata chunk on the retried stream,
        // it is extracted without triggering the 'only-once' metadata validation error.
        if self.last_resume_token.is_empty() {
            self.local_metadata = None;
        }

        match &mut self.operation {
            StreamOperation::Query(req) => {
                req.resume_token = self.last_resume_token.clone();
                req.transaction = transaction_selector
                    .clone()
                    .or_else(|| req.transaction.take());
                let stream = self
                    .client
                    .spanner
                    .execute_streaming_sql(req.clone(), self.gax_options.clone(), self.channel_hint)
                    .send()
                    .await?;
                self.stream = Some(stream);
            }
            StreamOperation::Read(req) => {
                req.resume_token = self.last_resume_token.clone();
                req.transaction = transaction_selector
                    .clone()
                    .or_else(|| req.transaction.take());
                let stream = self
                    .client
                    .spanner
                    .streaming_read(req.clone(), self.gax_options.clone(), self.channel_hint)
                    .send()
                    .await?;
                self.stream = Some(stream);
            }
        }
        Ok(())
    }

    fn check_retry(&self, e: crate::Error) -> Result<(), crate::Error> {
        if let Some(policy) = self.gax_options.retry_policy() {
            let state =
                RetryState::new(self.safe_to_retry).set_attempt_count(self.retry_count as u32);

            match policy.on_error(&state, e) {
                RetryResult::Continue(_) => return Ok(()),
                RetryResult::Permanent(err) | RetryResult::Exhausted(err) => return Err(err),
            }
        }
        Err(e)
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        // If the query stream has finished sending all chunks (seen_last is true), but
        // the client hasn't read the trailers/EOF yet, dropping the stream receiver
        // would cause tonic to send an HTTP/2 RST_STREAM.
        // If an application often executes a query that it knows only returns one or a
        // few rows, and the application stops reading after that many rows, then these
        // stream resets could trigger GFE/frontend security protection (too_many_internal_resets).
        // To prevent this, we drain the remaining trailers asynchronously in a background task.
        // Note: We only do this if seen_last is true, to prevent a background task from potentially
        // iterating through a large number of partial results.
        if self.seen_last
            && let Some(handle) = &self.tokio_handle
            && let Some(s) = self.stream.take()
        {
            drain_stream_in_background(handle, s);
        }
    }
}

fn drain_stream_in_background(handle: &Handle, mut stream: PartialResultSetStream) {
    handle.spawn(async move {
        let _ = timeout(Duration::from_secs(5), async move {
            while let Some(Ok(_)) = stream.next_message().await {}
        })
        .await;
    });
}

/// Merges two values from successive `PartialResultSet`s into a single value.
///
/// Cloud Spanner can return a single logical row or column value split across multiple
/// `PartialResultSet` messages. This occurs when a value (especially large strings or
/// arrays) exceeds the message size limits of the underlying stream. In these cases,
/// the `chunked_value` flag is set on the first `PartialResultSet`, indicating that the
/// final value in the message's `values` array is incomplete and must be combined with
/// the first value in the `values` array of the subsequent `PartialResultSet`.
///
/// This function handles the concatenation of split `StringValue` and `ListValue` types.
fn merge_values(target: &mut prost_types::Value, source: prost_types::Value) -> crate::Result<()> {
    use prost_types::value::Kind;
    match (&mut target.kind, source.kind) {
        (Some(Kind::StringValue(s)), Some(Kind::StringValue(source_s))) => {
            s.push_str(&source_s);
            Ok(())
        }
        (Some(Kind::ListValue(target_list)), Some(Kind::ListValue(mut source_list))) => {
            if source_list.values.is_empty() {
                return Ok(());
            }
            if target_list.values.is_empty() {
                target_list.values = source_list.values;
                return Ok(());
            }

            let source_first = source_list.values.remove(0);
            if let Some(target_last) = target_list.values.last_mut() {
                match (&target_last.kind, &source_first.kind) {
                    (Some(Kind::StringValue(_)), Some(Kind::StringValue(_)))
                    | (Some(Kind::ListValue(_)), Some(Kind::ListValue(_))) => {
                        merge_values(target_last, source_first)?;
                    }
                    _ => {
                        target_list.values.push(source_first);
                    }
                }
            } else {
                target_list.values.push(source_first);
            }
            target_list.values.extend(source_list.values);
            Ok(())
        }
        // This is not expected to happen and indicates that Spanner returned data that
        // violates the contract. In this case we return a service error with error code
        // Internal.
        _ => Err(internal_error(
            "Incompatible types for merging chunked values",
        )),
    }
}

#[cfg(test)]
impl ResultSet {
    pub(crate) fn set_max_buffered_partial_result_sets(&mut self, limit: usize) {
        self.max_buffered_partial_result_sets = limit;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::client::Spanner;
    use crate::key::KeySet;
    use crate::read::ReadRequest;
    use crate::statement::Statement;
    use crate::transaction::BeginTransactionOption;
    use gaxi::grpc::tonic::{Code as GrpcCode, MetadataMap, Response, Status};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_test_macros::tokio_test_no_panics;
    use prost_types::Value;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1 as spanner_v1;
    use spanner_grpc_mock::google::spanner::v1::struct_type::Field;
    use spanner_grpc_mock::google::spanner::v1::{
        MultiplexedSessionPrecommitToken, PartialResultSet, ResultSetMetadata, Session, StructType,
    };
    use spanner_grpc_mock::start;
    use spanner_v1::result_set_stats::RowCount;
    use std::time::Duration;

    mockall::mock! {
        #[derive(Debug)]
        BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> Duration;
        }
    }

    pub(crate) fn string_val(s: &str) -> Value {
        Value {
            kind: Some(prost_types::value::Kind::StringValue(s.to_string())),
        }
    }

    fn list_val(vals: Vec<Value>) -> Value {
        Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue { values: vals },
            )),
        }
    }

    fn metadata(cols: usize) -> Option<ResultSetMetadata> {
        let mut fields = vec![];
        for i in 0..cols {
            fields.push(Field {
                name: format!("col{}", i),
                r#type: None,
            });
        }
        Some(ResultSetMetadata {
            row_type: Some(StructType { fields }),
            transaction: None,
            undeclared_parameters: None,
        })
    }

    pub(crate) fn adapt<I, T>(items: I) -> tokio::sync::mpsc::Receiver<T>
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        let items = items.into_iter();
        let (tx, rx) = tokio::sync::mpsc::channel(items.len().max(1));
        for i in items {
            tx.try_send(i)
                .expect("can't fail, we allocated enough capacity.");
        }
        rx
    }

    async fn run_mock_query(results: Vec<PartialResultSet>) -> ResultSet {
        run_mock_query_fallible(results).await.unwrap()
    }

    async fn run_mock_query_fallible(results: Vec<PartialResultSet>) -> crate::Result<ResultSet> {
        let mut mock = MockSpanner::new();
        let rx = adapt(results.into_iter().map(Ok));
        mock.expect_execute_streaming_sql()
            .return_once(move |_request| Ok(Response::from(rx)));

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock)
            .await
            .expect("Failed to start mock server");

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client: crate::database_client::DatabaseClient =
            client.database_client("db").build().await.unwrap();
        let tx: crate::read_only_transaction::SingleUseReadOnlyTransaction =
            db_client.single_use().build();
        tx.execute_query("SELECT 1").await
    }

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(ResultSet: std::fmt::Debug, Send, Sync);
    }

    #[tokio_test_no_panics]
    async fn test_result_set_zero_rows() {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            values: vec![],
            chunked_value: false,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            last: true,
            cache_update: None,
        }])
        .await;

        let next = rs.next().await;
        assert!(next.is_none());
    }

    #[tokio_test_no_panics]
    async fn test_result_set_metadata() -> anyhow::Result<()> {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            values: vec![string_val("a"), string_val("b")],
            last: true,
            ..Default::default()
        }])
        .await;

        // Called before next() -> metadata is immediately available.
        let meta = rs.metadata().expect("metadata available");
        assert_eq!(
            meta.column_names(),
            &["col0".to_string(), "col1".to_string()]
        );

        // Advance
        let _next = rs.next().await.expect("Expected a row")?;

        // Called after next() -> returns metadata
        let meta = rs.metadata().expect("metadata available");
        assert_eq!(
            meta.column_names(),
            &["col0".to_string(), "col1".to_string()]
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_handle_partial_result_set_error() -> anyhow::Result<()> {
        let res = run_mock_query_fallible(vec![PartialResultSet {
            values: vec![string_val("row1")],
            ..Default::default()
        }])
        .await;

        assert!(res.is_err(), "Expected an error but got Ok");
        let err_str = res.expect_err("Expected should be an error").to_string();
        assert!(
            err_str.contains("First PartialResultSet did not contain metadata"),
            "Expected error to contain 'First PartialResultSet did not contain metadata', but got '{}'",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_handle_partial_result_set_error_immediate() -> anyhow::Result<()> {
        let res = run_mock_query_fallible(vec![
            PartialResultSet {
                values: vec![string_val("row1")],
                ..Default::default()
            },
            PartialResultSet {
                resume_token: b"token".to_vec(),
                ..Default::default()
            },
        ])
        .await;

        assert!(res.is_err(), "Expected an error but got Ok");
        let err_str = res.expect_err("Expected should be an error").to_string();
        assert!(
            err_str.contains("First PartialResultSet did not contain metadata"),
            "Expected error to contain 'First PartialResultSet did not contain metadata', but got '{}'",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_stream_ended_with_chunked_value() -> anyhow::Result<()> {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            values: vec![string_val("a")],
            chunked_value: true,
            ..Default::default()
        }])
        .await;

        let res = rs.next().await;
        assert!(res.is_some(), "Expected an error but got None");
        let res = res.expect("Expected some response but got None");
        assert!(res.is_err(), "Expected an error but got Ok");
        let err_str = res.expect_err("Expected should be an error").to_string();
        assert!(
            err_str.contains("Stream ended with chunked_value=true"),
            "Expected error to contain 'Stream ended with chunked_value=true', but got '{}'",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_duplicate_metadata() -> anyhow::Result<()> {
        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(2),
                values: vec![string_val("a"), string_val("b")],
                resume_token: b"token1".to_vec(),
                ..Default::default()
            },
            PartialResultSet {
                metadata: metadata(2),
                values: vec![string_val("c"), string_val("d")],
                ..Default::default()
            },
        ])
        .await;

        rs.next().await.expect("Expected a row")?;

        let res2 = rs.next().await;
        assert!(res2.is_some(), "Expected an error but got None");
        let res2 = res2.expect("Expected some response but got None");
        assert!(res2.is_err(), "Expected an error but got Ok");
        let err_str = res2.expect_err("Expected should be an error").to_string();
        assert!(
            err_str.contains("Additional metadata after first result set"),
            "Expected error to contain 'Additional metadata after first result set', but got '{}'",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_empty_column_metadata() -> anyhow::Result<()> {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: Some(ResultSetMetadata {
                row_type: Some(StructType { fields: vec![] }),
                ..Default::default()
            }),
            values: vec![string_val("a")],
            ..Default::default()
        }])
        .await;

        let res = rs.next().await;
        assert!(res.is_some(), "Expected an error but got None");
        let res = res.expect("Expected some response but got None");
        assert!(res.is_err(), "Expected an error but got Ok");
        let err_str = res.expect_err("Expected should be an error").to_string();
        assert!(
            err_str
                .contains("PartialResultSet contained values but no column metadata was provided"),
            "Expected error to contain 'PartialResultSet contained values but no column metadata was provided', but got '{}'",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_default_policies_applied() -> anyhow::Result<()> {
        let rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            last: true,
            ..Default::default()
        }])
        .await;

        assert!(
            rs.gax_options.retry_policy().is_some(),
            "Default retry policy should be applied"
        );
        assert!(
            rs.gax_options.backoff_policy().is_some(),
            "Default backoff policy should be applied"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_retry_read_stream() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([
                    Ok(PartialResultSet {
                        metadata: metadata(2),
                        values: vec![string_val("row1"), string_val("b")],
                        resume_token: b"token1".to_vec(),
                        ..Default::default()
                    }),
                    Err(Status::unavailable("Unavailable error")),
                ]);
                Ok(Response::from(stream))
            });

        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([Ok(PartialResultSet {
                    values: vec![string_val("row2"), string_val("d")],
                    resume_token: b"token2".to_vec(),
                    last: true,
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();
        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .returning(|_| Duration::from_nanos(1));

        let read_req = crate::read::ReadRequest::builder("table", vec!["Id", "Value"])
            .with_keys(crate::key::KeySet::all())
            .with_backoff_policy(mock_backoff)
            .build();
        let mut rs: ResultSet = tx.execute_read(read_req).await?;

        let row1 = rs.next().await.expect("Stream ended unexpectedly")?;
        assert_eq!(row1.raw_values()[0].0, string_val("row1"));

        let row2 = rs.next().await.expect("Stream ended unexpectedly")?;
        assert_eq!(row2.raw_values()[0].0, string_val("row2"));

        assert!(rs.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_custom_retry_policy() -> anyhow::Result<()> {
        // Extend the default retry policy to also retry on ResourceExhausted.
        let retry_policy = Aip194Strict.continue_on_too_many_requests();

        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // Fail with RESOURCE_EXHAUSTED on first call
        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([
                    Ok(PartialResultSet {
                        metadata: metadata(2),
                        values: vec![string_val("row1"), string_val("b")],
                        resume_token: b"token1".to_vec(),
                        ..Default::default()
                    }),
                    Err(Status::new(GrpcCode::ResourceExhausted, "Quota exceeded")),
                ]);
                Ok(Response::from(stream))
            });

        // Succeed on second call
        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([Ok(PartialResultSet {
                    values: vec![string_val("row2"), string_val("d")],
                    resume_token: b"token2".to_vec(),
                    last: true,
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .times(1)
            .returning(|_| Duration::from_nanos(1));

        let read_req = ReadRequest::builder("table", vec!["Id", "Value"])
            .with_keys(KeySet::all())
            .with_retry_policy(retry_policy)
            .with_backoff_policy(mock_backoff)
            .build();

        let mut rs: ResultSet = tx.execute_read(read_req).await?;

        let row1 = rs.next().await.expect("Stream ended unexpectedly")?;
        assert_eq!(row1.raw_values()[0].0, string_val("row1"));

        // This next() call should trigger the retry because the previous stream ended with error!
        let row2 = rs.next().await.expect("Stream ended unexpectedly")?;
        assert_eq!(row2.raw_values()[0].0, string_val("row2"));

        assert!(rs.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_transport_error_retry() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // Fail with transport error in the middle of stream on first call
        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let mut status = Status::unavailable("connection reset");
                let mut headers = std::mem::take(status.metadata_mut()).into_headers();
                headers.insert("content-type", http::HeaderValue::from_static("text/html"));
                *status.metadata_mut() = MetadataMap::from_headers(headers);
                let stream = adapt([
                    Ok(PartialResultSet {
                        metadata: metadata(2),
                        values: vec![string_val("row1"), string_val("b")],
                        resume_token: b"token1".to_vec(),
                        ..Default::default()
                    }),
                    Err(status),
                ]);
                Ok(Response::from(stream))
            });

        // Succeed on second call
        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([Ok(PartialResultSet {
                    values: vec![string_val("row2"), string_val("d")],
                    resume_token: b"token2".to_vec(),
                    last: true,
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .times(1)
            .returning(|_| Duration::from_nanos(1));

        let read_req = ReadRequest::builder("table", vec!["Id", "Value"])
            .with_keys(KeySet::all())
            .with_backoff_policy(mock_backoff)
            .build();

        let mut rs: ResultSet = tx.execute_read(read_req).await?;

        let row1 = rs.next().await.expect("Stream ended unexpectedly")?;
        assert_eq!(
            row1.raw_values()[0].0,
            string_val("row1"),
            "Expected row1 to be read successfully before transport error"
        );

        // This next() call should trigger the retry because the previous stream ended with a transport error.
        let row2 = rs.next().await.expect("Stream ended unexpectedly")?;
        assert_eq!(
            row2.raw_values()[0].0,
            string_val("row2"),
            "Expected stream to resume and return row2 after transport error retry"
        );

        assert!(
            rs.next().await.is_none(),
            "Expected stream to end successfully"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_one_row() {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            values: vec![string_val("a"), string_val("b")],
            chunked_value: false,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            last: true,
            cache_update: None,
        }])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values().len(), 2);
        assert_eq!(row.raw_values()[0].0, string_val("a"));
        assert_eq!(row.raw_values()[1].0, string_val("b"));

        assert!(rs.next().await.is_none());
    }

    #[tokio_test_no_panics]
    async fn result_set_last_flag() -> anyhow::Result<()> {
        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(2),
                values: vec![string_val("a"), string_val("b")],
                last: true,
                ..Default::default()
            },
            // Note: This is not something that would be returned by Spanner.
            // Once a PartialResultSet with last == true is returned, no more
            // PartialResultSets will be returned by Spanner.
            PartialResultSet {
                values: vec![string_val("c"), string_val("d")],
                ..Default::default()
            },
        ])
        .await;

        let row = rs.next().await.expect("Expected a row")?;
        assert_eq!(row.raw_values()[0].0, string_val("a"));

        // Verify that the second PartialResultSet is ignored.
        assert!(rs.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_last_flag_drained_in_background() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        mock.expect_execute_streaming_sql()
            .return_once(move |_request| Ok(Response::from(rx)));

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx_single = db_client.single_use().build();

        // 1. Send the first message with last: true
        tx.send(Ok(PartialResultSet {
            metadata: metadata(2),
            values: vec![string_val("a"), string_val("b")],
            last: true,
            ..Default::default()
        }))
        .await
        .expect("Failed to send first message");

        let mut rs: ResultSet = tx_single.execute_query("SELECT 1").await?;

        // 2. Consume the first message
        let row = rs.next().await.expect("Expected a row")?;
        assert_eq!(row.raw_values()[0].0, string_val("a"));

        // 3. Call next again. It should see seen_last and return None early.
        assert!(rs.next().await.is_none());
        drop(rs);

        // 4. Since the stream is being drained in a background task, the connection
        // receiver should still be alive, and therefore tx should NOT be closed yet.
        assert!(
            !tx.is_closed(),
            "Expected stream to remain open in background task for draining"
        );

        // 5. Drop the sender to close the stream.
        drop(tx);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_last_flag_drained_in_background_on_drop() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        mock.expect_execute_streaming_sql()
            .return_once(move |_request| Ok(Response::from(rx)));

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx_single = db_client.single_use().build();

        // 1. Send the first message with last: true
        tx.send(Ok(PartialResultSet {
            metadata: metadata(2),
            values: vec![string_val("a"), string_val("b")],
            last: true,
            ..Default::default()
        }))
        .await
        .expect("Failed to send first message");

        let mut result_set: ResultSet = tx_single.execute_query("SELECT 1").await?;

        // 2. Consume the first message
        let row = result_set.next().await.expect("Expected a row")?;
        assert_eq!(row.raw_values()[0].0, string_val("a"));

        // 3. Drop result_set early (without calling next() to get None).
        // Since we got a message with last: true, seen_last is true.
        // It should spawn a background task on drop.
        drop(result_set);

        // 4. Since the stream is being drained in a background task, the connection
        // receiver should still be alive, and therefore tx should NOT be closed yet.
        assert!(
            !tx.is_closed(),
            "Expected stream to remain open in background task for draining"
        );

        // 5. Drop the sender to close the stream.
        drop(tx);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_last_flag_drained_in_background_on_drop_outside_runtime()
    -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        mock.expect_execute_streaming_sql()
            .return_once(move |_request| Ok(Response::from(rx)));

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx_single = db_client.single_use().build();

        // 1. Send the first message with last: true
        tx.send(Ok(PartialResultSet {
            metadata: metadata(2),
            values: vec![string_val("a"), string_val("b")],
            last: true,
            ..Default::default()
        }))
        .await
        .expect("Failed to send first message");

        let mut result_set: ResultSet = tx_single.execute_query("SELECT 1").await?;

        // 2. Consume the first message
        let row = result_set.next().await.expect("Expected a row")?;
        assert_eq!(row.raw_values()[0].0, string_val("a"));

        // 3. Move the ResultSet to a separate non-Tokio OS thread and drop it there.
        std::thread::spawn(move || {
            drop(result_set);
        })
        .join()
        .expect("Thread panicked");

        // 4. Since the stream is being drained in a background task on the captured runtime,
        // the connection receiver should still be alive, and therefore tx should NOT be closed yet.
        assert!(
            !tx.is_closed(),
            "Expected stream to remain open in background task for draining when dropped outside runtime"
        );

        // 5. Drop the sender to close the stream.
        drop(tx);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_chunked_values_string() {
        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![string_val("hello ")],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                last: false,
                cache_update: None,
            },
            PartialResultSet {
                metadata: None,
                values: vec![string_val("world")],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                last: true,
                cache_update: None,
            },
        ])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values().len(), 1);
        if let Some(prost_types::value::Kind::StringValue(ref s)) = row.raw_values()[0].0.kind {
            assert_eq!(s, "hello world");
        } else {
            panic!("Expected StringValue");
        }
        assert!(rs.next().await.is_none());
    }

    #[tokio_test_no_panics]
    async fn test_result_set_chunked_values_list() {
        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![list_val(vec![string_val("A")])],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                last: false,
                cache_update: None,
            },
            PartialResultSet {
                metadata: None,
                values: vec![list_val(vec![string_val("B")])],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                last: true,
                cache_update: None,
            },
        ])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values().len(), 1);
        if let Some(prost_types::value::Kind::ListValue(ref l)) = row.raw_values()[0].0.kind {
            assert_eq!(l.values.len(), 1);
            if let Some(prost_types::value::Kind::StringValue(ref s)) = l.values[0].kind {
                assert_eq!(s, "AB");
            } else {
                panic!("Expected StringValue");
            }
        } else {
            panic!("Expected ListValue");
        }
        assert!(rs.next().await.is_none());
    }

    #[tokio_test_no_panics]
    async fn test_multi_response_chunking_bool_array() {
        fn bool_val(b: bool) -> Value {
            Value {
                kind: Some(prost_types::value::Kind::BoolValue(b)),
            }
        }
        fn null_val() -> Value {
            Value {
                kind: Some(prost_types::value::Kind::NullValue(0)),
            }
        }

        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![
                    list_val(vec![bool_val(true)]),
                    list_val(vec![bool_val(false), null_val(), bool_val(true)]),
                ],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![list_val(vec![bool_val(true), bool_val(true)])],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![
                    list_val(vec![null_val(), null_val(), bool_val(false)]),
                    list_val(vec![bool_val(true)]),
                ],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        ])
        .await;

        let row1 = rs.next().await.unwrap().unwrap();
        assert_eq!(row1.raw_values()[0].0, list_val(vec![bool_val(true)]));

        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row2.raw_values()[0].0,
            list_val(vec![
                bool_val(false),
                null_val(),
                bool_val(true),
                bool_val(true),
                bool_val(true),
                null_val(),
                null_val(),
                bool_val(false)
            ])
        );

        let row3 = rs.next().await.unwrap().unwrap();
        assert_eq!(row3.raw_values()[0].0, list_val(vec![bool_val(true)]));

        assert!(rs.next().await.is_none());
    }

    #[tokio_test_no_panics]
    async fn test_multi_response_chunking_int64_array() {
        fn null_val() -> Value {
            Value {
                kind: Some(prost_types::value::Kind::NullValue(0)),
            }
        }

        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![
                    list_val(vec![string_val("10")]),
                    list_val(vec![string_val("1"), string_val("2"), null_val()]),
                ],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![list_val(vec![null_val(), string_val("5")])],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![
                    list_val(vec![null_val(), string_val("7"), string_val("8")]),
                    list_val(vec![string_val("20")]),
                ],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        ])
        .await;

        let row1 = rs.next().await.unwrap().unwrap();
        assert_eq!(row1.raw_values()[0].0, list_val(vec![string_val("10")]));

        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row2.raw_values()[0].0,
            list_val(vec![
                string_val("1"),
                string_val("2"),
                null_val(),
                null_val(),
                string_val("5"),
                null_val(),
                string_val("7"),
                string_val("8")
            ])
        );

        let row3 = rs.next().await.unwrap().unwrap();
        assert_eq!(row3.raw_values()[0].0, list_val(vec![string_val("20")]));

        assert!(rs.next().await.is_none());
    }

    #[tokio_test_no_panics]
    async fn test_result_set_precommit_token_tracked() -> anyhow::Result<()> {
        let token = MultiplexedSessionPrecommitToken {
            precommit_token: b"test_token".to_vec(),
            seq_num: 99,
        };
        let results = vec![PartialResultSet {
            metadata: metadata(1),
            precommit_token: Some(token.clone()),
            last: true,
            ..Default::default()
        }];

        let mut mock = MockSpanner::new();
        let rx = adapt(results.into_iter().map(Ok));
        mock.expect_execute_streaming_sql()
            .return_once(move |_request| Ok(Response::from(rx)));

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock)
            .await
            .expect("Failed to start mock server");

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client: crate::database_client::DatabaseClient =
            client.database_client("db").build().await.unwrap();

        let tracker = PrecommitTokenTracker::new();

        let req = crate::model::ExecuteSqlRequest::default()
            .set_session("session".to_string())
            .set_sql("SELECT 1".to_string());

        let stream = db_client
            .spanner
            .execute_streaming_sql(req.clone(), GaxRequestOptions::default(), 0)
            .send()
            .await?;

        let mut rs = ResultSet::create(ResultSetParams {
            stream,
            transaction_selector: None,
            precommit_token_tracker: tracker.clone(),
            client: db_client,
            session_name: "session".to_string(),
            transaction_tag: None,
            operation: StreamOperation::Query(req),
            channel_hint: 0,
            gax_options: GaxRequestOptions::default(),
        })
        .await?;

        // Read a row to trigger precommit token extraction
        assert!(
            rs.next().await.is_none(),
            "Expected no rows, but received one"
        );

        // Validate the tracker correctly intercepted and preserved the token
        let tracked_token = tracker.get().expect("token should be tracked");
        assert_eq!(tracked_token.seq_num, 99);
        assert_eq!(
            tracked_token.precommit_token,
            bytes::Bytes::from("test_token")
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_retry_simple() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([
                    Ok(PartialResultSet {
                        metadata: metadata(1),
                        values: vec![string_val("row1")],
                        resume_token: b"token1".to_vec(),
                        ..Default::default()
                    }),
                    Err(Status::unavailable("Transient error")),
                ]);
                Ok(Response::from(stream))
            });

        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([Ok(PartialResultSet {
                    values: vec![string_val("row2")],
                    resume_token: b"token2".to_vec(),
                    last: true,
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();
        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .returning(|_| Duration::from_nanos(1));

        let stmt = Statement::builder("SELECT 1")
            .with_backoff_policy(mock_backoff)
            .build();
        let mut rs = tx.execute_query(stmt).await?;

        let row1 = rs.next().await.expect("Stream ended unexpectedly")?;
        assert_eq!(row1.raw_values()[0].0, string_val("row1"));

        let row2 = rs.next().await.expect("Stream ended unexpectedly")?;
        assert_eq!(row2.raw_values()[0].0, string_val("row2"));

        assert!(rs.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_retry_non_retriable_error() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_execute_streaming_sql()
            .times(1)
            .returning(|_request| {
                let stream = adapt([
                    Ok(PartialResultSet {
                        metadata: metadata(1),
                        values: vec![string_val("row1")],
                        resume_token: b"token1".to_vec(),
                        ..Default::default()
                    }),
                    Err(Status::invalid_argument("Non-retriable error")),
                ]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();
        let mut rs = tx.execute_query("SELECT 1").await?;

        let row1 = rs.next().await.expect("Stream ended unexpectedly")?;
        assert_eq!(row1.raw_values()[0].0, string_val("row1"));

        let res = rs.next().await;
        assert!(res.is_some(), "Expected an error but got None");
        let res = res.expect("Expected some response but got None");
        assert!(res.is_err(), "Expected an error but got Ok");
        let err_str = res.expect_err("Expected should be an error").to_string();
        assert!(
            err_str.contains("Non-retriable error"),
            "Expected error to contain 'Non-retriable error', but got '{}'",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_buffer_overflow() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let (tx_msg, rx_msg) = tokio::sync::mpsc::channel(10);
        mock.expect_execute_streaming_sql()
            // Should only be called once, as it is not retried due to missing resume tokens.
            .times(1)
            .return_once(move |_request| Ok(Response::from(rx_msg)));

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();

        tx_msg
            .send(Ok(PartialResultSet {
                metadata: metadata(1),
                values: vec![string_val("row1")],
                resume_token: b"token1".to_vec(),
                ..Default::default()
            }))
            .await?;

        let mut rs = tx.execute_query("SELECT 1").await?;

        // Set max buffer size to 2
        rs.set_max_buffered_partial_result_sets(2);

        tx_msg
            .send(Ok(PartialResultSet {
                values: vec![string_val("row2")],
                ..Default::default()
            }))
            .await?;
        tx_msg
            .send(Ok(PartialResultSet {
                values: vec![string_val("row3")],
                ..Default::default()
            }))
            .await?;
        tx_msg
            .send(Ok(PartialResultSet {
                values: vec![string_val("row4")],
                ..Default::default()
            }))
            .await?;
        tx_msg
            .send(Err(Status::unavailable("Unavailable error")))
            .await?;

        // Read row 1.
        let row1 = rs.next().await.expect("Expected row1")?;
        assert_eq!(row1.raw_values()[0].0, string_val("row1"));

        // Read row 2 (flushed when row4 overflows buffer).
        let row2 = rs.next().await.expect("Expected row2")?;
        assert_eq!(row2.raw_values()[0].0, string_val("row2"));

        // Try to read next row. As the buffer is now full/unsafe, it will not retry and return the error.
        let res = rs.next().await;
        assert!(res.is_some(), "Expected an error but got None");
        let res = res.expect("Expected some response but got None");
        assert!(res.is_err(), "Expected an error but got Ok");
        let err_str = res.expect_err("Expected should be an error").to_string();
        assert!(
            err_str.contains("Unavailable error"),
            "Expected error to contain 'Unavailable error', but got '{}'",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_retry_missing_resume_token_safe() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([
                    Ok(PartialResultSet {
                        metadata: metadata(1),
                        values: vec![string_val("row1")],
                        // no resume token
                        ..Default::default()
                    }),
                    Err(Status::unavailable("Unavailable error")),
                ]);
                Ok(Response::from(stream))
            });

        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([Ok(PartialResultSet {
                    metadata: metadata(1),
                    values: vec![string_val("row1_retry")],
                    resume_token: b"token_retry".to_vec(),
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();
        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .returning(|_| Duration::from_nanos(1));

        let stmt = Statement::builder("SELECT 1")
            .with_backoff_policy(mock_backoff)
            .build();
        let mut rs = tx.execute_query(stmt).await?;

        let row1 = rs.next().await.expect("Expected row1")?;
        assert_eq!(row1.raw_values()[0].0, string_val("row1_retry"));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_retry_under_limit_no_resume_token() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // First stream: 2 messages without resume token, then Error.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([
                    Ok(PartialResultSet {
                        metadata: metadata(1),
                        values: vec![string_val("row1")],
                        ..Default::default()
                    }),
                    Ok(PartialResultSet {
                        values: vec![string_val("row2")],
                        ..Default::default()
                    }),
                    Err(Status::unavailable("Unavailable error")),
                ]);
                Ok(Response::from(stream))
            });

        // Second stream: Retried from the start as the initial stream
        // returned Unavailable before the buffer was full.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|request| {
                assert!(
                    request.get_ref().resume_token.is_empty(),
                    "Expected empty resume token for retry"
                );
                let stream = adapt([Ok(PartialResultSet {
                    metadata: metadata(1),
                    values: vec![string_val("row1_retry")],
                    resume_token: b"token_retry".to_vec(),
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();
        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .returning(|_| Duration::from_nanos(1));

        let stmt = Statement::builder("SELECT 1")
            .with_backoff_policy(mock_backoff)
            .build();
        let mut rs = tx.execute_query(stmt).await?;

        // Set max buffer size to 3 (so 2 messages is under the limit)
        rs.set_max_buffered_partial_result_sets(3);

        // Read row 1.
        // It reads row1, row2, and then the error from the first stream.
        // Since it is less than the buffer size, it retries without a resume token.
        // The retry stream returns "row1_retry".
        let row1 = rs.next().await.expect("Expected row1")?;
        assert_eq!(row1.raw_values()[0].0, string_val("row1_retry"));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_retry_limit_exceeded() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();

        mock.expect_execute_streaming_sql()
            .times(11) // 1 initial + 10 retries
            .returning(|_request| {
                let stream = adapt([Err(Status::unavailable("Unavailable error"))]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();
        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .times(10)
            .returning(|_| Duration::from_nanos(1));

        let stmt = Statement::builder("SELECT 1")
            .with_backoff_policy(mock_backoff)
            .build();
        let res = tx.execute_query(stmt).await;

        assert!(res.is_err(), "Expected an error but got Ok");
        let err_str = res.expect_err("Expected should be an error").to_string();
        assert!(
            err_str.contains("Unavailable error"),
            "Expected error to contain 'Unavailable error', but got '{}'",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_inline_begin_stream_error_fallback() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // 1. Stream yields an error on the first chunk before returning transaction metadata.
        // E.g., INVALID_ARGUMENT because the query is malformed.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([Err(Status::invalid_argument("Invalid query"))]);
                Ok(Response::from(stream))
            });

        // 2. The explicit BeginTransaction fallback gets triggered.
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(spanner_v1::Transaction {
                    id: vec![7, 8, 9],
                    read_timestamp: Some(prost_types::Timestamp {
                        seconds: 123456789,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        // 3. The ResultSet gracefully restarts the stream using the transaction ID returned by BeginTransaction.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                // Ensure the explicitly yielded ID is routed into the new stream transaction selector
                match req.transaction.unwrap().selector.unwrap() {
                    spanner_v1::transaction_selector::Selector::Id(id) => {
                        assert_eq!(id, vec![7, 8, 9]);
                    }
                    _ => panic!("Expected Selector::Id"),
                }

                let stream = adapt([Ok(PartialResultSet {
                    metadata: metadata(1),
                    values: vec![string_val("1")],
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;

        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;
        let mut rs = tx.execute_query("SELECT 1").await?;

        let row1 = rs.next().await.ok_or_else(|| {
            anyhow::anyhow!("Expected row returned successfully despite stream breaking")
        })??;
        assert_eq!(
            row1.raw_values()[0].0,
            string_val("1"),
            "Verify the returned stream successfully resumed with the correct payload"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_retry_inline_begin_transient_error() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // 1. Initial stream throws UNAVAILABLE before metadata.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([Err(Status::unavailable("Transient network issue"))]);
                Ok(Response::from(stream))
            });

        // 2. We retry the stream since it was a transient error.
        // The retry should use the same transaction selector as the original request.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                match req.transaction.unwrap().selector.unwrap() {
                    spanner_v1::transaction_selector::Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin on stream retry"),
                }

                let mut meta = metadata(1).unwrap();
                meta.transaction = Some(spanner_v1::Transaction {
                    id: vec![7, 8, 9],
                    read_timestamp: None,
                    ..Default::default()
                });

                let stream = adapt([Ok(PartialResultSet {
                    metadata: Some(meta),
                    values: vec![string_val("1")],
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;

        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;
        let mut rs = tx.execute_query("SELECT 1").await?;

        let row1 = rs
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("Expected stream to recover safely"))??;
        assert_eq!(
            row1.raw_values()[0].0,
            string_val("1"),
            "Verify resumed stream returns data"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_retry_inline_begin_id_recovered() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // 1. Stream successfully returns metadata chunk then throws UNAVAILABLE on chunk 2.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let mut meta = metadata(1).unwrap();
                meta.transaction = Some(spanner_v1::Transaction {
                    id: vec![7, 8, 9],
                    read_timestamp: None,
                    ..Default::default()
                });
                let stream = adapt([
                    Ok(PartialResultSet {
                        metadata: Some(meta),
                        values: vec![string_val("1")],
                        resume_token: b"token1".to_vec(),
                        ..Default::default()
                    }),
                    Err(Status::unavailable("Transient mid-stream network issue")),
                ]);
                Ok(Response::from(stream))
            });

        // 2. Stream resumes using Selector::Id.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                match req.transaction.unwrap().selector.unwrap() {
                    spanner_v1::transaction_selector::Selector::Id(id) => {
                        assert_eq!(id, vec![7, 8, 9]);
                    }
                    _ => panic!("Expected Selector::Id on stream retry"),
                }

                let stream = adapt([Ok(PartialResultSet {
                    values: vec![string_val("2")],
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;

        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;
        let mut rs = tx.execute_query("SELECT 1").await?;

        let row1 = rs
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("Expected stream row1 extracted"))??;
        assert_eq!(
            row1.raw_values()[0].0,
            string_val("1"),
            "Verified chunk 1 payload"
        );
        let row2 = rs
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("Expected stream row2 recovered"))??;
        assert_eq!(
            row2.raw_values()[0].0,
            string_val("2"),
            "Verified chunk 2 reboot dynamically intercepted ID bounds correctly"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_inline_begin_metadata_missing_transaction_fails() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // 1. Initial stream successfully returns metadata chunk but completely lacks the `Transaction` entity.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let stream = adapt([Ok(PartialResultSet {
                    metadata: metadata(1), // Missing `.transaction` natively
                    values: vec![string_val("1")],
                    ..Default::default()
                })]);
                Ok(Response::from(stream))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;

        // Use explicitly deferred Lazy begin transaction!
        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;
        let err = tx
            .execute_query("SELECT 1")
            .await
            .expect_err("Expected eager validation error");
        assert!(
            err.to_string()
                .contains("failed to return a transaction ID"),
            "Caught implicit gap boundary: {}",
            err
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_stats() -> anyhow::Result<()> {
        let mock_stats = spanner_v1::ResultSetStats {
            query_plan: Some(spanner_v1::QueryPlan::default()),
            ..Default::default()
        };

        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            values: vec![string_val("a"), string_val("b")],
            last: true,
            stats: Some(mock_stats),
            ..Default::default()
        }])
        .await;

        rs.next().await.transpose()?;

        let received_stats = rs.stats().expect("stats should be available");
        assert!(received_stats.query_plan.is_some());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_update_count() -> anyhow::Result<()> {
        let mock_stats = spanner_v1::ResultSetStats {
            row_count: Some(RowCount::RowCountExact(42_i64)),
            ..Default::default()
        };

        let mut result_set = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            values: vec![string_val("a"), string_val("b")],
            last: true,
            stats: Some(mock_stats),
            ..Default::default()
        }])
        .await;

        result_set.next().await.transpose()?;

        let update_count = result_set
            .update_count()
            .expect("Expected update count to be populated");
        assert_eq!(update_count, 42, "Expected exactly 42 rows updated");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn result_set_duplicate_stats() -> anyhow::Result<()> {
        let mock_stats = spanner_v1::ResultSetStats {
            query_plan: Some(spanner_v1::QueryPlan::default()),
            ..Default::default()
        };

        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(2),
                values: vec![string_val("a"), string_val("b")],
                stats: Some(mock_stats.clone()),
                resume_token: b"token1".to_vec(),
                ..Default::default()
            },
            PartialResultSet {
                values: vec![string_val("c"), string_val("d")],
                stats: Some(mock_stats),
                last: true,
                resume_token: b"token2".to_vec(),
                ..Default::default()
            },
        ])
        .await;

        // First row should be processed and returned successfully
        let next = rs.next().await;
        assert!(next.is_some());
        assert!(next.expect("should yield a row").is_ok());

        // Second call should process the second message and fail due to duplicate stats
        let res2 = rs.next().await;
        assert!(res2.is_some());
        let res2 = res2.expect("should yield an error");
        assert!(res2.is_err());
        let err_str = res2.expect_err("should be an error").to_string();
        assert!(err_str.contains("Additional stats received after first"));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_lazy_begin_deadlock_fixed() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();

        // Setup mock to return metadata with transaction ID on first query.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_request| {
                let mut meta = metadata(1).expect("failed to create metadata");
                meta.transaction = Some(spanner_v1::Transaction {
                    id: b"lazy_tx_id".to_vec(),
                    ..Default::default()
                });
                let rx = adapt(
                    vec![Ok(PartialResultSet {
                        metadata: Some(meta),
                        values: vec![string_val("1")],
                        ..Default::default()
                    })]
                    .into_iter(),
                );
                Ok(Response::from(rx))
            });

        // Mock call for second query which must carry the returned transaction ID
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                let selector = req
                    .transaction
                    .expect("missing transaction component")
                    .selector
                    .expect("missing selector component");

                match selector {
                    spanner_v1::transaction_selector::Selector::Id(id) => {
                        assert_eq!(id, b"lazy_tx_id".to_vec());
                    }
                    _ => panic!("Expected Selector::Id"),
                }

                let rx = adapt(
                    vec![Ok(PartialResultSet {
                        metadata: metadata(1),
                        values: vec![string_val("2")],
                        ..Default::default()
                    })]
                    .into_iter(),
                );
                Ok(Response::from(rx))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;

        // Use inline begin transaction
        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // Execute query but DO NOT call rs.next()
        let _rs = tx.execute_query("SELECT 1").await?;

        // Execute second query against same transaction
        let mut rs2 = tx.execute_query("SELECT 2").await?;

        // Assert it does not hang and yielded elements properly
        let row2 = rs2.next().await;
        assert!(
            row2.is_some(),
            "Implicit deadlock encountered; query 2 stalled"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn test_result_set_metadata_not_available() {
        // Test our explicit safeguard for when Spanner violates the API contract and returns a first PartialResultSet without metadata.
        let res = run_mock_query_fallible(vec![PartialResultSet {
            metadata: None,
            values: vec![string_val("1")],
            ..Default::default()
        }])
        .await;

        let err = res.expect_err("Expected query initialization to fail eagerly");
        assert!(
            err.to_string()
                .contains("First PartialResultSet did not contain metadata"),
            "Expected missing metadata safeguard error, got: {}",
            err
        );
    }

    #[tokio_test_no_panics]
    async fn test_result_set_metadata_available_before_next() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();

        // Setup mock to return metadata in first chunk.
        mock.expect_execute_streaming_sql().returning(|_request| {
            let rx = adapt(
                vec![Ok(PartialResultSet {
                    metadata: metadata(1),
                    values: vec![string_val("1")],
                    ..Default::default()
                })]
                .into_iter(),
            );
            Ok(Response::from(rx))
        });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client.database_client("db").build().await?;
        let tx = db_client.single_use().build();

        let mut rs = tx.execute_query("SELECT 1").await?;

        // Call metadata() BEFORE next(). It should succeed immediately.
        let metadata = rs.metadata().expect("metadata available");
        assert_eq!(metadata.column_names().len(), 1);
        assert_eq!(metadata.column_names()[0], "col0");

        // Now consume the row
        let row = rs.next().await;
        assert!(row.is_some());

        Ok(())
    }
}
