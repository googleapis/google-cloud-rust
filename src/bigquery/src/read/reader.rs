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

//! Helpers to reconnect a [ReadRows] stream.

use crate::builder::read::ReadRows;
use crate::model::ReadRowsResponse;
use crate::read::retry_policy::RetryableErrors;
use crate::{Error, Result};
use google_cloud_gax::backoff_policy::{BackoffPolicy, BackoffPolicyArg};
use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use google_cloud_gax::retry_policy::{RetryPolicy, RetryPolicyArg, RetryPolicyExt};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::retry_throttler::{CircuitBreaker, RetryThrottlerArg, SharedRetryThrottler};
use google_cloud_gax::streaming::ResponseStream;
use google_cloud_gax::throttle_result::ThrottleResult;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const INITIAL_DELAY: Duration = Duration::from_millis(100);
const MAXIMUM_DELAY: Duration = Duration::from_secs(60);
const SCALING_FACTOR: f64 = 1.3;
const MAX_ATTEMPTS: u32 = 10;

/// Default retry policy for mid-stream `read_rows` reconnections.
///
/// - `max_times` (10): Limits total consecutive failed reconnection attempts when no data progress is made.
fn default_retry_policy() -> Arc<dyn RetryPolicy> {
    Arc::new(RetryableErrors.with_attempt_limit(MAX_ATTEMPTS))
}

/// Default backoff policy for mid-stream `read_rows` reconnections.
///
/// - `min_delay` (100ms): Starts with a short initial backoff to quickly recover from brief network blips.
/// - `max_delay` (60s): Caps the backoff delay so exponential growth (100ms -> 130ms -> 169ms ...) does not produce excessively long single delays.
/// - `factor` (1.3): Multiplier scaling factor for exponential backoff (matching Python BigQuery Storage client standard).
fn default_backoff_policy() -> Arc<ExponentialBackoff> {
    Arc::new(
        ExponentialBackoffBuilder::new()
            .with_initial_delay(INITIAL_DELAY)
            .with_maximum_delay(MAXIMUM_DELAY)
            .with_scaling(SCALING_FACTOR)
            .build()
            .expect("hardcoded value guaranteed to be valid"),
    )
}

/// Default retry throttler for mid-stream `read_rows` reconnections.
fn default_retry_throttler() -> SharedRetryThrottler {
    Arc::new(Mutex::new(CircuitBreaker::default()))
}

/// State machine for reconnection logic. If reading fails at some point,
/// attempt to reconnect with the current row offset.
///
/// ```text
///                Start here
///                    │
///              ┌─────▼──────┐
///        ┌─────┼ Connecting ◄────────┐◄───────────────────┐
///        │     └─────┬──────┘        │                    │
///        │           │        Failed to read          Failed to
///   Exhausted        │         first message         read message
///    Retries         │      (retains retry state  (resets retry state)
///       or           │           from connecting)         ▲
/// Unrecoverable┌─────▼──────┐        ▲               ┌────┼────┐
///      Error   │ Connected  ┼────────┼───────────────► Reading │
///        │     └─────┬──────┘                        └────┬────┘
///        │           ▼                                    ▼
///        │      Unrecoverable                      Unrecoverable
///        │         Error                                Error
///        │           │            ┌────────────┐          │
///        │           └───────────►│            │          │
///        │                        │ Terminated │◄─────────┘
///        └───────────────────────►│            │
///                                 └────────────┘
/// ```
#[derive(Debug)]
pub(crate) enum ReaderState {
    /// Connect or reconnect to the BigQuery read stream, with exponential backoff.
    Connecting(RetryState),
    /// Waiting for the first message in a BigQuery read stream.
    Connected(RetryState, ResponseStream<ReadRowsResponse>),
    /// Waiting for the next message in a BigQuery read stream.
    Reading(ResponseStream<ReadRowsResponse>),
    /// Stream completed cleanly, fatal error occurred, or consumer dropped the receiver.
    Terminated(Option<Error>),
}

/// A stream reader for [`ReadRows`] that automatically reconnects on transient errors,
/// resuming from the current row offset.
#[derive(Debug)]
pub struct Reader {
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
    retry_throttler: SharedRetryThrottler,
    state: ReaderState,
    request: ReadRows,
    offset: i64,
}

impl Reader {
    pub(crate) fn new(request: ReadRows) -> Self {
        Self {
            retry_policy: default_retry_policy(),
            backoff_policy: default_backoff_policy(),
            retry_throttler: default_retry_throttler(),
            state: ReaderState::Connecting(RetryState::new(true)),
            request,
            offset: 0,
        }
    }

    /// Sets the initial row offset for the [`Reader`].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Read;
    /// # async fn sample(client: Read) -> anyhow::Result<()> {
    /// let mut reader = client
    ///     .read_rows()
    ///     .set_read_stream("projects/my-project/locations/us/sessions/s1/streams/st1")
    ///     .into_reader()
    ///     .with_offset(1_000);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_offset<T: Into<i64>>(mut self, v: T) -> Self {
        self.offset = v.into();
        self
    }

    /// Returns the next row offset that the [`Reader`] expects to read.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Configure the retry policy for reconnecting the stream.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Read;
    /// # async fn sample(client: Read) -> anyhow::Result<()> {
    /// use google_cloud_bigquery::read::retry_policy::RetryableErrors;
    /// use google_cloud_gax::retry_policy::RetryPolicyExt;
    /// let mut reader = client
    ///     .read_rows()
    ///     .set_read_stream("projects/my-project/locations/us/sessions/s1/streams/st1")
    ///     .into_reader()
    ///     .with_retry_policy(RetryableErrors.with_attempt_limit(5));
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.retry_policy = v.into().into();
        self
    }

    /// Configure the retry backoff policy for reconnecting the stream.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Read;
    /// # async fn sample(client: Read) -> anyhow::Result<()> {
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// let mut reader = client
    ///     .read_rows()
    ///     .set_read_stream("projects/my-project/locations/us/sessions/s1/streams/st1")
    ///     .into_reader()
    ///     .with_backoff_policy(ExponentialBackoff::default());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.backoff_policy = v.into().into();
        self
    }

    /// Configure the retry throttler for reconnecting the stream.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Read;
    /// # async fn sample(client: Read) -> anyhow::Result<()> {
    /// use google_cloud_gax::retry_throttler::AdaptiveThrottler;
    /// let mut reader = client
    ///     .read_rows()
    ///     .set_read_stream("projects/my-project/locations/us/sessions/s1/streams/st1")
    ///     .into_reader()
    ///     .with_retry_throttler(AdaptiveThrottler::default());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.retry_throttler = v.into().into();
        self
    }

    fn advance_offset(&mut self, row_count: i64) -> Result<()> {
        if row_count < 0 {
            return Err(Error::deser(format!(
                "ReadRowsResponse contained negative row_count: {row_count}"
            )));
        }
        self.offset = self.offset.checked_add(row_count).ok_or_else(|| {
            Error::deser(format!(
                "ReadRowsResponse row_count ({row_count}) overflowed stream offset ({})",
                self.offset
            ))
        })?;
        Ok(())
    }

    async fn handle_error(&mut self, mut retry_state: RetryState, err: Error) {
        retry_state.attempt_count += 1;
        let flow = self.retry_policy.on_error(&retry_state, err);
        self.retry_throttler
            .lock()
            .expect("retry throttler lock is poisoned")
            .on_retry_failure(&flow);
        let mut prev_err = match flow {
            RetryResult::Permanent(e) | RetryResult::Exhausted(e) => {
                self.state = ReaderState::Terminated(Some(e));
                return;
            }
            RetryResult::Continue(e) => e,
        };

        loop {
            let delay = self.backoff_policy.on_failure(&retry_state);
            if self
                .retry_policy
                .remaining_time(&retry_state)
                .is_some_and(|remaining| remaining <= delay)
            {
                self.state = ReaderState::Terminated(Some(Error::exhausted(prev_err)));
                return;
            }
            // Transition to Connecting before sleeping so that if `next()` is
            // cancelled during backoff, the Reader remains in a valid state to
            // reconnect on the next poll instead of terminating prematurely.
            self.state = ReaderState::Connecting(retry_state.clone());
            tokio::time::sleep(delay).await;

            let throttled = self
                .retry_throttler
                .lock()
                .expect("retry throttler lock is poisoned")
                .throttle_retry_attempt();
            if !throttled {
                return;
            }
            retry_state.attempt_count += 1;
            self.state = ReaderState::Connecting(retry_state.clone());
            prev_err = match self.retry_policy.on_throttle(&retry_state, prev_err) {
                ThrottleResult::Exhausted(e) => {
                    self.state = ReaderState::Terminated(Some(e));
                    return;
                }
                ThrottleResult::Continue(e) => e,
            };
        }
    }

    /// Advances the state machine by a single transition, returning a
    /// [`ReadRowsResponse`] if one was read during this step.
    async fn step(&mut self) -> Option<ReadRowsResponse> {
        match &mut self.state {
            ReaderState::Connecting(retry_state) => {
                let retry_state = retry_state.clone();
                let req = self.request.clone().set_offset(self.offset);
                match req.send().await {
                    Ok(stream) => {
                        self.state = ReaderState::Connected(retry_state, stream);
                    }
                    Err(err) => {
                        self.handle_error(retry_state, err).await;
                    }
                }
                None
            }
            ReaderState::Connected(retry_state, stream) => match stream.next().await {
                Some(Ok(response)) => {
                    self.retry_throttler
                        .lock()
                        .expect("retry throttler lock is poisoned")
                        .on_success();
                    if let Err(err) = self.advance_offset(response.row_count) {
                        self.state = ReaderState::Terminated(Some(err));
                        return None;
                    }
                    let ReaderState::Connected(_, stream) =
                        std::mem::replace(&mut self.state, ReaderState::Terminated(None))
                    else {
                        unreachable!("state is known to be Connected");
                    };
                    self.state = ReaderState::Reading(stream);
                    Some(response)
                }
                Some(Err(err)) => {
                    let retry_state = retry_state.clone();
                    self.handle_error(retry_state, err).await;
                    None
                }
                None => {
                    self.retry_throttler
                        .lock()
                        .expect("retry throttler lock is poisoned")
                        .on_success();
                    self.state = ReaderState::Terminated(None);
                    None
                }
            },
            ReaderState::Reading(stream) => match stream.next().await {
                Some(Ok(response)) => {
                    self.retry_throttler
                        .lock()
                        .expect("retry throttler lock is poisoned")
                        .on_success();
                    if let Err(err) = self.advance_offset(response.row_count) {
                        self.state = ReaderState::Terminated(Some(err));
                        return None;
                    }
                    Some(response)
                }
                Some(Err(err)) => {
                    self.handle_error(RetryState::new(true), err).await;
                    None
                }
                None => {
                    self.state = ReaderState::Terminated(None);
                    None
                }
            },
            ReaderState::Terminated(_) => None,
        }
    }

    /// Receives the next [`ReadRowsResponse`] from the stream, reconnecting if a
    /// retryable error occurs, or returns `None` when the stream completes.
    pub async fn next(&mut self) -> Option<Result<ReadRowsResponse>> {
        loop {
            let response = self.step().await;
            match &mut self.state {
                ReaderState::Connecting(_) | ReaderState::Connected(_, _) => continue,
                ReaderState::Reading(_) => return response.map(Ok),
                ReaderState::Terminated(err) => return err.take().map(Err),
            }
        }
    }
}

impl ReadRows {
    /// Returns a [`Reader`], which automatically reconnects and resumes reading
    /// from the latest row offset if the stream is interrupted by a retryable error.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::builder::read::ReadRows;
    /// # async fn sample(builder: ReadRows) -> google_cloud_bigquery::Result<()> {
    /// let mut rows = builder.into_reader();
    /// while let Some(response) = rows.next().await.transpose()? {
    ///     println!("Read {} rows", response.row_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_reader(self) -> Reader {
        Reader::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Read;
    use crate::model::ReadRowsRequest;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::retry_throttler::RetryThrottler;
    use std::error::Error as _;
    use tokio::sync::mpsc;

    #[derive(Debug, Default)]
    struct NoBackoff;

    impl BackoffPolicy for NoBackoff {
        fn on_failure(&self, _state: &RetryState) -> Duration {
            Duration::ZERO
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> Duration;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        ReadStub {}
        impl crate::stub::Read for ReadStub {
            async fn read_rows(
                &self,
                req: ReadRowsRequest,
                options: RequestOptions,
            ) -> Result<ResponseStream<ReadRowsResponse>>;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub RetryPolicy {}
        impl RetryPolicy for RetryPolicy {
            fn on_error(&self, state: &RetryState, error: Error) -> RetryResult;
            fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult;
            fn remaining_time(&self, state: &RetryState) -> Option<Duration>;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub RetryThrottler {}
        impl RetryThrottler for RetryThrottler {
            fn throttle_retry_attempt(&self) -> bool;
            fn on_retry_failure(&mut self, flow: &RetryResult);
            fn on_success(&mut self);
        }
    }

    /// A `MockRetryPolicy` without an overall deadline.
    fn mock_retry_policy() -> MockRetryPolicy {
        let mut retry = MockRetryPolicy::new();
        retry.expect_remaining_time().returning(|_| None);
        retry
    }

    fn transient_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("transient failure"),
        )
    }

    fn permanent_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("permission denied"),
        )
    }

    #[tokio::test]
    async fn step_connecting_to_connected() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().once().returning(|req, _| {
            assert_eq!(req.read_stream, "streams/1");
            assert_eq!(req.offset, 0);
            let (_tx, rx) = mpsc::channel(1);
            Ok(ResponseStream::from(rx))
        });

        let mut retry = mock_retry_policy();
        retry.expect_on_error().never();
        let mut backoff = MockBackoffPolicy::new();
        backoff.expect_on_failure().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);

        let initial_retry = RetryState::new(true).set_attempt_count(2_u32);
        reader.state = ReaderState::Connecting(initial_retry);
        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Connected(retry_state, _) = &reader.state else {
            anyhow::bail!("expected Connected state, got: {:?}", reader.state);
        };
        assert_eq!(retry_state.attempt_count, 2);
        Ok(())
    }

    #[tokio::test]
    async fn step_connecting_retryable_error() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows()
            .once()
            .returning(|_, _| Err(transient_error()));

        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|state, e| {
                state.attempt_count == 2 && e.status().is_some_and(|s| s.code == Code::Unavailable)
            })
            .once()
            .returning(|_, e| RetryResult::Continue(e));

        let mut backoff = MockBackoffPolicy::new();
        backoff
            .expect_on_failure()
            .withf(|state| state.attempt_count == 2)
            .once()
            .return_const(Duration::ZERO);

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);

        let initial_retry = RetryState::new(true).set_attempt_count(1_u32);
        reader.state = ReaderState::Connecting(initial_retry);
        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Connecting(retry_state) = &reader.state else {
            anyhow::bail!("expected Connecting state, got: {:?}", reader.state);
        };
        assert_eq!(retry_state.attempt_count, 2);
        Ok(())
    }

    #[tokio::test]
    async fn step_connecting_permanent_error() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows()
            .once()
            .returning(|_, _| Err(permanent_error()));

        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|state, e| {
                state.attempt_count == 1
                    && e.status().is_some_and(|s| s.code == Code::PermissionDenied)
            })
            .once()
            .returning(|_, e| RetryResult::Permanent(e));

        let mut backoff = MockBackoffPolicy::new();
        backoff.expect_on_failure().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);

        reader.state = ReaderState::Connecting(RetryState::new(true));
        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Terminated(Some(err)) = &reader.state else {
            anyhow::bail!("expected Terminated(Some(_)), got: {:?}", reader.state);
        };
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        Ok(())
    }

    #[tokio::test]
    async fn step_connecting_deadline_exhausted() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows()
            .once()
            .returning(|_, _| Err(transient_error()));

        let mut retry = MockRetryPolicy::new();
        retry
            .expect_remaining_time()
            .returning(|_| Some(Duration::from_secs(1)));
        retry
            .expect_on_error()
            .once()
            .returning(|_, e| RetryResult::Continue(e));

        let mut backoff = MockBackoffPolicy::new();
        backoff
            .expect_on_failure()
            .once()
            .return_const(Duration::from_secs(60));

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);

        reader.state = ReaderState::Connecting(RetryState::new(true));
        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Terminated(Some(err)) = &reader.state else {
            anyhow::bail!("expected Terminated(Some(_)), got: {:?}", reader.state);
        };
        assert!(err.is_exhausted(), "{err:?}");
        let last_error = err.source().expect("the error should have a source");
        assert!(
            last_error.to_string().contains("transient failure"),
            "{last_error:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn step_connected_to_reading() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();
        let mut retry = mock_retry_policy();
        retry.expect_on_error().never();
        let mut backoff = MockBackoffPolicy::new();
        backoff.expect_on_failure().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);

        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(ReadRowsResponse::new().set_row_count(5)))
            .await?;
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Connected(RetryState::new(true), stream);
        let resp = reader.step().await.expect("expected Some(response)");
        assert!(
            matches!(reader.state, ReaderState::Reading(_)),
            "expected Reading state, got: {:?}",
            reader.state
        );
        assert_eq!(resp.row_count, 5);
        assert_eq!(reader.offset(), 5);
        Ok(())
    }

    #[tokio::test]
    async fn step_connected_retryable_error_retains_retry_state() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|state, e| {
                state.attempt_count == 3 && e.status().is_some_and(|s| s.code == Code::Unavailable)
            })
            .once()
            .returning(|_, e| RetryResult::Continue(e));

        let mut backoff = MockBackoffPolicy::new();
        backoff
            .expect_on_failure()
            .withf(|state| state.attempt_count == 3)
            .once()
            .return_const(Duration::ZERO);

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);

        let (tx, rx) = mpsc::channel(1);
        tx.send(Err(transient_error())).await?;
        let stream = ResponseStream::from(rx);

        // Connected state starts with attempt_count = 2 from prior Connecting attempts.
        let prior_retry = RetryState::new(true).set_attempt_count(2_u32);
        reader.state = ReaderState::Connected(prior_retry, stream);
        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Connecting(retry_state) = &reader.state else {
            anyhow::bail!("expected Connecting state, got: {:?}", reader.state);
        };
        assert_eq!(retry_state.attempt_count, 3);
        Ok(())
    }

    #[tokio::test]
    async fn step_connected_permanent_error_to_terminated() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|state, e| {
                state.attempt_count == 1
                    && e.status().is_some_and(|s| s.code == Code::PermissionDenied)
            })
            .once()
            .returning(|_, e| RetryResult::Permanent(e));

        let mut backoff = MockBackoffPolicy::new();
        backoff.expect_on_failure().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);

        let (tx, rx) = mpsc::channel(1);
        tx.send(Err(permanent_error())).await?;
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Connected(RetryState::new(true), stream);
        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Terminated(Some(err)) = &reader.state else {
            anyhow::bail!("expected Terminated(Some(_)), got: {:?}", reader.state);
        };
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        Ok(())
    }

    #[tokio::test]
    async fn step_connected_eof_to_terminated() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Connected(RetryState::new(true), stream);
        let resp = reader.step().await;
        assert!(resp.is_none());
        assert!(
            matches!(reader.state, ReaderState::Terminated(None)),
            "expected Terminated(None), got: {:?}",
            reader.state
        );
        Ok(())
    }

    #[tokio::test]
    async fn step_reading_to_reading() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();
        let mut retry = mock_retry_policy();
        retry.expect_on_error().never();
        let mut backoff = MockBackoffPolicy::new();
        backoff.expect_on_failure().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);
        reader.advance_offset(5)?;

        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(ReadRowsResponse::new().set_row_count(3)))
            .await?;
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Reading(stream);
        let resp = reader.step().await.expect("expected Some(response)");
        assert!(
            matches!(reader.state, ReaderState::Reading(_)),
            "expected Reading state, got: {:?}",
            reader.state
        );
        assert_eq!(resp.row_count, 3);
        assert_eq!(reader.offset(), 8);
        Ok(())
    }

    #[tokio::test]
    async fn step_reading_retryable_error_resets_retry_state() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|state, e| {
                // Reading resets RetryState, so the first failure from Reading has attempt_count == 1.
                state.attempt_count == 1 && e.status().is_some_and(|s| s.code == Code::Unavailable)
            })
            .once()
            .returning(|_, e| RetryResult::Continue(e));

        let mut backoff = MockBackoffPolicy::new();
        backoff
            .expect_on_failure()
            .withf(|state| state.attempt_count == 1)
            .once()
            .return_const(Duration::ZERO);

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);

        let (tx, rx) = mpsc::channel(1);
        tx.send(Err(transient_error())).await?;
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Reading(stream);
        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Connecting(retry_state) = &reader.state else {
            anyhow::bail!("expected Connecting state, got: {:?}", reader.state);
        };
        assert_eq!(retry_state.attempt_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn step_reading_permanent_error_to_terminated() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|state, e| {
                state.attempt_count == 1
                    && e.status().is_some_and(|s| s.code == Code::PermissionDenied)
            })
            .once()
            .returning(|_, e| RetryResult::Permanent(e));

        let mut backoff = MockBackoffPolicy::new();
        backoff.expect_on_failure().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff);

        let (tx, rx) = mpsc::channel(1);
        tx.send(Err(permanent_error())).await?;
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Reading(stream);
        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Terminated(Some(err)) = &reader.state else {
            anyhow::bail!("expected Terminated(Some(_)), got: {:?}", reader.state);
        };
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        Ok(())
    }

    #[tokio::test]
    async fn step_reading_eof_to_terminated() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Reading(stream);
        let resp = reader.step().await;
        assert!(resp.is_none());
        assert!(
            matches!(reader.state, ReaderState::Terminated(None)),
            "expected Terminated(None), got: {:?}",
            reader.state
        );
        Ok(())
    }

    #[tokio::test]
    async fn step_terminated_stays_terminated() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        reader.state = ReaderState::Terminated(None);
        assert!(reader.step().await.is_none());
        assert!(
            matches!(reader.state, ReaderState::Terminated(None)),
            "expected Terminated(None), got: {:?}",
            reader.state
        );

        reader.state = ReaderState::Terminated(Some(permanent_error()));
        assert!(reader.step().await.is_none());
        let ReaderState::Terminated(Some(err)) = &reader.state else {
            anyhow::bail!("expected Terminated(Some(_)), got: {:?}", reader.state);
        };
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        Ok(())
    }

    #[tokio::test]
    async fn read_rows_success() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().once().returning(|req, _| {
            assert_eq!(req.read_stream, "streams/1");
            assert_eq!(req.offset, 0);
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(5))).await;
                let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(3))).await;
            });
            Ok(ResponseStream::from(rx))
        });

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req).with_backoff_policy(NoBackoff);

        let r1 = reader.next().await.transpose()?.expect("row batch 1");
        assert_eq!(r1.row_count, 5);
        let r2 = reader.next().await.transpose()?.expect("row batch 2");
        assert_eq!(r2.row_count, 3);
        assert!(reader.next().await.is_none());
        assert!(reader.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_mid_stream_resumes_at_offset() -> anyhow::Result<()> {
        let mut seq = mockall::Sequence::new();
        let mut mock = MockReadStub::new();

        // First stream yields 5 rows, then 3 rows, then fails with Unavailable.
        mock.expect_read_rows()
            .once()
            .in_sequence(&mut seq)
            .returning(|req, _| {
                assert_eq!(req.offset, 0);
                let (tx, rx) = mpsc::channel(4);
                tokio::spawn(async move {
                    let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(5))).await;
                    let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(3))).await;
                    let _ = tx.send(Err(transient_error())).await;
                });
                Ok(ResponseStream::from(rx))
            });

        // Second stream should reconnect at offset 8 (5 + 3).
        mock.expect_read_rows()
            .once()
            .in_sequence(&mut seq)
            .returning(|req, _| {
                assert_eq!(req.offset, 8);
                let (tx, rx) = mpsc::channel(4);
                tokio::spawn(async move {
                    let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(2))).await;
                });
                Ok(ResponseStream::from(rx))
            });

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req).with_backoff_policy(NoBackoff);

        assert_eq!(reader.next().await.transpose()?.unwrap().row_count, 5);
        assert_eq!(reader.next().await.transpose()?.unwrap().row_count, 3);
        assert_eq!(reader.next().await.transpose()?.unwrap().row_count, 2);
        assert_eq!(reader.offset(), 10);
        assert!(reader.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn with_offset_starts_and_reconnects_from_initial_offset() -> anyhow::Result<()> {
        let mut seq = mockall::Sequence::new();
        let mut mock = MockReadStub::new();

        mock.expect_read_rows()
            .once()
            .in_sequence(&mut seq)
            .returning(|req, _| {
                assert_eq!(req.offset, 1_000);
                let (tx, rx) = mpsc::channel(4);
                tokio::spawn(async move {
                    let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(25))).await;
                    let _ = tx
                        .send(Err(Error::service(
                            Status::default()
                                .set_code(Code::ResourceExhausted)
                                .set_message("rate limited"),
                        )))
                        .await;
                });
                Ok(ResponseStream::from(rx))
            });

        mock.expect_read_rows()
            .once()
            .in_sequence(&mut seq)
            .returning(|req, _| {
                assert_eq!(req.offset, 1_025);
                let (tx, rx) = mpsc::channel(4);
                tokio::spawn(async move {
                    let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(10))).await;
                });
                Ok(ResponseStream::from(rx))
            });

        let client = Read::from_stub(mock);
        let mut reader = client
            .read_rows()
            .set_read_stream("streams/1")
            .into_reader()
            .with_offset(1_000)
            .with_backoff_policy(NoBackoff);

        assert_eq!(reader.offset(), 1_000);
        assert_eq!(reader.next().await.transpose()?.unwrap().row_count, 25);
        assert_eq!(reader.offset(), 1_025);
        assert_eq!(reader.next().await.transpose()?.unwrap().row_count, 10);
        assert_eq!(reader.offset(), 1_035);
        assert!(reader.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn next_is_cancel_safe_while_waiting_for_stream() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        let (tx, rx) = mpsc::channel(4);

        mock.expect_read_rows()
            .once()
            .return_once(move |_, _| Ok(ResponseStream::from(rx)));

        let client = Read::from_stub(mock);
        let mut reader = client
            .read_rows()
            .set_read_stream("streams/1")
            .into_reader()
            .with_backoff_policy(NoBackoff);

        tx.send(Ok(ReadRowsResponse::new().set_row_count(4)))
            .await?;
        assert_eq!(reader.next().await.transpose()?.unwrap().row_count, 4);

        // Cancel `reader.next()` via timeout while the stream has no message ready yet.
        let timed_out = tokio::time::timeout(Duration::from_millis(10), reader.next()).await;
        assert!(timed_out.is_err(), "expected timeout while stream is idle");

        // Now send the next batch on the same stream; the reader must not have dropped
        // the stream or transitioned to Terminated(None).
        tx.send(Ok(ReadRowsResponse::new().set_row_count(6)))
            .await?;
        drop(tx);

        assert_eq!(reader.next().await.transpose()?.unwrap().row_count, 6);
        assert_eq!(reader.offset(), 10);
        assert!(reader.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn retry_throttler_stops_retries_when_exhausted() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows()
            .once()
            .returning(|_, _| Err(transient_error()));

        let mut throttler = MockRetryThrottler::new();
        throttler.expect_on_retry_failure().once().return_const(());
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .return_const(true);

        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .once()
            .returning(|_, e| RetryResult::Continue(e));
        retry
            .expect_on_throttle()
            .withf(|state, _| state.attempt_count == 2)
            .once()
            .returning(|_, e| ThrottleResult::Exhausted(Error::exhausted(e)));

        let client = Read::from_stub(mock);
        let mut reader = client
            .read_rows()
            .set_read_stream("streams/1")
            .into_reader()
            .with_retry_policy(retry)
            .with_backoff_policy(NoBackoff)
            .with_retry_throttler(throttler);

        let err = reader
            .next()
            .await
            .expect("should return error")
            .expect_err("should be exhausted by throttler");
        assert!(err.is_exhausted(), "{err:?}");
        assert!(reader.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn negative_or_overflowing_row_count_returns_deser_error() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().once().returning(|_, _| {
            let (tx, rx) = mpsc::channel(2);
            tokio::spawn(async move {
                let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(-1))).await;
            });
            Ok(ResponseStream::from(rx))
        });

        let client = Read::from_stub(mock);
        let mut reader = client
            .read_rows()
            .set_read_stream("streams/1")
            .into_reader();

        let err = reader
            .next()
            .await
            .expect("should return error")
            .expect_err("negative row_count should error");
        assert!(err.is_deserialization(), "{err:?}");
        assert!(reader.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn permanent_error_terminates_immediately() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().once().returning(|_, _| {
            let (tx, rx) = mpsc::channel(2);
            tokio::spawn(async move {
                let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(5))).await;
                let _ = tx.send(Err(permanent_error())).await;
            });
            Ok(ResponseStream::from(rx))
        });

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req).with_backoff_policy(NoBackoff);

        assert_eq!(reader.next().await.transpose()?.unwrap().row_count, 5);
        let err = reader
            .next()
            .await
            .expect("should return error")
            .expect_err("should be permanent error");
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        assert!(reader.next().await.is_none());
        Ok(())
    }
}
