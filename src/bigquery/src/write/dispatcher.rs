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

use super::append_response::{AppendResponse, to_result};
use super::entry::StreamEntry;
use super::error::{AppendError, AppendResult};
use super::pool::StreamPool;
use super::retry_policy::RetryOptions;
use crate::Error;
use crate::google::cloud::bigquery::storage::v1::AppendRowsRequest as AppendRowsRequestProto;
use crate::model::AppendRowsRequest;
use arc_swap::ArcSwap;
use gaxi::prost::{FromProto, ToProto};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use std::sync::Arc;
use std::time::Duration;

/// Efficiently dispatches writes to a stream in a stream pool.
///
/// This struct caches a `StreamEntry` and loads it atomically on each write.
///
/// On a transient error, the `Dispatcher` notifies the `StreamPool` of the
/// failed `StreamEntry` and receives a new `StreamEntry` to use for future
/// writes.
///
/// This struct is also responsible for retrying individual writes.
#[derive(Debug)]
pub(crate) struct Dispatcher {
    pub(crate) pool: Arc<StreamPool>,
    pub(crate) entry: ArcSwap<StreamEntry>,
    pub(crate) options: RetryOptions,
}

impl Dispatcher {
    /// Creates a new `Dispatcher` for a given `StreamPool`.
    pub(crate) fn new(pool: Arc<StreamPool>, options: RetryOptions) -> Self {
        let stream = pool.get();
        Self {
            pool,
            entry: ArcSwap::from_pointee(stream),
            options,
        }
    }

    /// Send the write and process the response.
    ///
    /// Evicts and updates its cached stream on terminal stream errors.
    pub(crate) async fn send(&self, req: AppendRowsRequest) -> AppendResult<AppendResponse> {
        let req = req.to_proto().map_err(Error::ser)?;

        // The default stream has at-least-once semantics, so all writes are
        // idempotent.
        let mut state = RetryState::new(true);
        loop {
            state.attempt_count += 1;
            let timeout = effective_timeout(
                self.options.attempt_timeout,
                self.options.retry_policy.remaining_time(&state),
            );
            let err = match self.send_one_attempt(req.clone(), timeout).await {
                Ok(resp) => return Ok(resp),
                Err(err) => err,
            };
            let err = match err {
                // RowErrors are always permanent.
                AppendError::RowErrors(_) => return Err(err),

                // Adapt the error into a `gax::Error::io()`. This lets us reuse
                // the standard gax retry and backoff policy interfaces.
                //
                // We could always retry these requests, but that may be
                // surprising to an application that supplies a policy with an
                // attempt limit.
                AppendError::UnexpectedEndOfStream => {
                    let err = Error::io(AppendError::UnexpectedEndOfStream);
                    match self.options.retry_policy.on_error(&state, err) {
                        RetryResult::Continue(e) => e,
                        RetryResult::Exhausted(_) | RetryResult::Permanent(_) => {
                            // Return the original error.
                            return Err(AppendError::UnexpectedEndOfStream);
                        }
                    }
                }

                AppendError::Rpc { source } => {
                    match self.options.retry_policy.on_error(&state, source) {
                        RetryResult::Continue(e) => e,
                        RetryResult::Exhausted(e) | RetryResult::Permanent(e) => {
                            return Err(e.into());
                        }
                    }
                }
            };

            // Give up if the retry loop expires before the next attempt could
            // start. Note that we query the policy again, as the attempt above
            // consumed some of the remaining time.
            let delay = self.options.backoff_policy.on_failure(&state);
            if self
                .options
                .retry_policy
                .remaining_time(&state)
                .is_some_and(|remaining| remaining <= delay)
            {
                return Err(Error::exhausted(err).into());
            }
            tokio::time::sleep(delay).await;
        }
    }

    /// Makes one attempt to send the write and process the response.
    ///
    /// Evicts the cached stream if the stream itself fails. Errors reported in
    /// the response leave the stream in the pool, as it is still healthy.
    async fn send_one_attempt(
        &self,
        req: AppendRowsRequestProto,
        timeout: Option<Duration>,
    ) -> AppendResult<AppendResponse> {
        let stream = self.entry.load_full();
        let stream_id = stream.id;

        // A timeout abandons the write, but does not remove it from the
        // stream's queue, so the service may still receive it. That is
        // acceptable, because the default stream has at-least-once semantics.
        // The late response is discarded by the runner, not misattributed to
        // another write.
        let result = match timeout {
            None => stream.send(req).await,
            Some(timeout) => match tokio::time::timeout(timeout, stream.send(req)).await {
                Ok(result) => result,
                Err(_) => Err(Error::timeout("the write attempt timed out").into()),
            },
        };

        let resp = match result {
            Ok(resp) => resp,
            Err(err) => {
                // Any error here means the stream is dead. Either the runner
                // task exited (`UnexpectedEndOfStream`), or it forwarded a
                // stream-level gRPC error, or it is not responding. Note that
                // `AppendError::RowErrors` cannot appear here. It is produced
                // by `to_result()` below.
                //
                // It is fine to replace the stream entry on a typically
                // permanent error, as streams are lazily initialized.

                // Atomically evicts failed_id and returns a new stream for use.
                let new_stream = self.pool.evict_and_replace(stream_id);

                // The application can `send()` multiple writes concurrently.
                // Only one `send()` will update the cached stream.
                let _ = self.entry.compare_and_swap(&stream, Arc::new(new_stream));

                return Err(err);
            }
        };

        let resp = resp.cnv().map_err(Error::deser)?;
        to_result(resp)
    }
}

/// Computes the time budget for an attempt.
///
/// The attempt cannot outlast the retry loop, so this is the smaller of the
/// attempt timeout and the time remaining in the retry loop.
fn effective_timeout(
    attempt_timeout: Option<Duration>,
    remaining_time: Option<Duration>,
) -> Option<Duration> {
    match (attempt_timeout, remaining_time) {
        (None, None) => None,
        (None, Some(t)) | (Some(t), None) => Some(t),
        (Some(a), Some(r)) => Some(std::cmp::min(a, r)),
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::AppendError;
    use super::super::pool::StreamPoolOptions;
    use super::super::retry_policy::RetryableErrors;
    use super::*;
    use crate::google::cloud::bigquery::storage::v1;
    use crate::write::test::*;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::{Response as TonicResponse, Status as TonicStatus};
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::retry_policy::{NeverRetry, RetryPolicy, RetryPolicyExt};
    use google_cloud_gax::retry_result::RetryResult;
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_gax::throttle_result::ThrottleResult;
    use std::error::Error as _;
    use std::time::Duration;
    use test_case::test_case;
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinSet;

    mockall::mock! {
        #[derive(Debug)]
        pub RetryPolicy {}
        impl RetryPolicy for RetryPolicy {
            fn on_error(&self, state: &RetryState, error: Error) -> RetryResult;
            fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult;
            fn remaining_time(&self, state: &RetryState) -> Option<Duration>;
        }
    }

    /// A `MockRetryPolicy` without an overall deadline.
    ///
    /// Tests that care about the deadline set their own expectation.
    fn mock_retry_policy() -> MockRetryPolicy {
        let mut retry = MockRetryPolicy::new();
        retry.expect_remaining_time().returning(|_| None);
        retry
    }

    fn test_req() -> AppendRowsRequest {
        AppendRowsRequest::new()
    }

    #[tokio::test]
    async fn success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Arc::new(Dispatcher::new(pool, RetryOptions::default()));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write1 = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };
        let write2 = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        // Respond to the writes
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        assert_eq!(write1.await??.offset, Some(1));

        response_tx.send(Ok(convert(&test_response(2)))).await?;
        assert_eq!(write2.await??.offset, Some(2));

        // Verify we are still on the same stream.
        assert_eq!(dispatcher.entry.load().id, 1);

        Ok(())
    }

    #[tokio::test]
    async fn stream_closed() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        // The dispatcher adapts `UnexpectedEndOfStream` into a `gax` io error
        // so it can consult the standard retry policy interface.
        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|_, e: &Error| e.is_io())
            .once()
            .returning(|_, e| RetryResult::Permanent(e));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Arc::new(Dispatcher::new(
            pool,
            RetryOptions {
                retry_policy: Arc::new(retry),
                backoff_policy: Arc::new(NoBackoff),
                ..test_retry_options()
            },
        ));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        // Simulate the stream closing before responding to the request.
        drop(response_tx);

        // The caller sees the original error, not the adapted io error.
        let err = write.await?.expect_err("should return an error");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));

        // We ran into a transient error. We should now have a new stream.
        assert_eq!(dispatcher.entry.load().id, 2);

        Ok(())
    }

    #[tokio::test]
    async fn rpc_error_evicts_stream() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Arc::new(Dispatcher::new(pool.clone(), RetryOptions::default()));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        // Simulate a stream-level error. The error is not retryable, but the
        // stream is still dead.
        response_tx
            .send(Err(TonicStatus::failed_precondition("fail")))
            .await?;

        let err = write.await?.expect_err("should return an error");
        assert!(matches!(err, AppendError::Rpc { source: _ }));

        // The stream terminated, so it should not remain in the pool.
        assert_eq!(dispatcher.entry.load().id, 2);
        assert_eq!(pool.stream_ids(), [2]);

        Ok(())
    }

    #[tokio::test]
    async fn transient_error_evict_contention() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Arc::new(Dispatcher::new(
            pool.clone(),
            RetryOptions {
                retry_policy: Arc::new(NeverRetry),
                backoff_policy: Arc::new(NoBackoff),
                ..test_retry_options()
            },
        ));
        assert_eq!(dispatcher.entry.load().id, 1);

        let mut writes = JoinSet::new();
        for _ in 0..1000 {
            let d = dispatcher.clone();
            writes.spawn(async move { d.send(test_req()).await });
        }

        // Simulate the stream closing before responding to the requests.
        drop(response_tx);

        while let Some(write) = writes.join_next().await {
            let err = write?.expect_err("should return an error");
            assert!(matches!(err, AppendError::UnexpectedEndOfStream));
        }

        // We ran into a transient error. We should now have a new stream. Only
        // one of the callers should have evicted the failed stream.
        assert_eq!(dispatcher.entry.load().id, 2);
        assert_eq!(pool.stream_ids(), [2]);

        Ok(())
    }

    #[tokio::test]
    async fn writes_bypass_pool_lock() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Arc::new(Dispatcher::new(pool.clone(), RetryOptions::default()));

        // Acquire the stream pool's lock to simulate a pool scaling event. This
        // needs to run in a separate thread because we don't want to hold the
        // `std::sync::MutexGuard` across `await` points.
        let (lock_acquired_tx, lock_acquired_rx) = oneshot::channel();
        let (release_lock_tx, release_lock_rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let _guard = pool.lock();
            let _ = lock_acquired_tx.send(());
            let _ = release_lock_rx.recv();
        });

        // Wait until the lock is acquired to send a write.
        lock_acquired_rx.await?;
        let write = tokio::spawn(async move { dispatcher.send(test_req()).await });

        // Verify the write goes through, even with the pool's lock held.
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        assert_eq!(write.await??.offset, Some(1));

        // Release the lock
        drop(release_lock_tx);

        Ok(())
    }

    #[tokio::test]
    async fn retry_then_success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        // Fail to open the first stream.
        mock.expect_append_rows()
            .once()
            .return_once(|_| Err(TonicStatus::unavailable("try again")));
        // The retry opens a new stream, which succeeds.
        mock.expect_append_rows()
            .once()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|_, e: &Error| e.status().is_some_and(|s| s.code == Code::Unavailable))
            .once()
            .returning(|_, e| RetryResult::Continue(e));

        let mut backoff = MockBackoffPolicy::new();
        backoff
            .expect_on_failure()
            .once()
            .return_const(Duration::ZERO);

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Arc::new(Dispatcher::new(
            pool.clone(),
            RetryOptions {
                retry_policy: Arc::new(retry),
                backoff_policy: Arc::new(backoff),
                ..test_retry_options()
            },
        ));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        // Respond to the write.
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        assert_eq!(write.await??.offset, Some(1));

        // Verify the failed stream was replaced.
        assert_eq!(dispatcher.entry.load().id, 2);
        assert_eq!(pool.stream_ids(), [2]);

        Ok(())
    }

    #[tokio::test]
    async fn retry_exhausted() -> anyhow::Result<()> {
        const NUM_ATTEMPTS: u32 = 3;
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .times(NUM_ATTEMPTS as usize)
            .returning(|_| Err(TonicStatus::unavailable("try again")));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Dispatcher::new(
            pool.clone(),
            RetryOptions {
                retry_policy: Arc::new(RetryableErrors.with_attempt_limit(NUM_ATTEMPTS)),
                backoff_policy: Arc::new(NoBackoff),
                ..test_retry_options()
            },
        );

        let err = dispatcher
            .send(test_req())
            .await
            .expect_err("should return an error");
        let AppendError::Rpc { source } = err else {
            anyhow::bail!("expected an RPC error, got: {err:?}");
        };
        let status = source.status().expect("the error should have a status");
        assert_eq!(status.code, Code::Unavailable);
        assert_eq!(status.message, "try again");

        let expected_stream_id = 1 + NUM_ATTEMPTS as u64;
        assert_eq!(dispatcher.entry.load().id, expected_stream_id);
        assert_eq!(pool.stream_ids(), [expected_stream_id]);

        Ok(())
    }

    #[tokio::test]
    async fn row_errors() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        // Row errors are permanent. The policy is never consulted.
        let mut retry = mock_retry_policy();
        retry.expect_on_error().never();

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Arc::new(Dispatcher::new(
            pool.clone(),
            RetryOptions {
                retry_policy: Arc::new(retry),
                backoff_policy: Arc::new(NoBackoff),
                ..test_retry_options()
            },
        ));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        let resp = v1::AppendRowsResponse {
            row_errors: vec![v1::RowError {
                index: 42,
                code: v1::row_error::RowErrorCode::FieldsError as i32,
                message: "fail".to_string(),
            }],
            ..Default::default()
        };
        response_tx.send(Ok(convert(&resp))).await?;

        let err = write.await?.expect_err("should return an error");
        let AppendError::RowErrors(errors) = err else {
            anyhow::bail!("expected row errors, got: {err:?}");
        };
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(errors[0].index, 42);
        assert_eq!(errors[0].message, "fail");

        // The service responded on a healthy stream. It should not be evicted.
        assert_eq!(dispatcher.entry.load().id, 1);
        assert_eq!(pool.stream_ids(), [1]);

        Ok(())
    }

    #[tokio::test]
    async fn response_error() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        // Unlike row errors, errors in the response are subject to the policy.
        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|_, e: &Error| e.status().is_some_and(|s| s.code == Code::InvalidArgument))
            .once()
            .returning(|_, e| RetryResult::Permanent(e));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Arc::new(Dispatcher::new(
            pool.clone(),
            RetryOptions {
                retry_policy: Arc::new(retry),
                backoff_policy: Arc::new(NoBackoff),
                ..test_retry_options()
            },
        ));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        let resp = v1::AppendRowsResponse {
            response: Some(v1::append_rows_response::Response::Error(
                crate::google::rpc::Status {
                    code: Code::InvalidArgument as i32,
                    message: "fail".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        response_tx.send(Ok(convert(&resp))).await?;

        let err = write.await?.expect_err("should return an error");
        let AppendError::Rpc { source } = err else {
            anyhow::bail!("expected an RPC error, got: {err:?}");
        };
        let status = source.status().expect("the error should have a status");
        assert_eq!(status.code, Code::InvalidArgument);
        assert_eq!(status.message, "fail");

        // The service responded on a healthy stream. It should not be evicted.
        assert_eq!(dispatcher.entry.load().id, 1);
        assert_eq!(pool.stream_ids(), [1]);

        Ok(())
    }

    #[test_case(None, None, None)]
    #[test_case(None, Some(Duration::from_secs(1)), Some(Duration::from_secs(1)))]
    #[test_case(Some(Duration::from_secs(1)), None, Some(Duration::from_secs(1)))]
    #[test_case(
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(1))
    )]
    #[test_case(
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(1))
    )]
    fn effective_timeouts(
        attempt_timeout: Option<Duration>,
        remaining_time: Option<Duration>,
        want: Option<Duration>,
    ) {
        assert_eq!(effective_timeout(attempt_timeout, remaining_time), want);
    }

    #[tokio::test]
    async fn attempt_timeout() -> anyhow::Result<()> {
        // The first stream opens, but never responds to the write.
        let (hung_tx, hung_rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .once()
            .return_once(move |_| Ok(TonicResponse::from(hung_rx)));
        // The retry opens a new stream, which responds.
        mock.expect_append_rows()
            .once()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let mut retry = mock_retry_policy();
        retry
            .expect_on_error()
            .withf(|_, e: &Error| e.is_timeout())
            .once()
            .returning(|_, e| RetryResult::Continue(e));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Arc::new(Dispatcher::new(
            pool.clone(),
            RetryOptions {
                retry_policy: Arc::new(retry),
                attempt_timeout: Some(Duration::from_millis(100)),
                ..test_retry_options()
            },
        ));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        // Respond to the write on the second stream.
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        assert_eq!(write.await??.offset, Some(1));

        // The unresponsive stream should be evicted.
        assert_eq!(dispatcher.entry.load().id, 2);
        assert_eq!(pool.stream_ids(), [2]);

        // Holding this sender is what keeps the first stream unresponsive.
        drop(hung_tx);

        Ok(())
    }

    #[tokio::test]
    async fn deadline_exhausted() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .once()
            .return_once(|_| Err(TonicStatus::unavailable("try again")));

        // The policy would retry, but the backoff outlasts the retry loop.
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

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));
        let dispatcher = Dispatcher::new(
            pool,
            RetryOptions {
                retry_policy: Arc::new(retry),
                backoff_policy: Arc::new(backoff),
                ..test_retry_options()
            },
        );

        let err = dispatcher
            .send(test_req())
            .await
            .expect_err("should return an error");
        let AppendError::Rpc { source } = err else {
            anyhow::bail!("expected an RPC error, got: {err:?}");
        };
        assert!(source.is_exhausted(), "{source:?}");

        // The last error is preserved.
        let last_error = source.source().expect("the error should have a source");
        assert!(
            last_error.to_string().contains("try again"),
            "{last_error:?}"
        );

        Ok(())
    }
}
