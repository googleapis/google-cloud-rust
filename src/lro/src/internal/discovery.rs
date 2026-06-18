// Copyright 2025 Google LLC
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

//! This module implements LROs for discovery-based client libraries.
//!
//! The discovery-based services use a different (older) form of LROs, where the
//! "Operation" type does not include the final result, and the errors, if any,
//! are not represented using the `google.rpc.Status` proto.
//!
//! In discovery-based services, the LRO functions return an "Operation" type.
//! This type is specific to each service, that is, it is not a shared type that
//! we can name directly.
//!

use crate::{
    Poller, PollingBackoffPolicy, PollingErrorPolicy, PollingResult, Result,
    sealed::Poller as SealedPoller,
};
use google_cloud_gax::error::rpc::Status;
use google_cloud_gax::polling_state::PollingState;
use google_cloud_gax::retry_result::RetryResult;
use std::sync::Arc;

#[cfg(google_cloud_unstable_tracing)]
use super::LroRecorder;

/// Defines the trait for an "Operation" type in the discovery poller.
///
/// In discovery-based services each client library defines a different type as
/// the "Operation" type for long-running operations.
///
/// The client libraries must implement the `DiscoveryOperation` trait for this
/// type. The trait defines how to determine if an operation has completed, if
/// it completed with an error, and how to extract its name to perform
/// additional polling requests.
///
/// Extracting the error may require hand-crafted code, as it is service
/// specific and requires substantial coding.
pub trait DiscoveryOperation {
    /// Returns true if the operation has completed, with or without an error.
    fn done(&self) -> bool;

    /// Returns the name of the operation.
    ///
    /// It may be `None` in which case the polling loop stops.
    fn name(&self) -> Option<&String>;

    /// Returns the error status of the operation, if any.
    fn error(&self) -> Option<Status> {
        None
    }
}

pub fn new_discovery_poller<S, SF, Q, QF, O>(
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: S,
    query: Q,
) -> impl Poller<O, O>
where
    O: DiscoveryOperation + Send,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<O>> + Send + 'static,
    Q: FnMut(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<O>> + Send + 'static,
{
    DiscoveryPoller::new(polling_error_policy, polling_backoff_policy, start, query)
}

struct DiscoveryPoller<S, Q> {
    error_policy: Arc<dyn PollingErrorPolicy>,
    backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: Option<S>,
    query: Q,
    operation: Option<String>,
    state: PollingState,
}

impl<S, Q> DiscoveryPoller<S, Q> {
    pub fn new(
        error_policy: Arc<dyn PollingErrorPolicy>,
        backoff_policy: Arc<dyn PollingBackoffPolicy>,
        start: S,
        query: Q,
    ) -> Self {
        Self {
            error_policy,
            backoff_policy,
            start: Some(start),
            query,
            operation: None,
            state: PollingState::default(),
        }
    }
}

impl<S, Q> SealedPoller for DiscoveryPoller<S, Q>
where
    S: Send,
    Q: Send,
{
    async fn backoff(&mut self, state: &PollingState) {
        let backoff = self.backoff_policy.wait_period(state);
        tokio::time::sleep(backoff).await;
    }
}

impl<O, S, SF, Q, QF> crate::Poller<O, O> for DiscoveryPoller<S, Q>
where
    O: DiscoveryOperation + Send,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<O>> + Send + 'static,
    Q: FnMut(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<O>> + Send + 'static,
{
    async fn poll(&mut self) -> Option<PollingResult<O, O>> {
        if let Some(start) = self.start.take() {
            let result = start().await;
            #[cfg(google_cloud_unstable_tracing)]
            if let Ok(ref op) = result {
                let name = op.name();
                if let (Some(name), Some(recorder)) = (name, LroRecorder::current()) {
                    recorder.record_destination_id(name);
                }
            }
            let (op, poll) = self::handle_start(result);
            #[cfg(google_cloud_unstable_tracing)]
            self::maybe_record_completed_error(&poll);
            self.operation = op;
            return Some(poll);
        }
        if let Some(name) = self.operation.take() {
            self.state.attempt_count += 1;
            let result = (self.query)(name.clone()).await;
            let (op, poll) =
                self::handle_poll(self.error_policy.clone(), &self.state, name, result);
            #[cfg(google_cloud_unstable_tracing)]
            {
                if let (Some(next_name), Some(recorder)) = (&op, LroRecorder::current()) {
                    recorder.record_destination_id(next_name);
                }
                self::maybe_record_completed_error(&poll);
            }
            self.operation = op;
            return Some(poll);
        }
        None
    }
    async fn until_done(self) -> Result<O> {
        crate::until_done(self).await
    }

    #[cfg(feature = "unstable-stream")]
    fn into_stream(self) -> impl futures::Stream<Item = PollingResult<O, O>> + Unpin {
        crate::into_stream(self)
    }
}

#[cfg(google_cloud_unstable_tracing)]
fn maybe_record_completed_error<O>(poll: &PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    let op_details = match poll {
        PollingResult::Completed(Ok(op)) => LroRecorder::current().zip(op.error()),
        _ => None,
    };
    if let Some((recorder, status)) = op_details {
        recorder.span().record("otel.status_code", "ERROR");
        recorder
            .span()
            .record("otel.status_description", &status.message);
        recorder
            .span()
            .record("error.type", status.code.to_string());
    }
}

fn handle_start<O>(result: Result<O>) -> (Option<String>, PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    match result {
        Err(ref _e) => (None, PollingResult::Completed(result)),
        Ok(o) if o.done() => (None, PollingResult::Completed(Ok(o))),
        Ok(o) => handle_polling_success(o),
    }
}

fn handle_poll<O>(
    error_policy: Arc<dyn PollingErrorPolicy>,
    state: &PollingState,
    operation_name: String,
    result: Result<O>,
) -> (Option<String>, PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    match result {
        Err(e) => {
            let state = error_policy.on_error(state, e);
            handle_polling_error(state, operation_name)
        }
        Ok(o) if o.done() => (None, PollingResult::Completed(Ok(o))),
        Ok(o) => handle_polling_success(o),
    }
}

fn handle_polling_error<O>(
    state: RetryResult,
    operation_name: String,
) -> (Option<String>, PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    match state {
        RetryResult::Continue(e) => (Some(operation_name), PollingResult::PollingError(e)),
        RetryResult::Exhausted(e) | RetryResult::Permanent(e) => {
            (None, PollingResult::Completed(Err(e)))
        }
    }
}

fn handle_polling_success<O>(o: O) -> (Option<String>, PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    (o.name().cloned(), PollingResult::InProgress(Some(o)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
    use google_cloud_gax::polling_error_policy::{Aip194Strict, AlwaysContinue};
    use std::time::Duration;

    #[cfg(not(google_cloud_unstable_tracing))]
    pub(crate) struct DummySpan;

    #[cfg(not(google_cloud_unstable_tracing))]
    fn test_span() -> DummySpan {
        DummySpan
    }

    #[cfg(not(google_cloud_unstable_tracing))]
    pub(crate) trait Instrument: Sized {
        fn instrument(self, _span: DummySpan) -> Self {
            self
        }
    }

    #[cfg(not(google_cloud_unstable_tracing))]
    impl<T> Instrument for T {}

    #[cfg(google_cloud_unstable_tracing)]
    use tracing::Instrument;

    #[cfg(google_cloud_unstable_tracing)]
    fn test_span() -> tracing::Span {
        tracing::info_span!(
            "test_span",
            gcp.resource.destination.id = tracing::field::Empty,
        )
    }

    #[tokio::test]
    async fn poller_until_done_success() {
        let start = || async move {
            let op = TestOperation {
                name: Some("start-name".into()),
                ..TestOperation::default()
            };
            Ok(op)
        };
        let query = |_name| async move {
            let op = TestOperation {
                done: true,
                value: Some(42),
                ..TestOperation::default()
            };
            Ok(op)
        };
        let got = new_discovery_poller(
            Arc::new(AlwaysContinue),
            Arc::new(test_backoff()),
            start,
            query,
        )
        .until_done()
        .instrument(test_span())
        .await;
        assert!(
            matches!(
                got,
                Ok(TestOperation {
                    value: Some(42),
                    ..
                })
            ),
            "{got:?}"
        );
    }

    #[tokio::test]
    async fn poller_until_done_success_with_transient() {
        let start = || async move {
            let op = TestOperation {
                name: Some("start-name".into()),
                ..TestOperation::default()
            };
            Ok(op)
        };
        let mut query_count = 0;
        let query = move |_name| {
            query_count += 1;
            let count = query_count;
            async move {
                match count {
                    1 => Err(transient()),
                    _ => {
                        let op = TestOperation {
                            done: true,
                            value: Some(42),
                            ..TestOperation::default()
                        };
                        Ok(op)
                    }
                }
            }
        };
        let got = new_discovery_poller(
            Arc::new(AlwaysContinue),
            Arc::new(test_backoff()),
            start,
            query,
        )
        .until_done()
        .instrument(test_span())
        .await;
        assert!(
            matches!(
                got,
                Ok(TestOperation {
                    value: Some(42),
                    ..
                })
            ),
            "{got:?}"
        );
    }

    #[tokio::test]
    async fn poller_until_done_error_on_start() {
        let start = || async move { Err(Error::service(permanent_status())) };
        let query = async |_name| -> Result<TestOperation> {
            panic!();
        };
        let got = new_discovery_poller(
            Arc::new(AlwaysContinue),
            Arc::new(test_backoff()),
            start,
            query,
        )
        .until_done()
        .await;
        assert!(
            matches!(
                got,
                Err(ref e) if e.status() == Some(&permanent_status())
            ),
            "{got:?}"
        );
    }

    #[tokio::test]
    async fn poller_into_stream() {
        use futures::StreamExt;
        let start = || async move {
            let op = TestOperation {
                name: Some("start-name".into()),
                ..TestOperation::default()
            };
            Ok(op)
        };
        let query = |_name| async move {
            let op = TestOperation {
                done: true,
                value: Some(42),
                ..TestOperation::default()
            };
            Ok(op)
        };
        let mut stream = new_discovery_poller(
            Arc::new(AlwaysContinue),
            Arc::new(test_backoff()),
            start,
            query,
        )
        .into_stream();
        // The stream should return 2 Some(t) and a None.
        let got = stream.next().await;
        assert!(
            matches!(got, Some(PollingResult::InProgress(Some(_)))),
            "{got:?}"
        );
        let got = stream.next().await;
        assert!(
            matches!(
                got,
                Some(PollingResult::Completed(Ok(TestOperation {
                    value: Some(42),
                    ..
                })))
            ),
            "{got:?}"
        );
        let got = stream.next().await;
        assert!(got.is_none(), "{got:?}");
    }

    #[test]
    fn start_error() {
        let got = handle_start::<TestOperation>(Err(transient()));
        assert!(got.0.is_none(), "{got:?}");
        assert!(
            matches!(&got.1, PollingResult::Completed(Err(_))),
            "{got:?}"
        );
    }

    #[test]
    fn start_done() {
        let input = TestOperation {
            done: true,
            ..TestOperation::default()
        };
        let got = handle_start(Ok(input));
        assert!(got.0.is_none(), "{got:?}");
        assert!(matches!(&got.1, PollingResult::Completed(Ok(_))), "{got:?}");
    }

    #[test]
    fn start_in_progress() {
        let input = TestOperation {
            done: false,
            name: Some("in-progress".to_string()),
            ..TestOperation::default()
        };
        let got = handle_start(Ok(input));
        assert_eq!(got.0.as_deref(), Some("in-progress"), "{got:?}");
        assert!(
            matches!(&got.1, PollingResult::InProgress(Some(_))),
            "{got:?}"
        );
    }

    #[test]
    fn poll_error() {
        let policy = Aip194Strict;
        let state = PollingState::default();
        let got = handle_poll::<TestOperation>(
            Arc::new(policy),
            &state,
            "started".to_string(),
            Err(transient()),
        );
        assert_eq!(got.0.as_deref(), Some("started"), "{got:?}");
        assert!(matches!(got.1, PollingResult::PollingError(_)), "{got:?}");
    }

    #[test]
    fn poll_done_success() {
        let policy = Aip194Strict;
        let state = PollingState::default();
        let input = TestOperation {
            done: true,
            name: Some("in-progress".into()),
            ..TestOperation::default()
        };
        let got = handle_poll(Arc::new(policy), &state, "started".to_string(), Ok(input));
        assert!(got.0.is_none(), "{got:?}");
        assert!(matches!(got.1, PollingResult::Completed(Ok(_))), "{got:?}");
    }

    #[test]
    fn poll_in_progress() {
        let policy = Aip194Strict;
        let state = PollingState::default();
        let input = TestOperation {
            done: false,
            name: Some("in-progress".into()),
            ..TestOperation::default()
        };
        let got = handle_poll(Arc::new(policy), &state, "started".to_string(), Ok(input));
        assert_eq!(got.0.as_deref(), Some("in-progress"), "{got:?}");
        assert!(matches!(got.1, PollingResult::InProgress(_)), "{got:?}");
    }

    #[test]
    fn polling_error() {
        let got = handle_polling_error::<TestOperation>(
            RetryResult::Continue(transient()),
            "name-for-continue".to_string(),
        );
        assert_eq!(got.0.as_deref(), Some("name-for-continue"), "{got:?}");
        assert!(
            matches!(got.1, PollingResult::PollingError(ref e) if is_transient(e)),
            "{got:?}"
        );

        let got = handle_polling_error::<TestOperation>(
            RetryResult::Exhausted(transient()),
            "name-for-exhausted".to_string(),
        );
        assert!(got.0.is_none(), "{got:?}");
        assert!(
            matches!(got.1, PollingResult::Completed(Err(ref e)) if is_transient(e)),
            "{got:?}"
        );

        let got = handle_polling_error::<TestOperation>(
            RetryResult::Permanent(transient()),
            "name-for-permanent".to_string(),
        );
        assert!(got.0.is_none(), "{got:?}");
        assert!(
            matches!(got.1, PollingResult::Completed(Err(ref e)) if is_transient(e)),
            "{got:?}"
        );
    }

    #[test]
    fn polling_success() {
        let input = TestOperation {
            name: Some("in-progress".to_string()),
            ..TestOperation::default()
        };
        let got = handle_polling_success(input);
        assert_eq!(got.0.as_deref(), Some("in-progress"), "{got:?}");
        assert!(
            matches!(&got.1, PollingResult::InProgress(Some(_))),
            "{got:?}"
        );
    }

    fn is_transient(error: &Error) -> bool {
        error.status().is_some_and(|s| s == &transient_status())
    }

    fn transient() -> Error {
        Error::service(transient_status())
    }

    fn transient_status() -> Status {
        Status::default()
            .set_code(Code::Unavailable)
            .set_message("try-again")
    }

    fn permanent_status() -> Status {
        Status::default()
            .set_code(Code::PermissionDenied)
            .set_message("uh-oh")
    }

    fn test_backoff() -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(1))
            .with_maximum_delay(Duration::from_millis(1))
            .build()
            .expect("hard-coded values should succeed")
    }

    #[derive(Debug, Default, PartialEq)]
    struct TestOperation {
        done: bool,
        name: Option<String>,
        value: Option<i32>,
    }

    impl DiscoveryOperation for TestOperation {
        fn done(&self) -> bool {
            self.done
        }
        fn name(&self) -> Option<&String> {
            self.name.as_ref()
        }
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn test_discovery_poller_tracing() {
        let guard = google_cloud_test_utils::test_layer::TestLayer::initialize();

        let start = || async move {
            let op = TestOperation {
                name: Some("discovery-operation-123".into()),
                ..TestOperation::default()
            };
            Ok(op)
        };

        let count = Arc::new(std::sync::Mutex::new(0));
        let query_count = count.clone();
        let query = move |_: String| {
            let mut c = query_count.lock().unwrap();
            *c += 1;
            let is_done = *c > 1;
            async move {
                if is_done {
                    let op = TestOperation {
                        done: true,
                        value: Some(42),
                        ..TestOperation::default()
                    };
                    Ok(op)
                } else {
                    let op = TestOperation {
                        name: Some("discovery-operation-123".into()),
                        ..TestOperation::default()
                    };
                    Ok(op)
                }
            }
        };

        let mut poller = DiscoveryPoller::new(
            Arc::new(AlwaysContinue),
            Arc::new(test_backoff()),
            start,
            query,
        );

        let span = test_span();
        let poller_ref = &mut poller;
        let recorder = crate::internal::LroRecorder::new(span.clone());
        let _ = recorder
            .scope(async move { poller_ref.poll().instrument(span).await })
            .await;

        {
            let captured = google_cloud_test_utils::test_layer::TestLayer::capture(&guard);
            let got = captured
                .iter()
                .find(|s| s.name == "test_span")
                .unwrap_or_else(|| panic!("missing `test_span` in captured spans: {captured:?}"));
            assert_eq!(
                got.attributes
                    .get("gcp.resource.destination.id")
                    .and_then(|v| v.as_string()),
                Some("discovery-operation-123".to_string())
            );
        }

        let span = test_span();
        let poller_ref2 = &mut poller;
        let recorder2 = crate::internal::LroRecorder::new(span.clone());
        let _ = recorder2
            .scope(async move { poller_ref2.poll().instrument(span).await })
            .await;

        {
            let captured = google_cloud_test_utils::test_layer::TestLayer::capture(&guard);
            let got = captured
                .iter()
                .find(|s| s.name == "test_span")
                .unwrap_or_else(|| panic!("missing `test_span` in captured spans: {captured:?}"));
            assert_eq!(
                got.attributes
                    .get("gcp.resource.destination.id")
                    .and_then(|v| v.as_string()),
                Some("discovery-operation-123".to_string())
            );
        }
    }
}
