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

use crate::{Error, Poller, PollingBackoffPolicy, PollingErrorPolicy, PollingResult, Result};
use gax::error::rpc::Status;
use gax::polling_state::PollingState;
use gax::retry_result::RetryResult;
use std::sync::Arc;

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

    /// Determines if the operation completed with an error.
    ///
    /// If the operation completed successfully this returns `None`. On an error
    /// the trait must convert the error details to `gax::error::rpc::Status` as
    /// it always indicates a service error.
    fn status(&self) -> Option<Status>;
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

impl<S, Q> crate::sealed::Poller for DiscoveryPoller<S, Q> {}

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
            let (op, poll) = self::handle_start(result);
            self.operation = op;
            return Some(poll);
        }
        if let Some(name) = self.operation.take() {
            self.state.attempt_count += 1;
            let result = (self.query)(name.clone()).await;
            let (op, poll) =
                self::handle_poll(self.error_policy.clone(), &self.state, name, result);
            self.operation = op;
            return Some(poll);
        }
        None
    }

    async fn until_done(mut self) -> Result<O> {
        while let Some(p) = self.poll().await {
            match p {
                // Return, the operation completed or the polling policy is
                // exhausted.
                PollingResult::Completed(r) => return r,
                // Continue, the operation was successfully polled and the
                // polling policy was queried.
                PollingResult::InProgress(_) => (),
                // Continue, the polling policy was queried and decided the
                // error is recoverable.
                PollingResult::PollingError(_) => (),
            }
            tokio::time::sleep(self.backoff_policy.wait_period(&self.state)).await;
        }
        // We can only get here if `poll()` returns `None`, but it only returns
        // `None` after it returned `Polling::Completed` and therefore this is
        // never reached.
        unreachable!("loop should exit via the `Completed` branch vs. this line");
    }

    #[cfg(feature = "unstable-stream")]
    fn into_stream(self) -> impl futures::Stream<Item = PollingResult<O, O>> + Unpin {
        use futures::stream::unfold;
        Box::pin(unfold(Some(self), move |state| async move {
            if let Some(mut poller) = state {
                if let Some(pr) = poller.poll().await {
                    return Some((pr, Some(poller)));
                }
            };
            None
        }))
    }
}

fn handle_start<O>(result: Result<O>) -> (Option<String>, PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    match result {
        Err(ref _e) => (None, PollingResult::Completed(result)),
        Ok(o) if o.done() => (None, handle_polling_done(o)),
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
            self::handle_polling_error(state, operation_name)
        }
        Ok(o) if o.done() => (None, handle_polling_done(o)),
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

fn handle_polling_done<O>(o: O) -> PollingResult<O, O>
where
    O: DiscoveryOperation,
{
    match o.status() {
        None => PollingResult::Completed(Ok(o)),
        Some(s) => PollingResult::Completed(Err(Error::service(s))),
    }
}

fn handle_polling_success<O>(o: O) -> (Option<String>, PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    match o.status() {
        Some(s) => (None, PollingResult::Completed(Err(Error::service(s)))),
        None => (o.name().cloned(), PollingResult::InProgress(Some(o))),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use gax::error::rpc::Code;
    use gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
    use gax::polling_error_policy::{Aip194Strict, AlwaysContinue};

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
    async fn poller_until_done_error_on_done() {
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
                status: Some(permanent_status()),
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
    fn start_done_with_error() {
        let input = TestOperation {
            done: true,
            status: Some(permanent_status()),
            ..TestOperation::default()
        };
        let got = handle_start(Ok(input));
        assert!(got.0.is_none(), "{got:?}");
        assert!(
            matches!(&got.1, PollingResult::Completed(Err(_))),
            "{got:?}"
        );
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
            status: None,
            ..TestOperation::default()
        };
        let got = handle_poll(Arc::new(policy), &state, "started".to_string(), Ok(input));
        assert!(got.0.is_none(), "{got:?}");
        assert!(matches!(got.1, PollingResult::Completed(Ok(_))), "{got:?}");
    }

    #[test]
    fn poll_done_error() {
        let policy = Aip194Strict;
        let state = PollingState::default();
        let input = TestOperation {
            done: true,
            name: Some("in-progress".into()),
            status: Some(permanent_status()),
            ..TestOperation::default()
        };
        let got = handle_poll(Arc::new(policy), &state, "started".to_string(), Ok(input));
        assert!(got.0.is_none(), "{got:?}");
        assert!(matches!(got.1, PollingResult::Completed(Err(_))), "{got:?}");
    }

    #[test]
    fn poll_in_progress() {
        let policy = Aip194Strict;
        let state = PollingState::default();
        let input = TestOperation {
            done: false,
            name: Some("in-progress".into()),
            status: None,
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
    fn polling_done() {
        let input = TestOperation {
            status: Some(transient_status()),
            ..TestOperation::default()
        };
        let got = handle_polling_done(input);
        assert!(
            matches!(&got, PollingResult::Completed(Err(e)) if e.status() == Some(&transient_status())),
            "{got:?}"
        );

        let input = TestOperation {
            status: None,
            ..TestOperation::default()
        };
        let got = handle_polling_done(input);
        assert!(matches!(&got, PollingResult::Completed(Ok(_))), "{got:?}");
    }

    #[test]
    fn polling_success() {
        let input = TestOperation {
            status: Some(transient_status()),
            ..TestOperation::default()
        };
        let got = handle_polling_success(input);
        assert!(got.0.is_none(), "{got:?}");
        assert!(
            matches!(&got.1, PollingResult::Completed(Err(e)) if e.status() == Some(&transient_status())),
            "{got:?}"
        );

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
        status: Option<gax::error::rpc::Status>,
        value: Option<i32>,
    }

    impl DiscoveryOperation for TestOperation {
        fn done(&self) -> bool {
            self.done
        }
        fn name(&self) -> Option<&String> {
            self.name.as_ref()
        }
        fn status(&self) -> Option<Status> {
            self.status.clone()
        }
    }
}
