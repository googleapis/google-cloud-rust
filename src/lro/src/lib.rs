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

//! Types and functions to make LROs easier to use and to require less boilerplate.

use gax::Result;
use gax::error::Error;
use gax::polling_backoff_policy::PollingBackoffPolicy;
use gax::polling_error_policy::PollingErrorPolicy;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

/// The result of polling a Long-Running Operation (LRO).
///
/// # Parameters
/// * `R` - the response type. This is the type returned when the LRO completes
///   successfully.
/// * `M` - the metadata type. While operations are in progress the LRO may
///   return values of this type.
#[derive(Debug)]
pub enum PollingResult<R, M> {
    /// The operation is still in progress.
    InProgress(Option<M>),
    /// The operation completed. This includes the result.
    Completed(Result<R>),
    /// An error trying to poll the LRO.
    ///
    /// Not all errors indicate that the operation failed. For example, this
    /// may fail because it was not possible to connect to Google Cloud. Such
    /// transient errors may disappear in the next polling attempt.
    ///
    /// Other errors will never recover. For example, a [ServiceError] with
    /// a [NOT_FOUND], [ABORTED], or [PERMISSION_DENIED] code will never
    /// recover.
    ///
    /// [ServiceError]: gax::error::ServiceError
    /// [NOT_FOUND]: rpc::model::Code::NOT_FOUND
    /// [ABORTED]: rpc::model::Code::ABORTED
    /// [PERMISSION_DENIED]: rpc::model::Code::PERMISSION_DENIED
    PollingError(Error),
}

/// A wrapper around [longrunning::model::Operation] with typed responses.
///
/// This is intended as an implementation detail of the generated clients.
/// Applications should have no need to create or use this struct.
#[doc(hidden)]
pub struct Operation<R, M> {
    inner: longrunning::model::Operation,
    response: std::marker::PhantomData<R>,
    metadata: std::marker::PhantomData<M>,
}

impl<R, M> Operation<R, M> {
    pub fn new(inner: longrunning::model::Operation) -> Self {
        Self {
            inner,
            response: PhantomData,
            metadata: PhantomData,
        }
    }

    fn name(&self) -> String {
        self.inner.name.clone()
    }
    fn done(&self) -> bool {
        self.inner.done
    }
    fn metadata(&self) -> Option<&wkt::Any> {
        self.inner.metadata.as_ref()
    }
    fn response(&self) -> Option<&wkt::Any> {
        use longrunning::model::operation::Result;
        self.inner.result.as_ref().and_then(|r| match r {
            Result::Error(_) => None,
            Result::Response(r) => Some(r.as_ref()),
            _ => None,
        })
    }
    fn error(&self) -> Option<&rpc::model::Status> {
        use longrunning::model::operation::Result;
        self.inner.result.as_ref().and_then(|r| match r {
            Result::Error(rpc) => Some(rpc.as_ref()),
            Result::Response(_) => None,
            _ => None,
        })
    }
}

/// The trait implemented by LRO helpers.
///
/// # Parameters
/// * `R` - the response type, that is, the type of response included when the
///   long-running operation completes successfully.
/// * `M` - the metadata type, that is, the type returned by the service when
///   the long-running operation is still in progress.
pub trait Poller<R, M> {
    /// Query the current status of the long-running operation.
    fn poll(&mut self) -> impl Future<Output = Option<PollingResult<R, M>>>;

    /// Poll the long-running operation until it completes.
    fn until_done(self) -> impl Future<Output = Result<R>>;

    /// Convert a poller to a [futures::Stream].
    #[cfg(feature = "unstable-stream")]
    fn into_stream(self) -> impl futures::Stream<Item = PollingResult<R, M>>;
}

/// Creates a new `impl Poller<R, M>` from the closures created by the generator.
///
/// This is intended as an implementation detail of the generated clients.
/// Applications should have no need to create or use this struct.
#[doc(hidden)]
pub fn new_poller<ResponseType, MetadataType, S, SF, Q, QF>(
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: S,
    query: Q,
) -> impl Poller<ResponseType, MetadataType>
where
    ResponseType: wkt::message::Message + serde::de::DeserializeOwned,
    MetadataType: wkt::message::Message + serde::de::DeserializeOwned,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
{
    PollerImpl::new(polling_error_policy, polling_backoff_policy, start, query)
}

/// An implementation of `Poller` based on closures.
///
/// Thanks to this implementation, the code generator (`sidekick`) needs to
/// produce two closures: one to start the operation, and one to query progress.
///
/// Applications should not need to create this type, or use it directly. It is
/// only public so the generated code can use it.
///
/// # Parameters
/// * `ResponseType` - the response type. Typically this is a message
///   representing the final disposition of the long-running operation.
/// * `MetadataType` - the metadata type. The data included with partially
///   completed instances of this long-running operations.
/// * `S` - the start closure. Starts a LRO. This implementation expects that
///   all necessary parameters, and request options, including retry options
///   are captured by this function.
/// * `SF` - the type of future returned by `S`.
/// * `Q` - the query closure. Queries the status of the LRO created by `start`.
///   It receives the name of the operation as its only input parameter. It
///   should have captured any stubs and request options.
/// * `QF` - the type of future returned by `Q`.
struct PollerImpl<ResponseType, MetadataType, S, SF, Q, QF>
where
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
{
    error_policy: Arc<dyn PollingErrorPolicy>,
    backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: Option<S>,
    query: Q,
    operation: Option<String>,
    loop_start: Instant,
    attempt_count: u32,
}

impl<ResponseType, MetadataType, S, SF, Q, QF> PollerImpl<ResponseType, MetadataType, S, SF, Q, QF>
where
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
{
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
            loop_start: Instant::now(),
            attempt_count: 0,
        }
    }
}

impl<ResponseType, MetadataType, S, SF, P, PF> Poller<ResponseType, MetadataType>
    for PollerImpl<ResponseType, MetadataType, S, SF, P, PF>
where
    ResponseType: wkt::message::Message + serde::de::DeserializeOwned,
    MetadataType: wkt::message::Message + serde::de::DeserializeOwned,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
    P: Fn(String) -> PF + Send + Sync + Clone,
    PF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
{
    async fn poll(&mut self) -> Option<PollingResult<ResponseType, MetadataType>> {
        if let Some(start) = self.start.take() {
            let result = start().await;
            let (op, poll) = details::handle_start(result);
            self.operation = op;
            return Some(poll);
        }
        if let Some(name) = self.operation.take() {
            self.attempt_count += 1;
            let result = (self.query)(name.clone()).await;
            let (op, poll) = details::handle_poll(
                self.error_policy.clone(),
                self.loop_start,
                self.attempt_count,
                name,
                result,
            );
            self.operation = op;
            return Some(poll);
        }
        None
    }

    async fn until_done(mut self) -> Result<ResponseType> {
        let loop_start = std::time::Instant::now();
        let mut attempt_count = 0;
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
            attempt_count += 1;
            tokio::time::sleep(self.backoff_policy.wait_period(loop_start, attempt_count)).await;
        }
        // We can only get here if `poll()` returns `None`, but it only returns
        // `None` after it returned `Polling::Completed` and therefore this is
        // never reached.
        unreachable!("loop should exit via the `Completed` branch vs. this line");
    }

    #[cfg(feature = "unstable-stream")]
    fn into_stream(self) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>>
    where
        ResponseType: wkt::message::Message + serde::de::DeserializeOwned,
        MetadataType: wkt::message::Message + serde::de::DeserializeOwned,
    {
        use futures::stream::unfold;
        unfold(Some(self), move |state| async move {
            if let Some(mut poller) = state {
                if let Some(pr) = poller.poll().await {
                    return Some((pr, Some(poller)));
                }
            };
            None
        })
    }
}

mod details;

#[cfg(test)]
mod test {
    use super::*;
    use gax::exponential_backoff::ExponentialBackoff;
    use gax::exponential_backoff::ExponentialBackoffBuilder;
    use gax::polling_error_policy::*;
    use std::time::Duration;

    type ResponseType = wkt::Duration;
    type MetadataType = wkt::Timestamp;
    type TestOperation = Operation<ResponseType, MetadataType>;

    #[test]
    fn typed_operation_with_metadata() -> Result<()> {
        let any = wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))
            .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
        let op = longrunning::model::Operation::default()
            .set_name("test-only-name")
            .set_metadata(any);
        let op = TestOperation::new(op);
        assert_eq!(op.name(), "test-only-name");
        assert!(!op.done());
        assert!(op.metadata().is_some());
        assert!(op.response().is_none());
        assert!(op.error().is_none());
        let got = op
            .metadata()
            .unwrap()
            .try_into_message::<wkt::Timestamp>()
            .map_err(Error::other)?;
        assert_eq!(got, wkt::Timestamp::clamp(123, 0));

        Ok(())
    }

    #[test]
    fn typed_operation_with_response() -> Result<()> {
        let any = wkt::Any::try_from(&wkt::Duration::clamp(23, 0))
            .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
        let op = longrunning::model::Operation::default()
            .set_name("test-only-name")
            .set_result(longrunning::model::operation::Result::Response(any.into()));
        let op = TestOperation::new(op);
        assert_eq!(op.name(), "test-only-name");
        assert!(!op.done());
        assert!(op.metadata().is_none());
        assert!(op.response().is_some());
        assert!(op.error().is_none());
        let got = op
            .response()
            .unwrap()
            .try_into_message::<wkt::Duration>()
            .map_err(Error::other)?;
        assert_eq!(got, wkt::Duration::clamp(23, 0));

        Ok(())
    }

    #[test]
    fn typed_operation_with_error() -> Result<()> {
        let rpc = rpc::model::Status::default()
            .set_message("test only")
            .set_code(16);
        let op = longrunning::model::Operation::default()
            .set_name("test-only-name")
            .set_result(longrunning::model::operation::Result::Error(
                rpc.clone().into(),
            ));
        let op = TestOperation::new(op);
        assert_eq!(op.name(), "test-only-name");
        assert!(!op.done());
        assert!(op.metadata().is_none());
        assert!(op.response().is_none());
        assert!(op.error().is_some());
        let got = op.error().unwrap();
        assert_eq!(got, &rpc);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_basic_flow() {
        let start = || async move {
            let any = wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))
                .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::try_from(&wkt::Duration::clamp(234, 0))
                .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
            let op = TestOperation::new(op);

            Ok::<TestOperation, Error>(op)
        };

        let mut poller = PollerImpl::new(
            Arc::new(AlwaysContinue),
            Arc::new(ExponentialBackoff::default()),
            start,
            query,
        );
        let p0 = poller.poll().await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(wkt::Timestamp::clamp(123, 0)));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p1 = poller.poll().await;
        match p1.unwrap() {
            PollingResult::Completed(r) => {
                let response = r.unwrap();
                assert_eq!(response, wkt::Duration::clamp(234, 0));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p2 = poller.poll().await;
        assert!(p2.is_none(), "{p2:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_basic_stream() {
        let start = || async move {
            let any = wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))
                .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::try_from(&wkt::Duration::clamp(234, 0))
                .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
            let op = TestOperation::new(op);

            Ok::<TestOperation, Error>(op)
        };

        use futures::StreamExt;
        let mut stream = new_poller(
            Arc::new(AlwaysContinue),
            Arc::new(ExponentialBackoff::default()),
            start,
            query,
        )
        .into_stream();
        let mut stream = std::pin::pin!(stream);
        let p0 = stream.next().await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(wkt::Timestamp::clamp(123, 0)));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p1 = stream.next().await;
        match p1.unwrap() {
            PollingResult::Completed(r) => {
                let response = r.unwrap();
                assert_eq!(response, wkt::Duration::clamp(234, 0));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p2 = stream.next().await;
        assert!(p2.is_none(), "{p2:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn until_done_basic_flow() -> Result<()> {
        let start = || async move {
            let any = wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))
                .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::try_from(&wkt::Duration::clamp(234, 0))
                .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
            let op = TestOperation::new(op);

            Ok::<TestOperation, Error>(op)
        };

        let poller = PollerImpl::new(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        let response = poller.until_done().await?;
        assert_eq!(response, wkt::Duration::clamp(234, 0));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn until_done_with_recoverable_polling_error() -> Result<()> {
        let start = || async move {
            let any = wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))
                .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let count = Arc::new(std::sync::Mutex::new(0_u32));
        let query = move |_: String| {
            let mut guard = count.lock().unwrap();
            let c = *guard;
            *guard = c + 1;
            drop(guard);
            async move {
                if c == 0 {
                    return Err::<TestOperation, Error>(Error::other(
                        "recoverable (see policy below)",
                    ));
                }
                let any = wkt::Any::try_from(&wkt::Duration::clamp(234, 0))
                    .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
                let result = longrunning::model::operation::Result::Response(any.into());
                let op = longrunning::model::Operation::default()
                    .set_done(true)
                    .set_result(result);
                let op = TestOperation::new(op);

                Ok::<TestOperation, Error>(op)
            }
        };

        let poller = PollerImpl::new(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        let response = poller.until_done().await?;
        assert_eq!(response, wkt::Duration::clamp(234, 0));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn until_done_with_unrecoverable_polling_error() -> Result<()> {
        let start = || async move {
            let any = wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))
                .map_err(|e| Error::other(format!("unexpected error in Any::try_from {e}")))?;
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = move |_: String| async move {
            Err::<TestOperation, Error>(Error::other("unrecoverable (see policy below)"))
        };

        let poller = PollerImpl::new(
            Arc::new(Aip194Strict),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        let response = poller.until_done().await;
        assert!(response.is_err());
        assert!(
            format!("{response:?}").contains("unrecoverable"),
            "{response:?}"
        );

        Ok(())
    }
}
