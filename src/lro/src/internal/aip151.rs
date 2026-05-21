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

//! This module contains common implementation details for generated code.
//!
//! It is not part of the public API of this crate. Types and functions in this
//! module may be changed or removed without warnings. Applications should not
//! use any types contained within.

use crate::{
    Poller, PollingBackoffPolicy, PollingErrorPolicy, PollingResult, Result,
    sealed::Poller as SealedPoller,
};
use google_cloud_gax::polling_state::PollingState;
use google_cloud_wkt::Empty;
use google_cloud_wkt::message::Message;
use std::sync::Arc;

pub type Operation<R, M> = crate::details::Operation<R, M>;

/// Creates a new `impl Poller<R, M>` from the closures created by the generator.
///
/// This is intended as an implementation detail of the generated clients.
/// Applications should have no need to use this function directly.
pub fn new_poller<ResponseType, MetadataType, S, SF, Q, QF>(
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: S,
    query: Q,
) -> impl Poller<ResponseType, MetadataType>
where
    ResponseType: Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    MetadataType: Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
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

/// Creates a new `impl Poller<(), M>` from the closures created by the generator.
///
/// This is intended as an implementation detail of the generated clients.
/// Applications should have no need to use this function directly.
pub fn new_unit_response_poller<MetadataType, S, SF, Q, QF>(
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: S,
    query: Q,
) -> impl Poller<(), MetadataType>
where
    MetadataType: Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<Empty, MetadataType>>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<Operation<Empty, MetadataType>>> + Send + 'static,
{
    let poller = new_poller(polling_error_policy, polling_backoff_policy, start, query);
    UnitResponsePoller::new(poller)
}

/// Creates a new `impl Poller<(), M>` from the closures created by the generator.
///
/// This is intended as an implementation detail of the generated clients.
/// Applications should have no need to use this function directly.
pub fn new_unit_metadata_poller<ResponseType, S, SF, Q, QF>(
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: S,
    query: Q,
) -> impl Poller<ResponseType, ()>
where
    ResponseType: Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<ResponseType, Empty>>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<Operation<ResponseType, Empty>>> + Send + 'static,
{
    let poller = new_poller(polling_error_policy, polling_backoff_policy, start, query);
    UnitMetadataPoller::new(poller)
}

/// Creates a new `impl Poller<(), ()>` from the closures created by the generator.
///
/// This is intended as an implementation detail of the generated clients.
/// Applications should have no need to use this function directly.
pub fn new_unit_poller<S, SF, Q, QF>(
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: S,
    query: Q,
) -> impl Poller<(), ()>
where
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<Empty, Empty>>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<Operation<Empty, Empty>>> + Send + 'static,
{
    let poller = new_poller(polling_error_policy, polling_backoff_policy, start, query);
    UnitResponsePoller::new(UnitMetadataPoller::new(poller))
}

struct UnitResponsePoller<P> {
    poller: P,
}

impl<P> UnitResponsePoller<P> {
    pub(crate) fn new(poller: P) -> Self {
        Self { poller }
    }
}

impl<P> SealedPoller for UnitResponsePoller<P>
where
    P: SealedPoller + Send,
{
    async fn backoff(&mut self, state: &PollingState) {
        self.poller.backoff(state).await
    }
}

impl<P, M> Poller<(), M> for UnitResponsePoller<P>
where
    P: Poller<Empty, M>,
{
    async fn poll(&mut self) -> Option<PollingResult<(), M>> {
        self.poller.poll().await.map(self::map_polling_result)
    }
    async fn until_done(self) -> Result<()> {
        self.poller.until_done().await.map(|_| ())
    }
    #[cfg(feature = "unstable-stream")]
    fn into_stream(self) -> impl futures::Stream<Item = PollingResult<(), M>> + Unpin {
        use futures::StreamExt;
        self.poller.into_stream().map(self::map_polling_result)
    }
}

struct UnitMetadataPoller<P> {
    poller: P,
}

impl<P> UnitMetadataPoller<P> {
    pub(crate) fn new(poller: P) -> Self {
        Self { poller }
    }
}

impl<P> SealedPoller for UnitMetadataPoller<P>
where
    P: SealedPoller + Send,
{
    async fn backoff(&mut self, state: &PollingState) {
        self.poller.backoff(state).await
    }
}

impl<P, R> Poller<R, ()> for UnitMetadataPoller<P>
where
    P: Poller<R, Empty>,
{
    async fn poll(&mut self) -> Option<PollingResult<R, ()>> {
        self.poller.poll().await.map(self::map_polling_metadata)
    }
    async fn until_done(self) -> Result<R> {
        self.poller.until_done().await
    }
    #[cfg(feature = "unstable-stream")]
    fn into_stream(self) -> impl futures::Stream<Item = PollingResult<R, ()>> + Unpin {
        use futures::StreamExt;
        self.poller.into_stream().map(self::map_polling_metadata)
    }
}

fn map_polling_result<M>(result: PollingResult<Empty, M>) -> PollingResult<(), M> {
    match result {
        PollingResult::Completed(r) => PollingResult::Completed(r.map(|_| ())),
        PollingResult::InProgress(m) => PollingResult::InProgress(m),
        PollingResult::PollingError(e) => PollingResult::PollingError(e),
    }
}

fn map_polling_metadata<R>(result: PollingResult<R, Empty>) -> PollingResult<R, ()> {
    match result {
        PollingResult::Completed(r) => PollingResult::Completed(r),
        PollingResult::InProgress(m) => PollingResult::InProgress(m.map(|_| ())),
        PollingResult::PollingError(e) => PollingResult::PollingError(e),
    }
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
/// * `S` - the start closure. Starts a LRO. This implementation expects that
///   all necessary parameters, and request options, including retry options
///   are captured by this function.
/// * `Q` - the query closure. Queries the status of the LRO created by `start`.
///   It receives the name of the operation as its only input parameter. It
///   should have captured any stubs and request options.
struct PollerImpl<S, Q> {
    error_policy: Arc<dyn PollingErrorPolicy>,
    backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: Option<S>,
    query: Q,
    operation: Option<String>,
    state: PollingState,
}

impl<S, Q> PollerImpl<S, Q> {
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

/// Implements the `Poller` trait for `PollerImpl`.
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
impl<ResponseType, MetadataType, S, SF, P, PF> Poller<ResponseType, MetadataType>
    for PollerImpl<S, P>
where
    ResponseType: Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    MetadataType: Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
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
            #[cfg(google_cloud_unstable_tracing)]
            if let Ok(ref op) = result {
                let name = op.name();
                if let Ok(span) = crate::internal::LRO_SPAN.try_with(|s| s.clone()) {
                    span.record("gcp.resource.destination.id", &name);
                }
            }
            let (op, poll) = crate::details::handle_start(result);
            self.operation = op;
            return Some(poll);
        }
        if let Some(name) = self.operation.take() {
            self.state.attempt_count += 1;
            let result = (self.query)(name.clone()).await;
            let (op, poll) =
                crate::details::handle_poll(self.error_policy.clone(), &self.state, name, result);
            #[cfg(google_cloud_unstable_tracing)]
            if let Some(ref next_name) = op {
                if let Ok(span) = crate::internal::LRO_SPAN.try_with(|s| s.clone()) {
                    span.record("gcp.resource.destination.id", next_name);
                }
            }
            self.operation = op;
            return Some(poll);
        }
        None
    }
    async fn until_done(self) -> Result<ResponseType> {
        crate::until_done(self).await
    }

    #[cfg(feature = "unstable-stream")]
    fn into_stream(
        self,
    ) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>> + Unpin {
        crate::into_stream(self)
    }
}
impl<S, Q> SealedPoller for PollerImpl<S, Q>
where
    S: Send,
    Q: Send,
{
    async fn backoff(&mut self, state: &PollingState) {
        let backoff = self.backoff_policy.wait_period(state);
        tokio::time::sleep(backoff).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use google_cloud_gax::polling_error_policy::{Aip194Strict, AlwaysContinue};
    use google_cloud_longrunning::model::{
        Operation as OperationAny, operation::Result as ResultAny,
    };
    use google_cloud_wkt::{Any, Duration, Timestamp};
    use std::time::Duration as StdDuration;

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

    type ResponseType = Duration;
    type MetadataType = Timestamp;
    type TestOperation = Operation<ResponseType, MetadataType>;
    type EmptyResponseOperation = Operation<Empty, MetadataType>;
    type EmptyMetadataOperation = Operation<ResponseType, Empty>;

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_basic_flow() {
        let start = || async move {
            let any = Any::from_msg(&Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Duration::clamp(234, 0))
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
            let op = TestOperation::new(op);

            Ok::<TestOperation, Error>(op)
        };

        let mut poller = PollerImpl::new(
            Arc::new(AlwaysContinue),
            Arc::new(ExponentialBackoff::default()),
            start,
            query,
        );
        let p0 = poller.poll().instrument(test_span()).await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(Timestamp::clamp(123, 0)));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p1 = poller.poll().instrument(test_span()).await;
        match p1.unwrap() {
            PollingResult::Completed(r) => {
                let response = r.unwrap();
                assert_eq!(response, Duration::clamp(234, 0));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p2 = poller.poll().instrument(test_span()).await;
        assert!(p2.is_none(), "{p2:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_basic_stream() {
        let start = || async move {
            let any = Any::from_msg(&Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Duration::clamp(234, 0))
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
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
        let p0 = stream.next().await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(Timestamp::clamp(123, 0)));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p1 = stream.next().await;
        match p1.unwrap() {
            PollingResult::Completed(r) => {
                let response = r.unwrap();
                assert_eq!(response, Duration::clamp(234, 0));
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
            let any = Any::from_msg(&Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Duration::clamp(234, 0))
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
            let op = TestOperation::new(op);

            Ok::<TestOperation, Error>(op)
        };

        let poller = PollerImpl::new(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(StdDuration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        let response = poller.until_done().instrument(test_span()).await?;
        assert_eq!(response, Duration::clamp(234, 0));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unit_poll_basic_flow() {
        let start = || async move {
            let any = Any::from_msg(&Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyResponseOperation::new(op);
            Ok::<EmptyResponseOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
            let op = EmptyResponseOperation::new(op);

            Ok::<EmptyResponseOperation, Error>(op)
        };

        let mut poller = new_unit_response_poller(
            Arc::new(AlwaysContinue),
            Arc::new(ExponentialBackoff::default()),
            start,
            query,
        );
        let p0 = poller.poll().await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(Timestamp::clamp(123, 0)));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p1 = poller.poll().await;
        match p1.unwrap() {
            PollingResult::Completed(Ok(_)) => {}
            PollingResult::Completed(Err(e)) => {
                panic!("{e}");
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p2 = poller.poll().await;
        assert!(p2.is_none(), "{p2:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unit_poll_basic_stream() {
        let start = || async move {
            let any = Any::from_msg(&Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyResponseOperation::new(op);
            Ok::<EmptyResponseOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
            let op = EmptyResponseOperation::new(op);

            Ok::<EmptyResponseOperation, Error>(op)
        };

        use futures::StreamExt;
        let mut stream = new_unit_response_poller(
            Arc::new(AlwaysContinue),
            Arc::new(ExponentialBackoff::default()),
            start,
            query,
        )
        .into_stream();
        let p0 = stream.next().await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(Timestamp::clamp(123, 0)));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p1 = stream.next().await;
        match p1.unwrap() {
            PollingResult::Completed(Ok(_)) => {}
            PollingResult::Completed(Err(e)) => {
                panic!("{e}");
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p2 = stream.next().await;
        assert!(p2.is_none(), "{p2:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unit_until_done_basic_flow() -> Result<()> {
        let start = || async move {
            let any = Any::from_msg(&Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyResponseOperation::new(op);
            Ok::<EmptyResponseOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
            let op = EmptyResponseOperation::new(op);

            Ok::<EmptyResponseOperation, Error>(op)
        };

        let poller = new_unit_response_poller(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(StdDuration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        poller.until_done().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unit_metadata_poll_basic_flow() {
        let start = || async move {
            let any = Any::from_msg(&Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyMetadataOperation::new(op);
            Ok::<EmptyMetadataOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Duration::clamp(123, 456))
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
            let op = EmptyMetadataOperation::new(op);

            Ok::<EmptyMetadataOperation, Error>(op)
        };

        let mut poller = new_unit_metadata_poller(
            Arc::new(AlwaysContinue),
            Arc::new(ExponentialBackoff::default()),
            start,
            query,
        );
        let p0 = poller.poll().await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(()));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p1 = poller.poll().await;
        match p1.unwrap() {
            PollingResult::Completed(Ok(_)) => {}
            PollingResult::Completed(Err(e)) => {
                panic!("{e}");
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p2 = poller.poll().await;
        assert!(p2.is_none(), "{p2:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unit_metadata_poll_basic_stream() {
        let start = || async move {
            let any = Any::from_msg(&Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyMetadataOperation::new(op);
            Ok::<EmptyMetadataOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Duration::clamp(123, 456))
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
            let op = EmptyMetadataOperation::new(op);

            Ok::<EmptyMetadataOperation, Error>(op)
        };

        use futures::StreamExt;
        let mut stream = new_unit_metadata_poller(
            Arc::new(AlwaysContinue),
            Arc::new(ExponentialBackoff::default()),
            start,
            query,
        )
        .into_stream();
        let p0 = stream.next().await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(()));
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p1 = stream.next().await;
        match p1.unwrap() {
            PollingResult::Completed(Ok(d)) => {
                assert_eq!(d, Duration::clamp(123, 456));
            }
            PollingResult::Completed(Err(e)) => {
                panic!("{e}");
            }
            r => {
                panic!("{r:?}");
            }
        }

        let p2 = stream.next().await;
        assert!(p2.is_none(), "{p2:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unit_metadata_until_done_basic_flow() -> Result<()> {
        let start = || async move {
            let any = Any::from_msg(&Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyMetadataOperation::new(op);
            Ok::<EmptyMetadataOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Duration::clamp(123, 456))
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
            let op = EmptyMetadataOperation::new(op);

            Ok::<EmptyMetadataOperation, Error>(op)
        };

        let poller = new_unit_metadata_poller(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(StdDuration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        let d = poller.until_done().await?;
        assert_eq!(d, Duration::clamp(123, 456));
        Ok(())
    }

    #[test]
    fn unit_result_map() {
        use PollingResult::{Completed, InProgress, PollingError};
        type TestResult = PollingResult<Empty, Timestamp>;
        let got = map_polling_result(TestResult::Completed(Ok(Empty::default())));
        assert!(matches!(got, Completed(Ok(_))), "{got:?}");
        let got = map_polling_result(TestResult::Completed(Err(service_error())));
        assert!(
            matches!(&got, Completed(Err(e)) if e.status() == service_error().status()),
            "{got:?}"
        );
        let got = map_polling_result(TestResult::InProgress(None));
        assert!(matches!(got, InProgress(None)), "{got:?}");
        let got = map_polling_result(TestResult::InProgress(Some(Timestamp::clamp(123, 456))));
        assert!(matches!(got, InProgress(Some(t)) if t == Timestamp::clamp(123, 456)));
        let got = map_polling_result(TestResult::PollingError(polling_error()));
        assert!(matches!(&got, PollingError(e) if e.is_io()), "{got:?}");
    }

    // The other cases are already tested.
    #[tokio::test(flavor = "multi_thread")]
    async fn unit_both_until_done_basic_flow() -> Result<()> {
        type EmptyOperation = Operation<Empty, Empty>;
        let start = || async move {
            let any = Any::from_msg(&Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyOperation::new(op);
            Ok::<EmptyOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = Any::from_msg(&Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default().set_done(true).set_result(result);
            let op = EmptyOperation::new(op);

            Ok::<EmptyOperation, Error>(op)
        };

        let poller = new_unit_poller(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(StdDuration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        poller.until_done().await?;
        Ok(())
    }

    #[test]
    fn unit_metadata_map() {
        use PollingResult::{Completed, InProgress, PollingError};
        type TestResult = PollingResult<Duration, Empty>;
        let got = map_polling_metadata(TestResult::Completed(Ok(Duration::clamp(123, 456))));
        assert!(matches!(got, Completed(Ok(_))), "{got:?}");
        let got = map_polling_metadata(TestResult::Completed(Err(service_error())));
        assert!(
            matches!(&got, Completed(Err(e)) if e.status() == service_error().status()),
            "{got:?}"
        );
        let got = map_polling_metadata(TestResult::InProgress(None));
        assert!(matches!(got, InProgress(None)), "{got:?}");
        let got = map_polling_metadata(TestResult::InProgress(Some(Empty::default())));
        assert!(matches!(got, InProgress(Some(_))), "{got:?}");
        let got = map_polling_metadata(TestResult::PollingError(polling_error()));
        assert!(matches!(&got, PollingError(e) if e.is_io()), "{got:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn until_done_with_recoverable_polling_error() -> Result<()> {
        let start = || async move {
            let any = Any::from_msg(&Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
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
                    return Err::<TestOperation, Error>(polling_error());
                }
                let any = Any::from_msg(&Duration::clamp(234, 0))
                    .expect("test message deserializes via Any::from_msg");
                let result = ResultAny::Response(any.into());
                let op = OperationAny::default().set_done(true).set_result(result);
                let op = TestOperation::new(op);

                Ok::<TestOperation, Error>(op)
            }
        };

        let poller = PollerImpl::new(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(StdDuration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        let response = poller.until_done().await?;
        assert_eq!(response, Duration::clamp(234, 0));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn until_done_with_unrecoverable_polling_error() -> Result<()> {
        let start = || async move {
            let any = Any::from_msg(&Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = move |_: String| async move { Err::<TestOperation, Error>(unrecoverable()) };

        let poller = PollerImpl::new(
            Arc::new(Aip194Strict),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(StdDuration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        let response = poller.until_done().await;
        assert!(response.is_err(), "{response:?}");
        assert!(
            format!("{response:?}").contains("unrecoverable"),
            "{response:?}"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unit_pollers_backoff() {
        use crate::sealed::Poller as _;

        let start_resp = || async move {
            Ok::<EmptyResponseOperation, Error>(
                EmptyResponseOperation::new(OperationAny::default()),
            )
        };
        let query_resp = |_: String| async move {
            Ok::<EmptyResponseOperation, Error>(
                EmptyResponseOperation::new(OperationAny::default()),
            )
        };

        let mut poller = new_unit_response_poller(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(StdDuration::from_millis(1))
                    .clamp(),
            ),
            start_resp,
            query_resp,
        );
        poller.backoff(&PollingState::default()).await;

        let start_meta = || async move {
            Ok::<EmptyMetadataOperation, Error>(
                EmptyMetadataOperation::new(OperationAny::default()),
            )
        };
        let query_meta = |_: String| async move {
            Ok::<EmptyMetadataOperation, Error>(
                EmptyMetadataOperation::new(OperationAny::default()),
            )
        };

        let mut poller = new_unit_metadata_poller(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(StdDuration::from_millis(1))
                    .clamp(),
            ),
            start_meta,
            query_meta,
        );
        poller.backoff(&PollingState::default()).await;

        type EmptyOperation = Operation<Empty, Empty>;
        let start_unit = || async move {
            Ok::<EmptyOperation, Error>(EmptyOperation::new(OperationAny::default()))
        };
        let query_unit = |_: String| async move {
            Ok::<EmptyOperation, Error>(EmptyOperation::new(OperationAny::default()))
        };

        let mut poller = new_unit_poller(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(StdDuration::from_millis(1))
                    .clamp(),
            ),
            start_unit,
            query_unit,
        );
        poller.backoff(&PollingState::default()).await;
    }

    fn service_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::ResourceExhausted)
                .set_message("too many things"),
        )
    }

    fn unrecoverable() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::Aborted)
                .set_message("unrecoverable"),
        )
    }

    fn polling_error() -> Error {
        Error::io("something failed")
    }

    #[cfg(google_cloud_unstable_tracing)]
    struct TestLayer {
        recorded: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    }

    #[cfg(google_cloud_unstable_tracing)]
    impl<S> tracing_subscriber::layer::Layer<S> for TestLayer
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        fn on_record(
            &self,
            _id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Visitor {
                recorded: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
            }
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.recorded
                        .lock()
                        .unwrap()
                        .insert(field.name().to_string(), value.to_string());
                }
                fn record_debug(
                    &mut self,
                    _field: &tracing::field::Field,
                    _value: &dyn std::fmt::Debug,
                ) {
                }
            }
            let mut visitor = Visitor {
                recorded: self.recorded.clone(),
            };
            values.record(&mut visitor);
        }
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn test_poller_tracing() {
        use tracing_subscriber::layer::SubscriberExt;

        let recorded = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let layer = TestLayer {
            recorded: recorded.clone(),
        };
        let subscriber = tracing_subscriber::registry::Registry::default().with(layer);

        let start = || async move {
            let any = Any::from_msg(&Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = OperationAny::default()
                .set_name("test-operation-123")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let count = Arc::new(std::sync::Mutex::new(0));
        let query_count = count.clone();
        let query = move |_: String| {
            let mut c = query_count.lock().unwrap();
            *c += 1;
            let is_done = *c > 1;
            async move {
                if is_done {
                    let any = Any::from_msg(&Duration::clamp(234, 0))
                        .expect("test message deserializes via Any::from_msg");
                    let result = ResultAny::Response(any.into());
                    let op = OperationAny::default().set_done(true).set_result(result);
                    let op = TestOperation::new(op);
                    Ok::<TestOperation, Error>(op)
                } else {
                    let any = Any::from_msg(&Timestamp::clamp(123, 0))
                        .expect("test message deserializes via Any::from_msg");
                    let op = OperationAny::default()
                        .set_name("test-operation-123")
                        .set_metadata(any);
                    let op = TestOperation::new(op);
                    Ok::<TestOperation, Error>(op)
                }
            }
        };

        let mut poller = PollerImpl::new(
            Arc::new(AlwaysContinue),
            Arc::new(ExponentialBackoff::default()),
            start,
            query,
        );

        let _guard = tracing::subscriber::set_default(subscriber);

        let span = test_span();
        let poller_ref = &mut poller;
        let _ = crate::internal::LRO_SPAN
            .scope(span.clone(), async move {
                poller_ref.poll().instrument(span).await
            })
            .await;

        {
            let map = recorded.lock().unwrap();
            assert_eq!(
                map.get("gcp.resource.destination.id").map(|s| s.as_str()),
                Some("test-operation-123")
            );
        }

        recorded.lock().unwrap().clear();

        let span = test_span();
        let poller_ref2 = &mut poller;
        let _ = crate::internal::LRO_SPAN
            .scope(span.clone(), async move {
                poller_ref2.poll().instrument(span).await
            })
            .await;

        {
            let map = recorded.lock().unwrap();
            assert_eq!(
                map.get("gcp.resource.destination.id").map(|s| s.as_str()),
                Some("test-operation-123")
            );
        }
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn test_poller_tracing_immediate_done() {
        use tracing_subscriber::layer::SubscriberExt;

        let recorded = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let layer = TestLayer {
            recorded: recorded.clone(),
        };
        let subscriber = tracing_subscriber::registry::Registry::default().with(layer);

        let start = || async move {
            let any = Any::from_msg(&Duration::clamp(234, 0))
                .expect("test message deserializes via Any::from_msg");
            let result = ResultAny::Response(any.into());
            let op = OperationAny::default()
                .set_name("immediate-operation-123")
                .set_done(true)
                .set_result(result);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move { panic!("should not query") };

        let mut poller = PollerImpl::new(
            Arc::new(AlwaysContinue),
            Arc::new(ExponentialBackoff::default()),
            start,
            query,
        );

        let _guard = tracing::subscriber::set_default(subscriber);

        let span = test_span();
        let poller_ref = &mut poller;
        let _ = crate::internal::LRO_SPAN
            .scope(span.clone(), async move {
                poller_ref.poll().instrument(span).await
            })
            .await;

        {
            let map = recorded.lock().unwrap();
            assert_eq!(
                map.get("gcp.resource.destination.id").map(|s| s.as_str()),
                Some("immediate-operation-123")
            );
        }
    }
}
