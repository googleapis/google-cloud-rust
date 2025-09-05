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

use super::{Poller, PollingBackoffPolicy, PollingErrorPolicy, PollingResult, Result};
use gax::polling_state::PollingState;
use std::sync::Arc;

pub type Operation<R, M> = super::details::Operation<R, M>;

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
    ResponseType:
        wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    MetadataType:
        wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
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
    MetadataType:
        wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<wkt::Empty, MetadataType>>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<Operation<wkt::Empty, MetadataType>>> + Send + 'static,
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
    ResponseType:
        wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<ResponseType, wkt::Empty>>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<Operation<ResponseType, wkt::Empty>>> + Send + 'static,
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
    SF: std::future::Future<Output = Result<Operation<wkt::Empty, wkt::Empty>>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<Operation<wkt::Empty, wkt::Empty>>> + Send + 'static,
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

impl<P> super::sealed::Poller for UnitResponsePoller<P> {}

impl<P, M> Poller<(), M> for UnitResponsePoller<P>
where
    P: Poller<wkt::Empty, M>,
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

impl<P> super::sealed::Poller for UnitMetadataPoller<P> {}

impl<P, R> Poller<R, ()> for UnitMetadataPoller<P>
where
    P: Poller<R, wkt::Empty>,
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

fn map_polling_result<M>(result: PollingResult<wkt::Empty, M>) -> PollingResult<(), M> {
    match result {
        PollingResult::Completed(r) => PollingResult::Completed(r.map(|_| ())),
        PollingResult::InProgress(m) => PollingResult::InProgress(m),
        PollingResult::PollingError(e) => PollingResult::PollingError(e),
    }
}

fn map_polling_metadata<R>(result: PollingResult<R, wkt::Empty>) -> PollingResult<R, ()> {
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
    state: PollingState,
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
            state: PollingState::default(),
        }
    }
}

impl<ResponseType, MetadataType, S, SF, P, PF> Poller<ResponseType, MetadataType>
    for PollerImpl<ResponseType, MetadataType, S, SF, P, PF>
where
    ResponseType:
        wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    MetadataType:
        wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
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
            let (op, poll) = super::details::handle_start(result);
            self.operation = op;
            return Some(poll);
        }
        if let Some(name) = self.operation.take() {
            self.state.attempt_count += 1;
            let result = (self.query)(name.clone()).await;
            let (op, poll) =
                super::details::handle_poll(self.error_policy.clone(), &self.state, name, result);
            self.operation = op;
            return Some(poll);
        }
        None
    }

    async fn until_done(mut self) -> Result<ResponseType> {
        let mut state = PollingState::default();
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
            state.attempt_count += 1;
            tokio::time::sleep(self.backoff_policy.wait_period(&state)).await;
        }
        // We can only get here if `poll()` returns `None`, but it only returns
        // `None` after it returned `Polling::Completed` and therefore this is
        // never reached.
        unreachable!("loop should exit via the `Completed` branch vs. this line");
    }

    #[cfg(feature = "unstable-stream")]
    fn into_stream(
        self,
    ) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>> + Unpin
    where
        ResponseType: wkt::message::Message + serde::de::DeserializeOwned,
        MetadataType: wkt::message::Message + serde::de::DeserializeOwned,
    {
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

impl<ResponseType, MetadataType, S, SF, P, PF> super::sealed::Poller
    for PollerImpl<ResponseType, MetadataType, S, SF, P, PF>
where
    ResponseType:
        wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    MetadataType:
        wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned + Send,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
    P: Fn(String) -> PF + Send + Sync + Clone,
    PF: std::future::Future<Output = Result<Operation<ResponseType, MetadataType>>>
        + Send
        + 'static,
{
}

#[cfg(test)]
mod tests {
    use super::super::Error;
    use super::*;
    use gax::error::rpc::{Code, Status};
    use gax::exponential_backoff::ExponentialBackoff;
    use gax::exponential_backoff::ExponentialBackoffBuilder;
    use gax::polling_error_policy::*;
    use std::time::Duration;

    type ResponseType = wkt::Duration;
    type MetadataType = wkt::Timestamp;
    type TestOperation = Operation<ResponseType, MetadataType>;
    type EmptyResponseOperation = Operation<wkt::Empty, MetadataType>;
    type EmptyMetadataOperation = Operation<ResponseType, wkt::Empty>;

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_basic_flow() {
        let start = || async move {
            let any = wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Duration::clamp(234, 0))
                .expect("test message deserializes via Any::from_msg");
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
            let any = wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Duration::clamp(234, 0))
                .expect("test message deserializes via Any::from_msg");
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
            let any = wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = TestOperation::new(op);
            Ok::<TestOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Duration::clamp(234, 0))
                .expect("test message deserializes via Any::from_msg");
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
    async fn unit_poll_basic_flow() {
        let start = || async move {
            let any = wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyResponseOperation::new(op);
            Ok::<EmptyResponseOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
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
                assert_eq!(m, Some(wkt::Timestamp::clamp(123, 0)));
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
            let any = wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyResponseOperation::new(op);
            Ok::<EmptyResponseOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
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
                assert_eq!(m, Some(wkt::Timestamp::clamp(123, 0)));
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
            let any = wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyResponseOperation::new(op);
            Ok::<EmptyResponseOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
            let op = EmptyResponseOperation::new(op);

            Ok::<EmptyResponseOperation, Error>(op)
        };

        let poller = new_unit_response_poller(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
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
            let any = wkt::Any::from_msg(&wkt::Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyMetadataOperation::new(op);
            Ok::<EmptyMetadataOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Duration::clamp(123, 456))
                .expect("test message deserializes via Any::from_msg");
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
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
            let any = wkt::Any::from_msg(&wkt::Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyMetadataOperation::new(op);
            Ok::<EmptyMetadataOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Duration::clamp(123, 456))
                .expect("test message deserializes via Any::from_msg");
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
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
                assert_eq!(d, wkt::Duration::clamp(123, 456));
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
            let any = wkt::Any::from_msg(&wkt::Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyMetadataOperation::new(op);
            Ok::<EmptyMetadataOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Duration::clamp(123, 456))
                .expect("test message deserializes via Any::from_msg");
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
            let op = EmptyMetadataOperation::new(op);

            Ok::<EmptyMetadataOperation, Error>(op)
        };

        let poller = new_unit_metadata_poller(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            ),
            start,
            query,
        );
        let d = poller.until_done().await?;
        assert_eq!(d, wkt::Duration::clamp(123, 456));
        Ok(())
    }

    #[test]
    fn unit_result_map() {
        use PollingResult::{Completed, InProgress, PollingError};
        type TestResult = PollingResult<wkt::Empty, wkt::Timestamp>;
        let got = map_polling_result(TestResult::Completed(Ok(wkt::Empty::default())));
        assert!(matches!(got, Completed(Ok(_))), "{got:?}");
        let got = map_polling_result(TestResult::Completed(Err(service_error())));
        assert!(
            matches!(&got, Completed(Err(e)) if e.status() == service_error().status()),
            "{got:?}"
        );
        let got = map_polling_result(TestResult::InProgress(None));
        assert!(matches!(got, InProgress(None)), "{got:?}");
        let got = map_polling_result(TestResult::InProgress(Some(wkt::Timestamp::clamp(
            123, 456,
        ))));
        assert!(matches!(got, InProgress(Some(t)) if t == wkt::Timestamp::clamp(123, 456)));
        let got = map_polling_result(TestResult::PollingError(polling_error()));
        assert!(matches!(&got, PollingError(e) if e.is_io()), "{got:?}");
    }

    // The other cases are already tested.
    #[tokio::test(flavor = "multi_thread")]
    async fn unit_both_until_done_basic_flow() -> Result<()> {
        type EmptyOperation = Operation<wkt::Empty, wkt::Empty>;
        let start = || async move {
            let any = wkt::Any::from_msg(&wkt::Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
                .set_name("test-only-name")
                .set_metadata(any);
            let op = EmptyOperation::new(op);
            Ok::<EmptyOperation, Error>(op)
        };

        let query = |_: String| async move {
            let any = wkt::Any::from_msg(&wkt::Empty::default())
                .expect("test message deserializes via Any::from_msg");
            let result = longrunning::model::operation::Result::Response(any.into());
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
            let op = EmptyOperation::new(op);

            Ok::<EmptyOperation, Error>(op)
        };

        let poller = new_unit_poller(
            Arc::new(AlwaysContinue),
            Arc::new(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
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
        type TestResult = PollingResult<wkt::Duration, wkt::Empty>;
        let got = map_polling_metadata(TestResult::Completed(Ok(wkt::Duration::clamp(123, 456))));
        assert!(matches!(got, Completed(Ok(_))), "{got:?}");
        let got = map_polling_metadata(TestResult::Completed(Err(service_error())));
        assert!(
            matches!(&got, Completed(Err(e)) if e.status() == service_error().status()),
            "{got:?}"
        );
        let got = map_polling_metadata(TestResult::InProgress(None));
        assert!(matches!(got, InProgress(None)), "{got:?}");
        let got = map_polling_metadata(TestResult::InProgress(Some(wkt::Empty::default())));
        assert!(matches!(got, InProgress(Some(_))), "{got:?}");
        let got = map_polling_metadata(TestResult::PollingError(polling_error()));
        assert!(matches!(&got, PollingError(e) if e.is_io()), "{got:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn until_done_with_recoverable_polling_error() -> Result<()> {
        let start = || async move {
            let any = wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
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
                    return Err::<TestOperation, Error>(polling_error());
                }
                let any = wkt::Any::from_msg(&wkt::Duration::clamp(234, 0))
                    .expect("test message deserializes via Any::from_msg");
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
            let any = wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))
                .expect("test message deserializes via Any::from_msg");
            let op = longrunning::model::Operation::default()
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

    fn service_error() -> gax::error::Error {
        gax::error::Error::service(
            Status::default()
                .set_code(Code::ResourceExhausted)
                .set_message("too many things"),
        )
    }

    fn unrecoverable() -> gax::error::Error {
        gax::error::Error::service(
            Status::default()
                .set_code(Code::Aborted)
                .set_message("unrecoverable"),
        )
    }

    fn polling_error() -> gax::error::Error {
        gax::error::Error::io("something failed")
    }
}
