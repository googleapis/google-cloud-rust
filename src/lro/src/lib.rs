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

use gax::error::Error;
use gax::Result;
use std::future::Future;
use std::marker::PhantomData;

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
    /// [NOT_FOUND]: rpc::model::code::NOT_FOUND
    /// [ABORTED]: rpc::model::code::ABORTED
    /// [PERMISSION_DENIED]: rpc::model::code::PERMISSION_DENIED
    PollingError(Error),
}

/// The result of starting and polling a long-running operation.
///
/// In most cases this is backed by [Operation][longrunning::model::Operation].
pub trait Operation {
    type ResponseType;
    type MetadataType;

    fn name(&self) -> String;
    fn done(&self) -> bool;
    fn metadata(&self) -> Option<&wkt::Any>;
    fn response(&self) -> Option<&wkt::Any>;
    fn error(&self) -> Option<&rpc::model::Status>;
}

/// Implements [Operation] using [longrunning::model::Operation].
pub struct TypedOperation<R, M> {
    inner: longrunning::model::Operation,
    response: std::marker::PhantomData<R>,
    metadata: std::marker::PhantomData<M>,
}

impl<R, M> TypedOperation<R, M> {
    pub fn new(inner: longrunning::model::Operation) -> Self {
        Self {
            inner,
            response: PhantomData,
            metadata: PhantomData,
        }
    }
}

impl<R, M> Operation for TypedOperation<R, M> {
    type ResponseType = R;
    type MetadataType = M;

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
            Result::Response(r) => Some(r),
            _ => None,
        })
    }
    fn error(&self) -> Option<&rpc::model::Status> {
        use longrunning::model::operation::Result;
        self.inner.result.as_ref().and_then(|r| match r {
            Result::Error(rpc) => Some(rpc),
            Result::Response(_) => None,
            _ => None,
        })
    }
}

/// The trait implemented by LRO helpers.
pub trait Poller<R, M> {
    fn poll(&mut self) -> impl Future<Output = Option<PollingResult<R, M>>>;
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
/// - `O` - the operation type. Typically this is a `TypedOperation<R, M>`, so
///   it encodes the types of the response and metadata.
/// - `S` - the start closure. Starts a LRO. This implementation expects that
///   all necessary parameters, and request options, including retry options
///   are captured by this function.
/// - `SF` - the type of future returned by `S`.
/// - `Q` - the query closure. Queries the status of the LRO created by `start`.
///   It receives the name of the operation as its only input parameter. It
///   should have captured any stubs and request options.
/// - `QF` - the type of future returned by `Q`.
pub struct PollerImpl<O, S, SF, Q, QF>
where
    O: Operation,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<O>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<O>> + Send + 'static,
{
    start: Option<S>,
    query: Q,
    operation: Option<String>,
}

impl<O, S, SF, Q, QF> PollerImpl<O, S, SF, Q, QF>
where
    O: Operation,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<O>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<O>> + Send + 'static,
{
    pub fn new(start: S, query: Q) -> Self {
        Self {
            start: Some(start),
            query,
            operation: None,
        }
    }

    #[cfg(feature = "unstable-stream")]
    pub fn to_stream(
        self,
    ) -> impl futures::Stream<Item = PollingResult<O::ResponseType, O::MetadataType>>
    where
        O::ResponseType: wkt::message::Message + serde::de::DeserializeOwned,
        O::MetadataType: wkt::message::Message + serde::de::DeserializeOwned,
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

impl<O, S, SF, P, PF> Poller<O::ResponseType, O::MetadataType> for PollerImpl<O, S, SF, P, PF>
where
    O: Operation,
    O::ResponseType: wkt::message::Message + serde::de::DeserializeOwned,
    O::MetadataType: wkt::message::Message + serde::de::DeserializeOwned,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<O>> + Send + 'static,
    P: Fn(String) -> PF + Send + Sync + Clone,
    PF: std::future::Future<Output = Result<O>> + Send + 'static,
{
    async fn poll(&mut self) -> Option<PollingResult<O::ResponseType, O::MetadataType>> {
        if let Some(start) = self.start.take() {
            let result = start().await;
            let (op, poll) = details::handle_start(result);
            self.operation = op;
            return Some(poll);
        }
        if let Some(name) = self.operation.take() {
            let query = self.query.clone();
            let result = query(name).await;
            let (op, poll) = details::handle_poll(result);
            self.operation = op;
            return Some(poll);
        }
        None
    }
}

mod details;

#[cfg(test)]
mod test {
    use super::*;

    type ResponseType = wkt::Duration;
    type MetadataType = wkt::Timestamp;
    type TestOperation = TypedOperation<ResponseType, MetadataType>;

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
            let result = longrunning::model::operation::Result::Response(any);
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
            let op = TestOperation::new(op);

            Ok::<TestOperation, Error>(op)
        };

        let mut poller = PollerImpl::new(start, query);
        let p0 = poller.poll().await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(wkt::Timestamp::clamp(123, 0)));
            }
            r => {
                assert!(false, "{r:?}");
            }
        }

        let p1 = poller.poll().await;
        match p1.unwrap() {
            PollingResult::Completed(r) => {
                let response = r.unwrap();
                assert_eq!(response, wkt::Duration::clamp(234, 0));
            }
            r => {
                assert!(false, "{r:?}");
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
            let result = longrunning::model::operation::Result::Response(any);
            let op = longrunning::model::Operation::default()
                .set_done(true)
                .set_result(result);
            let op = TestOperation::new(op);

            Ok::<TestOperation, Error>(op)
        };

        use futures::StreamExt;
        let mut stream = PollerImpl::new(start, query).to_stream();
        let mut stream = std::pin::pin!(stream);
        let p0 = stream.next().await;
        match p0.unwrap() {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(wkt::Timestamp::clamp(123, 0)));
            }
            r => {
                assert!(false, "{r:?}");
            }
        }

        let p1 = stream.next().await;
        match p1.unwrap() {
            PollingResult::Completed(r) => {
                let response = r.unwrap();
                assert_eq!(response, wkt::Duration::clamp(234, 0));
            }
            r => {
                assert!(false, "{r:?}");
            }
        }

        let p2 = stream.next().await;
        assert!(p2.is_none(), "{p2:?}");
    }
}
