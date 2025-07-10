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

//! Simplifies the implementation of `PollerImpl`

use super::*;
use gax::polling_error_policy::PollingErrorPolicy;
use gax::retry_result::RetryResult;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

/// A wrapper around [longrunning::model::Operation] with typed responses.
///
/// This is intended as an implementation detail of the generated clients.
/// Applications should have no need to create or use this struct.
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

pub(crate) fn handle_start<R, M>(
    result: Result<Operation<R, M>>,
) -> (Option<String>, PollingResult<R, M>)
where
    R: wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
{
    match result {
        Err(e) => (None, PollingResult::Completed(Err(e))),
        Ok(op) => handle_common(op),
    }
}

pub(crate) fn handle_poll<R, M>(
    error_policy: Arc<dyn PollingErrorPolicy>,
    loop_start: Instant,
    attempt_count: u32,
    operation_name: String,
    result: Result<Operation<R, M>>,
) -> (Option<String>, PollingResult<R, M>)
where
    R: wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
{
    match result {
        Err(e) => {
            let state = error_policy.on_error(loop_start, attempt_count, e);
            handle_polling_error(state, operation_name)
        }
        Ok(op) => {
            let (name, result) = handle_common(op);
            match &result {
                PollingResult::Completed(_) => (name, result),
                PollingResult::InProgress(_) => {
                    match error_policy.on_in_progress(loop_start, attempt_count, &operation_name) {
                        Ok(()) => (name, result),
                        Err(e) => (None, PollingResult::Completed(Err(e))),
                    }
                }
                PollingResult::PollingError(_) => {
                    unreachable!("handle_common never returns PollingResult::PollingError")
                }
            }
        }
    }
}

fn handle_polling_error<R, M>(
    state: gax::retry_result::RetryResult,
    operation_name: String,
) -> (Option<String>, PollingResult<R, M>)
where
    R: wkt::message::Message + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::de::DeserializeOwned,
{
    match state {
        RetryResult::Continue(e) => (Some(operation_name), PollingResult::PollingError(e)),
        RetryResult::Exhausted(e) | RetryResult::Permanent(e) => {
            (None, PollingResult::Completed(Err(e)))
        }
    }
}

fn handle_common<R, M>(op: Operation<R, M>) -> (Option<String>, PollingResult<R, M>)
where
    R: wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
{
    if op.done() {
        let result = as_result(op);
        return (None, PollingResult::Completed(result));
    }
    let name = op.name().clone();
    let metadata = as_metadata(op);
    (Some(name), PollingResult::InProgress(metadata))
}

fn as_result<R, M>(op: Operation<R, M>) -> Result<R>
where
    R: wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
{
    // The result must set either the response *or* the error. Setting neither
    // is a deserialization error, as the incoming data does not satisfy the
    // invariants required by the receiving type.
    match (op.response(), op.error()) {
        (Some(any), None) => any.to_msg::<R>().map_err(Error::deser),
        (None, Some(e)) => Err(Error::service(gax::error::rpc::Status::from(e.clone()))),
        (None, None) => Err(Error::deser("neither result nor error set in LRO result")),
        (Some(_), Some(_)) => unreachable!("result and error held in a oneof"),
    }
}

fn as_metadata<R, M>(op: Operation<R, M>) -> Option<M>
where
    M: wkt::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
{
    op.metadata().and_then(|a| a.to_msg::<M>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gax::polling_error_policy::*;
    use std::error::Error as _;
    use wkt::Any;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
    type ResponseType = wkt::Duration;
    type MetadataType = wkt::Timestamp;
    type TestOperation = Operation<ResponseType, MetadataType>;

    #[test]
    fn typed_operation_with_metadata() -> Result<()> {
        let any = wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))
            .expect("Any::from_msg should succeed");
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
            .to_msg::<wkt::Timestamp>()
            .expect("Any::from_msg should succeed");
        assert_eq!(got, wkt::Timestamp::clamp(123, 0));

        Ok(())
    }

    #[test]
    fn typed_operation_with_response() -> Result<()> {
        let any = wkt::Any::from_msg(&wkt::Duration::clamp(23, 0))
            .expect("successful deserialization via Any::from_msg");
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
            .to_msg::<wkt::Duration>()
            .expect("successful deserialization via Any::from_msg");
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

    #[test]
    fn start_success() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default()
            .set_name("test-only-name")
            .set_metadata(wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))?);
        let op = super::Operation::new(op);
        let result = Ok::<O, Error>(op);
        let (name, poll) = handle_start(result);
        assert_eq!(name.as_deref(), Some("test-only-name"));
        match poll {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(wkt::Timestamp::clamp(123, 0)));
            }
            _ => panic!("{poll:?}"),
        };
        Ok(())
    }

    #[test]
    fn start_error() {
        fn starting_error() -> gax::error::Error {
            use gax::error::rpc::{Code, Status};
            gax::error::Error::service(
                Status::default()
                    .set_code(Code::AlreadyExists)
                    .set_message("thing already there"),
            )
        }
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = Operation<R, M>;
        let result = Err::<O, Error>(starting_error());
        let (name, poll) = handle_start(result);
        assert_eq!(name, None);
        match poll {
            PollingResult::Completed(Err(e)) => {
                assert!(e.status().is_some(), "{e:?}");
                assert_eq!(e.status(), starting_error().status());
            }
            _ => panic!("{poll:?}"),
        };
    }

    #[test]
    fn poll_success() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default()
            .set_name("test-only-name")
            .set_metadata(wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))?);
        let op = super::Operation::new(op);
        let result = Ok::<O, Error>(op);
        let (name, poll) = handle_poll(
            Arc::new(AlwaysContinue),
            Instant::now(),
            0,
            "test-123".to_string(),
            result,
        );
        assert_eq!(name.as_deref(), Some("test-only-name"));
        match poll {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(wkt::Timestamp::clamp(123, 0)));
            }
            _ => panic!("{poll:?}"),
        };
        Ok(())
    }

    #[test]
    fn poll_success_exhausted() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default()
            .set_name("test-only-name")
            .set_metadata(wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))?);
        let op = super::Operation::new(op);
        let result = Ok::<O, Error>(op);
        let (name, poll) = handle_poll(
            Arc::new(AlwaysContinue.with_attempt_limit(3)),
            Instant::now(),
            5,
            String::from("test-123"),
            result,
        );
        assert_eq!(name, None);
        match poll {
            PollingResult::Completed(Err(error)) => {
                assert!(
                    error
                        .source()
                        .and_then(|e| e.downcast_ref::<gax::polling_error_policy::Exhausted>())
                        .is_some(),
                    "{error:?}"
                );
            }
            _ => panic!("{poll:?}"),
        };
        Ok(())
    }

    #[test]
    fn poll_error_continue() {
        fn continuing_error() -> gax::error::Error {
            gax::error::Error::io("test-only-error")
        }

        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let result = Err::<O, Error>(continuing_error());
        let (name, poll) = handle_poll(
            Arc::new(AlwaysContinue),
            Instant::now(),
            0,
            String::from("test-123"),
            result,
        );
        assert_eq!(name.as_deref(), Some("test-123"));
        match poll {
            PollingResult::PollingError(e) => {
                assert!(e.is_io(), "{e:?}");
                assert!(format!("{e}").contains("test-only-error"), "{e}")
            }
            _ => panic!("{poll:?}"),
        };
    }

    #[test]
    fn poll_error_finishes() {
        fn stopping_error() -> gax::error::Error {
            use gax::error::rpc::{Code, Status};
            gax::error::Error::service(
                Status::default()
                    .set_code(Code::Aborted)
                    .set_message("operation-aborted"),
            )
        }

        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let result = Err::<O, Error>(stopping_error());
        let (name, poll) = handle_poll(
            Arc::new(Aip194Strict),
            Instant::now(),
            0,
            String::from("test-123"),
            result,
        );
        assert_eq!(name, None);
        match poll {
            PollingResult::Completed(Err(e)) => {
                assert!(e.status().is_some(), "{e:?}");
                assert_eq!(e.status(), stopping_error().status());
            }
            _ => panic!("{poll:?}"),
        };
    }

    #[test]
    fn common_done() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default()
            .set_name("test-only-name")
            .set_done(true)
            .set_metadata(wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))?)
            .set_result(operation::Result::Response(
                wkt::Any::from_msg(&wkt::Duration::clamp(234, 0))?.into(),
            ));
        let op = O::new(op);
        let (name, polling) = handle_common(op);
        assert_eq!(name, None);
        match polling {
            PollingResult::Completed(Ok(response)) => {
                assert_eq!(response, wkt::Duration::clamp(234, 0));
            }
            _ => panic!("{polling:?}"),
        };
        Ok(())
    }

    #[test]
    fn common_not_done() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default()
            .set_name("test-only-name")
            .set_metadata(wkt::Any::from_msg(&wkt::Timestamp::clamp(123, 0))?);
        let op = O::new(op);
        let (name, polling) = handle_common(op);
        assert_eq!(name.as_deref(), Some("test-only-name"));
        match &polling {
            PollingResult::InProgress(m) => {
                assert_eq!(m, &Some(wkt::Timestamp::clamp(123, 0)));
            }
            _ => panic!("{polling:?}"),
        };
        Ok(())
    }

    #[test]
    fn extract_result() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default().set_result(operation::Result::Response(
            Any::from_msg(&wkt::Duration::clamp(123, 0))?.into(),
        ));
        let op = O::new(op);
        let result = as_result(op)?;
        assert_eq!(result, wkt::Duration::clamp(123, 0));

        Ok(())
    }

    #[test]
    fn extract_result_with_error() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default().set_result(operation::Result::Error(
            rpc::model::Status::default()
                .set_code(gax::error::rpc::Code::FailedPrecondition as i32)
                .set_message("test only")
                .into(),
        ));
        let op = O::new(op);
        let result = as_result(op);
        let err = result.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");
        let want = gax::error::rpc::Status::default()
            .set_code(gax::error::rpc::Code::FailedPrecondition)
            .set_message("test only");
        assert_eq!(err.status(), Some(&want));

        Ok(())
    }

    #[test]
    fn extract_result_bad_type() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default().set_result(operation::Result::Response(
            Any::from_msg(&wkt::Timestamp::clamp(123, 0))?.into(),
        ));
        let op = O::new(op);
        let err = as_result(op).unwrap_err();
        assert!(err.is_deserialization(), "{err:?}");
        assert!(
            matches!(
                err.source().and_then(|e| e.downcast_ref::<wkt::AnyError>()),
                Some(wkt::AnyError::TypeMismatch { .. })
            ),
            "{err:?}",
        );

        Ok(())
    }

    #[test]
    fn extract_result_not_set() -> TestResult {
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = Operation<R, M>;
        let op = longrunning::model::Operation::default();
        let op = O::new(op);
        let err = as_result(op).err().unwrap();
        assert!(err.is_deserialization(), "{err:?}");

        Ok(())
    }

    #[test]
    fn extract_metadata() -> TestResult {
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = Operation<R, M>;
        let op = longrunning::model::Operation::default()
            .set_metadata(Any::from_msg(&wkt::Timestamp::clamp(123, 0))?);

        let op = O::new(op);

        let metadata = as_metadata(op);
        assert_eq!(metadata, Some(wkt::Timestamp::clamp(123, 0)));

        Ok(())
    }

    #[test]
    fn extract_metadata_bad_type() -> TestResult {
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = Operation<R, M>;
        let op = longrunning::model::Operation::default()
            .set_metadata(Any::from_msg(&wkt::Duration::clamp(123, 0))?);
        let op = O::new(op);
        let metadata = as_metadata(op);
        assert_eq!(metadata, None);

        Ok(())
    }

    #[test]
    fn extract_metadata_not_set() -> TestResult {
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = Operation<R, M>;
        let op = longrunning::model::Operation::default();
        let op = O::new(op);
        let metadata = as_metadata(op);
        assert_eq!(metadata, None);

        Ok(())
    }
}
