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
use google_cloud_gax::error::rpc::Status;
use google_cloud_gax::polling_error_policy::PollingErrorPolicy;
use google_cloud_gax::polling_state::PollingState;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_longrunning::model::{Operation as OperationAny, operation::Result as ResultAny};
use google_cloud_wkt::Any;
use google_cloud_wkt::message::Message;
use std::marker::PhantomData;
use std::sync::Arc;

/// A wrapper around [Operation][OperationAny] with typed responses.
///
/// This is intended as an implementation detail of the generated clients.
/// Applications should have no need to create or use this struct.
pub struct Operation<R, M> {
    inner: OperationAny,
    response: std::marker::PhantomData<R>,
    metadata: std::marker::PhantomData<M>,
}

impl<R, M> Operation<R, M> {
    pub fn new(inner: OperationAny) -> Self {
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
    fn metadata(&self) -> Option<&Any> {
        self.inner.metadata.as_ref()
    }
    fn response(&self) -> Option<&Any> {
        self.inner.result.as_ref().and_then(|r| match r {
            ResultAny::Error(_) => None,
            ResultAny::Response(r) => Some(r.as_ref()),
            _ => None,
        })
    }
    fn error(&self) -> Option<&google_cloud_rpc::model::Status> {
        self.inner.result.as_ref().and_then(|r| match r {
            ResultAny::Error(rpc) => Some(rpc.as_ref()),
            ResultAny::Response(_) => None,
            _ => None,
        })
    }
}

pub(crate) fn handle_start<R, M>(
    result: Result<Operation<R, M>>,
) -> (Option<String>, PollingResult<R, M>)
where
    R: Message + serde::ser::Serialize + serde::de::DeserializeOwned,
    M: Message + serde::ser::Serialize + serde::de::DeserializeOwned,
{
    match result {
        Err(e) => (None, PollingResult::Completed(Err(e))),
        Ok(op) => handle_common(op),
    }
}

pub(crate) fn handle_poll<R, M>(
    error_policy: Arc<dyn PollingErrorPolicy>,
    state: &PollingState,
    operation_name: String,
    result: Result<Operation<R, M>>,
) -> (Option<String>, PollingResult<R, M>)
where
    R: Message + serde::ser::Serialize + serde::de::DeserializeOwned,
    M: Message + serde::ser::Serialize + serde::de::DeserializeOwned,
{
    match result {
        Err(e) => {
            let state = error_policy.on_error(state, e);
            handle_polling_error(state, operation_name)
        }
        Ok(op) => {
            let (name, result) = handle_common(op);
            match &result {
                PollingResult::Completed(_) => (name, result),
                PollingResult::InProgress(_) => {
                    match error_policy.on_in_progress(state, &operation_name) {
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
    state: RetryResult,
    operation_name: String,
) -> (Option<String>, PollingResult<R, M>)
where
    R: Message + serde::de::DeserializeOwned,
    M: Message + serde::de::DeserializeOwned,
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
    R: Message + serde::ser::Serialize + serde::de::DeserializeOwned,
    M: Message + serde::ser::Serialize + serde::de::DeserializeOwned,
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
    R: Message + serde::ser::Serialize + serde::de::DeserializeOwned,
{
    // The result must set either the response *or* the error. Setting neither
    // is a deserialization error, as the incoming data does not satisfy the
    // invariants required by the receiving type.
    match (op.response(), op.error()) {
        (Some(any), None) => any.to_msg::<R>().map_err(Error::deser),
        (None, Some(e)) => Err(Error::service(Status::from(e))),
        (None, None) => Err(Error::deser("neither result nor error set in LRO result")),
        (Some(_), Some(_)) => unreachable!("result and error held in a oneof"),
    }
}

fn as_metadata<R, M>(op: Operation<R, M>) -> Option<M>
where
    M: Message + serde::ser::Serialize + serde::de::DeserializeOwned,
{
    op.metadata().and_then(|a| a.to_msg::<M>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::polling_error_policy::{
        Aip194Strict, AlwaysContinue, Exhausted, PollingErrorPolicyExt,
    };
    use google_cloud_wkt::{Any, AnyError, Duration, Timestamp};
    use std::error::Error as _;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
    type ResponseType = Duration;
    type MetadataType = Timestamp;
    type TestOperation = Operation<ResponseType, MetadataType>;

    #[test]
    fn typed_operation_with_metadata() -> Result<()> {
        let any = Any::from_msg(&Timestamp::clamp(123, 0)).expect("Any::from_msg should succeed");
        let opa = OperationAny::default()
            .set_name("test-only-name")
            .set_metadata(any);
        let op = TestOperation::new(opa.clone());
        assert_eq!(op.name(), "test-only-name");
        assert!(!op.done());
        assert!(op.metadata().is_some(), "{opa:?}");
        assert!(op.response().is_none(), "{opa:?}");
        assert!(op.error().is_none(), "{opa:?}");
        let got = op
            .metadata()
            .unwrap()
            .to_msg::<Timestamp>()
            .expect("Any::from_msg should succeed");
        assert_eq!(got, Timestamp::clamp(123, 0));

        Ok(())
    }

    #[test]
    fn typed_operation_with_response() -> Result<()> {
        let any = Any::from_msg(&Duration::clamp(23, 0))
            .expect("successful deserialization via Any::from_msg");
        let opa = OperationAny::default()
            .set_name("test-only-name")
            .set_result(ResultAny::Response(any.into()));
        let op = TestOperation::new(opa.clone());
        assert_eq!(op.name(), "test-only-name");
        assert!(!op.done());
        assert!(op.metadata().is_none(), "{opa:?}");
        assert!(op.response().is_some(), "{opa:?}");
        assert!(op.error().is_none(), "{opa:?}");
        let got = op
            .response()
            .unwrap()
            .to_msg::<Duration>()
            .expect("successful deserialization via Any::from_msg");
        assert_eq!(got, Duration::clamp(23, 0));

        Ok(())
    }

    #[test]
    fn typed_operation_with_error() -> Result<()> {
        let rpc = google_cloud_rpc::model::Status::default()
            .set_message("test only")
            .set_code(16);
        let opa = OperationAny::default()
            .set_name("test-only-name")
            .set_result(ResultAny::Error(rpc.clone().into()));
        let op = TestOperation::new(opa.clone());
        assert_eq!(op.name(), "test-only-name");
        assert!(!op.done());
        assert!(op.metadata().is_none(), "{opa:?}");
        assert!(op.response().is_none(), "{opa:?}");
        assert!(op.error().is_some(), "{opa:?}");
        let got = op.error().unwrap();
        assert_eq!(got, &rpc);

        Ok(())
    }

    #[test]
    fn start_success() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let op = OperationAny::default()
            .set_name("test-only-name")
            .set_metadata(Any::from_msg(&Timestamp::clamp(123, 0))?);
        let op = super::Operation::new(op);
        let result = Ok::<O, Error>(op);
        let (name, poll) = handle_start(result);
        assert_eq!(name.as_deref(), Some("test-only-name"));
        match poll {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(Timestamp::clamp(123, 0)));
            }
            _ => panic!("{poll:?}"),
        };
        Ok(())
    }

    #[test]
    fn start_error() {
        fn starting_error() -> Error {
            Error::service(
                Status::default()
                    .set_code(Code::AlreadyExists)
                    .set_message("thing already there"),
            )
        }
        type R = Duration;
        type M = Timestamp;
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
        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let op = OperationAny::default()
            .set_name("test-only-name")
            .set_metadata(Any::from_msg(&Timestamp::clamp(123, 0))?);
        let op = super::Operation::new(op);
        let result = Ok::<O, Error>(op);
        let (name, poll) = handle_poll(
            Arc::new(AlwaysContinue),
            &PollingState::default(),
            "test-123".to_string(),
            result,
        );
        assert_eq!(name.as_deref(), Some("test-only-name"));
        match poll {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(Timestamp::clamp(123, 0)));
            }
            _ => panic!("{poll:?}"),
        };
        Ok(())
    }

    #[test]
    fn poll_success_exhausted() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let op = OperationAny::default()
            .set_name("test-only-name")
            .set_metadata(Any::from_msg(&Timestamp::clamp(123, 0))?);
        let op = super::Operation::new(op);
        let result = Ok::<O, Error>(op);
        let (name, poll) = handle_poll(
            Arc::new(AlwaysContinue.with_attempt_limit(3)),
            &PollingState::default().set_attempt_count(5_u32),
            String::from("test-123"),
            result,
        );
        assert_eq!(name, None);
        match poll {
            PollingResult::Completed(Err(error)) => {
                assert!(
                    error
                        .source()
                        .and_then(|e| e.downcast_ref::<Exhausted>())
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
        fn continuing_error() -> Error {
            Error::io("test-only-error")
        }

        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let result = Err::<O, Error>(continuing_error());
        let (name, poll) = handle_poll(
            Arc::new(AlwaysContinue),
            &PollingState::default(),
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
        fn stopping_error() -> Error {
            Error::service(
                Status::default()
                    .set_code(Code::Aborted)
                    .set_message("operation-aborted"),
            )
        }

        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let result = Err::<O, Error>(stopping_error());
        let (name, poll) = handle_poll(
            Arc::new(Aip194Strict),
            &PollingState::default(),
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
        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let op = OperationAny::default()
            .set_name("test-only-name")
            .set_done(true)
            .set_metadata(Any::from_msg(&Timestamp::clamp(123, 0))?)
            .set_result(ResultAny::Response(
                Any::from_msg(&Duration::clamp(234, 0))?.into(),
            ));
        let op = O::new(op);
        let (name, polling) = handle_common(op);
        assert_eq!(name, None);
        match polling {
            PollingResult::Completed(Ok(response)) => {
                assert_eq!(response, Duration::clamp(234, 0));
            }
            _ => panic!("{polling:?}"),
        };
        Ok(())
    }

    #[test]
    fn common_not_done() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let op = OperationAny::default()
            .set_name("test-only-name")
            .set_metadata(Any::from_msg(&Timestamp::clamp(123, 0))?);
        let op = O::new(op);
        let (name, polling) = handle_common(op);
        assert_eq!(name.as_deref(), Some("test-only-name"));
        match &polling {
            PollingResult::InProgress(m) => {
                assert_eq!(m, &Some(Timestamp::clamp(123, 0)));
            }
            _ => panic!("{polling:?}"),
        };
        Ok(())
    }

    #[test]
    fn extract_result() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let op = OperationAny::default().set_result(ResultAny::Response(
            Any::from_msg(&Duration::clamp(123, 0))?.into(),
        ));
        let op = O::new(op);
        let result = as_result(op)?;
        assert_eq!(result, Duration::clamp(123, 0));

        Ok(())
    }

    #[test]
    fn extract_result_with_error() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let op = OperationAny::default().set_result(ResultAny::Error(
            google_cloud_rpc::model::Status::default()
                .set_code(Code::FailedPrecondition as i32)
                .set_message("test only")
                .into(),
        ));
        let op = O::new(op);
        let result = as_result(op);
        let err = result.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");
        let want = Status::default()
            .set_code(Code::FailedPrecondition)
            .set_message("test only");
        assert_eq!(err.status(), Some(&want));

        Ok(())
    }

    #[test]
    fn extract_result_bad_type() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = super::Operation<R, M>;
        let op = OperationAny::default().set_result(ResultAny::Response(
            Any::from_msg(&Timestamp::clamp(123, 0))?.into(),
        ));
        let op = O::new(op);
        let err = as_result(op).unwrap_err();
        assert!(err.is_deserialization(), "{err:?}");
        assert!(
            matches!(
                err.source().and_then(|e| e.downcast_ref::<AnyError>()),
                Some(AnyError::TypeMismatch { .. })
            ),
            "{err:?}",
        );

        Ok(())
    }

    #[test]
    fn extract_result_not_set() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = Operation<R, M>;
        let op = OperationAny::default();
        let op = O::new(op);
        let err = as_result(op).err().unwrap();
        assert!(err.is_deserialization(), "{err:?}");

        Ok(())
    }

    #[test]
    fn extract_metadata() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = Operation<R, M>;
        let op = OperationAny::default().set_metadata(Any::from_msg(&Timestamp::clamp(123, 0))?);

        let op = O::new(op);

        let metadata = as_metadata(op);
        assert_eq!(metadata, Some(Timestamp::clamp(123, 0)));

        Ok(())
    }

    #[test]
    fn extract_metadata_bad_type() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = Operation<R, M>;
        let op = OperationAny::default().set_metadata(Any::from_msg(&Duration::clamp(123, 0))?);
        let op = O::new(op);
        let metadata = as_metadata(op);
        assert_eq!(metadata, None);

        Ok(())
    }

    #[test]
    fn extract_metadata_not_set() -> TestResult {
        type R = Duration;
        type M = Timestamp;
        type O = Operation<R, M>;
        let op = OperationAny::default();
        let op = O::new(op);
        let metadata = as_metadata(op);
        assert_eq!(metadata, None);

        Ok(())
    }
}
