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
use gax::loop_state::LoopState;
use gax::polling_policy::PollingPolicy;
use std::sync::Arc;
use std::time::Instant;

pub(crate) fn handle_start<R, M>(
    result: Result<Operation<R, M>>,
) -> (Option<String>, PollingResult<R, M>)
where
    R: wkt::message::Message + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::de::DeserializeOwned,
{
    match result {
        Err(e) => (None, PollingResult::Completed(Err(e))),
        Ok(op) => handle_common(op),
    }
}

pub(crate) fn handle_poll<R, M>(
    polling_policy: Arc<dyn PollingPolicy>,
    loop_start: Instant,
    attempt_count: u32,
    operation_name: String,
    result: Result<Operation<R, M>>,
) -> (Option<String>, PollingResult<R, M>)
where
    R: wkt::message::Message + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::de::DeserializeOwned,
{
    match result {
        Err(e) => {
            let state = polling_policy.on_error(loop_start, attempt_count, e);
            handle_polling_error(state, operation_name)
        }
        Ok(op) => {
            let (name, result) = handle_common(op);
            match &result {
                PollingResult::Completed(_) => (name, result),
                PollingResult::InProgress(_) => {
                    match polling_policy.on_in_progress(loop_start, attempt_count, &operation_name)
                    {
                        None => (name, result),
                        Some(e) => (None, PollingResult::Completed(Err(e))),
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
    state: gax::loop_state::LoopState,
    operation_name: String,
) -> (Option<String>, PollingResult<R, M>)
where
    R: wkt::message::Message + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::de::DeserializeOwned,
{
    match state {
        LoopState::Continue(e) => (Some(operation_name), PollingResult::PollingError(e)),
        LoopState::Exhausted(e) | LoopState::Permanent(e) => {
            (None, PollingResult::Completed(Err(e)))
        }
    }
}

fn handle_common<R, M>(op: Operation<R, M>) -> (Option<String>, PollingResult<R, M>)
where
    R: wkt::message::Message + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::de::DeserializeOwned,
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
    R: wkt::message::Message + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::de::DeserializeOwned,
{
    if let Some(any) = op.response() {
        return any.try_into_message::<R>().map_err(Error::other);
    }
    if let Some(e) = op.error() {
        return Err(Error::rpc(gax::error::ServiceError::from(e.clone())));
    }
    Err(Error::other("missing result in completed operation"))
}

fn as_metadata<R, M>(op: Operation<R, M>) -> Option<M>
where
    R: wkt::message::Message + serde::de::DeserializeOwned,
    M: wkt::message::Message + serde::de::DeserializeOwned,
{
    op.metadata().and_then(|a| a.try_into_message::<M>().ok())
}

#[cfg(test)]
mod test {
    use super::*;
    use gax::polling_policy::*;
    use wkt::Any;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn start_success() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default()
            .set_name("test-only-name")
            .set_metadata(wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))?);
        let op = super::Operation::new(op);
        let result = Ok::<O, Error>(op);
        let (name, poll) = handle_start(result);
        assert_eq!(name.as_deref(), Some("test-only-name"));
        match poll {
            PollingResult::InProgress(m) => {
                assert_eq!(m, Some(wkt::Timestamp::clamp(123, 0)));
            }
            _ => assert!(false, "{poll:?}"),
        };
        Ok(())
    }

    #[test]
    fn start_error() {
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = Operation<R, M>;
        let result = Err::<O, Error>(Error::other("test-only-error"));
        let (name, poll) = handle_start(result);
        assert_eq!(name, None);
        match poll {
            PollingResult::Completed(r) => {
                let e = r.err().unwrap();
                assert_eq!(e.kind(), gax::error::ErrorKind::Other, "{e}")
            }
            _ => assert!(false, "{poll:?}"),
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
            .set_metadata(wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))?);
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
            _ => assert!(false, "{poll:?}"),
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
            .set_metadata(wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))?);
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
            PollingResult::Completed(r) => {
                let error = r.err().unwrap();
                assert!(format!("{error}").contains("exhausted"), "{error}");
            }
            _ => assert!(false, "{poll:?}"),
        };
        Ok(())
    }

    #[test]
    fn poll_error_continue() {
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let result = Err::<O, Error>(Error::other("test-only-error"));
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
                assert_eq!(e.kind(), gax::error::ErrorKind::Other, "{e}")
            }
            _ => assert!(false, "{poll:?}"),
        };
    }

    #[test]
    fn poll_error_finishes() {
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let result = Err::<O, Error>(Error::other("test-only-error"));
        let (name, poll) = handle_poll(
            Arc::new(Aip194Strict),
            Instant::now(),
            0,
            String::from("test-123"),
            result,
        );
        assert_eq!(name, None);
        match poll {
            PollingResult::Completed(r) => {
                assert!(r.is_err(), "{r:?}");
                let e = r.err().unwrap();
                assert_eq!(e.kind(), gax::error::ErrorKind::Other, "{e}")
            }
            _ => assert!(false, "{poll:?}"),
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
            .set_metadata(wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))?)
            .set_result(operation::Result::Response(
                wkt::Any::try_from(&wkt::Duration::clamp(234, 0))?.into(),
            ));
        let op = O::new(op);
        let (name, polling) = handle_common(op);
        assert_eq!(name, None);
        match polling {
            PollingResult::Completed(r) => {
                let response = r?;
                assert_eq!(response, wkt::Duration::clamp(234, 0));
            }
            _ => assert!(false, "{polling:?}"),
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
            .set_metadata(wkt::Any::try_from(&wkt::Timestamp::clamp(123, 0))?);
        let op = O::new(op);
        let (name, polling) = handle_common(op);
        assert_eq!(name.as_deref(), Some("test-only-name"));
        match &polling {
            PollingResult::InProgress(m) => {
                assert_eq!(m, &Some(wkt::Timestamp::clamp(123, 0)));
            }
            _ => assert!(false, "{polling:?}"),
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
            Any::try_from(&wkt::Duration::clamp(123, 0))?.into(),
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
                .set_message("test only")
                .into(),
        ));
        let op = O::new(op);
        let result = as_result(op);
        let err = result.err().unwrap();
        assert_eq!(err.kind(), gax::error::ErrorKind::Rpc, "{err}");

        Ok(())
    }

    #[test]
    fn extract_result_bad_type() -> TestResult {
        use longrunning::model::*;
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = super::Operation<R, M>;
        let op = Operation::default().set_result(operation::Result::Response(
            Any::try_from(&wkt::Timestamp::clamp(123, 0))?.into(),
        ));
        let op = O::new(op);
        let err = as_result(op).err().unwrap();
        assert_eq!(err.kind(), gax::error::ErrorKind::Other, "{err}");
        assert!(
            format!("{err}").contains("/google.protobuf.Timestamp"),
            "{err}"
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
        assert_eq!(err.kind(), gax::error::ErrorKind::Other, "{err}");
        assert!(format!("{err}").contains("missing result"), "{err}");

        Ok(())
    }

    #[test]
    fn extract_metadata() -> TestResult {
        type R = wkt::Duration;
        type M = wkt::Timestamp;
        type O = Operation<R, M>;
        let op = longrunning::model::Operation::default()
            .set_metadata(Any::try_from(&wkt::Timestamp::clamp(123, 0))?);

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
            .set_metadata(Any::try_from(&wkt::Duration::clamp(123, 0))?);
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
