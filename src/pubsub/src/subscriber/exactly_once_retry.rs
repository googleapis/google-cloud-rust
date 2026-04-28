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

use super::handler::AckResult;
use crate::error::AckError;
use google_cloud_gax::error::rpc::{Code, StatusDetails};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) fn process_attempt_error(
    ack_ids: Vec<String>,
    shared_err: Arc<crate::Error>,
) -> (HashMap<String, AckResult>, Vec<String>) {
    let (transient_failures, permanent_failures) = extract_failures(&shared_err);

    // If the response lacks specific per ack_id failure info, we treat the
    // response as all sharing the same RPC error.
    if transient_failures.is_empty() && permanent_failures.is_empty() {
        // The error is transient, retry.
        if let Some(status) = shared_err.status() {
            match status.code {
                Code::DeadlineExceeded
                | Code::ResourceExhausted
                | Code::Aborted
                | Code::Internal
                | Code::Unavailable => {
                    return (HashMap::new(), ack_ids);
                }
                _ => {}
            }
        }

        let to_confirm = ack_ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    Err(AckError::Rpc {
                        source: shared_err.clone(),
                    }),
                )
            })
            .collect();
        return (to_confirm, Vec::new());
    }

    // Otherwise, we extract specific failures:
    // - ack_ids with transient failures are to be retried.
    // - ack_ids with permanent failures are resolved with the RPC error.
    // - Unlisted ack_ids are considered successfully acknowledged.
    let mut transient = Vec::new();
    let mut to_confirm = HashMap::new();
    for id in ack_ids {
        if transient_failures.contains(&id) {
            transient.push(id);
        } else if permanent_failures.contains(&id) {
            to_confirm.insert(
                id,
                Err(AckError::Rpc {
                    source: shared_err.clone(),
                }),
            );
        } else {
            to_confirm.insert(id, Ok(()));
        }
    }

    (to_confirm, transient)
}

pub(super) fn extract_failures(e: &crate::Error) -> (HashSet<String>, HashSet<String>) {
    let mut transient = HashSet::new();
    let mut permanent = HashSet::new();
    if let Some(status) = e.status() {
        for detail in &status.details {
            if let StatusDetails::ErrorInfo(info) = detail {
                for (k, v) in &info.metadata {
                    if v.starts_with("TRANSIENT_FAILURE_") {
                        transient.insert(k.clone());
                    } else if v.starts_with("PERMANENT_FAILURE_") {
                        permanent.insert(k.clone());
                    }
                }
            }
        }
    }
    (transient, permanent)
}

#[cfg(test)]
mod tests {
    use super::super::lease_state::tests::{test_id, test_ids};
    use super::*;
    use crate::{Error, Response, Result};
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_rpc::model::ErrorInfo;
    use test_case::test_case;

    fn response_with_error_info(infos: Vec<ErrorInfo>) -> Result<Response<()>> {
        Err(Error::service(
            Status::default()
                .set_code(Code::FailedPrecondition)
                .set_message("fail")
                .set_details(infos.into_iter().map(StatusDetails::ErrorInfo)),
        ))
    }

    #[test]
    fn extract_failures() {
        let info = ErrorInfo::new()
            .set_reason("reason")
            .set_domain("domain")
            .set_metadata([
                ("ack_1", "TRANSIENT_FAILURE_UNORDERED_ACK_ID"),
                ("ack_2", "GIBBERISH_IGNORE"),
                ("ack_3", "TRANSIENT_FAILURE_OTHER"),
                ("ack_4", "PERMANENT_FAILURE_INVALID_ACK_ID"),
                ("ack_5", "PERMANENT_FAILURE_OTHER"),
            ]);

        let err = response_with_error_info(vec![info]).unwrap_err();
        let (transient, permanent) = super::extract_failures(&err);

        assert_eq!(
            transient,
            HashSet::from(["ack_1".to_string(), "ack_3".to_string()])
        );
        assert_eq!(
            permanent,
            HashSet::from(["ack_4".to_string(), "ack_5".to_string()])
        );
    }

    #[test]
    fn extract_failures_multiple_error_info() {
        let info1 =
            ErrorInfo::new().set_metadata([("ack_1", "TRANSIENT_FAILURE_UNORDERED_ACK_ID")]);
        let info2 = ErrorInfo::new().set_metadata([("ack_2", "PERMANENT_FAILURE_INVALID_ACK_ID")]);

        let err = response_with_error_info(vec![info1, info2]).unwrap_err();
        let (transient, permanent) = super::extract_failures(&err);

        assert_eq!(transient, HashSet::from(["ack_1".to_string()]));
        assert_eq!(permanent, HashSet::from(["ack_2".to_string()]));
    }

    #[test_case(Code::DeadlineExceeded)]
    #[test_case(Code::ResourceExhausted)]
    #[test_case(Code::Aborted)]
    #[test_case(Code::Internal)]
    #[test_case(Code::Unavailable)]
    #[tokio::test]
    async fn process_attempt_error_retryable_code_without_error_info(
        code: Code,
    ) -> anyhow::Result<()> {
        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("retryable error"),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..3), err);

        assert_eq!(remaining, test_ids(1..3));
        assert!(confirmed_acks.is_empty(), "{confirmed_acks:?}");

        Ok(())
    }

    #[test_case(Code::DeadlineExceeded)]
    #[test_case(Code::ResourceExhausted)]
    #[test_case(Code::Aborted)]
    #[test_case(Code::Internal)]
    #[test_case(Code::Unavailable)]
    #[tokio::test]
    async fn process_attempt_error_retryable_code_with_error_info(
        code: Code,
    ) -> anyhow::Result<()> {
        let info = ErrorInfo::new().set_metadata([
            (test_id(1), "PERMANENT_FAILURE_INVALID_ACK_ID"),
            (test_id(2), "TRANSIENT_FAILURE_OTHER"),
        ]);
        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("retryable error")
                .set_details([StatusDetails::ErrorInfo(info)]),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..4), err.clone());

        assert_eq!(remaining, vec![test_id(2)]);

        let err = AckError::Rpc { source: err };
        let expected = [(test_id(1), Err(err)), (test_id(3), Ok(()))]
            .into_iter()
            .collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }

    #[test_case(Code::Cancelled)]
    #[test_case(Code::Unknown)]
    #[test_case(Code::InvalidArgument)]
    #[test_case(Code::NotFound)]
    #[test_case(Code::AlreadyExists)]
    #[test_case(Code::PermissionDenied)]
    #[test_case(Code::Unauthenticated)]
    #[test_case(Code::FailedPrecondition)]
    #[test_case(Code::OutOfRange)]
    #[test_case(Code::Unimplemented)]
    #[test_case(Code::DataLoss)]
    #[tokio::test]
    async fn process_attempt_error_non_retryable_code_without_error_info(
        code: Code,
    ) -> anyhow::Result<()> {
        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("non-retryable error"),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..3), err);

        assert!(remaining.is_empty(), "{remaining:?}");

        let expected = test_ids(1..3)
            .into_iter()
            .map(|id| {
                let err = AckError::Rpc {
                    source: Arc::new(Error::service(
                        Status::default()
                            .set_code(code)
                            .set_message("non-retryable error"),
                    )),
                };
                (id, Err(err))
            })
            .collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }

    #[test_case(Code::Cancelled)]
    #[test_case(Code::Unknown)]
    #[test_case(Code::InvalidArgument)]
    #[test_case(Code::NotFound)]
    #[test_case(Code::AlreadyExists)]
    #[test_case(Code::PermissionDenied)]
    #[test_case(Code::Unauthenticated)]
    #[test_case(Code::FailedPrecondition)]
    #[test_case(Code::OutOfRange)]
    #[test_case(Code::Unimplemented)]
    #[test_case(Code::DataLoss)]
    #[tokio::test]
    async fn process_attempt_error_non_retryable_code_permanent_failure(
        code: Code,
    ) -> anyhow::Result<()> {
        let info =
            ErrorInfo::new().set_metadata([(test_id(1), "PERMANENT_FAILURE_INVALID_ACK_ID")]);

        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("non-retryable error")
                .set_details([StatusDetails::ErrorInfo(info.clone())]),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..3), err);

        assert!(remaining.is_empty(), "{remaining:?}");

        let err = AckError::Rpc {
            source: Arc::new(Error::service(
                Status::default()
                    .set_code(code)
                    .set_message("non-retryable error")
                    .set_details([StatusDetails::ErrorInfo(info)]),
            )),
        };
        let expected = [(test_id(1), Err(err)), (test_id(2), Ok(()))]
            .into_iter()
            .collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }

    #[test_case(Code::Cancelled)]
    #[test_case(Code::Unknown)]
    #[test_case(Code::InvalidArgument)]
    #[test_case(Code::NotFound)]
    #[test_case(Code::AlreadyExists)]
    #[test_case(Code::PermissionDenied)]
    #[test_case(Code::Unauthenticated)]
    #[test_case(Code::FailedPrecondition)]
    #[test_case(Code::OutOfRange)]
    #[test_case(Code::Unimplemented)]
    #[test_case(Code::DataLoss)]
    #[tokio::test]
    async fn process_attempt_error_non_retryable_code_transient_failure(
        code: Code,
    ) -> anyhow::Result<()> {
        let info = ErrorInfo::new().set_metadata([(test_id(1), "TRANSIENT_FAILURE_OTHER")]);

        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("non-retryable error")
                .set_details([StatusDetails::ErrorInfo(info)]),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..3), err);

        assert_eq!(remaining, test_ids(1..2));

        let expected = [(test_id(2), Ok(()))].into_iter().collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }
}
