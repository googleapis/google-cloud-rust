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

//! Defines a retry policy for BigQuery.

use google_cloud_bigquery_v2::model::ErrorProto;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_policy::{RetryPolicy, RetryPolicyExt};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use std::sync::Arc;

/// Follows the retry strategy recommended by the BigQuery guides on error handling.
///
/// ```ignore
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_gax::retry_policy::RetryPolicyExt;
/// # use google_cloud_bigquery::client::BigQuery;
/// # use google_cloud_bigquery::retry_policy::RetryableErrors;
/// let policy = RetryableErrors.with_time_limit(std::time::Duration::from_secs(60));
/// let client = BigQuery::builder()
///     .build()
///     .await?;
/// let query = client.query("SELECT 1")
///     .with_retry_policy(policy);
/// # Ok(())
/// # }
/// ```
///
/// This policy must be decorated to limit the duration or attempts of the retry loop.
#[derive(Clone, Debug)]
pub struct RetryableErrors;

impl RetryPolicy for RetryableErrors {
    fn on_error(&self, _state: &RetryState, error: GaxError) -> RetryResult {
        if error.is_transient_and_before_rpc() || error.is_io() || error.is_timeout() {
            return RetryResult::Continue(error);
        }
        if error.is_transport() && error.http_status_code().is_none() {
            return RetryResult::Continue(error);
        }
        if let Some(429 | 500 | 502 | 503 | 504) = error.http_status_code() {
            return RetryResult::Continue(error);
        }
        if let Some(status) = error.status() {
            return match status.code {
                Code::Aborted
                | Code::DeadlineExceeded
                | Code::Internal
                | Code::ResourceExhausted
                | Code::Unavailable
                | Code::Unknown => RetryResult::Continue(error),
                _ => RetryResult::Permanent(error),
            };
        }
        RetryResult::Permanent(error)
    }
}

#[allow(dead_code)]
pub(crate) fn default_retry_policy() -> Arc<dyn RetryPolicy> {
    // TODO(#6218): Define better attempt limits.
    Arc::new(RetryableErrors.with_attempt_limit(3))
}

#[allow(dead_code)]
pub(crate) fn default_backoff_policy() -> Arc<dyn BackoffPolicy> {
    Arc::new(
        ExponentialBackoffBuilder::default()
            .with_initial_delay(std::time::Duration::from_secs(1))
            .with_maximum_delay(std::time::Duration::from_secs(32))
            .with_scaling(2.0)
            .build()
            .expect("valid backoff configuration"),
    )
}

/// Maps BigQuery job error reasons (`backendError`, `rateLimitExceeded`, etc.) to
/// canonical gRPC status codes (`Code`).
///
/// See the official [error messages] BigQuery documentation:
///
/// [error messages]: https://cloud.google.com/bigquery/docs/error-messages
#[allow(dead_code)]
pub(crate) fn error_reason_to_code(reason: &str) -> Code {
    match reason {
        "backendError" | "jobBackendError" => Code::Unavailable,
        "internalError" => Code::Internal,
        "rateLimitExceeded" | "jobRateLimitExceeded" => Code::ResourceExhausted,
        _ => Code::InvalidArgument,
    }
}

#[allow(dead_code)]
pub(crate) fn is_query_error_retryable(err: &crate::error::QueryError) -> bool {
    match err {
        crate::error::QueryError::JobFailed { errors } => is_retryable_errors(errors),
        _ => false,
    }
}

#[allow(dead_code)]
pub(crate) fn query_job_failed_to_gax_error(err: &crate::error::QueryError) -> Option<GaxError> {
    let crate::error::QueryError::JobFailed { errors } = err else {
        return None;
    };
    let code = errors
        .iter()
        .find_map(|e| is_retryable_error_reason(&e.reason).then(|| error_reason_to_code(&e.reason)))
        .unwrap_or(Code::Unavailable);

    Some(GaxError::service(
        Status::default()
            .set_code(code)
            .set_message("JobFailed: retryable server error"),
    ))
}

#[allow(dead_code)]
pub(crate) fn is_retryable_error_reason(reason: &str) -> bool {
    matches!(
        reason,
        "backendError"
            | "jobBackendError"
            | "rateLimitExceeded"
            | "jobRateLimitExceeded"
            | "internalError"
    )
}

#[allow(dead_code)]
pub(crate) fn is_retryable_errors(errors: &[ErrorProto]) -> bool {
    !errors.is_empty() && errors.iter().any(|e| is_retryable_error_reason(&e.reason))
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_bigquery_v2::model::ErrorProto;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::retry_state::RetryState;
    use http::HeaderMap;
    use test_case::test_case;

    #[test_case("backendError", true)]
    #[test_case("jobBackendError", true)]
    #[test_case("rateLimitExceeded", true)]
    #[test_case("jobRateLimitExceeded", true)]
    #[test_case("internalError", true)]
    #[test_case("invalidQuery", false)]
    #[test_case("notFound", false)]
    fn test_is_retryable_error_reason(reason: &str, expected: bool) {
        assert_eq!(is_retryable_error_reason(reason), expected);
    }

    #[test]
    fn test_is_retryable_errors() {
        assert!(!is_retryable_errors(&[]));

        let non_retryable = vec![ErrorProto::new().set_reason("invalidQuery")];
        assert!(!is_retryable_errors(&non_retryable));

        let retryable = vec![
            ErrorProto::new().set_reason("invalidQuery"),
            ErrorProto::new().set_reason("backendError"),
        ];
        assert!(is_retryable_errors(&retryable));
    }

    #[test]
    fn test_retryable_errors_on_error() {
        let p = RetryableErrors;
        let state = RetryState::default();

        let retryable_codes = [
            Code::Aborted,
            Code::DeadlineExceeded,
            Code::Internal,
            Code::ResourceExhausted,
            Code::Unavailable,
            Code::Unknown,
        ];
        for code in retryable_codes {
            let err = GaxError::service(Status::default().set_code(code));
            assert!(
                p.on_error(&state, err).is_continue(),
                "expected continue for {:?}",
                code
            );
        }

        let permanent_codes = [
            Code::NotFound,
            Code::PermissionDenied,
            Code::InvalidArgument,
        ];
        for code in permanent_codes {
            let err = GaxError::service(Status::default().set_code(code));
            assert!(
                p.on_error(&state, err).is_permanent(),
                "expected permanent for {:?}",
                code
            );
        }

        let retryable_http = [429, 500, 502, 503, 504];
        for code in retryable_http {
            let err = GaxError::http(code, HeaderMap::new(), bytes::Bytes::new());
            assert!(
                p.on_error(&state, err).is_continue(),
                "expected continue for HTTP {}",
                code
            );
        }

        let permanent_http = [400, 404, 408, 409, 501];
        for code in permanent_http {
            let err = GaxError::http(code, HeaderMap::new(), bytes::Bytes::new());
            assert!(
                p.on_error(&state, err).is_permanent(),
                "expected permanent for HTTP {}",
                code
            );
        }
    }

    #[test]
    fn test_attempt_limit() {
        let policy = default_retry_policy();
        let retryable_err = || GaxError::service(Status::default().set_code(Code::Unavailable));

        let mut state = RetryState::default();
        assert!(policy.on_error(&state, retryable_err()).is_continue());

        state.attempt_count = 1;
        assert!(policy.on_error(&state, retryable_err()).is_continue());

        state.attempt_count = 2;
        assert!(policy.on_error(&state, retryable_err()).is_continue());

        state.attempt_count = 3;
        assert!(policy.on_error(&state, retryable_err()).is_exhausted());

        state.attempt_count = 4;
        assert!(policy.on_error(&state, retryable_err()).is_exhausted());

        state.attempt_count = 0;
        let perm_err = GaxError::service(Status::default().set_code(Code::InvalidArgument));
        assert!(policy.on_error(&state, perm_err).is_permanent());
    }
}
