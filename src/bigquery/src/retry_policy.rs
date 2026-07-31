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

use crate::error::QueryError;
use google_cloud_bigquery_v2::model::ErrorProto;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use std::sync::Arc;
use std::time::Duration;

/// Follows the RPC retry strategy recommended by the BigQuery guides on error handling.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct RetryableErrors;

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
    Arc::new(RetryableErrors)
}

#[allow(dead_code)]
pub(crate) fn default_backoff_policy() -> Arc<dyn BackoffPolicy> {
    Arc::new(
        ExponentialBackoffBuilder::default()
            .with_initial_delay(Duration::from_secs(1))
            .with_maximum_delay(Duration::from_secs(32))
            .with_scaling(2.0)
            .build()
            .expect("valid backoff configuration"),
    )
}

/// The result of evaluating a BigQuery job error against a [`JobRetryPolicy`].
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum JobRetryResult {
    Continue(Duration, QueryError),
    Exhausted(QueryError),
    Permanent(QueryError),
}

#[cfg(test)]
impl JobRetryResult {
    pub(crate) fn is_continue(&self) -> bool {
        matches!(self, Self::Continue(_, _))
    }
    pub(crate) fn is_exhausted(&self) -> bool {
        matches!(self, Self::Exhausted(_))
    }
    pub(crate) fn is_permanent(&self) -> bool {
        matches!(self, Self::Permanent(_))
    }
}

/// A policy trait for handling BigQuery job execution retries.
///
/// Note: This trait is kept crate-internal for now. It is a copy/adaptation of GAX's
/// `RetryPolicy` trait specifically tailored for BigQuery job-level re-issuance.
/// In the future, we plan to refine this trait and discuss how to make it public
/// so customers can provide their own custom job retry policies.
#[allow(dead_code)]
pub(crate) trait JobRetryPolicy<S = RetryState>: Send + Sync + std::fmt::Debug {
    fn on_error(&self, state: &S, error: crate::error::QueryError) -> JobRetryResult;
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct RetryableJobErrors {
    attempt_limit: u32,
    backoff: Arc<dyn BackoffPolicy>,
}

impl Default for RetryableJobErrors {
    fn default() -> Self {
        Self {
            attempt_limit: 3,
            backoff: default_backoff_policy(),
        }
    }
}

impl RetryableJobErrors {
    #[allow(dead_code)]
    pub fn with_attempt_limit(mut self, attempt_limit: u32) -> Self {
        self.attempt_limit = attempt_limit;
        self
    }

    #[allow(dead_code)]
    pub fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.backoff = v.into().into();
        self
    }
}

impl JobRetryPolicy for RetryableJobErrors {
    fn on_error(&self, state: &RetryState, error: crate::error::QueryError) -> JobRetryResult {
        if !is_query_error_retryable(&error) {
            return JobRetryResult::Permanent(error);
        }
        if state.attempt_count >= self.attempt_limit {
            return JobRetryResult::Exhausted(error);
        }
        let delay = self.backoff.on_failure(state);
        JobRetryResult::Continue(delay, error)
    }
}

#[allow(dead_code)]
pub(crate) fn default_job_retry_policy() -> Arc<dyn JobRetryPolicy> {
    Arc::new(RetryableJobErrors::default())
}

#[allow(dead_code)]
pub(crate) fn is_query_error_retryable(err: &crate::error::QueryError) -> bool {
    match err {
        crate::error::QueryError::JobFailed { errors } => is_retryable_errors(errors),
        _ => false,
    }
}

#[allow(dead_code)]
pub(crate) fn is_retryable_errors(errors: &[ErrorProto]) -> bool {
    !errors.is_empty() && errors.iter().any(|e| is_retryable_error_reason(&e.reason))
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::tests::create_test_backoff_policy;
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
    fn test_job_retryable_errors() {
        let policy = RetryableJobErrors::default();
        let state = RetryState::default();

        let retryable_err = crate::error::QueryError::JobFailed {
            errors: vec![ErrorProto::new().set_reason("backendError")],
        };
        assert!(policy.on_error(&state, retryable_err).is_continue());

        let permanent_err = crate::error::QueryError::JobFailed {
            errors: vec![ErrorProto::new().set_reason("invalidQuery")],
        };
        assert!(policy.on_error(&state, permanent_err).is_permanent());
    }

    #[test]
    fn test_job_attempt_limit() {
        let policy = default_job_retry_policy();
        let retryable_err = || crate::error::QueryError::JobFailed {
            errors: vec![ErrorProto::new().set_reason("backendError")],
        };

        let mut state = RetryState::default();
        assert!(policy.on_error(&state, retryable_err()).is_continue());

        state.attempt_count = 1;
        assert!(policy.on_error(&state, retryable_err()).is_continue());

        state.attempt_count = 2;
        assert!(policy.on_error(&state, retryable_err()).is_continue());

        state.attempt_count = 3;
        assert!(policy.on_error(&state, retryable_err()).is_exhausted());
    }

    #[test]
    fn test_job_backoff_policy() {
        let mut backoff = create_test_backoff_policy();
        backoff
            .expect_on_failure()
            .return_const(Duration::from_secs(5));

        let policy = RetryableJobErrors::default().with_backoff_policy(backoff);
        let retryable_err = crate::error::QueryError::JobFailed {
            errors: vec![ErrorProto::new().set_reason("backendError")],
        };

        let state = RetryState::default();
        if let JobRetryResult::Continue(delay, _) = policy.on_error(&state, retryable_err) {
            assert_eq!(delay, Duration::from_secs(5));
        } else {
            panic!("expected Continue with 5s delay");
        }
    }
}
