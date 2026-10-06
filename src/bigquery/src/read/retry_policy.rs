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

//! Defines the retry policy for the BigQuery Storage Read API.

use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;

/// Follows the RPC retry strategy recommended for the BigQuery Storage Read API.
///
/// This policy retries transient network/transport errors, HTTP/2 stream resets,
/// rate-limit errors (`ResourceExhausted`), and transient server statuses
/// (`Unavailable`, `Internal`, `Aborted`, `Cancelled`, `DeadlineExceeded`) when
/// the operation is idempotent.
///
/// This policy must be decorated to limit the duration of the retry loop or
/// the number of attempts.
///
/// # Example
/// ```
/// # use google_cloud_bigquery::client::Read;
/// # use google_cloud_bigquery::read::retry_policy::RetryableErrors;
/// # use google_cloud_gax::retry_policy::RetryPolicyExt;
/// # async fn sample(client: Read) -> anyhow::Result<()> {
/// let mut reader = client
///     .read_rows()
///     .set_read_stream("projects/my-project/locations/us/sessions/s1/streams/st1")
///     .into_reader()
///     .with_retry_policy(RetryableErrors.with_attempt_limit(5));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct RetryableErrors;

impl RetryPolicy for RetryableErrors {
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        if error.is_transient_and_before_rpc() {
            return RetryResult::Continue(error);
        }
        if !state.idempotent {
            return RetryResult::Permanent(error);
        }
        if error.is_io() || error.is_timeout() || error.is_connect() {
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
                | Code::Cancelled
                | Code::DeadlineExceeded
                | Code::Internal
                | Code::ResourceExhausted
                | Code::Unavailable => RetryResult::Continue(error),
                _ => RetryResult::Permanent(error),
            };
        }
        RetryResult::Permanent(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::CredentialsError;
    use google_cloud_gax::error::rpc::Status;
    use http::HeaderMap;
    use test_case::test_case;

    #[test_case(Code::Aborted)]
    #[test_case(Code::Cancelled)]
    #[test_case(Code::DeadlineExceeded)]
    #[test_case(Code::Internal)]
    #[test_case(Code::ResourceExhausted)]
    #[test_case(Code::Unavailable)]
    fn retryable_status_codes(code: Code) {
        let err = || Error::service(Status::default().set_code(code).set_message("try again"));
        assert!(matches!(
            RetryableErrors.on_error(&RetryState::new(true), err()),
            RetryResult::Continue(_)
        ));
        assert!(matches!(
            RetryableErrors.on_error(&RetryState::new(false), err()),
            RetryResult::Permanent(_)
        ));
    }

    #[test_case(Code::Unknown)]
    #[test_case(Code::InvalidArgument)]
    #[test_case(Code::NotFound)]
    #[test_case(Code::PermissionDenied)]
    #[test_case(Code::FailedPrecondition)]
    #[test_case(Code::DataLoss)]
    fn permanent_status_codes(code: Code) {
        let err = Error::service(Status::default().set_code(code).set_message("fail"));
        assert!(matches!(
            RetryableErrors.on_error(&RetryState::new(true), err),
            RetryResult::Permanent(_)
        ));
    }

    #[test_case(429)]
    #[test_case(500)]
    #[test_case(502)]
    #[test_case(503)]
    #[test_case(504)]
    fn retryable_http_status_codes(code: u16) {
        let err = Error::http(code, HeaderMap::new(), bytes::Bytes::new());
        assert!(matches!(
            RetryableErrors.on_error(&RetryState::new(true), err),
            RetryResult::Continue(_)
        ));
    }

    #[test_case(400)]
    #[test_case(401)]
    #[test_case(403)]
    #[test_case(404)]
    #[test_case(408)]
    #[test_case(409)]
    #[test_case(501)]
    fn permanent_http_status_codes(code: u16) {
        let err = Error::http(code, HeaderMap::new(), bytes::Bytes::new());
        assert!(matches!(
            RetryableErrors.on_error(&RetryState::new(true), err),
            RetryResult::Permanent(_)
        ));
    }

    #[test]
    fn retry_transient_before_rpc() {
        let err = Error::authentication(CredentialsError::from_msg(true, "try again"));
        assert!(matches!(
            RetryableErrors.on_error(&RetryState::new(false), err),
            RetryResult::Continue(_)
        ));

        let err = Error::authentication(CredentialsError::from_msg(false, "fail"));
        assert!(matches!(
            RetryableErrors.on_error(&RetryState::new(true), err),
            RetryResult::Permanent(_)
        ));
    }

    #[test]
    fn retry_io_timeout_connect_transport() {
        let state = RetryState::new(true);
        assert!(
            RetryableErrors
                .on_error(&state, Error::io("io"))
                .is_continue()
        );
        assert!(
            RetryableErrors
                .on_error(&state, Error::timeout("timeout"))
                .is_continue()
        );
        assert!(
            RetryableErrors
                .on_error(&state, Error::connect("connect"))
                .is_continue()
        );
        assert!(
            RetryableErrors
                .on_error(&state, Error::transport(HeaderMap::new(), "rst_stream"))
                .is_continue()
        );
    }

    #[test]
    fn permanent_serde() {
        let state = RetryState::new(true);
        assert!(
            RetryableErrors
                .on_error(&state, Error::ser("fail"))
                .is_permanent()
        );
        assert!(
            RetryableErrors
                .on_error(&state, Error::deser("fail"))
                .is_permanent()
        );
    }
}
