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

//! Defines the retry policies for Google Cloud Storage.
//!
//! The storage service [recommends] retrying several 408, 429, and all 5xx HTTP
//! status codes. This is confirmed in the description of each status code:
//!
//! - [408 - Request Timeout][408]
//! - [429 - Too Many Requests][429]
//! - [500 - Internal Server Error][500]
//! - [502 - Bad Gateway][502]
//! - [503 - Service Unavailable][503]
//! - [504 - Gateway Timeout][504]
//!
//! In addition, resumable uploads return [308 - Resume Incomplete][308]. This
//! is not handled by the [RetryableErrors] retry policy.
//!
//! [recommends]: https://cloud.google.com/storage/docs/retry-strategy
//! [308]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#308_Resume_Incomplete
//! [408]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#408_Request_Timeout
//! [429]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#429_Too_Many_Requests
//! [500]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#500_Internal_Server_Error
//! [502]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#502_Bad_Gateway
//! [503]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#503_Service_Unavailable
//! [504]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#504_Gateway_Timeout

use gax::error::Error;
use gax::{
    retry_policy::{RetryLoopState, RetryPolicy, RetryPolicyExt},
    retry_result::RetryResult,
};
use std::sync::Arc;
use std::time::Duration;

/// The default retry policy for the Storage client.
///
/// The client will retry all the errors shown as retryable in the service
/// documentation, and stop retrying after 10 seconds.
pub(crate) fn storage_default() -> impl RetryPolicy {
    RetryableErrors.with_time_limit(Duration::from_secs(10))
}

/// Follows the [retry strategy] recommended by the Cloud Storage service guides.
///
/// This policy must be decorated to limit the number of retry attempts and/or
/// the duration of the retry loop.
///
/// # Example
/// ```
/// # use google_cloud_storage::retry_policy::RetryableErrors;
/// use gax::retry_policy::RetryPolicyExt;
/// use google_cloud_storage::client::Storage;
/// use std::time::Duration;
/// let builder = Storage::builder().with_retry_policy(
///     RetryableErrors
///         .with_time_limit(Duration::from_secs(60))
///         .with_attempt_limit(10),
/// );
/// ```
///
/// [retry strategy]: https://cloud.google.com/storage/docs/retry-strategy
#[derive(Clone, Debug)]
pub struct RetryableErrors;

impl RetryPolicy for RetryableErrors {
    fn on_error(&self, state: &RetryLoopState, error: Error) -> RetryResult {
        if error.is_transient_and_before_rpc() {
            return RetryResult::Continue(error);
        }
        if !state.idempotent {
            return RetryResult::Permanent(error);
        }
        if error.is_io() || error.is_timeout() {
            return RetryResult::Continue(error);
        }
        if error.is_transport() && error.http_status_code().is_none() {
            // Sometimes gRPC returns a transport error without an HTTP status
            // code. We treat all of these as I/O errors and therefore
            // retryable.
            return RetryResult::Continue(error);
        }
        if let Some(code) = error.http_status_code() {
            return match code {
                408 | 429 | 500..600 => RetryResult::Continue(error),
                _ => RetryResult::Permanent(error),
            };
        }
        if let Some(code) = error.status().map(|s| s.code) {
            use gax::error::rpc::Code;
            return match code {
                Code::Internal | Code::ResourceExhausted | Code::Unavailable => {
                    RetryResult::Continue(error)
                }
                // Over gRPC, the service returns DeadlineExceeded for some
                // "Internal Error; please retry" conditions.
                Code::DeadlineExceeded => RetryResult::Continue(error),
                _ => RetryResult::Permanent(error),
            };
        }
        RetryResult::Permanent(error)
    }
}

/// Decorate the retry policy to continue on 308 errors.
///
/// Used internally to handle the resumable upload loop.
#[derive(Clone, Debug)]
pub(crate) struct ContinueOn308<T> {
    inner: T,
}

impl<T> ContinueOn308<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl RetryPolicy for ContinueOn308<Arc<dyn RetryPolicy + 'static>> {
    fn on_error(&self, state: &RetryLoopState, error: Error) -> RetryResult {
        if error.http_status_code() == Some(308) {
            return RetryResult::Continue(error);
        }
        self.inner.on_error(state, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gax::error::rpc::Code;
    use gax::throttle_result::ThrottleResult;
    use http::HeaderMap;
    use test_case::test_case;

    #[test_case(408)]
    #[test_case(429)]
    #[test_case(500)]
    #[test_case(502)]
    #[test_case(503)]
    #[test_case(504)]
    fn retryable_http(code: u16) {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryLoopState::new(true), http_error(code))
                .is_continue()
        );
        assert!(
            p.on_error(&RetryLoopState::new(false), http_error(code))
                .is_permanent()
        );

        let t = p.on_throttle(&RetryLoopState::new(true), http_error(code));
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }

    #[test_case(401)]
    #[test_case(403)]
    fn not_recommended_http(code: u16) {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryLoopState::new(true), http_error(code))
                .is_permanent()
        );
        assert!(
            p.on_error(&RetryLoopState::new(false), http_error(code))
                .is_permanent()
        );

        let t = p.on_throttle(&RetryLoopState::new(true), http_error(code));
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }

    #[test_case(Code::Unavailable)]
    #[test_case(Code::Internal)]
    #[test_case(Code::ResourceExhausted)]
    #[test_case(Code::DeadlineExceeded)]
    fn retryable_grpc(code: Code) {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryLoopState::new(true), grpc_error(code))
                .is_continue()
        );
        assert!(
            p.on_error(&RetryLoopState::new(false), grpc_error(code))
                .is_permanent()
        );

        let t = p.on_throttle(&RetryLoopState::new(true), grpc_error(code));
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }

    #[test_case(Code::Unauthenticated)]
    #[test_case(Code::PermissionDenied)]
    fn not_recommended_grpc(code: Code) {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryLoopState::new(true), grpc_error(code))
                .is_permanent()
        );
        assert!(
            p.on_error(&RetryLoopState::new(false), grpc_error(code))
                .is_permanent()
        );

        let t = p.on_throttle(&RetryLoopState::new(true), grpc_error(code));
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }

    #[test]
    fn io() {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryLoopState::new(true), io_error())
                .is_continue()
        );
        assert!(
            p.on_error(&RetryLoopState::new(false), io_error())
                .is_permanent()
        );

        let t = p.on_throttle(&RetryLoopState::new(true), io_error());
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }

    #[test]
    fn timeout() {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryLoopState::new(true), timeout_error())
                .is_continue()
        );
        assert!(
            p.on_error(&RetryLoopState::new(false), timeout_error())
                .is_permanent()
        );

        let t = p.on_throttle(&RetryLoopState::new(true), timeout_error());
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }

    #[test]
    fn continue_on_308() {
        let inner: Arc<dyn RetryPolicy + 'static> = Arc::new(RetryableErrors);
        let p = ContinueOn308::new(inner);
        assert!(
            p.on_error(&RetryLoopState::new(true), http_error(308))
                .is_continue()
        );
        assert!(
            p.on_error(&RetryLoopState::new(false), http_error(308))
                .is_continue()
        );

        assert!(
            p.on_error(&RetryLoopState::new(true), http_error(429))
                .is_continue()
        );
        assert!(
            p.on_error(&RetryLoopState::new(false), http_error(429))
                .is_permanent()
        );

        let t = p.on_throttle(&RetryLoopState::new(true), http_error(308));
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");

        let t = p.on_throttle(&RetryLoopState::new(true), http_error(429));
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }

    fn http_error(code: u16) -> Error {
        Error::http(code, HeaderMap::new(), bytes::Bytes::new())
    }

    fn grpc_error(code: Code) -> Error {
        let status = gax::error::rpc::Status::default().set_code(code);
        Error::service(status)
    }

    fn timeout_error() -> Error {
        Error::timeout(tonic::Status::deadline_exceeded("try again"))
    }

    fn io_error() -> Error {
        Error::io(tonic::Status::unavailable("try again"))
    }
}
