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

//! Defines the retry policies for the Google Cloud Pub/Sub Publisher.
//!
//! The Pub/Sub service [recommends] retrying several transient error codes.
//!
//! - [Unavailable][Code::Unavailable]
//! - [Internal][Code::Internal]
//! - [Resource Exhausted][Code::ResourceExhausted]
//! - [Aborted][Code::Aborted]
//! - [Deadline Exceeded][Code::DeadlineExceeded]
//! - [Cancelled][Code::Cancelled]
//! - [Unknown][Code::Unknown]
//!
//! [recommends]: https://docs.cloud.google.com/pubsub/docs/reference/error-codes
//! [Code::Unavailable]: google_cloud_gax::error::rpc::Code::Unavailable
//! [Code::Internal]: google_cloud_gax::error::rpc::Code::Internal
//! [Code::ResourceExhausted]: google_cloud_gax::error::rpc::Code::ResourceExhausted
//! [Code::Aborted]: google_cloud_gax::error::rpc::Code::Aborted
//! [Code::DeadlineExceeded]: google_cloud_gax::error::rpc::Code::DeadlineExceeded
//! [Code::Cancelled]: google_cloud_gax::error::rpc::Code::Cancelled
//! [Code::Unknown]: google_cloud_gax::error::rpc::Code::Unknown

use crate::Error;
use google_cloud_gax::retry_policy::{RetryPolicy, RetryPolicyExt};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use std::time::Duration;

/// The default retry policy for the Pub/Sub publisher.
///
/// The client will retry all the errors shown as retryable in the service
/// documentation, and stop retrying after 10 minutes.
pub(crate) fn default_retry_policy() -> impl RetryPolicy {
    RetryableErrors.with_time_limit(Duration::from_secs(600))
}

/// Follows the retry strategy recommended by the Cloud Pub/Sub guides on
/// [error codes].
///
/// This policy must be decorated to limit the duration of the retry loop.
///
/// [error codes]: https://docs.cloud.google.com/pubsub/docs/reference/error-codes
#[derive(Clone, Debug)]
pub struct RetryableErrors;

impl RetryPolicy for RetryableErrors {
    fn on_error(&self, _state: &RetryState, error: Error) -> RetryResult {
        if error.is_transient_and_before_rpc() {
            return RetryResult::Continue(error);
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

        // Catch raw HTTP errors that may not have been mapped to a gRPC status.
        // - 408: Request Timeout
        // - 429: Resource Exhausted
        // - 499: Cancelled Request
        // - 5xx: Internal Server Error, Bad Gateway, etc.
        if let Some(408 | 429 | 499 | 500..600) = error.http_status_code() {
            return RetryResult::Continue(error);
        }

        if let Some(status) = error.status() {
            use google_cloud_gax::error::rpc::Code;
            return match status.code {
                Code::Aborted
                | Code::Cancelled
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

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::retry_state::RetryState;
    use http::HeaderMap;
    use test_case::test_case;

    #[test]
    fn transport_reset() {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryState::default(), transport_err())
                .is_continue()
        );
    }

    #[test_case(408)]
    #[test_case(429)]
    #[test_case(499)]
    #[test_case(500)]
    #[test_case(502)]
    #[test_case(503)]
    #[test_case(504)]
    fn retryable_http(code: u16) {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryState::default(), http_error(code))
                .is_continue()
        );
    }

    #[test_case(409)]
    #[test_case(400)]
    #[test_case(404)]
    fn permanent_http(code: u16) {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryState::default(), http_error(code))
                .is_permanent()
        );
    }

    #[test_case(Code::Unavailable)]
    #[test_case(Code::Internal)]
    #[test_case(Code::Aborted)]
    #[test_case(Code::ResourceExhausted)]
    #[test_case(Code::DeadlineExceeded)]
    #[test_case(Code::Cancelled)]
    #[test_case(Code::Unknown)]
    fn retryable_grpc(code: Code) {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryState::default(), grpc_error(code))
                .is_continue()
        );
    }

    #[test_case(Code::NotFound)]
    #[test_case(Code::PermissionDenied)]
    #[test_case(Code::InvalidArgument)]
    fn permanent_grpc(code: Code) {
        let p = RetryableErrors;
        assert!(
            p.on_error(&RetryState::default(), grpc_error(code))
                .is_permanent()
        );
    }

    #[test]
    fn io() {
        let p = RetryableErrors;
        assert!(p.on_error(&RetryState::default(), io_error()).is_continue());
    }

    #[test]
    fn permanent_auth() {
        let p = RetryableErrors;
        let auth_error =
            google_cloud_gax::error::CredentialsError::from_msg(false, "permanent auth error");
        assert!(
            p.on_error(&RetryState::default(), Error::authentication(auth_error))
                .is_permanent()
        );
    }

    #[test]
    fn transient_auth() {
        let p = RetryableErrors;
        let auth_error =
            google_cloud_gax::error::CredentialsError::from_msg(true, "transient auth error");
        assert!(
            p.on_error(&RetryState::default(), Error::authentication(auth_error))
                .is_continue()
        );
    }

    fn transport_err() -> Error {
        Error::transport(HeaderMap::new(), "connection closed")
    }

    fn http_error(code: u16) -> Error {
        Error::http(code, HeaderMap::new(), bytes::Bytes::new())
    }

    fn grpc_error(code: Code) -> Error {
        let status = Status::default().set_code(code).set_message("try again");
        Error::service(status)
    }

    fn io_error() -> Error {
        Error::io(gaxi::grpc::tonic::Status::unavailable("try again"))
    }
}
