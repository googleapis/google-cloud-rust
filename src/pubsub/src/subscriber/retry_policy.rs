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

use crate::Error;
use gax::error::rpc::Code;
use gax::retry_policy::RetryPolicy;
use gax::retry_result::RetryResult;
use gax::retry_state::RetryState;

/// The subscriber's retry policy, specifically for StreamingPull RPCs.
#[derive(Debug)]
pub(super) struct StreamRetryPolicy;

impl StreamRetryPolicy {
    /// Whether a stream error is transient (retry-able).
    pub(super) fn is_transient(error: Error) -> RetryResult {
        if error.is_transient_and_before_rpc() {
            return RetryResult::Continue(error);
        }
        if error.is_io() {
            return RetryResult::Continue(error);
        }
        if let Some(status) = error.status() {
            return match status.code {
                Code::ResourceExhausted | Code::Aborted | Code::Internal | Code::Unavailable => {
                    RetryResult::Continue(error)
                }
                Code::Unknown if is_rst_stream(&error) => RetryResult::Continue(error),
                _ => RetryResult::Permanent(error),
            };
        }
        RetryResult::Permanent(error)
    }
}

impl RetryPolicy for StreamRetryPolicy {
    fn on_error(&self, _state: &RetryState, error: Error) -> RetryResult {
        Self::is_transient(error)
    }
}

// Tonic surfaces [RST_STREAM] errors with an UNKNOWN status code. While UNKNOWN
// errors are not typically retry-able, RST_STREAM frames always are.
//
// We have seen a stream die with this error:
//
// ```
// tonic::Status {
//   code: Unknown,
//   message: "error reading a body from connection",
//   source: Some(hyper::Error(
//     Body,
//     h2::Error {
//       kind: Reset(StreamId(1), INTERNAL_ERROR, Remote)
//     }
//   ))
// }
// ```
//
// [RST_STREAM]: https://datatracker.ietf.org/doc/html/rfc7540#section-6.4
fn is_rst_stream(s: &Error) -> bool {
    // Sadly, there is no unit test coverage for this variant as
    // `h2::Error::is_reset()` is too difficult to trigger/fake.
    as_inner::<h2::Error, Error>(s).is_some_and(|e| e.is_reset())
}

fn as_inner<T, E>(error: &E) -> Option<&T>
where
    T: std::error::Error + 'static,
    E: std::error::Error,
{
    let mut e = error.source()?;
    // Prevent infinite loops due to cycles in the `source()` errors. This seems
    // unlikely, and it would require effort to create, but it is easy to
    // prevent.
    for _ in 0..32 {
        if let Some(value) = e.downcast_ref::<T>() {
            return Some(value);
        }
        e = e.source()?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use gax::error::CredentialsError;
    use gax::error::rpc::Status;
    use gax::throttle_result::ThrottleResult;
    use test_case::test_case;

    #[test]
    fn retry_transient_before_rpc() {
        let err = Error::authentication(CredentialsError::from_msg(true, "try again"));
        assert!(matches!(
            StreamRetryPolicy::is_transient(err),
            RetryResult::Continue(_)
        ));
    }

    #[test]
    fn retry_io() {
        let err = Error::io("try again");
        assert!(matches!(
            StreamRetryPolicy::is_transient(err),
            RetryResult::Continue(_)
        ));
    }

    #[test]
    fn retry_serde() {
        let err = Error::ser("fail");
        assert!(matches!(
            StreamRetryPolicy::is_transient(err),
            RetryResult::Permanent(_)
        ));

        let err = Error::deser("fail");
        assert!(matches!(
            StreamRetryPolicy::is_transient(err),
            RetryResult::Permanent(_)
        ));
    }

    #[test_case(Code::ResourceExhausted)]
    #[test_case(Code::Aborted)]
    #[test_case(Code::Internal)]
    #[test_case(Code::Unavailable)]
    fn retryable_status_codes(code: Code) {
        let err = Error::service(Status::default().set_code(code).set_message("try again"));
        assert!(matches!(
            StreamRetryPolicy::is_transient(err),
            RetryResult::Continue(_)
        ));
    }

    #[test_case(Code::Cancelled)]
    #[test_case(Code::Unknown)]
    #[test_case(Code::InvalidArgument)]
    #[test_case(Code::FailedPrecondition)]
    #[test_case(Code::DataLoss)]
    fn non_retryable_status_codes(code: Code) {
        let err = Error::service(Status::default().set_code(code).set_message("fail"));
        assert!(matches!(
            StreamRetryPolicy::is_transient(err),
            RetryResult::Permanent(_)
        ));
    }

    #[test]
    fn non_retryable_h2_error() {
        let inner = h2::Error::from(h2::Reason::PROTOCOL_ERROR);
        let err = Error::service_full(
            Status::default()
                .set_code(Code::Unknown)
                .set_message("fail"),
            None,
            None,
            Some(Box::new(inner)),
        );
        assert!(matches!(
            StreamRetryPolicy::is_transient(err),
            RetryResult::Permanent(_)
        ));
    }

    #[test]
    fn stream_retry_policy() {
        let retry = StreamRetryPolicy;
        let state = RetryState::default();

        let err = Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try again"),
        );
        assert!(matches!(
            retry.on_error(&state, err),
            RetryResult::Continue(_)
        ));

        let err = Error::service(
            Status::default()
                .set_code(Code::InvalidArgument)
                .set_message("fail"),
        );
        assert!(matches!(
            retry.on_error(&state, err),
            RetryResult::Permanent(_)
        ));

        let err = Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try again"),
        );
        assert!(matches!(
            retry.on_throttle(&state, err),
            ThrottleResult::Continue(_)
        ));

        assert_eq!(retry.remaining_time(&state), None);
    }
}
