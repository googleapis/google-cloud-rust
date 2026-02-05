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
use gax::backoff_policy::BackoffPolicy;
use gax::error::rpc::Code;
use gax::options::RequestOptions;
use gax::retry_policy::{RetryPolicy, RetryPolicyExt};
use gax::retry_result::RetryResult;
use gax::retry_state::RetryState;
use std::time::Duration;

/// The subscriber's retry policy, specifically for StreamingPull RPCs.
///
/// This same policy applies to both starting and resuming a stream.
#[derive(Debug)]
pub(super) struct StreamRetryPolicy;

impl StreamRetryPolicy {
    pub(super) fn on_midstream_error(error: Error) -> RetryResult {
        if error.is_transport() {
            // Resume streams that fail with transport errors.
            return RetryResult::Continue(error);
        }
        let s = Self;
        s.on_error(&RetryState::default(), error)
    }
}

impl RetryPolicy for StreamRetryPolicy {
    fn on_error(&self, _state: &RetryState, error: Error) -> RetryResult {
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
                _ => RetryResult::Permanent(error),
            };
        }
        RetryResult::Permanent(error)
    }
}

#[derive(Debug)]
struct OnlyTransportErrors;

impl RetryPolicy for OnlyTransportErrors {
    fn on_error(&self, _state: &RetryState, error: Error) -> RetryResult {
        if error.is_transport() {
            RetryResult::Continue(error)
        } else {
            RetryResult::Permanent(error)
        }
    }
}

#[derive(Debug)]
struct NoBackoff;

impl BackoffPolicy for NoBackoff {
    fn on_failure(&self, _state: &RetryState) -> Duration {
        Duration::ZERO
    }
}

#[allow(dead_code)] // TODO(#4471) - use in DefaultLeaser
/// The policies for lease management RPCs in at-least-once delivery.
///
/// Specifically, these are the `Acknowledge` and `ModifyAckDeadline` RPCs.
///
/// The GFE will send a GO_AWAY frame to a tonic channel that has been open for
/// an hour. Tonic will reset the connection, but this can race with accepting
/// one of our requests.
///
/// These requests fail with a variety of transport errors. It is safe to retry
/// these requests immediately.
///
/// Note that an RPC attempt that fails because a channel is closed may be
/// retried on another channel that is closed. That is why we retry up to N
/// times where N is the number of channels in the client.
pub(super) fn at_least_once_options(grpc_subchannel_count: usize) -> RequestOptions {
    let mut o = RequestOptions::default();
    o.set_retry_policy(OnlyTransportErrors.with_attempt_limit(grpc_subchannel_count as u32 + 1));
    o.set_backoff_policy(NoBackoff);
    o
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use gax::error::CredentialsError;
    use gax::error::rpc::Status;
    use gax::throttle_result::ThrottleResult;
    use http::HeaderMap;
    use test_case::test_case;

    #[test]
    fn retry_transient_before_rpc() {
        let err = Error::authentication(CredentialsError::from_msg(true, "try again"));
        assert!(matches!(
            StreamRetryPolicy::on_midstream_error(err),
            RetryResult::Continue(_)
        ));
    }

    #[test]
    fn retry_io() {
        let err = Error::io("try again");
        assert!(matches!(
            StreamRetryPolicy::on_midstream_error(err),
            RetryResult::Continue(_)
        ));
    }

    #[test]
    fn retry_serde() {
        let err = Error::ser("fail");
        assert!(matches!(
            StreamRetryPolicy::on_midstream_error(err),
            RetryResult::Permanent(_)
        ));

        let err = Error::deser("fail");
        assert!(matches!(
            StreamRetryPolicy::on_midstream_error(err),
            RetryResult::Permanent(_)
        ));
    }

    #[test]
    fn retry_transport_errors() {
        let midstream_err = Error::transport(HeaderMap::new(), "RST_STREAM");
        assert!(matches!(
            StreamRetryPolicy::on_midstream_error(midstream_err),
            RetryResult::Continue(_)
        ));

        let retry = StreamRetryPolicy;
        let open_stream_err = Error::transport(HeaderMap::new(), "bad endpoint");
        assert!(matches!(
            retry.on_error(&RetryState::default(), open_stream_err),
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
            StreamRetryPolicy::on_midstream_error(err),
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
            StreamRetryPolicy::on_midstream_error(err),
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

    #[test]
    fn only_transport_errors() {
        let retry = OnlyTransportErrors;

        assert!(matches!(
            retry.on_error(&RetryState::default(), transport_err()),
            RetryResult::Continue(_)
        ));

        assert!(matches!(
            retry.on_error(&RetryState::default(), non_transport_err()),
            RetryResult::Permanent(_)
        ));
    }

    #[test]
    fn no_backoff() {
        let backoff = NoBackoff;
        assert_eq!(backoff.on_failure(&RetryState::default()), Duration::ZERO);
    }

    #[test]
    fn at_least_once_options() {
        let o = super::at_least_once_options(42);
        verify_policies(o, 42);
    }

    #[track_caller]
    pub(in super::super) fn verify_policies(o: RequestOptions, grpc_subchannel_count: u32) {
        let retry = o.retry_policy().clone().unwrap();
        let backoff = o.backoff_policy().clone().unwrap();

        let mut state = RetryState::default();
        state.attempt_count = 1;
        assert!(
            matches!(
                retry.on_error(&state, transport_err()),
                RetryResult::Continue(_)
            ),
            "initial transport error should be retried"
        );
        assert!(
            matches!(
                retry.on_error(&state, non_transport_err()),
                RetryResult::Permanent(_)
            ),
            "non-transport error should not be retried"
        );
        assert_eq!(
            backoff.on_failure(&state),
            Duration::ZERO,
            "the backoff should always be 0"
        );

        state.attempt_count = grpc_subchannel_count;
        assert!(
            matches!(
                retry.on_error(&state, transport_err()),
                RetryResult::Continue(_)
            ),
            "we should retry transport errors up to once for each gRPC channel"
        );
        assert!(
            matches!(
                retry.on_error(&state, non_transport_err()),
                RetryResult::Permanent(_)
            ),
            "non-transport error should not be retried"
        );
        assert_eq!(
            backoff.on_failure(&state),
            Duration::ZERO,
            "the backoff should always be 0"
        );

        state.attempt_count = grpc_subchannel_count + 1;
        assert!(
            matches!(
                retry.on_error(&state, transport_err()),
                RetryResult::Exhausted(_)
            ),
            "the retry policy should be exhausted after trying once for each gRPC channel"
        );
        assert!(
            matches!(
                retry.on_error(&state, non_transport_err()),
                RetryResult::Permanent(_)
            ),
            "non-transport error should not be retried"
        );
        assert_eq!(
            backoff.on_failure(&state),
            Duration::ZERO,
            "the backoff should always be 0"
        );
    }

    fn transport_err() -> Error {
        Error::transport(HeaderMap::new(), "connection closed")
    }

    fn non_transport_err() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("bad gateway, try again"),
        )
    }
}
