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

//! RPC retry policies used by the Spanner client.

use google_cloud_gax::error::Error;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;
use std::time::Duration;

/// The retry policy the Spanner client applies to RPCs that do not configure
/// their own. It decorates/extends [google_cloud_gax::retry_policy::Aip194Strict].
///
/// Like `Aip194Strict`, this policy retries `UNAVAILABLE` errors and transient
/// failures that occur before the request reaches the service, but only for
/// idempotent requests. In addition — because Spanner allows transport and
/// connection errors to be retried on idempotent operations — it also retries
/// transport/network and I/O errors that `Aip194Strict` would classify as
/// permanent (such as a connection dropped after the request was sent), again
/// only if the request is idempotent.
///
/// The policy places no limit on the number of attempts or the elapsed time.
/// Applications that want to bound the client's default retry behavior can
/// decorate this policy with
/// [RetryPolicyExt][google_cloud_gax::retry_policy::RetryPolicyExt] instead of
/// re-implementing its error classification:
///
/// # Example
/// ```
/// # use std::time::Duration;
/// # use google_cloud_spanner::retry_policy::SpannerRetryPolicy;
/// # use google_cloud_spanner::statement::Statement;
/// # use google_cloud_gax::retry_policy::RetryPolicyExt;
/// let statement = Statement::builder("SELECT * FROM Users")
///     .with_retry_policy(
///         SpannerRetryPolicy::new()
///             .with_attempt_limit(5)
///             .with_time_limit(Duration::from_secs(30)),
///     )
///     .build();
/// ```
#[derive(Clone, Debug)]
pub struct SpannerRetryPolicy {
    inner: Aip194Strict,
}

impl SpannerRetryPolicy {
    /// Creates a new Spanner retry policy.
    pub fn new() -> Self {
        Self {
            inner: Aip194Strict,
        }
    }
}

impl Default for SpannerRetryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryPolicy for SpannerRetryPolicy {
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        // 1. Strict AIP-194 checks (Unavailable, is_transient_and_before_rpc)
        let result = self.inner.on_error(state, error);
        match result {
            // If the strict AIP-194 checks classified the error as permanent (such as a transport
            // error that occurred post-headers), we override it to Continue if the request is idempotent.
            RetryResult::Permanent(error)
                if state.idempotent && (error.is_transport() || error.is_io()) =>
            {
                RetryResult::Continue(error)
            }
            res => res,
        }
    }

    fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult {
        self.inner.on_throttle(state, error)
    }

    fn remaining_time(&self, state: &RetryState) -> Option<Duration> {
        self.inner.remaining_time(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use http::HeaderMap;

    #[test]
    fn test_spanner_retry_policy_idempotent() {
        let policy = SpannerRetryPolicy::new();
        let state = RetryState::new(true); // idempotent = true

        // 1. Service UNAVAILABLE error should be retried (via inner AIP-194)
        let status = Status::default()
            .set_code(Code::Unavailable)
            .set_message("Service Unavailable");
        let err = GaxError::service(status);
        assert!(
            policy.on_error(&state, err).is_continue(),
            "Expected UNAVAILABLE to be retried when idempotent"
        );

        // 2. Service PERMISSION_DENIED error should not be retried
        let status = Status::default()
            .set_code(Code::PermissionDenied)
            .set_message("Denied");
        let err = GaxError::service(status);
        assert!(
            policy.on_error(&state, err).is_permanent(),
            "Expected PERMISSION_DENIED to not be retried"
        );

        // 3. IO/Transport error should be retried when idempotent
        let err = GaxError::transport(
            HeaderMap::new(),
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection closed"),
        );
        assert!(
            policy.on_error(&state, err).is_continue(),
            "Expected transport connection reset to be retried when idempotent"
        );
    }

    #[test]
    fn test_spanner_retry_policy_non_idempotent() {
        let policy = SpannerRetryPolicy::new();
        let state = RetryState::new(false); // idempotent = false

        // 1. Service UNAVAILABLE error should NOT be retried (AIP-194 requires idempotency)
        let status = Status::default()
            .set_code(Code::Unavailable)
            .set_message("Service Unavailable");
        let err = GaxError::service(status);
        assert!(
            policy.on_error(&state, err).is_permanent(),
            "Expected UNAVAILABLE to be permanent when non-idempotent"
        );

        // 2. IO/Transport error should NOT be retried when non-idempotent
        let err = GaxError::transport(
            HeaderMap::new(),
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection closed"),
        );
        assert!(
            policy.on_error(&state, err).is_permanent(),
            "Expected transport connection reset to not be retried when non-idempotent"
        );
    }
}
