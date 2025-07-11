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
//! In addition, resumable uploads return [308 - Resume Incomplete]. This is
//! not handled by the retry policy.
//!
//! [recommends]: https://cloud.google.com/storage/docs/retry-strategy
//! [408]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#408_Request_Timeout
//! [429]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#429_Too_Many_Requests
//! [500]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#500_Internal_Server_Error
//! [502]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#502_Bad_Gateway
//! [503]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#503_Service_Unavailable
//! [504]: https://cloud.google.com/storage/docs/json_api/v1/status-codes#504_Gateway_Timeout

use gax::error::Error;
use gax::{
    retry_policy::{RetryPolicy, RetryPolicyExt},
    retry_result::RetryResult,
};
use std::time::Duration;

/// The default retry policy for the Storage client.
///
/// The client will retry all the errors shown as retryable in the service
/// documentation, and stop retrying after 10 seconds.
pub(crate) fn default() -> impl RetryPolicy {
    RecommendedPolicy.with_time_limit(Duration::from_secs(10))
}

/// The default retry policy for Google Cloud Storage requests.
///
/// This policy must be decorated to limit the number of retry attempts or the
/// duration of the retry loop.
///
/// The policy follows the [retry strategy] recommended by Google Cloud Storage.
///
/// # Example
/// ```
/// # use google_cloud_storage::retry_policy::RecommendedPolicy;
/// use gax::retry_policy::RetryPolicyExt;
/// use google_cloud_storage::client::Storage;
/// use std::time::Duration;
/// let builder = Storage::builder().with_retry_policy(
///     RecommendedPolicy
///         .with_time_limit(Duration::from_secs(60))
///         .with_attempt_limit(10),
/// );
/// ```
///
/// [retry strategy]: https://cloud.google.com/storage/docs/retry-strategy
#[derive(Clone, Debug)]
pub struct RecommendedPolicy;

impl RetryPolicy for RecommendedPolicy {
    fn on_error(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        idempotent: bool,
        error: Error,
    ) -> RetryResult {
        if error.is_transient_and_before_rpc() {
            return RetryResult::Continue(error);
        }
        if !idempotent {
            return RetryResult::Permanent(error);
        }
        if error.is_io() {
            return RetryResult::Continue(error);
        }
        if let Some(code) = error.http_status_code() {
            return match code {
                408 | 429 | 500..600 => RetryResult::Continue(error),
                _ => RetryResult::Permanent(error),
            };
        }
        RetryResult::Permanent(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gax::throttle_result::ThrottleResult;
    use http::HeaderMap;
    use test_case::test_case;

    #[test_case(408)]
    #[test_case(429)]
    #[test_case(500)]
    #[test_case(502)]
    #[test_case(503)]
    #[test_case(504)]
    fn retryable(code: u16) {
        let p = RecommendedPolicy;
        let now = std::time::Instant::now();
        assert!(p.on_error(now, 0, true, http_error(code)).is_continue());
        assert!(p.on_error(now, 0, false, http_error(code)).is_permanent());

        let t = p.on_throttle(now, 0, http_error(code));
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }

    #[test_case(401)]
    #[test_case(403)]
    fn not_recommended(code: u16) {
        let p = RecommendedPolicy;
        let now = std::time::Instant::now();
        assert!(p.on_error(now, 0, true, http_error(code)).is_permanent());
        assert!(p.on_error(now, 0, false, http_error(code)).is_permanent());

        let t = p.on_throttle(now, 0, http_error(code));
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }

    fn http_error(code: u16) -> Error {
        Error::http(code, HeaderMap::new(), bytes::Bytes::new())
    }
}
