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

//! Defines the internal retry policies for the Cloud Pub/Sub Publisher.

use crate::retry_policy::RetryableErrors;
use google_cloud_gax::retry_policy::{RetryPolicy, RetryPolicyExt};
use std::time::Duration;

/// The default retry policy for the Pub/Sub publisher.
///
/// The client will retry all the errors shown as retryable in the service
/// documentation, and stop retrying after 10 minutes.
pub(crate) fn default_retry_policy() -> impl RetryPolicy {
    RetryableErrors.with_time_limit(Duration::from_secs(600))
}
