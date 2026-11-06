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

//! Default retry and backoff policies for Cloud Datastore.

use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy, RetryPolicyExt};
use std::time::Duration;

/// Returns the default retry policy for Cloud Datastore RPCs.
///
/// Idempotent RPCs (`Lookup`, `RunQuery`, `RunAggregationQuery`, `ReserveIds`)
/// are retried on transient `UNAVAILABLE` and `DEADLINE_EXCEEDED` errors for up
/// to 60 seconds.
pub fn default_retry_policy() -> impl RetryPolicy {
    Aip194Strict.with_time_limit(Duration::from_secs(60))
}

/// Returns the default exponential backoff policy for Cloud Datastore RPCs.
///
/// Uses a 100ms initial delay, 60s maximum delay, and a 1.3x multiplier matching
/// `datastore_grpc_service_config.json`.
pub fn default_backoff_policy() -> impl BackoffPolicy {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(100))
        .with_maximum_delay(Duration::from_secs(60))
        .with_scaling(1.3)
        .clamp()
}

/// Constructs an [ExponentialBackoff] instance with Datastore's default parameters.
pub(crate) fn default_exponential_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(100))
        .with_maximum_delay(Duration::from_secs(60))
        .with_scaling(1.3)
        .clamp()
}
