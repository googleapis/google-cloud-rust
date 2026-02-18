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

//! Defines the backoff policies for the Google Cloud Pub/Sub Publisher.
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use std::time::Duration;

/// The default backoff policy for the Pub/Sub publisher.
pub(crate) fn default_backoff_policy() -> impl BackoffPolicy {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(100))
        .with_maximum_delay(Duration::from_secs(60))
        .with_scaling(4)
        .clamp()
}
