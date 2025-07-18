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

//! Types and functions related to the default backoff policy.

use gax::{backoff_policy::BackoffPolicy, exponential_backoff::ExponentialBackoffBuilder};
use std::time::Duration;

/// The default backoff policy for the Storage clients.
///
/// The service recommends exponential backoff with jitter, starting with a one
/// second backoff and doubling on each attempt.
pub(crate) fn default() -> impl BackoffPolicy {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_secs(1))
        .with_maximum_delay(Duration::from_secs(60))
        .with_scaling(2.0)
        .build()
        .expect("statically configured policy should succeed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default() {
        let now = std::time::Instant::now() - Duration::from_millis(100);
        let policy = super::default();

        let delay = policy.on_failure(now, 1);
        assert!(
            delay <= Duration::from_secs(1),
            "{delay:?}, policy={policy:?}"
        );

        let delay = policy.on_failure(now, 2);
        assert!(
            delay <= Duration::from_secs(2),
            "{delay:?}, policy={policy:?}"
        );
    }
}
