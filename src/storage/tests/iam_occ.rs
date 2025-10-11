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

//! Unit tests для OCC loop реализации

#[cfg(test)]
mod tests {
    use gax::exponential_backoff::ExponentialBackoffBuilder;
    use google_cloud_storage::iam_occ::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_occ_config_default() {
        let config = OccConfig::default();

        assert_eq!(config.max_attempts, 10, "Default max_attempts = 10");
        assert_eq!(
            config.max_duration,
            Duration::from_secs(30),
            "Default max_duration = 30s"
        );
        // backoff_policy is an Arc<dyn BackoffPolicy>, can't easily test internal values
        // but we can verify it exists
        assert!(
            Arc::strong_count(&config.backoff_policy) >= 1,
            "backoff_policy should be initialized"
        );
    }

    #[test]
    fn test_occ_config_custom() {
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(50))
            .with_maximum_delay(Duration::from_secs(5))
            .with_scaling(2.0)
            .build()
            .expect("valid backoff config");

        let config = OccConfig {
            max_attempts: 5,
            max_duration: Duration::from_secs(10),
            backoff_policy: Arc::new(backoff),
        };

        assert_eq!(config.max_attempts, 5, "Custom max_attempts = 5");
        assert_eq!(
            config.max_duration,
            Duration::from_secs(10),
            "Custom max_duration = 10s"
        );
    }

    #[test]
    fn test_occ_config_clone() {
        let config1 = OccConfig::default();
        let config2 = config1.clone();

        assert_eq!(
            config1.max_attempts, config2.max_attempts,
            "Cloned config should have same max_attempts"
        );
        assert_eq!(
            config1.max_duration, config2.max_duration,
            "Cloned config should have same max_duration"
        );

        // Arc should share the same backoff policy
        assert!(
            Arc::ptr_eq(&config1.backoff_policy, &config2.backoff_policy),
            "Cloned config should share backoff_policy Arc"
        );
    }

    #[test]
    fn test_occ_config_send_sync() {
        // Verify OccConfig implements Send + Sync (required for async usage)
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<OccConfig>();
        assert_sync::<OccConfig>();
    }
}
