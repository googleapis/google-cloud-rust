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

use crate::options::ClientConfig;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_error_policy::{
    Aip194Strict as PollingAip194Strict, PollingErrorPolicy,
};
use google_cloud_gax::retry_policy::{
    Aip194Strict as RetryAip194Strict, RetryPolicy, RetryPolicyExt as _,
};
use google_cloud_gax::retry_throttler::SharedRetryThrottler;
use std::sync::Arc;
use std::time::Duration;

/// Policies governing gRPC client transport behaviors.
/// These policies are initialized from a [`ClientConfig`] and can
/// be overridden on a per-request basis using [`RequestOptions`].
#[derive(Clone, Debug)]
pub(crate) struct TransportPolicies {
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
    retry_throttler: SharedRetryThrottler,
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    attempt_timeout: Option<Duration>,
}

impl TransportPolicies {
    /// Creates a new `TransportPolicies` from the given [`ClientConfig`].
    /// Missing policies are populated with default values.
    pub(crate) fn from_config(config: &ClientConfig) -> Self {
        Self {
            retry_policy: config.retry_policy.clone().unwrap_or_else(|| {
                Arc::new(
                    RetryAip194Strict
                        .with_attempt_limit(10)
                        .with_time_limit(Duration::from_secs(60)),
                )
            }),
            backoff_policy: config
                .backoff_policy
                .clone()
                .unwrap_or_else(|| Arc::new(ExponentialBackoff::default())),
            retry_throttler: config.retry_throttler.clone(),
            polling_error_policy: config
                .polling_error_policy
                .clone()
                .unwrap_or_else(|| Arc::new(PollingAip194Strict)),
            polling_backoff_policy: config
                .polling_backoff_policy
                .clone()
                .unwrap_or_else(|| Arc::new(ExponentialBackoff::default())),
            attempt_timeout: config.attempt_timeout,
        }
    }

    pub(crate) fn get_retry_policy(&self, options: &RequestOptions) -> Arc<dyn RetryPolicy> {
        options
            .retry_policy()
            .clone()
            .unwrap_or_else(|| self.retry_policy.clone())
    }

    pub(crate) fn get_backoff_policy(&self, options: &RequestOptions) -> Arc<dyn BackoffPolicy> {
        options
            .backoff_policy()
            .clone()
            .unwrap_or_else(|| self.backoff_policy.clone())
    }

    pub(crate) fn get_retry_throttler(&self, options: &RequestOptions) -> SharedRetryThrottler {
        options
            .retry_throttler()
            .clone()
            .unwrap_or_else(|| self.retry_throttler.clone())
    }

    pub(crate) fn get_polling_error_policy(
        &self,
        options: &RequestOptions,
    ) -> Arc<dyn PollingErrorPolicy> {
        options
            .polling_error_policy()
            .clone()
            .unwrap_or_else(|| self.polling_error_policy.clone())
    }

    pub(crate) fn get_polling_backoff_policy(
        &self,
        options: &RequestOptions,
    ) -> Arc<dyn PollingBackoffPolicy> {
        options
            .polling_backoff_policy()
            .clone()
            .unwrap_or_else(|| self.polling_backoff_policy.clone())
    }

    pub(crate) fn attempt_timeout(&self) -> Option<Duration> {
        self.attempt_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::retry_throttler::AdaptiveThrottler;
    use pretty_assertions::assert_eq;
    use std::sync::Mutex;

    #[test]
    fn transport_policies_from_config_defaults() {
        // Arrange
        let config = ClientConfig::default();

        // Act
        let policies = TransportPolicies::from_config(&config);

        // Assert
        let expected_retry_policy = RetryAip194Strict
            .with_attempt_limit(10)
            .with_time_limit(Duration::from_secs(60));
        let expected_backoff_policy = ExponentialBackoff::default();
        let expected_polling_error_policy = PollingAip194Strict;
        let expected_polling_backoff_policy = ExponentialBackoff::default();

        // Some of these are dyn, so we compare their string representations.
        let options = RequestOptions::default();
        assert_eq!(
            format!("{:?}", policies.get_retry_policy(&options)),
            format!("{:?}", expected_retry_policy)
        );
        assert_eq!(
            format!("{:?}", policies.get_backoff_policy(&options)),
            format!("{:?}", expected_backoff_policy)
        );
        assert!(Arc::ptr_eq(
            &policies.get_retry_throttler(&options),
            &config.retry_throttler
        ));
        assert_eq!(
            format!("{:?}", policies.get_polling_error_policy(&options)),
            format!("{:?}", expected_polling_error_policy)
        );
        assert_eq!(
            format!("{:?}", policies.get_polling_backoff_policy(&options)),
            format!("{:?}", expected_polling_backoff_policy)
        );
        assert_eq!(policies.attempt_timeout(), None);
    }

    #[test]
    fn transport_policies_from_config() {
        // Arrange
        let config = create_test_config();

        // Act
        let policies = TransportPolicies::from_config(&config);

        // Assert
        let options = RequestOptions::default();
        assert!(Arc::ptr_eq(
            &policies.get_retry_policy(&options),
            config
                .retry_policy
                .as_ref()
                .expect("retry policy should be set")
        ));
        assert!(Arc::ptr_eq(
            &policies.get_backoff_policy(&options),
            config
                .backoff_policy
                .as_ref()
                .expect("backoff policy should be set")
        ));
        assert!(Arc::ptr_eq(
            &policies.get_retry_throttler(&options),
            &config.retry_throttler
        ));
        assert!(Arc::ptr_eq(
            &policies.get_polling_error_policy(&options),
            config
                .polling_error_policy
                .as_ref()
                .expect("polling error policy should be set")
        ));
        assert!(Arc::ptr_eq(
            &policies.get_polling_backoff_policy(&options),
            config
                .polling_backoff_policy
                .as_ref()
                .expect("polling backoff policy should be set")
        ));
        assert_eq!(policies.attempt_timeout(), config.attempt_timeout);
    }

    #[test]
    fn transport_policies_request_options_overrides() {
        // Arrange
        let config = create_test_config();
        let policies = TransportPolicies::from_config(&config);

        let mut overrides = RequestOptions::default();
        let override_retry_policy: Arc<dyn RetryPolicy> =
            Arc::new(RetryAip194Strict.with_attempt_limit(3));
        let override_backoff_policy: Arc<dyn BackoffPolicy> =
            Arc::new(ExponentialBackoff::default());
        let override_retry_throttler: SharedRetryThrottler =
            Arc::new(Mutex::new(AdaptiveThrottler::default()));
        let override_polling_error_policy: Arc<dyn PollingErrorPolicy> =
            Arc::new(PollingAip194Strict);
        let override_polling_backoff_policy: Arc<dyn PollingBackoffPolicy> =
            Arc::new(ExponentialBackoff::default());

        overrides.set_retry_policy(override_retry_policy.clone());
        overrides.set_backoff_policy(override_backoff_policy.clone());
        overrides.set_retry_throttler(override_retry_throttler.clone());
        overrides.set_polling_error_policy(override_polling_error_policy.clone());
        overrides.set_polling_backoff_policy(override_polling_backoff_policy.clone());

        // Act & Assert
        assert!(Arc::ptr_eq(
            &policies.get_retry_policy(&overrides),
            &override_retry_policy
        ));
        assert!(Arc::ptr_eq(
            &policies.get_backoff_policy(&overrides),
            &override_backoff_policy
        ));
        assert!(Arc::ptr_eq(
            &policies.get_retry_throttler(&overrides),
            &override_retry_throttler
        ));
        assert!(Arc::ptr_eq(
            &policies.get_polling_error_policy(&overrides),
            &override_polling_error_policy
        ));
        assert!(Arc::ptr_eq(
            &policies.get_polling_backoff_policy(&overrides),
            &override_polling_backoff_policy
        ));
    }

    fn create_test_config() -> ClientConfig {
        let mut config = ClientConfig::default();
        let retry_policy: Arc<dyn RetryPolicy> = Arc::new(RetryAip194Strict.with_attempt_limit(5));
        let backoff_policy: Arc<dyn BackoffPolicy> = Arc::new(ExponentialBackoff::default());
        let retry_throttler: SharedRetryThrottler =
            Arc::new(Mutex::new(AdaptiveThrottler::default()));
        let polling_error_policy: Arc<dyn PollingErrorPolicy> = Arc::new(PollingAip194Strict);
        let polling_backoff_policy: Arc<dyn PollingBackoffPolicy> =
            Arc::new(ExponentialBackoff::default());
        let attempt_timeout = Some(Duration::from_secs(42));

        config.retry_policy = Some(retry_policy);
        config.backoff_policy = Some(backoff_policy);
        config.retry_throttler = retry_throttler;
        config.polling_error_policy = Some(polling_error_policy);
        config.polling_backoff_policy = Some(polling_backoff_policy);
        config.attempt_timeout = attempt_timeout;

        config
    }
}
