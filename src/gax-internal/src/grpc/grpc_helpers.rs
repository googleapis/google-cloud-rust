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
use google_cloud_auth::credentials::{
    Builder as CredentialsBuilder, CacheableResource, Credentials,
};
use google_cloud_gax::Result;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::client_builder::{Error as BuilderError, Result as ClientBuilderResult};
use google_cloud_gax::error::Error;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::options::internal::RequestOptionsExt as _;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_error_policy::{
    Aip194Strict as PollingAip194Strict, PollingErrorPolicy,
};
use google_cloud_gax::retry_policy::{
    Aip194Strict as RetryAip194Strict, RetryPolicy, RetryPolicyExt as _,
};
use google_cloud_gax::retry_throttler::SharedRetryThrottler;
use http::{HeaderMap, header::HeaderName};
use std::sync::Arc;
use std::time::Duration;

const X_GOOG_API_CLIENT: HeaderName = HeaderName::from_static("x-goog-api-client");
const X_GOOG_REQUEST_PARAMS: HeaderName = HeaderName::from_static("x-goog-request-params");
const X_GOOG_USER_PROJECT: HeaderName = HeaderName::from_static("x-goog-user-project");

/// Extends the supplied `headers` map with authentication headers from a
/// `Credentials` object. For entries with the same header name, the one in
/// `headers` takes precedence.
pub(crate) async fn add_auth_headers(
    headers: HeaderMap,
    credentials: &Credentials,
) -> Result<HeaderMap> {
    let h = credentials
        .headers(http::Extensions::new())
        .await
        .map_err(Error::authentication)?;

    let CacheableResource::New { mut data, .. } = h else {
        unreachable!("headers are not cached");
    };

    // Note that client headers override credential headers (e.g. for `x-goog-user-project`).
    data.extend(headers);
    Ok(data)
}

/// Returns a clone of `Credentials` if already present in `config`;
/// otherwise, returns a new default `Credentials` object.
pub(crate) fn make_credentials(config: &ClientConfig) -> ClientBuilderResult<Credentials> {
    if let Some(c) = config.cred.clone() {
        return Ok(c);
    }

    CredentialsBuilder::default()
        .build()
        .map_err(BuilderError::cred)
}

/// Constructs the headers required for Google Cloud API requests.
/// Custom headers can be provided through `RequestOptions`.
/// Returns an error if any of the header values fail to parse.
pub(crate) fn make_headers(
    api_client_header: &'static str,
    request_params: &str,
    options: &RequestOptions,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();

    if let Some(user_agent) = options.user_agent() {
        headers.insert(
            http::header::USER_AGENT,
            http::header::HeaderValue::from_str(user_agent).map_err(Error::ser)?,
        );
    }

    if let Some(quota_project) = options.quota_project() {
        headers.insert(
            X_GOOG_USER_PROJECT,
            http::header::HeaderValue::from_str(quota_project).map_err(Error::ser)?,
        );
    }

    headers.append(
        X_GOOG_API_CLIENT,
        http::header::HeaderValue::from_static(api_client_header),
    );

    if !request_params.is_empty() {
        // When using routing info to populate the request parameters it is
        // possible that none of the path template matches. AIP-4222 says:
        //
        //     If none of the routing parameters matched their respective
        //     fields, the routing header **must not** be sent.
        //
        headers.append(
            X_GOOG_REQUEST_PARAMS,
            http::header::HeaderValue::from_str(request_params).map_err(Error::ser)?,
        );
    }

    if let Some(custom_headers) = options.get_extension::<HeaderMap>() {
        headers.extend(custom_headers.clone());
    }

    Ok(headers)
}

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
    use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
    use google_cloud_auth::errors::CredentialsError;
    use google_cloud_gax::retry_throttler::AdaptiveThrottler;
    use http::{Extensions, header::HeaderName, header::HeaderValue};
    use pretty_assertions::assert_eq;
    use std::sync::Mutex;

    type AuthResult<T> = std::result::Result<T, CredentialsError>;
    type TestResult = anyhow::Result<()>;

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsProvider for Credentials {
            async fn headers(&self, extensions: Extensions) -> AuthResult<CacheableResource<HeaderMap>>;
            async fn universe_domain(&self) -> Option<String>;
        }
    }

    const API_CLIENT_HEADER: &str = "test-client/1.0";

    #[tokio::test]
    async fn add_auth_headers_merges_auth_and_client_headers() -> TestResult {
        // Arrange
        let credential_auth = "authorization";
        let credential_token = "bearer test-token";
        let credential_project = "credential-quota-project";

        let request_project = "request-quota-project";
        let request_header = "x-request-header";
        let request_value = "request-value";

        let auth_headers = HeaderMap::from_iter([
            (
                HeaderName::from_static(credential_auth),
                HeaderValue::from_static(credential_token),
            ),
            (
                X_GOOG_USER_PROJECT,
                HeaderValue::from_static(credential_project),
            ),
        ]);

        let mut provider = MockCredentials::new();
        provider.expect_headers().return_once(|_extensions| {
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: auth_headers,
            })
        });
        let credentials = Credentials::from(provider);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(
            // This one should take precedence.
            X_GOOG_USER_PROJECT,
            HeaderValue::from_static(request_project),
        );
        request_headers.insert(
            HeaderName::from_static(request_header),
            HeaderValue::from_static(request_value),
        );

        // Act
        let headers = add_auth_headers(request_headers, &credentials).await?;

        // Assert
        assert_eq!(
            headers.get(credential_auth).expect("auth header"),
            credential_token
        );
        assert_eq!(
            headers
                .get(&X_GOOG_USER_PROJECT)
                .expect("user project header"),
            request_project
        );
        assert_eq!(
            headers.get(request_header).expect("request header"),
            request_value
        );
        Ok(())
    }

    #[tokio::test]
    async fn make_credentials_uses_config_credentials() -> TestResult {
        // No good way to directly check which credentials are used, so
        // we check it indirectly.
        // Arrange
        let expected_domain = "domain";

        let mut provider = MockCredentials::new();
        provider
            .expect_universe_domain()
            .times(1)
            .return_once(|| Some(expected_domain.to_string()));
        let credentials = Credentials::from(provider);

        let mut config = ClientConfig::default();
        config.cred = Some(credentials);

        // Act
        let result = make_credentials(&config)?;

        // Assert
        assert_eq!(
            result.universe_domain().await.as_deref(),
            Some(expected_domain)
        );
        Ok(())
    }

    #[test]
    fn make_headers_with_standard_headers() -> TestResult {
        // Arrange
        const USER_AGENT: &str = "custom-user-agent/v1.2.3";
        const QUOTA_PROJECT: &str = "user-quota-project";
        const REQUEST_PARAMS: &str = "resource=projects%2Ftest";

        let mut options = RequestOptions::default();
        options.set_user_agent(USER_AGENT);
        options.set_quota_project(QUOTA_PROJECT);

        // Act
        let headers = make_headers(API_CLIENT_HEADER, REQUEST_PARAMS, &options)?;

        // Assert
        assert_eq!(headers.get(X_GOOG_API_CLIENT).unwrap(), API_CLIENT_HEADER);
        assert_eq!(headers.get(X_GOOG_REQUEST_PARAMS).unwrap(), REQUEST_PARAMS);
        assert_eq!(headers.get(X_GOOG_USER_PROJECT).unwrap(), QUOTA_PROJECT);
        assert_eq!(headers.get(http::header::USER_AGENT).unwrap(), USER_AGENT);
        Ok(())
    }

    #[test]
    fn make_headers_omits_unset_params() -> TestResult {
        // Act
        let headers = make_headers(API_CLIENT_HEADER, "", &RequestOptions::default())?;

        // Assert
        assert_eq!(headers.get(X_GOOG_API_CLIENT).unwrap(), API_CLIENT_HEADER);
        assert!(headers.get(X_GOOG_REQUEST_PARAMS).is_none(), "{headers:?}");
        assert!(headers.get(X_GOOG_USER_PROJECT).is_none(), "{headers:?}");
        assert!(
            headers.get(http::header::USER_AGENT).is_none(),
            "{headers:?}"
        );
        Ok(())
    }

    #[test]
    fn make_headers_with_custom_headers() -> TestResult {
        // Arrange
        const CUSTOM_HEADER_NAME: &str = "x-custom-header";
        const CUSTOM_HEADER: &str = "custom-value";
        const CUSTOM_REQUEST_PARAMS: &str = "param=1";

        let mut custom_headers = HeaderMap::new();
        custom_headers.insert(CUSTOM_HEADER_NAME, HeaderValue::from_static(CUSTOM_HEADER));

        let options = RequestOptions::default().insert_extension(custom_headers);

        // Act
        let headers = make_headers(API_CLIENT_HEADER, CUSTOM_REQUEST_PARAMS, &options)?;

        // Assert
        assert_eq!(headers.get(X_GOOG_API_CLIENT).unwrap(), API_CLIENT_HEADER);
        assert_eq!(
            headers.get(X_GOOG_REQUEST_PARAMS).unwrap(),
            CUSTOM_REQUEST_PARAMS
        );
        assert_eq!(headers.get(CUSTOM_HEADER_NAME).unwrap(), CUSTOM_HEADER);
        Ok(())
    }

    #[test]
    fn make_headers_with_invalid_header_values() {
        // Invalid user agent
        let mut options = RequestOptions::default();
        options.set_user_agent("invalid\nagent");
        let res = make_headers(API_CLIENT_HEADER, "param=1", &options);
        assert!(res.is_err(), "{res:?}");

        // Invalid quota project
        let mut options = RequestOptions::default();
        options.set_quota_project("invalid\nproject");
        let res = make_headers(API_CLIENT_HEADER, "param=1", &options);
        assert!(res.is_err(), "{res:?}");

        // Invalid request params
        let options = RequestOptions::default();
        let res = make_headers(API_CLIENT_HEADER, "invalid\nparams", &options);
        assert!(res.is_err(), "{res:?}");
    }

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

        // Overrides
        let mut options = RequestOptions::default();
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

        options.set_retry_policy(override_retry_policy.clone());
        options.set_backoff_policy(override_backoff_policy.clone());
        options.set_retry_throttler(override_retry_throttler.clone());
        options.set_polling_error_policy(override_polling_error_policy.clone());
        options.set_polling_backoff_policy(override_polling_backoff_policy.clone());

        // Act & Assert
        assert!(Arc::ptr_eq(
            &policies.get_retry_policy(&options),
            &override_retry_policy
        ));
        assert!(Arc::ptr_eq(
            &policies.get_backoff_policy(&options),
            &override_backoff_policy
        ));
        assert!(Arc::ptr_eq(
            &policies.get_retry_throttler(&options),
            &override_retry_throttler
        ));
        assert!(Arc::ptr_eq(
            &policies.get_polling_error_policy(&options),
            &override_polling_error_policy
        ));
        assert!(Arc::ptr_eq(
            &policies.get_polling_backoff_policy(&options),
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
