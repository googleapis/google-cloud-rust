// Copyright 2024 Google LLC
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

//! Client configuration and per request options.
//!
//! While the client library  defaults are intended to work for most
//! applications, it is sometimes necessary to change the configuration. Notably
//! the default endpoint, and the default authentication credentials do not work
//! for some applications.
//!
//! Likewise, applications may need to customize the behavior of some calls made
//! via a client, even a customized one. Applications sometimes change the
//! timeout for an specific call, or change the retry configuration. The
//! `*Builder` returned by each client method implements the
//! [RequestOptionsBuilder] trait where applications can override some defaults.

use crate::backoff_policy::{BackoffPolicy, BackoffPolicyArg};
use crate::polling_backoff_policy::{PollingBackoffPolicy, PollingBackoffPolicyArg};
use crate::polling_error_policy::{PollingErrorPolicy, PollingErrorPolicyArg};
use crate::retry_policy::{RetryPolicy, RetryPolicyArg};
use crate::retry_throttler::{RetryThrottlerArg, SharedRetryThrottler};
use auth::credentials::Credential;
use std::sync::Arc;

/// A set of options configuring a single request.
///
/// Application only use this class directly in mocks, where they may want to
/// verify their application has configured all the right request parameters and
/// options.
///
/// All other code uses this type indirectly, via the per-request builders.
#[derive(Clone, Debug, Default)]
pub struct RequestOptions {
    idempotent: Option<bool>,
    user_agent: Option<String>,
    attempt_timeout: Option<std::time::Duration>,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: Option<SharedRetryThrottler>,
    polling_error_policy: Option<Arc<dyn PollingErrorPolicy>>,
    polling_backoff_policy: Option<Arc<dyn PollingBackoffPolicy>>,
}

impl RequestOptions {
    /// Gets the idempotency
    pub fn idempotent(&self) -> Option<bool> {
        self.idempotent
    }

    /// Treat the RPC underlying RPC in this method as idempotent.
    ///
    /// If a retry policy is configured, the policy may examine the idempotency
    /// and the error details to decide if the error is retryable. Typically
    /// [idempotent] RPCs are safe to retry under more error conditions
    /// than non-idempotent RPCs.
    ///
    /// The client libraries provide a default for RPC idempotency, based on the
    /// HTTP method (`GET`, `POST`, `DELETE`, etc.).
    ///
    /// [idempotent]: https://en.wikipedia.org/wiki/Idempotence
    pub fn set_idempotency(&mut self, value: bool) {
        self.idempotent = Some(value);
    }

    /// Set the idempotency for the underlying RPC unless it is already set.
    ///
    /// If [set_idempotency][Self::set_idempotency] was already called this
    /// method has no effect. Otherwise it sets the idempotency. The client
    /// libraries use this to provide a default idempotency value.
    pub fn set_default_idempotency(mut self, default: bool) -> Self {
        self.idempotent.get_or_insert(default);
        self
    }

    /// Prepends this prefix to the user agent header value.
    pub fn set_user_agent<T: Into<String>>(&mut self, v: T) {
        self.user_agent = Some(v.into());
    }

    /// Gets the current user-agent prefix
    pub fn user_agent(&self) -> &Option<String> {
        &self.user_agent
    }

    /// Sets the per-attempt timeout.
    ///
    /// When using a retry loop, this affects the timeout for each attempt. The
    /// overall timeout for a request is set by the retry policy.
    pub fn set_attempt_timeout<T: Into<std::time::Duration>>(&mut self, v: T) {
        self.attempt_timeout = Some(v.into());
    }

    /// Gets the current per-attempt timeout.
    pub fn attempt_timeout(&self) -> &Option<std::time::Duration> {
        &self.attempt_timeout
    }

    /// Get the current retry policy override, if any.
    pub fn retry_policy(&self) -> &Option<Arc<dyn RetryPolicy>> {
        &self.retry_policy
    }

    /// Sets the retry policy configuration.
    pub fn set_retry_policy<V: Into<RetryPolicyArg>>(&mut self, v: V) {
        self.retry_policy = Some(v.into().0);
    }

    /// Get the current backoff policy override, if any.
    pub fn backoff_policy(&self) -> &Option<Arc<dyn BackoffPolicy>> {
        &self.backoff_policy
    }

    /// Sets the backoff policy configuration.
    pub fn set_backoff_policy<V: Into<BackoffPolicyArg>>(&mut self, v: V) {
        self.backoff_policy = Some(v.into().0);
    }

    /// Get the current retry throttler override, if any.
    pub fn retry_throttler(&self) -> &Option<SharedRetryThrottler> {
        &self.retry_throttler
    }

    /// Sets the retry throttling configuration.
    pub fn set_retry_throttler<V: Into<RetryThrottlerArg>>(&mut self, v: V) {
        self.retry_throttler = Some(v.into().0);
    }

    /// Get the current polling policy override, if any.
    pub fn polling_error_policy(&self) -> &Option<Arc<dyn PollingErrorPolicy>> {
        &self.polling_error_policy
    }

    /// Sets the polling policy configuration.
    pub fn set_polling_error_policy<V: Into<PollingErrorPolicyArg>>(&mut self, v: V) {
        self.polling_error_policy = Some(v.into().0);
    }

    /// Get the current polling backoff policy override, if any.
    pub fn polling_backoff_policy(&self) -> &Option<Arc<dyn PollingBackoffPolicy>> {
        &self.polling_backoff_policy
    }

    /// Sets the backoff policy configuration.
    pub fn set_polling_backoff_policy<V: Into<PollingBackoffPolicyArg>>(&mut self, v: V) {
        self.polling_backoff_policy = Some(v.into().0);
    }
}

/// Implementations of this trait provide setters to configure request options.
///
/// The Google Cloud Client Libraries for Rust provide a builder for each RPC.
/// These builders can be used to set the request parameters, e.g., the name of
/// the resource targeted by the RPC, as well as any options affecting the
/// request, such as additional headers or timeouts.
pub trait RequestOptionsBuilder: internal::RequestBuilder {
    /// If `v` is `true`, treat the RPC underlying this method as idempotent.
    fn with_idempotency(self, v: bool) -> Self;

    /// Set the user agent header.
    fn with_user_agent<V: Into<String>>(self, v: V) -> Self;

    /// Sets the per-attempt timeout.
    ///
    /// When using a retry loop, this affects the timeout for each attempt. The
    /// overall timeout for a request is set by the retry policy.
    fn with_attempt_timeout<V: Into<std::time::Duration>>(self, v: V) -> Self;

    /// Sets the retry policy configuration.
    fn with_retry_policy<V: Into<RetryPolicyArg>>(self, v: V) -> Self;

    /// Sets the backoff policy configuration.
    fn with_backoff_policy<V: Into<BackoffPolicyArg>>(self, v: V) -> Self;

    /// Sets the retry throttler configuration.
    fn with_retry_throttler<V: Into<RetryThrottlerArg>>(self, v: V) -> Self;

    /// Sets the polling error policy configuration.
    fn with_polling_error_policy<V: Into<PollingErrorPolicyArg>>(self, v: V) -> Self;

    /// Sets the polling backoff policy configuration.
    fn with_polling_backoff_policy<V: Into<PollingBackoffPolicyArg>>(self, v: V) -> Self;
}

/// This module contains implementation details. It is not part of the public
/// API. Types inside may be changed or removed without warnings. Applications
///  should not use any types contained within.
#[doc(hidden)]
pub mod internal {
    /// Simplify implementation of the [super::RequestOptionsBuilder] trait in
    /// generated code.
    ///
    /// This is an implementation detail, most applications have little need to
    /// worry about or use this trait.
    pub trait RequestBuilder {
        fn request_options(&mut self) -> &mut super::RequestOptions;
    }
}

/// Implements the sealed [RequestOptionsBuilder] trait.
impl<T> RequestOptionsBuilder for T
where
    T: internal::RequestBuilder,
{
    fn with_idempotency(mut self, v: bool) -> Self {
        self.request_options().set_idempotency(v);
        self
    }

    fn with_user_agent<V: Into<String>>(mut self, v: V) -> Self {
        self.request_options().set_user_agent(v);
        self
    }

    fn with_attempt_timeout<V: Into<std::time::Duration>>(mut self, v: V) -> Self {
        self.request_options().set_attempt_timeout(v);
        self
    }

    fn with_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.request_options().set_retry_policy(v);
        self
    }

    fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.request_options().set_backoff_policy(v);
        self
    }

    fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.request_options().set_retry_throttler(v);
        self
    }

    fn with_polling_error_policy<V: Into<PollingErrorPolicyArg>>(mut self, v: V) -> Self {
        self.request_options().set_polling_error_policy(v);
        self
    }

    fn with_polling_backoff_policy<V: Into<PollingBackoffPolicyArg>>(mut self, v: V) -> Self {
        self.request_options().set_polling_backoff_policy(v);
        self
    }
}

/// Configure a client.
///
/// A client represents a connection to a Google Cloud Service. Each service
/// has one or more client types. The default configuration for each client
/// should work for most applications. But some applications may need to
/// override the default endpoint, the default authentication credentials,
/// the retry policies, and/or other behaviors of the client.
pub struct ClientConfig {
    endpoint: Option<String>,
    cred: Option<Credential>,
    tracing: bool,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: SharedRetryThrottler,
    polling_error_policy: Option<Arc<dyn PollingErrorPolicy>>,
    polling_backoff_policy: Option<Arc<dyn PollingBackoffPolicy>>,
}

const LOGGING_VAR: &str = "GOOGLE_CLOUD_RUST_LOGGING";

impl ClientConfig {
    /// Returns a default [ClientConfig].
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tracing_enabled(&self) -> bool {
        if self.tracing {
            return true;
        }
        std::env::var(LOGGING_VAR)
            .map(|v| v == "true")
            .unwrap_or(false)
    }

    /// Gets the current endpoint override, if any
    pub fn endpoint(&self) -> &Option<String> {
        &self.endpoint
    }

    /// Sets an endpoint that overrides the default endpoint for a service.
    pub fn set_endpoint<T: Into<String>>(mut self, v: T) -> Self {
        self.endpoint = Some(v.into());
        self
    }

    /// Enables tracing.
    pub fn enable_tracing(mut self) -> Self {
        self.tracing = true;
        self
    }

    /// Gets the current credential override, if any.
    pub fn credential(&self) -> &Option<Credential> {
        &self.cred
    }

    /// Configure the authentication credentials.
    pub fn set_credential<T: Into<Credential>>(mut self, v: T) -> Self {
        self.cred = Some(v.into());
        self
    }

    /// Get the current retry policy override, if any.
    pub fn retry_policy(&self) -> &Option<Arc<dyn RetryPolicy>> {
        &self.retry_policy
    }

    /// Configure the retry policy.
    pub fn set_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.retry_policy = Some(v.into().0);
        self
    }

    /// Get the current backoff policy override, if any.
    pub fn backoff_policy(&self) -> &Option<Arc<dyn BackoffPolicy>> {
        &self.backoff_policy
    }

    /// Configure the retry backoff policy.
    pub fn set_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.backoff_policy = Some(v.into().0);
        self
    }

    /// Get the current retry throttler.
    pub fn retry_throttler(&self) -> SharedRetryThrottler {
        self.retry_throttler.clone()
    }

    /// Configure the retry throttler.
    pub fn set_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.retry_throttler = v.into().0;
        self
    }

    /// Get the current polling policy override, if any.
    pub fn polling_error_policy(&self) -> &Option<Arc<dyn PollingErrorPolicy>> {
        &self.polling_error_policy
    }

    /// Configure the polling backoff policy.
    pub fn set_polling_error_policy<V: Into<PollingErrorPolicyArg>>(mut self, v: V) -> Self {
        self.polling_error_policy = Some(v.into().0);
        self
    }

    /// Get the current polling backoff policy override, if any.
    pub fn polling_backoff_policy(&self) -> &Option<Arc<dyn PollingBackoffPolicy>> {
        &self.polling_backoff_policy
    }

    /// Configure the polling backoff policy.
    pub fn set_polling_backoff_policy<V: Into<PollingBackoffPolicyArg>>(mut self, v: V) -> Self {
        self.polling_backoff_policy = Some(v.into().0);
        self
    }
}

impl std::default::Default for ClientConfig {
    fn default() -> Self {
        use crate::retry_throttler::AdaptiveThrottler;
        use std::sync::{Arc, Mutex};
        Self {
            endpoint: None,
            cred: None,
            tracing: false,
            retry_policy: None,
            backoff_policy: None,
            retry_throttler: Arc::new(Mutex::new(AdaptiveThrottler::default())),
            polling_error_policy: None,
            polling_backoff_policy: None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::internal::*;
    use super::*;
    use crate::exponential_backoff::ExponentialBackoffBuilder;
    use crate::polling_error_policy;
    use crate::retry_policy::LimitedAttemptCount;
    use crate::retry_throttler::AdaptiveThrottler;
    use scoped_env::ScopedEnv;
    use std::time::Duration;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[derive(Debug, Default)]
    struct TestBuilder {
        request_options: RequestOptions,
    }
    impl RequestBuilder for TestBuilder {
        fn request_options(&mut self) -> &mut RequestOptions {
            &mut self.request_options
        }
    }

    #[test]
    fn request_options() {
        let mut opts = RequestOptions::default();

        assert_eq!(opts.idempotent, None);
        opts.set_idempotency(true);
        assert_eq!(opts.idempotent(), Some(true));
        opts.set_idempotency(false);
        assert_eq!(opts.idempotent(), Some(false));

        opts.set_user_agent("test-only");
        assert_eq!(opts.user_agent().as_deref(), Some("test-only"));
        assert_eq!(opts.attempt_timeout(), &None);

        let d = Duration::from_secs(123);
        opts.set_attempt_timeout(d);
        assert_eq!(opts.user_agent().as_deref(), Some("test-only"));
        assert_eq!(opts.attempt_timeout(), &Some(d));

        opts.set_retry_policy(LimitedAttemptCount::new(3));
        assert!(opts.retry_policy().is_some(), "{opts:?}");

        opts.set_backoff_policy(ExponentialBackoffBuilder::new().clamp());
        assert!(opts.backoff_policy().is_some(), "{opts:?}");

        opts.set_retry_throttler(AdaptiveThrottler::default());
        assert!(opts.retry_throttler().is_some(), "{opts:?}");

        opts.set_polling_error_policy(polling_error_policy::Aip194Strict);
        assert!(opts.polling_error_policy().is_some(), "{opts:?}");

        opts.set_polling_backoff_policy(ExponentialBackoffBuilder::new().clamp());
        assert!(opts.polling_backoff_policy().is_some(), "{opts:?}");
    }

    #[test]
    fn request_options_idempotency() {
        let opts = RequestOptions::default().set_default_idempotency(true);
        assert_eq!(opts.idempotent(), Some(true));
        let opts = opts.set_default_idempotency(false);
        assert_eq!(opts.idempotent(), Some(true));

        let opts = RequestOptions::default().set_default_idempotency(false);
        assert_eq!(opts.idempotent(), Some(false));
        let opts = opts.set_default_idempotency(true);
        assert_eq!(opts.idempotent(), Some(false));
    }

    #[test]
    fn request_options_builder() -> Result {
        let mut builder = TestBuilder::default();
        assert_eq!(builder.request_options().user_agent(), &None);
        assert_eq!(builder.request_options().attempt_timeout(), &None);

        let mut builder = TestBuilder::default().with_idempotency(true);
        assert_eq!(builder.request_options().idempotent(), Some(true));
        let mut builder = TestBuilder::default().with_idempotency(false);
        assert_eq!(builder.request_options().idempotent(), Some(false));

        let mut builder = TestBuilder::default().with_user_agent("test-only");
        assert_eq!(
            builder.request_options().user_agent().as_deref(),
            Some("test-only")
        );
        assert_eq!(builder.request_options().attempt_timeout(), &None);

        let d = Duration::from_secs(123);
        let mut builder = TestBuilder::default().with_attempt_timeout(d);
        assert_eq!(builder.request_options().user_agent(), &None);
        assert_eq!(builder.request_options().attempt_timeout(), &Some(d));

        let mut builder = TestBuilder::default().with_retry_policy(LimitedAttemptCount::new(3));
        assert!(
            builder.request_options().retry_policy().is_some(),
            "{builder:?}"
        );

        let mut builder =
            TestBuilder::default().with_backoff_policy(ExponentialBackoffBuilder::new().build()?);
        assert!(
            builder.request_options().backoff_policy().is_some(),
            "{builder:?}"
        );

        let mut builder = TestBuilder::default().with_retry_throttler(AdaptiveThrottler::default());
        assert!(
            builder.request_options().retry_throttler().is_some(),
            "{builder:?}"
        );

        let mut builder =
            TestBuilder::default().with_polling_error_policy(polling_error_policy::Aip194Strict);
        assert!(
            builder.request_options().polling_error_policy().is_some(),
            "{builder:?}"
        );

        let mut builder = TestBuilder::default()
            .with_polling_backoff_policy(ExponentialBackoffBuilder::new().build()?);
        assert!(
            builder.request_options().polling_backoff_policy().is_some(),
            "{builder:?}"
        );

        Ok(())
    }

    // This test must run serially because it manipulates the environment.
    #[test]
    #[serial_test::serial]
    fn config_tracing() {
        let _e = ScopedEnv::remove(LOGGING_VAR);
        let config = ClientConfig::new();
        assert!(!config.tracing_enabled(), "expected tracing to be disabled");
        let config = ClientConfig::new().enable_tracing();
        assert!(config.tracing_enabled(), "expected tracing to be enabled");

        let _e = ScopedEnv::set(LOGGING_VAR, "true");
        let config = ClientConfig::new();
        assert!(config.tracing_enabled(), "expected tracing to be enabled");

        let _e = ScopedEnv::set(LOGGING_VAR, "not-true");
        let config = ClientConfig::new();
        assert!(!config.tracing_enabled(), "expected tracing to be disabled");
    }

    #[test]
    fn config_endpoint() {
        let config = ClientConfig::new().set_endpoint("http://storage.googleapis.com");
        assert_eq!(
            config.endpoint,
            Some("http://storage.googleapis.com".to_string())
        );
    }

    #[tokio::test]
    async fn config_credentials() -> Result {
        let config =
            ClientConfig::new().set_credential(auth::credentials::testing::test_credentials());
        let cred = config.cred.unwrap();
        let token = cred.get_token().await?;
        assert_eq!(token.token, "test-only-token");
        Ok(())
    }

    #[test]
    fn config_retry_policy() {
        let config = ClientConfig::new().set_retry_policy(LimitedAttemptCount::new(5));
        assert!(config.retry_policy.is_some());
    }

    #[test]
    fn config_backoff() {
        let config =
            ClientConfig::new().set_backoff_policy(ExponentialBackoffBuilder::new().clamp());
        assert!(config.backoff_policy.is_some());
    }

    fn map_lock_err<T>(e: std::sync::PoisonError<T>) -> Box<dyn std::error::Error> {
        format!("cannot acquire lock {e}").into()
    }

    #[test]
    fn config_retry_throttler() -> Result {
        use crate::retry_throttler::CircuitBreaker;
        let config = ClientConfig::new();
        let throttler = config.retry_throttler.lock().map_err(map_lock_err)?;
        assert!(!throttler.throttle_retry_attempt());

        let config = ClientConfig::new().set_retry_throttler(CircuitBreaker::default());
        let throttler = config.retry_throttler.lock().map_err(map_lock_err)?;
        assert!(!throttler.throttle_retry_attempt());

        Ok(())
    }

    #[test]
    fn config_polling() {
        let config =
            ClientConfig::new().set_polling_error_policy(polling_error_policy::AlwaysContinue);
        assert!(config.polling_error_policy.is_some());
    }

    #[test]
    fn config_polling_backoff() {
        let config = ClientConfig::new()
            .set_polling_backoff_policy(ExponentialBackoffBuilder::new().clamp());
        assert!(config.polling_backoff_policy.is_some());
    }
}
