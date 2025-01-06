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

use crate::retry_policy::{RetryPolicy, RetryPolicyArg};
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
    user_agent: Option<String>,
    attempt_timeout: Option<std::time::Duration>,
    pub(crate) retry_policy: Option<Arc<dyn RetryPolicy>>,
}

impl RequestOptions {
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

    /// Sets the retry policy configuration.
    pub fn set_retry_policy<V: Into<RetryPolicyArg>>(&mut self, v: V) {
        self.retry_policy = Some(v.into().0);
    }
}

/// Implementations of this trait provide setters to configure request options.
///
/// The Google Cloud Client Libraries for Rust provide a builder for each RPC.
/// These builders can be used to set the request parameters, e.g., the name of
/// the resource targeted by the RPC, as well as any options affecting the
/// request, such as additional headers or timeouts.
pub trait RequestOptionsBuilder {
    /// Set the user agent header.
    fn with_user_agent<V: Into<String>>(self, v: V) -> Self;

    /// Sets the per-attempt timeout.
    ///
    /// When using a retry loop, this affects the timeout for each attempt. The
    /// overall timeout for a request is set by the retry policy.
    fn with_attempt_timeout<V: Into<std::time::Duration>>(self, v: V) -> Self;

    /// Sets the retry policy configuration.
    fn with_retry_policy<V: Into<RetryPolicyArg>>(self, v: V) -> Self;
}

/// Simplify implementation of the [RequestOptionsBuilder] trait in generated
/// code.
///
/// This is an implementation detail, most applications have little need to
/// worry about or use this trait.
pub trait RequestBuilder {
    fn request_options(&mut self) -> &mut RequestOptions;
}

/// Implements the [RequestOptionsBuilder] trait for any [RequestBuilder]
/// implementation.
impl<T> RequestOptionsBuilder for T
where
    T: RequestBuilder,
{
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
}

/// Configure a client.
///
/// A client represents a connection to a Google Cloud Service. Each service
/// has one or more client types. The default configuration for each client
/// should work for most applications. But some applications may need to
/// override the default endpoint, the default authentication credentials,
/// the retry policies, and/or other behaviors of the client.
#[derive(Default)]
pub struct ClientConfig {
    pub(crate) endpoint: Option<String>,
    pub(crate) cred: Option<Credential>,
    pub(crate) tracing: bool,
    pub(crate) retry_policy: Option<Arc<dyn RetryPolicy>>,
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

    /// Disables tracing.
    pub fn disable_tracing(mut self) -> Self {
        self.tracing = false;
        self
    }

    pub fn set_credential<T: Into<Option<Credential>>>(mut self, v: T) -> Self {
        self.cred = v.into();
        self
    }

    pub fn set_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.retry_policy = Some(v.into().0);
        self
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::retry_policy::LimitedAttemptCount;
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

        opts.set_user_agent("test-only");
        assert_eq!(opts.user_agent().as_deref(), Some("test-only"));
        assert_eq!(opts.attempt_timeout(), &None);

        let d = Duration::from_secs(123);
        opts.set_attempt_timeout(d);
        assert_eq!(opts.user_agent().as_deref(), Some("test-only"));
        assert_eq!(opts.attempt_timeout(), &Some(d));

        opts.set_retry_policy(LimitedAttemptCount::new(3));
        assert!(opts.retry_policy.is_some(), "{opts:?}");
    }

    #[test]
    fn request_options_builder() {
        let mut builder = TestBuilder::default();
        assert_eq!(builder.request_options().user_agent(), &None);
        assert_eq!(builder.request_options().attempt_timeout(), &None);

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
            builder.request_options().retry_policy.is_some(),
            "{builder:?}"
        );
    }

    // This test must run serially because `std::env::remove_var` and
    // `std::env::set_var` are unsafe otherwise.
    #[test]
    #[serial_test::serial]
    fn config_tracing() {
        unsafe {
            std::env::remove_var(LOGGING_VAR);
        }
        let config = ClientConfig::new();
        assert!(!config.tracing_enabled(), "expected tracing to be disabled");
        let config = ClientConfig::new().enable_tracing();
        assert!(config.tracing_enabled(), "expected tracing to be enabled");
        let config = config.disable_tracing();
        assert!(
            !config.tracing_enabled(),
            "expected tracing to be disaabled"
        );

        unsafe {
            std::env::set_var(LOGGING_VAR, "true");
        }
        let config = ClientConfig::new();
        assert!(config.tracing_enabled(), "expected tracing to be enabled");

        unsafe {
            std::env::set_var(LOGGING_VAR, "not-true");
        }
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
        use auth::credentials::CredentialTrait;
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
}
