use auth::credentials::Credential;
use retry::backoff_policy::{BackoffPolicy, BackoffPolicyArg};
use retry::retry_policy::{RetryPolicy, RetryPolicyArg};
use retry::polling_policy::{PollingPolicy, PollingPolicyArg};
use retry::polling_backoff_policy::{PollingBackoffPolicy, PollingBackoffPolicyArg};
use retry::retry_throttler::{AdaptiveThrottler, RetryThrottlerWrapped, RetryThrottlerArg};
use std::sync::Arc;


/// Configure a client.
///
/// A client represents a connection to a Google Cloud Service. Each service
/// has one or more client types. The default configuration for each client
/// should work for most applications. But some applications may need to
/// override the default endpoint, the default authentication credentials,
/// the retry policies, and/or other behaviors of the client.
pub struct ClientConfig {
    pub(crate) endpoint: Option<String>,
    pub(crate) cred: Option<Credential>,
    pub(crate) tracing: bool,
    pub(crate) retry_policy: Option<Arc<dyn RetryPolicy>>,
    pub(crate) backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    pub(crate) retry_throttler: RetryThrottlerWrapped,
    pub(crate) polling_policy: Option<Arc<dyn PollingPolicy>>,
    pub(crate) polling_backoff_policy: Option<Arc<dyn PollingBackoffPolicy>>,
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

    /// Configure the authentication credentials.
    pub fn set_credential<T: Into<Option<Credential>>>(mut self, v: T) -> Self {
        self.cred = v.into();
        self
    }

    /// Configure the retry policy.
    pub fn set_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.retry_policy = Some(v.into().0);
        self
    }

    /// Configure the retry backoff policy.
    pub fn set_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.backoff_policy = Some(v.into().0);
        self
    }

    /// Configure the retry throttler.
    pub fn set_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.retry_throttler = v.into().0;
        self
    }

    /// Configure the polling backoff policy.
    pub fn set_polling_policy<V: Into<PollingPolicyArg>>(mut self, v: V) -> Self {
        self.polling_policy = Some(v.into().0);
        self
    }

    /// Configure the polling backoff policy.
    pub fn set_polling_backoff_policy<V: Into<PollingBackoffPolicyArg>>(mut self, v: V) -> Self {
        self.polling_backoff_policy = Some(v.into().0);
        self
    }
}

impl std::default::Default for ClientConfig {
    fn default() -> Self {
        use AdaptiveThrottler;
        use std::sync::{Arc, Mutex};
        Self {
            endpoint: None,
            cred: None,
            tracing: false,
            retry_policy: None,
            backoff_policy: None,
            retry_throttler: Arc::new(Mutex::new(AdaptiveThrottler::default())),
            polling_policy: None,
            polling_backoff_policy: None,
        }
    }
}