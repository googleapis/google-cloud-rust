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

//! Provide types for client construction.
//!
//! Some applications need to construct clients with custom configuration, for
//! example, they may need to override the endpoint or the authentication
//! credentials. The types in this module implement the client builders.
//!
//! Applications should not create builders directly, instead each client type
//! defines a `builder()` function to obtain the correct type of builder:
//!
//! ## Example: create a client with the default configuration.
//!
//! ```
//! # use google_cloud_gax::client_builder::examples;
//! # use google_cloud_gax::Result;
//! # tokio_test::block_on(async {
//! pub use examples::Client; // Placeholder for examples
//! let client = Client::builder().build().await?;
//! # Result::<()>::Ok(()) });
//! ```
//!
//! ## Example: create a client with a different endpoint
//!
//! ```
//! # use google_cloud_gax::client_builder::examples;
//! # use google_cloud_gax::Result;
//! # tokio_test::block_on(async {
//! pub use examples::Client; // Placeholder for examples
//! let client = Client::builder()
//!     .with_endpoint("https://private.googleapis.com")
//!     .build().await?;
//! # Result::<()>::Ok(()) });
//! ```

use crate::Result;
use crate::backoff_policy::{BackoffPolicy, BackoffPolicyArg};
use crate::polling_backoff_policy::{PollingBackoffPolicy, PollingBackoffPolicyArg};
use crate::polling_error_policy::{PollingErrorPolicy, PollingErrorPolicyArg};
use crate::retry_policy::{RetryPolicy, RetryPolicyArg};
use crate::retry_throttler::{RetryThrottlerArg, SharedRetryThrottler};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ClientBuilder<F, Cr> {
    config: internal::ClientConfig<Cr>,
    factory: F,
}

impl<F, Cr> ClientBuilder<F, Cr> {
    /// Creates a new client.
    ///
    /// ```
    /// # use google_cloud_gax::client_builder::examples;
    /// # use google_cloud_gax::Result;
    /// # tokio_test::block_on(async {
    /// use examples::Client; // Placeholder for examples
    /// let client = Client::builder()
    ///     .build().await?;
    /// # Result::<()>::Ok(()) });
    /// ```
    pub async fn build<C>(self) -> Result<C>
    where
        F: internal::ClientFactory<Client = C, Credentials = Cr>,
    {
        self.factory.build(self.config).await
    }

    /// Sets the endpoint.
    ///
    /// ```
    /// # use google_cloud_gax::client_builder::examples;
    /// # use google_cloud_gax::Result;
    /// # tokio_test::block_on(async {
    /// use examples::Client; // Placeholder for examples
    /// let client = Client::builder()
    ///     .with_endpoint("http://private.googleapis.com")
    ///     .build().await?;
    /// # Result::<()>::Ok(()) });
    /// ```
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.config.endpoint = Some(v.into());
        self
    }

    /// Enables tracing.
    ///
    /// The client libraries can be dynamically instrumented with the Tokio
    /// [tracing] framework. Setting this flag enables this instrumentation.
    ///
    /// ```
    /// # use google_cloud_gax::client_builder::examples;
    /// # use google_cloud_gax::Result;
    /// # tokio_test::block_on(async {
    /// use examples::Client; // Placeholder for examples
    /// let client = Client::builder()
    ///     .with_tracing()
    ///     .build().await?;
    /// # Result::<()>::Ok(()) });
    /// ```
    ///
    /// [tracing]: https://docs.rs/tracing/latest/tracing/
    pub fn with_tracing(mut self) -> Self {
        self.config.tracing = true;
        self
    }

    /// Configure the authentication credentials.
    ///
    /// Most Google Cloud services require authentication, though some services
    /// allow for anonymous access, and some services provide emulators where
    /// no authentication is required. More information about valid credential
    /// types can be found in the [google-cloud-auth] crate documentation.
    ///
    /// ```
    /// # use google_cloud_gax::client_builder::examples;
    /// # use google_cloud_gax::Result;
    /// # tokio_test::block_on(async {
    /// use examples::Client; // Placeholder for examples
    /// // Placeholder, normally use google_cloud_auth::credentials
    /// use examples::credentials;
    /// let client = Client::builder()
    ///     .with_credentials(
    ///         credentials::mds::Builder::new()
    ///             .scopes(["https://www.googleapis.com/auth/cloud-platform.read-only"])
    ///             .build())
    ///     .build().await?;
    /// # Result::<()>::Ok(()) });
    /// ```
    ///
    /// [google-cloud-auth]: https://docs.rs/google-cloud-auth
    pub fn with_credentials<T: Into<Cr>>(mut self, v: T) -> Self {
        self.config.cred = Some(v.into());
        self
    }

    /// Configure the retry policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// retry policy controls what errors are considered retryable, sets limits
    /// on the number of attempts or the time trying to make attempts.
    ///
    /// ```
    /// # use google_cloud_gax::client_builder::examples;
    /// # use google_cloud_gax as gax;
    /// # use google_cloud_gax::Result;
    /// # tokio_test::block_on(async {
    /// use examples::Client; // Placeholder for examples
    /// use gax::retry_policy;
    /// use gax::retry_policy::RetryPolicyExt;
    /// let client = Client::builder()
    ///     .with_retry_policy(retry_policy::AlwaysRetry.with_attempt_limit(3))
    ///     .build().await?;
    /// # Result::<()>::Ok(()) });
    /// ```
    pub fn with_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.config.retry_policy = Some(v.into().0);
        self
    }

    /// Configure the retry backoff policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// backoff policy controls how long to wait in between retry attempts.
    ///
    /// ```
    /// # use google_cloud_gax::client_builder::examples;
    /// # use google_cloud_gax as gax;
    /// # use google_cloud_gax::Result;
    /// # tokio_test::block_on(async {
    /// use examples::Client; // Placeholder for examples
    /// use gax::exponential_backoff::ExponentialBackoffBuilder;
    /// use std::time::Duration;
    /// let policy = ExponentialBackoffBuilder::new()
    ///     .with_initial_delay(Duration::from_millis(100))
    ///     .with_maximum_delay(Duration::from_secs(5))
    ///     .with_scaling(4.0)
    ///     .build()?;
    /// let client = Client::builder()
    ///     .with_backoff_policy(policy)
    ///     .build().await?;
    /// # Result::<()>::Ok(()) });
    /// ```
    pub fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.config.backoff_policy = Some(v.into().0);
        self
    }

    /// Configure the retry throttler.
    ///
    /// Advanced applications may want to configure a retry throttler to
    /// [Address Cascading Failures] and when [Handling Overload] conditions.
    /// The client libraries throttle their retry loop, using a policy to
    /// control the throttling algorithm. Use this method to fine tune or
    /// customize the default retry throtler.
    ///
    /// [Handling Overload]: https://sre.google/sre-book/handling-overload/
    /// [Addressing Cascading Failures]: https://sre.google/sre-book/addressing-cascading-failures/
    ///
    /// ```
    /// # use google_cloud_gax::client_builder::examples;
    /// # use google_cloud_gax as gax;
    /// # use google_cloud_gax::Result;
    /// # tokio_test::block_on(async {
    /// use examples::Client; // Placeholder for examples
    /// use gax::retry_throttler::AdaptiveThrottler;
    /// let client = Client::builder()
    ///     .with_retry_throttler(AdaptiveThrottler::new(2.0)?)
    ///     .build().await?;
    /// # Result::<()>::Ok(()) });
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.config.retry_throttler = v.into().0;
        self
    }

    /// Configure the polling error policy.
    ///
    /// Some clients support long-running operations, the client libraries can
    /// automatically poll these operations until they complete. Polling may
    /// fail due to transient errors and applications may want to continue the
    /// polling loop despite such errors. The polling error policy controls
    /// which errors are treated as recoverable, and may limit the number
    /// of attempts and/or the total time polling the operation.
    ///
    /// ```
    /// # use google_cloud_gax::client_builder::examples;
    /// # use google_cloud_gax as gax;
    /// # use google_cloud_gax::Result;
    /// # tokio_test::block_on(async {
    /// use examples::Client; // Placeholder for examples
    /// use gax::polling_error_policy::Aip194Strict;
    /// use gax::polling_error_policy::PollingErrorPolicyExt;
    /// use std::time::Duration;
    /// let client = Client::builder()
    ///     .with_polling_error_policy(Aip194Strict
    ///         .with_time_limit(Duration::from_secs(15 * 60))
    ///         .with_attempt_limit(50))
    ///     .build().await?;
    /// # Result::<()>::Ok(()) });
    /// ```
    pub fn with_polling_error_policy<V: Into<PollingErrorPolicyArg>>(mut self, v: V) -> Self {
        self.config.polling_error_policy = Some(v.into().0);
        self
    }

    /// Configure the polling backoff policy.
    ///
    /// Some clients support long-running operations, the client libraries can
    /// automatically poll these operations until they complete. The polling
    /// backoff policy controls how long the client waits between polling
    /// attempts.
    ///
    /// ```
    /// # use google_cloud_gax::client_builder::examples;
    /// # use google_cloud_gax as gax;
    /// # use google_cloud_gax::Result;
    /// # tokio_test::block_on(async {
    /// use examples::Client; // Placeholder for examples
    /// use gax::exponential_backoff::ExponentialBackoffBuilder;
    /// use std::time::Duration;
    /// let policy = ExponentialBackoffBuilder::new()
    ///     .with_initial_delay(Duration::from_millis(100))
    ///     .with_maximum_delay(Duration::from_secs(5))
    ///     .with_scaling(4.0)
    ///     .build()?;
    /// let client = Client::builder()
    ///     .with_polling_backoff_policy(policy)
    ///     .build().await?;
    /// # Result::<()>::Ok(()) });
    /// ```
    pub fn with_polling_backoff_policy<V: Into<PollingBackoffPolicyArg>>(mut self, v: V) -> Self {
        self.config.polling_backoff_policy = Some(v.into().0);
        self
    }
}

#[doc(hidden)]
pub mod internal {
    use super::*;

    pub trait ClientFactory {
        type Client;
        type Credentials;
        fn build(
            self,
            config: internal::ClientConfig<Self::Credentials>,
        ) -> impl Future<Output = Result<Self::Client>>;
    }

    pub fn new_builder<F, Cr, C>(factory: F) -> super::ClientBuilder<F, Cr>
    where
        F: ClientFactory<Client = C, Credentials = Cr>,
    {
        super::ClientBuilder {
            factory,
            config: ClientConfig::default(),
        }
    }

    /// Configure a client.
    ///
    /// A client represents a connection to a Google Cloud Service. Each service
    /// has one or more client types. The default configuration for each client
    /// should work for most applications. But some applications may need to
    /// override the default endpoint, the default authentication credentials,
    /// the retry policies, and/or other behaviors of the client.
    #[derive(Clone, Debug)]
    pub struct ClientConfig<Cr> {
        pub endpoint: Option<String>,
        pub cred: Option<Cr>,
        pub tracing: bool,
        pub retry_policy: Option<Arc<dyn RetryPolicy>>,
        pub backoff_policy: Option<Arc<dyn BackoffPolicy>>,
        pub retry_throttler: SharedRetryThrottler,
        pub polling_error_policy: Option<Arc<dyn PollingErrorPolicy>>,
        pub polling_backoff_policy: Option<Arc<dyn PollingBackoffPolicy>>,
    }

    impl<Cr> std::default::Default for ClientConfig<Cr> {
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
}

#[doc(hidden)]
pub mod examples {
    type Config = super::internal::ClientConfig<Credentials>;
    use super::Result;

    /// A client type for use in examples.
    ///
    /// This type is used in examples as a placeholder for a real client. It
    /// does not work, but illustrates how to use `ClientBuilder`.
    #[allow(dead_code)]
    pub struct Client(Config);
    impl Client {
        /// Create a builder to initialize new instances of this client.
        pub fn builder() -> client::Builder {
            super::internal::new_builder(client::Factory)
        }

        async fn new(config: super::internal::ClientConfig<Credentials>) -> Result<Self> {
            Ok(Self(config))
        }
    }
    mod client {
        pub type Builder = super::super::ClientBuilder<Factory, super::Credentials>;
        pub struct Factory;
        impl super::super::internal::ClientFactory for Factory {
            type Credentials = super::Credentials;
            type Client = super::Client;
            async fn build(
                self,
                config: crate::client_builder::internal::ClientConfig<Self::Credentials>,
            ) -> crate::Result<Self::Client> {
                Self::Client::new(config).await
            }
        }
    }

    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct Credentials {
        pub scopes: Vec<String>,
    }

    pub mod credentials {
        pub mod mds {
            #[derive(Clone, Default)]
            pub struct Builder(super::super::Credentials);
            impl Builder {
                pub fn new() -> Self {
                    Self(super::super::Credentials::default())
                }
                pub fn build(self) -> super::super::Credentials {
                    self.0
                }
                pub fn scopes<I, V>(mut self, iter: I) -> Self
                where
                    I: IntoIterator<Item = V>,
                    V: Into<String>,
                {
                    self.0.scopes = iter.into_iter().map(|v| v.into()).collect();
                    self
                }
            }
        }
    }

    // We use the examples as scaffolding for the tests.
    #[cfg(test)]
    mod test {
        use super::*;

        #[tokio::test]
        async fn build_default() {
            let client = Client::builder().build().await.unwrap();
            let config = client.0;
            assert_eq!(config.endpoint, None);
            assert_eq!(config.cred, None);
            assert_eq!(config.tracing, false);
            assert!(
                format!("{:?}", &config).contains("AdaptiveThrottler"),
                "{config:?}"
            );
            assert!(config.retry_policy.is_none(), "{config:?}");
            assert!(config.backoff_policy.is_none(), "{config:?}");
            assert!(config.polling_error_policy.is_none(), "{config:?}");
            assert!(config.polling_backoff_policy.is_none(), "{config:?}");
        }

        #[tokio::test]
        async fn endpoint() {
            let client = Client::builder()
                .with_endpoint("http://example.com")
                .build()
                .await
                .unwrap();
            let config = client.0;
            assert_eq!(config.endpoint.as_deref(), Some("http://example.com"));
        }

        #[tokio::test]
        async fn tracing() {
            let client = Client::builder().with_tracing().build().await.unwrap();
            let config = client.0;
            assert_eq!(config.tracing, true);
        }

        #[tokio::test]
        async fn credentials() {
            let client = Client::builder()
                .with_credentials(
                    credentials::mds::Builder::new()
                        .scopes(["test-scope"])
                        .build(),
                )
                .build()
                .await
                .unwrap();
            let config = client.0;
            let cred = config.cred.unwrap();
            assert_eq!(cred.scopes, vec!["test-scope".to_string()]);
        }

        #[tokio::test]
        async fn retry_policy() {
            use crate::retry_policy::RetryPolicyExt;
            let client = Client::builder()
                .with_retry_policy(crate::retry_policy::AlwaysRetry.with_attempt_limit(3))
                .build()
                .await
                .unwrap();
            let config = client.0;
            assert!(config.retry_policy.is_some(), "{config:?}");
        }

        #[tokio::test]
        async fn backoff_policy() {
            let client = Client::builder()
                .with_backoff_policy(crate::exponential_backoff::ExponentialBackoff::default())
                .build()
                .await
                .unwrap();
            let config = client.0;
            assert!(config.backoff_policy.is_some(), "{config:?}");
        }

        #[tokio::test]
        async fn retry_throttler() {
            use crate::retry_throttler::CircuitBreaker;
            let client = Client::builder()
                .with_retry_throttler(CircuitBreaker::default())
                .build()
                .await
                .unwrap();
            let config = client.0;
            assert!(
                format!("{:?}", &config).contains("CircuitBreaker"),
                "{config:?}"
            );
        }

        #[tokio::test]
        async fn polling_error_policy() {
            use crate::polling_error_policy::PollingErrorPolicyExt;
            let client = Client::builder()
                .with_polling_error_policy(
                    crate::polling_error_policy::AlwaysContinue.with_attempt_limit(3),
                )
                .build()
                .await
                .unwrap();
            let config = client.0;
            assert!(config.polling_error_policy.is_some(), "{config:?}");
        }

        #[tokio::test]
        async fn polling_backoff_policy() {
            let client = Client::builder()
                .with_polling_backoff_policy(
                    crate::exponential_backoff::ExponentialBackoff::default(),
                )
                .build()
                .await
                .unwrap();
            let config = client.0;
            assert!(config.polling_backoff_policy.is_some(), "{config:?}");
        }
    }
}
