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

//! Per request options.
//!
//! Applications may need to customize the behavior of some calls made via a
//! client. The `*Builder` returned by each client method implements the
//! [RequestOptionsBuilder] trait where applications can override some defaults.

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
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

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
        assert_eq!(opts.user_agent(), &None);
        assert_eq!(opts.attempt_timeout(), &None);
        let debug = format!("{opts:?}");
        assert!(debug.contains("RequestOptions"), "{debug}");
        assert!(debug.contains("user_agent"), "{debug}");
        assert!(debug.contains("attempt_timeout"), "{debug}");

        opts.set_user_agent("test-only");
        assert_eq!(opts.user_agent().as_deref(), Some("test-only"));
        assert_eq!(opts.attempt_timeout(), &None);

        let d = Duration::from_secs(123);
        opts.set_attempt_timeout(d);
        assert_eq!(opts.user_agent().as_deref(), Some("test-only"));
        assert_eq!(opts.attempt_timeout(), &Some(d));

        let debug = format!("{opts:?}");
        assert!(debug.contains("RequestOptions"), "{debug}");
        assert!(debug.contains("user_agent"), "{debug}");
        assert!(debug.contains("Some(\"test-only\")"), "{debug}");
        assert!(debug.contains("attempt_timeout"), "{debug}");
        assert!(debug.contains("Some(123s)"), "{debug}");
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
    }
}
