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

//! Response types.
//!
//! This module contains types related to Google Cloud service responses.
//! Notably it contains the `Response` type itself. Typically you'll import
//! this type.
//!
//! # Examples
//!
//! Inspecting the result of a request
//!
//! ```no_run
//! # use google_cloud_gax::Result;
//! # use google_cloud_gax::response::Response;
//! // A type representing a Google Cloud service resource, for example, a
//! // secret manager secret.
//! struct Resource {
//!   // ...
//! }
//!
//! async fn make_google_service_rpc(project_id: &str) -> Result<Response<Resource>> {
//!   // ...
//! # panic!()
//! }
//!
//! # tokio_test::block_on(async {
//! let response = make_google_service_rpc("my-project").await?;
//! if let Some(date) = response.headers().get("Date") {
//!     // do something with the date
//! }
//! let resource = response.body();
//! // do something with
//! # Result::<()>::Ok(()) });
//! ```
//!
//! Creating a response for mocks
//!
//! ```
//! # use google_cloud_gax::Result;
//! # use google_cloud_gax::response::Response;
//! // A type representing a Google Cloud service resource, for example, a
//! // secret manager secret.
//! struct Resource {
//!   // ...
//! }
//!
//! fn make_mock_response(body: Resource) -> Result<Response<Resource>> {
//!     Ok(Response::from(body))
//! }
//! ```

use std::sync::{Arc, Mutex};

/// Represents a Google Cloud service response.
///
/// A response from a Google Cloud service consists of a body (potentially the
/// unit type), and some metadata, currently just headers.
///
/// Typically you get a response as the result of making a request via some
/// client in the Google Cloud client libraries for Rust. You may also create
/// responses directly when mocking clients for your own tests.
///
/// # Examples
/// Inspecting the result of a request
///
/// ```no_run
/// # use google_cloud_gax::Result;
/// # use google_cloud_gax::response::Response;
/// // A type representing a Google Cloud service resource, for example, a
/// // secret manager secret.
/// struct Resource {
///   // ...
/// }
///
/// async fn make_google_service_rpc(project_id: &str) -> Result<Response<Resource>> {
///   // ...
/// # panic!()
/// }
///
/// # tokio_test::block_on(async {
/// let response = make_google_service_rpc("my-project").await?;
/// if let Some(date) = response.headers().get("Date") {
///     // do something with the date
/// }
/// let resource = response.body();
/// // do something with
/// # Result::<()>::Ok(()) });
/// ```
///
/// Creating a response for mocks
///
/// ```
/// # use google_cloud_gax::Result;
/// # use google_cloud_gax::response::Response;
/// // A type representing a Google Cloud service resource, for example, a
/// // secret manager secret.
/// struct Resource {
///   // ...
/// }
///
/// fn make_mock_response(body: Resource) -> Result<Response<Resource>> {
///     Ok(Response::from(body))
/// }
/// ```
///
#[derive(Clone, Debug)]
pub struct Response<T> {
    parts: Parts,
    body: T,
}

impl<T> Response<T> {
    /// Creates a response from the body.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::response::Response;
    /// #[derive(Clone, Default)]
    /// pub struct Resource {
    ///   // ...
    /// }
    ///
    /// let body = Resource::default();
    /// let response = Response::from(body);
    /// ```
    pub fn from(body: T) -> Self {
        Self {
            body,
            parts: Parts::default(),
        }
    }

    /// Creates a response from the given parts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::response::Response;
    /// # use google_cloud_gax::response::Parts;
    /// #[derive(Clone, Default)]
    /// pub struct Resource {
    ///   // ...
    /// }
    ///
    /// let mut headers = http::HeaderMap::new();
    /// headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
    /// let body = Resource::default();
    /// let response : Response<Resource> = Response::from_parts(
    ///     Parts::new().set_headers(headers), body);
    /// assert!(response.headers().get(http::header::CONTENT_TYPE).is_some());
    /// ```
    pub fn from_parts(parts: Parts, body: T) -> Self {
        Self { parts, body }
    }

    /// Returns the headers associated with this response.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::response::Response;
    /// let response = Response::from(());
    /// assert!(response.headers().is_empty());
    /// ```
    pub fn headers(&self) -> &http::HeaderMap<http::HeaderValue> {
        &self.parts.headers
    }

    /// Returns the body associated with this response.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::response::Response;
    /// let response = Response::from("test".to_string());
    /// assert_eq!(response.body().as_str(), "test");
    /// ```
    pub fn body(&self) -> &T {
        &self.body
    }

    /// Consumes the response returning the metadata, and body.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::response::Response;
    /// let response = Response::from("test".to_string());
    /// let (parts, body) = response.into_parts();
    /// assert_eq!(body.as_str(), "test");
    /// assert!(parts.headers.is_empty());
    /// ```
    pub fn into_parts(self) -> (Parts, T) {
        (self.parts, self.body)
    }

    /// Consumes the response returning only its body.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::response::Response;
    /// let response = Response::from("test".to_string());
    /// let body = response.into_body();
    /// assert_eq!(body.as_str(), "test");
    /// ```
    pub fn into_body(self) -> T {
        self.body
    }
}

/// Component parts of a response.
///
/// The response parts, other than the body, consist of just headers. We
/// anticipate the addition of new fields over time.
///
/// The headers are used to return gRPC metadata, as well as (unsurprisingly)
/// HTTP headers.
///
/// # Example
/// ```
/// # use google_cloud_gax::response::Parts;
/// let mut headers = http::HeaderMap::new();
/// headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
/// let parts = Parts::new().set_headers(headers);
///
/// assert_eq!(
///     parts.headers.get(http::header::CONTENT_TYPE),
///     Some(&http::HeaderValue::from_static("application/json"))
/// );
/// ```
///
/// [tower]: https://github.com/tower-rs/tower
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct Parts {
    /// The HTTP headers or the gRPC metadata converted to HTTP headers.
    pub headers: http::HeaderMap<http::HeaderValue>,
    // Internal field for transport-specific observability data.
    // Wrapped in Arc<Mutex<>> to maintain UnwindSafe for Response<T>,
    // as the boxed trait object may not be UnwindSafe.
    // See https://github.com/googleapis/google-cloud-rust/issues/3463 for details.
    transport_summary: Arc<Mutex<Option<Box<dyn internal::TransportSummary>>>>,
}

impl Parts {
    /// Create a new instance.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::response::Parts;
    /// let parts = Parts::new();
    /// assert!(parts.headers.is_empty());
    /// ```
    pub fn new() -> Self {
        Parts::default()
    }

    /// Set the headers.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::response::Parts;
    /// let mut headers = http::HeaderMap::new();
    /// headers.insert(
    ///     http::header::CONTENT_TYPE,
    ///     http::HeaderValue::from_static("application/json"),
    /// );
    /// let parts = Parts::new().set_headers(headers.clone());
    /// assert_eq!(parts.headers, headers);
    /// ```
    pub fn set_headers<V>(mut self, v: V) -> Self
    where
        V: Into<http::HeaderMap>,
    {
        self.headers = v.into();
        self
    }
}

#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
pub mod internal {
    //! This module contains implementation details. It is not part of the
    //! public API. Types and functions in this module may be changed or removed
    //! without warnings. Applications should not use any types contained
    //! within.

    use super::Response;
    use std::any::Any;
    use std::fmt::Debug;
    /// Trait for transport-specific summaries to be passed from transport layers.
    pub trait TransportSummary: Debug + Send + Sync + 'static {
        /// Get the summary as a `dyn Any` to allow downcasting.
        fn as_any(&self) -> &dyn Any;
        /// Clone the boxed trait object.
        fn clone_box(&self) -> Box<dyn TransportSummary>;
    }

    impl Clone for Box<dyn TransportSummary> {
        fn clone(&self) -> Self {
            self.clone_box()
        }
    }

    /// Sets the transport summary for the response.
    /// This is intended for internal use by transport layers.
    pub fn set_transport_summary<T>(
        response: &mut Response<T>,
        summary: Box<dyn TransportSummary>,
    ) {
        let mut guard = response
            .parts
            .transport_summary
            .lock()
            .expect("Mutex poisoned");
        *guard = Some(summary);
    }
    /// Gets the transport summary from the response.
    /// This is intended for internal use by observability helpers.
    pub fn get_transport_summary<T>(response: &Response<T>) -> Option<Box<dyn TransportSummary>> {
        let guard = response
            .parts
            .transport_summary
            .lock()
            .expect("Mutex poisoned");
        guard.clone()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_from() {
        let response = Response::from("abc123".to_string());
        assert!(response.headers().is_empty());
        assert_eq!(response.body().as_str(), "abc123");

        let body = response.into_body();
        assert_eq!(body.as_str(), "abc123");
    }

    #[test]
    fn response_from_parts() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        let parts = Parts::new().set_headers(headers.clone());

        let response = Response::from_parts(parts, "abc123".to_string());
        assert_eq!(response.body().as_str(), "abc123");
        assert_eq!(response.headers(), &headers);

        let (parts, body) = response.into_parts();
        assert_eq!(body.as_str(), "abc123");
        assert_eq!(parts.headers, headers);
    }

    #[test]
    fn parts() {
        let parts = Parts::new();
        assert!(parts.headers.is_empty());

        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        let parts = Parts::new().set_headers(headers.clone());

        assert_eq!(parts.headers, headers);
        assert_eq!(
            parts.headers.get(http::header::CONTENT_TYPE),
            Some(&http::HeaderValue::from_static("application/json"))
        );
    }

    #[derive(Debug, Clone)]
    struct TestSummary {
        data: &'static str,
    }

    impl internal::TransportSummary for TestSummary {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn clone_box(&self) -> Box<dyn internal::TransportSummary> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn transport_summary_set_get() {
        let mut response = Response::from(());
        assert!(internal::get_transport_summary(&response).is_none());

        let summary = TestSummary {
            data: "test_data",
        };
        internal::set_transport_summary(&mut response, Box::new(summary.clone()));

        let retrieved = internal::get_transport_summary(&response);
        assert!(retrieved.is_some());

        let retrieved_summary = retrieved.unwrap();
        let mock_summary = retrieved_summary.as_any().downcast_ref::<TestSummary>();
        assert!(mock_summary.is_some());
        assert_eq!(mock_summary.unwrap().data, "test_data");
    }

    #[test]
    fn transport_summary_overwrite() {
        let mut response = Response::from(());
        let summary1 = TestSummary {
            data: "data1",
        };
        internal::set_transport_summary(&mut response, Box::new(summary1));

        let summary2 = TestSummary {
            data: "data2",
        };
        internal::set_transport_summary(&mut response, Box::new(summary2));

        let retrieved = internal::get_transport_summary(&response)
            .unwrap()
            .as_any()
            .downcast_ref::<TestSummary>()
            .unwrap()
            .data;
        assert_eq!(retrieved, "data2");
    }
}
