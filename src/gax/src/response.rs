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

    /// Returns a locked reference to the associated extensions.
    ///
    /// This method provides access to the `http::Extensions` for this response.
    /// The `Arc<Mutex<>>` wrapping is to maintain `UnwindSafe` (see github.com/googleapis/google-cloud-rust/issues/3463).
    ///
    /// # Panics
    ///
    /// Panics if the underlying mutex is poisoned.
    pub fn extensions(&self) -> std::sync::MutexGuard<'_, http::Extensions> {
        self.parts
            .extensions
            .lock()
            .expect("Extensions mutex is not poisoned")
    }

    /// Returns a locked mutable reference to the associated extensions.
    ///
    /// This method provides access to the `http::Extensions` for this response.
    /// The `Arc<Mutex<>>` wrapping is to maintain `UnwindSafe` (see github.com/googleapis/google-cloud-rust/issues/3463).
    ///
    /// # Panics
    ///
    /// Panics if the underlying mutex is poisoned.
    pub fn extensions_mut(&mut self) -> std::sync::MutexGuard<'_, http::Extensions> {
        self.parts
            .extensions
            .lock()
            .expect("Extensions mutex is not poisoned")
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
    /// Extensions for passing data from the transport.
    // Wrapped in Arc<Mutex<>> to maintain UnwindSafe, see github.com/googleapis/google-cloud-rust/issues/3463
    pub extensions: Arc<Mutex<http::Extensions>>,
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
}
