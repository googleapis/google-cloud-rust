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
//!

#[derive(Clone, Debug)]
pub struct Response<T> {
    parts: Parts,
    body: T,
}

impl<T> Response<T> {
    pub fn from<V: Into<T>>(v: V) -> Self {
        Self {
            body: v.into(),
            parts: Parts::default(),
        }
    }

    pub fn headers(&self) -> &http::HeaderMap<http::HeaderValue> {
        &self.parts.headers
    }
    pub fn body(&self) -> &T {
        &self.body
    }

    pub fn into_parts(self) -> (Parts, T) {
        (self.parts, self.body)
    }

    pub fn into_body(self) -> T {
        self.body
    }

    pub fn from_parts(parts: Parts, body: T) -> Self {
        Self { parts, body }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Parts {
    pub headers: http::HeaderMap<http::HeaderValue>,
    pub extensions: http::Extensions,
}

impl Parts {
    pub fn new() -> Self {
        Parts::default()
    }
    pub fn set_headers<V>(mut self, v: V) -> Self
    where
        V: Into<http::HeaderMap>,
    {
        self.headers = v.into();
        self
    }
    pub fn set_extensions<V>(mut self, v: V) -> Self
    where
        V: Into<http::Extensions>,
    {
        self.extensions = v.into();
        self
    }
}
