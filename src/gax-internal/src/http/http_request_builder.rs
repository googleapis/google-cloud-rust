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

//! Defines types to make raw HTTP requests.
//!
//! Most client libraries use [ReqwestClient] to issue RPCs with JSON
//! payloads and responses. Storage uses [ReqwestClient] to issue
//! regular HTTP requests, with the body and responses consisting of
//! byte streams.

use super::reqwest::{Body, Request, RequestBuilder, Response};
use super::reqwest::{HeaderName, HeaderValue};
use super::{RequestOptions, ReqwestClient, Result};
use std::time::Duration;

/// A builder for plain HTTP requests.
///
/// This builder is returned by [ReqwestClient::http_builder] and [RequestClient::http_builder_with_url].
///
/// # Example
/// ```
/// # use google_cloud_gax_internal::http::ReqwestClient;
/// use google_cloud_gax_internal::http::reqwest::Method;
/// use google_cloud_gax::options::RequestOptions;
/// async fn sample(client: &ReqwestClient, options: RequestOptions) -> anyhow::Result<()> {
///     let builder = client.http_builder_with_url(
///         Method::GET,
///         "https://storage.googleapis.com/storage/v1/b/my-bucket/o/my-object",
///         "https://storage.googleapis.com",
///     )?
///     .query("alt", "media")
///     .header("x-goog-api-client", "client/1.2.3");
///     let response = builder.send(options, None, 0).await?;
///     println!("status={:?}", response.status());
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct HttpRequestBuilder {
    client: ReqwestClient,
    builder: RequestBuilder,
}

impl HttpRequestBuilder {
    pub(crate) fn new(client: ReqwestClient, builder: RequestBuilder) -> Self {
        Self { client, builder }
    }

    /// Sends the request adding the required headers for an attempt.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax_internal::http::ReqwestClient;
    /// use google_cloud_gax_internal::http::reqwest::Method;
    /// use google_cloud_gax::options::RequestOptions;
    /// async fn sample(client: &ReqwestClient, options: RequestOptions) -> anyhow::Result<()> {
    ///     let mut err = Vec::new();
    ///     for count in (0..3) {
    ///         let builder = client.http_builder(Method::GET, "storage/v1/b/my-bucket/o/my-object")
    ///             .query("alt", "media");
    ///         let result = builder.send(options.clone(), None, count).await;
    ///         if result.is_ok() {
    ///             println!("success! {result:?}");
    ///             return Ok(())
    ///         }
    ///         err.push(result.unwrap_err());
    ///     }
    ///     println!("exhausted: {err:?}");
    ///     Ok(())
    /// }
    /// ```
    pub async fn send(
        self,
        options: RequestOptions,
        remaining_time: Option<Duration>,
        attempt_count: u32,
    ) -> Result<Response> {
        self.client
            .execute_http(self.builder, options, remaining_time, attempt_count)
            .await
    }

    /// Adds a query parameter to the request.
    pub fn query<V>(self, key: &str, value: V) -> Self
    where
        V: ToString,
    {
        Self::new(self.client, self.builder.query(&[(key, value.to_string())]))
    }

    /// Adds a header to the request.
    pub fn header<K, V>(self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<http::Error>,
    {
        Self::new(self.client, self.builder.header(key, value))
    }

    /// Adds the body to the request.
    pub fn body<B>(self, body: B) -> Self
    where
        B: Into<Body>,
    {
        Self::new(self.client, self.builder.body(body))
    }

    /// The storage client tests verify the right headers and query parameters are set.
    pub async fn build_for_tests(self) -> Result<Request> {
        self.client
            .request(self.builder, &RequestOptions::default(), None)
            .await
    }
}
