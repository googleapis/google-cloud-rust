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

use super::*;
use futures::stream::unfold;

/// A request builder for uploads without rewind.
pub struct UploadObjectBuffered<T> {
    inner: std::sync::Arc<StorageInner>,
    request: control::model::WriteObjectRequest,
    payload: InsertPayload<T>,
}

impl<T> UploadObjectBuffered<T> {
    pub(crate) fn new<B, O, P>(
        inner: std::sync::Arc<StorageInner>,
        bucket: B,
        object: O,
        payload: P,
    ) -> Self
    where
        B: Into<String>,
        O: Into<String>,
        P: Into<InsertPayload<T>>,
    {
        UploadObjectBuffered {
            inner,
            request: control::model::WriteObjectRequest::new().set_write_object_spec(
                control::model::WriteObjectSpec::new().set_resource(
                    control::model::Object::new()
                        .set_bucket(bucket)
                        .set_name(object),
                ),
            ),
            payload: payload.into(),
        }
    }
}

impl<T> UploadObjectBuffered<T>
where
    T: StreamingSource + Send + Sync + 'static,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    /// Upload an object from a streaming source without rewinds.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered(
    ///         "projects/_/buckets/my-bucket", "my-object",
    ///         "the quick brown fox jumped over the lazy dog")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub async fn send(self) -> crate::Result<Object> {
        let upload_url = self.start_resumable_upload().await?;
        let builder = self.upload_request(upload_url).await?;
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        let response = response.json::<v1::Object>().await.map_err(Error::io)?;

        Ok(Object::from(response))
    }

    /// Encrypt the data using a [Customer-Supplied Encryption Key].
    ///
    /// The key must be in raw bytes format, not base64-encoded.
    ///
    /// Example:
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::client::KeyAes256;
    /// # let client = Storage::builder().build().await?;
    /// let key: &[u8] = &[97; 32];
    /// let response = client
    ///     .upload_object_buffered(
    ///         "projects/_/buckets/my-bucket", "my-object",
    ///         "the quick brown fox jumped over the lazy dog")
    ///     .with_key(KeyAes256::new(key)?)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [Customer-Supplied Encryption Key]: https://cloud.google.com/storage/docs/encryption/customer-supplied-keys
    pub fn with_key(mut self, v: KeyAes256) -> Self {
        self.request.common_object_request_params = Some(v.into());
        self
    }

    async fn start_resumable_upload(&self) -> Result<String> {
        let builder = self.start_resumable_upload_request().await?;
        let response = builder.send().await.map_err(Error::io)?;
        self::handle_start_resumable_upload_response(response).await
    }

    async fn start_resumable_upload_request(&self) -> Result<reqwest::RequestBuilder> {
        use control::model::write_object_request::*;
        let resource = match &self.request.first_message {
            Some(FirstMessage::WriteObjectSpec(spec)) => spec.resource.as_ref().unwrap(),
            _ => unreachable!("write object spec set in constructor"),
        };
        let bucket = &resource.bucket;
        let bucket_id = bucket.strip_prefix("projects/_/buckets/").ok_or_else(|| {
            Error::binding(format!(
                "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
            ))
        })?;
        let object = &resource.name;
        let builder = self
            .inner
            .client
            .request(
                reqwest::Method::POST,
                format!("{}/upload/storage/v1/b/{bucket_id}/o", &self.inner.endpoint),
            )
            .query(&[("uploadType", "resumable")])
            .query(&[("name", enc(object))])
            .header("content-type", "application/json")
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        let builder = apply_customer_supplied_encryption_headers(
            builder,
            self.request.common_object_request_params.clone(),
        );
        let builder = self.inner.apply_auth_headers(builder).await?;
        let builder = builder.json(&v1::insert_body(resource));
        Ok(builder)
    }

    async fn upload_request(self, upload_url: String) -> Result<reqwest::RequestBuilder> {
        let builder = self
            .inner
            .client
            .request(reqwest::Method::PUT, upload_url)
            .header("content-type", "application/octet-stream")
            .header("Content-Range", "bytes */*")
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        let builder = apply_customer_supplied_encryption_headers(
            builder,
            self.request.common_object_request_params,
        );

        let builder = self.inner.apply_auth_headers(builder).await?;

        let stream = Box::pin(unfold(Some(self.payload), move |state| async move {
            use crate::upload_source::StreamingSource;
            if let Some(mut payload) = state {
                if let Some(next) = payload.next().await {
                    return Some((next, Some(payload)));
                }
            }
            None
        }));
        let builder = builder.body(reqwest::Body::wrap_stream(stream));
        Ok(builder)
    }
}

async fn handle_start_resumable_upload_response(response: reqwest::Response) -> Result<String> {
    if !response.status().is_success() {
        return gaxi::http::to_http_error(response).await;
    }
    let location = response
        .headers()
        .get("Location")
        .ok_or_else(|| Error::deser("missing Location header in start resumable upload"))?;
    location.to_str().map_err(Error::deser).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_key_helper;
    use super::super::tests::test_inner_client;
    use super::*;
    use crate::upload_source::test::VecStream;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use serde_json::{Value, json};
    use test_case::test_case;

    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    const SESSION: &str = "https://private.googleapis.com/test-only-session-123";

    #[tokio::test]
    async fn upload_object_buffered_normal() -> Result {
        let payload = serde_json::json!({
            "name": "test-object",
            "bucket": "test-bucket",
            "metadata": {
                "is-test-object": "true",
            }
        })
        .to_string();
        let server = Server::run();
        let session = server.url("/upload/session/test-only-001");
        let path = session.path().to_string();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "//upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
            .respond_with(
                status_code(200)
                    .append_header("location", session.to_string())
                    .body(""),
            ),
        );

        server.expect(
            Expectation::matching(all_of![request::method_path("PUT", path)]).respond_with(
                status_code(200)
                    .append_header("content-type", "application/json")
                    .body(payload),
            ),
        );

        let endpoint = server.url("");
        let client = Storage::builder()
            .with_endpoint(endpoint.to_string())
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let response = client
            .upload_object_buffered("projects/_/buckets/test-bucket", "test-object", "")
            .send()
            .await?;
        assert_eq!(response.name, "test-object");
        assert_eq!(response.bucket, "projects/_/buckets/test-bucket");
        assert_eq!(
            response.metadata.get("is-test-object").map(String::as_str),
            Some("true")
        );

        Ok(())
    }

    #[tokio::test]
    async fn upload_object_buffered_not_found() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "//upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
            .respond_with(status_code(404).body("NOT FOUND")),
        );

        let endpoint = server.url("");
        let client = Storage::builder()
            .with_endpoint(endpoint.to_string())
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let err = client
            .upload_object_buffered("projects/_/buckets/test-bucket", "test-object", "")
            .send()
            .await
            .expect_err("expected a not found error");
        assert_eq!(err.http_status_code(), Some(404), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let mut request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
                .start_resumable_upload_request()
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=resumable&name=object"
        );
        let body = request.body_mut().take().unwrap();
        let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
        let json = serde_json::from_slice::<Value>(&contents)?;
        assert_eq!(json, json!({}));
        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload_headers() -> Result {
        // Make a 32-byte key.
        let (key, key_base64, _, key_sha256_base64) = create_key_helper();

        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
                .with_key(KeyAes256::new(&key)?)
                .start_resumable_upload_request()
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=resumable&name=object"
        );

        let want = vec![
            ("x-goog-encryption-algorithm", "AES256".to_string()),
            ("x-goog-encryption-key", key_base64),
            ("x-goog-encryption-key-sha256", key_sha256_base64),
        ];

        for (name, value) in want {
            assert_eq!(
                request.headers().get(name).unwrap().as_bytes(),
                bytes::Bytes::from(value)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload_bad_bucket() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        UploadObjectBuffered::new(inner, "malformed", "object", "hello")
            .start_resumable_upload_request()
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload_credentials() -> Result {
        let config = gaxi::options::ClientConfig {
            cred: Some(auth::credentials::testing::error_credentials(false)),
            ..Default::default()
        };
        let inner = test_inner_client(config);
        let _ = UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .start_resumable_upload_request()
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
        Ok(())
    }

    #[test_case("projects/p", "projects%2Fp")]
    #[test_case("kebab-case", "kebab-case")]
    #[test_case("dot.name", "dot.name")]
    #[test_case("under_score", "under_score")]
    #[test_case("tilde~123", "tilde~123")]
    #[test_case("exclamation!point!", "exclamation%21point%21")]
    #[test_case("spaces   spaces", "spaces%20%20%20spaces")]
    #[test_case("preserve%percent%21", "preserve%percent%21")]
    #[test_case(
        "testall !#$&'()*+,/:;=?@[]",
        "testall%20%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D"
    )]
    #[tokio::test]
    async fn test_percent_encoding_object_name(name: &str, want: &str) -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", name, "hello")
            .start_resumable_upload_request()
            .await?
            .build()?;

        let got = request
            .url()
            .query_pairs()
            .find_map(|(key, val)| match key.to_string().as_str() {
                "name" => Some(val.to_string()),
                _ => None,
            })
            .unwrap();
        assert_eq!(got, want);
        Ok(())
    }

    #[tokio::test]
    async fn handle_start_resumable_upload_response() -> Result {
        let response = http::Response::builder()
            .header(
                "Location",
                "http://private.googleapis.com/test-only/session-123",
            )
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let url = super::handle_start_resumable_upload_response(response).await?;
        assert_eq!(url, "http://private.googleapis.com/test-only/session-123");
        Ok(())
    }

    #[tokio::test]
    async fn upload_request() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let mut request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
                .upload_request(SESSION.to_string())
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::PUT);
        assert_eq!(request.url().as_str(), SESSION);
        let body = request.body_mut().take().unwrap();
        let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
        assert_eq!(contents, "hello");
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_buffered_stream() -> Result {
        let stream = VecStream::new(
            [
                "the ", "quick ", "brown ", "fox ", "jumps ", "over ", "the ", "lazy ", "dog",
            ]
            .map(|x| bytes::Bytes::from_static(x.as_bytes()))
            .to_vec(),
        );
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let mut request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", stream)
                .upload_request(SESSION.to_string())
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::PUT);
        assert_eq!(request.url().as_str(), SESSION);
        let body = request.body_mut().take().unwrap();
        let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
        assert_eq!(contents, "the quick brown fox jumps over the lazy dog");
        Ok(())
    }

    #[tokio::test]
    async fn upload_request_headers() -> Result {
        // Make a 32-byte key.
        let (key, key_base64, _, key_sha256_base64) = create_key_helper();

        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
                .with_key(KeyAes256::new(&key)?)
                .upload_request(SESSION.to_string())
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::PUT);
        assert_eq!(request.url().as_str(), SESSION);

        let want = vec![
            ("x-goog-encryption-algorithm", "AES256".to_string()),
            ("x-goog-encryption-key", key_base64),
            ("x-goog-encryption-key-sha256", key_sha256_base64),
        ];

        for (name, value) in want {
            assert_eq!(
                request.headers().get(name).unwrap().as_bytes(),
                bytes::Bytes::from(value)
            );
        }
        Ok(())
    }
}
