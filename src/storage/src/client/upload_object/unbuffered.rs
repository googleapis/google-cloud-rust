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

impl<T> UploadObject<T>
where
    T: StreamingSource + Send + Sync + 'static,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    /// A simple upload from a buffer.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send_unbuffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub async fn send_unbuffered(self) -> crate::Result<Object> {
        // TODO(#2634) - use to resumable uploads for large payloads.
        let builder = self.single_shot_builder().await?;
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        let response = response.json::<v1::Object>().await.map_err(Error::io)?;

        Ok(Object::from(response))
    }

    async fn single_shot_builder(self) -> Result<reqwest::RequestBuilder> {
        let bucket = &self.resource.bucket;
        let bucket_id = bucket.strip_prefix("projects/_/buckets/").ok_or_else(|| {
            Error::binding(format!(
                "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
            ))
        })?;
        let object = &self.resource.name;
        let builder = self
            .inner
            .client
            .request(
                reqwest::Method::POST,
                format!("{}/upload/storage/v1/b/{bucket_id}/o", &self.inner.endpoint),
            )
            .query(&[("uploadType", "multipart")])
            .query(&[("name", enc(object))])
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        let builder = apply_customer_supplied_encryption_headers(builder, self.params);
        let builder = self.inner.apply_auth_headers(builder).await?;

        let stream = Box::pin(unfold(Some(self.payload), move |state| async move {
            if let Some(mut payload) = state {
                if let Some(next) = payload.next().await {
                    return Some((next, Some(payload)));
                }
            }
            None
        }));
        let metadata = reqwest::multipart::Part::text(v1::insert_body(&self.resource).to_string())
            .mime_str("application/json; charset=UTF-8")
            .map_err(Error::ser)?;
        let media = reqwest::multipart::Part::stream(reqwest::Body::wrap_stream(stream));
        let form = reqwest::multipart::Form::new()
            .part("metadata", metadata)
            .part("media", media);
        let builder = builder.header(
            "content-type",
            format!("multipart/related; boundary={}", form.boundary()),
        );
        Ok(builder.body(reqwest::Body::wrap_stream(form.into_stream())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::tests::create_key_helper;
    use crate::client::tests::test_inner_client;
    use crate::upload_source::tests::VecStream;
    use http_body_util::BodyExt;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use serde_json::{Value, json};

    type Result = anyhow::Result<()>;

    #[tokio::test]
    async fn send_unbuffered_normal() -> Result {
        let payload = serde_json::json!({
            "name": "test-object",
            "bucket": "test-bucket",
            "metadata": {
                "is-test-object": "true",
            }
        })
        .to_string();
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
            .respond_with(
                status_code(200)
                    .append_header("content-type", "application/json")
                    .body(payload),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let response = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", "")
            .send_unbuffered()
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
    async fn send_unbuffered_not_found() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
            .respond_with(status_code(404).body("NOT FOUND")),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let err = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", "")
            .send_unbuffered()
            .await
            .expect_err("expected a not found error");
        assert_eq!(err.http_status_code(), Some(404));

        Ok(())
    }

    async fn parse_multipart_body(
        mut request: reqwest::Request,
    ) -> anyhow::Result<(bytes::Bytes, bytes::Bytes)> {
        let boundary = request
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("multipart/related; boundary="))
            .expect("request should include content-type")
            .to_string();
        let stream = request
            .body_mut()
            .take()
            .expect("request should have a body")
            .into_data_stream();
        let mut multipart = multer::Multipart::new(stream, boundary);
        let Some(m) = multipart.next_field().await? else {
            return Err(anyhow::Error::msg("missing metadata field"));
        };
        let metadata = m.bytes().await?;

        let Some(p) = multipart.next_field().await? else {
            return Err(anyhow::Error::msg("missing payload field"));
        };
        let payload = p.bytes().await?;

        assert!(
            multipart.next_field().await?.is_none(),
            "unexpected extra fields"
        );

        Ok((metadata, payload))
    }

    #[tokio::test]
    async fn upload_object_bytes() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .single_shot_builder()
            .await?
            .build()?;

        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=multipart&name=object"
        );
        let (_metadata, contents) = parse_multipart_body(request).await?;
        assert_eq!(contents, "hello");
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_metadata() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .with_metadata([("k0", "v0"), ("k1", "v1")])
            .single_shot_builder()
            .await?
            .build()?;

        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=multipart&name=object"
        );
        let (metadata, contents) = parse_multipart_body(request).await?;
        assert_eq!(contents, "hello");
        let object = serde_json::from_slice::<Value>(&metadata)?;
        assert_eq!(object, json!({"metadata": {"k0": "v0", "k1": "v1"}}));
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_stream() -> Result {
        let stream = VecStream::new(
            [
                "the ", "quick ", "brown ", "fox ", "jumps ", "over ", "the ", "lazy ", "dog",
            ]
            .map(|x| bytes::Bytes::from_static(x.as_bytes()))
            .to_vec(),
        );
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = UploadObject::new(inner, "projects/_/buckets/bucket", "object", stream)
            .single_shot_builder()
            .await?
            .build()?;

        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=multipart&name=object"
        );
        let (_metadata, contents) = parse_multipart_body(request).await?;
        assert_eq!(contents, "the quick brown fox jumps over the lazy dog");
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_error_credentials() -> Result {
        let config = gaxi::options::ClientConfig {
            cred: Some(auth::credentials::testing::error_credentials(false)),
            ..Default::default()
        };
        let inner = test_inner_client(config);
        let _ = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .single_shot_builder()
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_bad_bucket() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        UploadObject::new(inner, "malformed", "object", "hello")
            .single_shot_builder()
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_headers() -> Result {
        // Make a 32-byte key.
        let (key, key_base64, _, key_sha256_base64) = create_key_helper();

        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .with_key(KeyAes256::new(&key)?)
            .single_shot_builder()
            .await?
            .build()?;

        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=multipart&name=object"
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
}
