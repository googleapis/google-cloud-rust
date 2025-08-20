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

use super::{
    ContinueOn308, Error, Object, PerformUpload, Result, ResumableUploadStatus, Seek, SizeHint,
    StreamingSource, X_GOOG_API_CLIENT_HEADER, apply_customer_supplied_encryption_headers,
    handle_object_response, v1,
};
use futures::stream::unfold;
use std::sync::Arc;

impl<S> PerformUpload<S>
where
    S: StreamingSource + Seek + Send + Sync + 'static,
    <S as StreamingSource>::Error: std::error::Error + Send + Sync + 'static,
    <S as Seek>::Error: std::error::Error + Send + Sync + 'static,
{
    pub(crate) async fn send_unbuffered(self) -> Result<Object> {
        let hint = self
            .payload
            .lock()
            .await
            .size_hint()
            .await
            .map_err(Error::deser)?;
        let threshold = self.options.resumable_upload_threshold as u64;
        if hint.upper().is_none_or(|max| max >= threshold) {
            self.send_unbuffered_resumable(hint).await
        } else {
            self.send_unbuffered_single_shot(hint).await
        }
    }

    async fn send_unbuffered_resumable(self, hint: SizeHint) -> Result<Object> {
        let mut upload_url = None;
        let throttler = self.options.retry_throttler.clone();
        let retry = Arc::new(ContinueOn308::new(self.options.retry_policy.clone()));
        let backoff = self.options.backoff_policy.clone();
        gax::retry_loop_internal::retry_loop(
            async move |_| self.resumable_attempt(&mut upload_url, hint.clone()).await,
            async |duration| tokio::time::sleep(duration).await,
            true,
            throttler,
            retry,
            backoff,
        )
        .await
    }

    async fn resumable_attempt(&self, url: &mut Option<String>, hint: SizeHint) -> Result<Object> {
        let (offset, url_ref) = if let Some(upload_url) = url.as_deref() {
            match self.query_resumable_upload_attempt(upload_url).await? {
                ResumableUploadStatus::Finalized(object) => {
                    return Ok(*object);
                }
                ResumableUploadStatus::Partial(offset) => (offset, upload_url),
            }
        } else {
            let upload_url = self.start_resumable_upload_attempt().await?;
            (0_u64, url.insert(upload_url).as_str())
        };

        let range = match (offset, hint.exact()) {
            (o, None) => format!("bytes {o}-*/*"),
            (_, Some(0)) => "bytes */0".to_string(),
            (o, Some(u)) => format!("bytes {o}-{}/{u}", u - 1),
        };
        let builder = self
            .inner
            .client
            .request(reqwest::Method::PUT, url_ref)
            .header("content-type", "application/octet-stream")
            .header("Content-Range", range)
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&X_GOOG_API_CLIENT_HEADER),
            );

        let builder = apply_customer_supplied_encryption_headers(builder, &self.params);
        let builder = self.inner.apply_auth_headers(builder).await?;

        self.payload
            .lock()
            .await
            .seek(offset)
            .await
            .map_err(Error::ser)?;
        let payload = self.payload_to_body().await?;
        let builder = builder.body(payload);
        let response = builder.send().await.map_err(Self::send_err)?;
        let object = self::handle_object_response(response).await?;
        self.validate_response_object(object).await
    }

    pub(super) async fn send_unbuffered_single_shot(self, hint: SizeHint) -> Result<Object> {
        // Single shot uploads are idempotent only if they have pre-conditions.
        let idempotent = self.options.idempotency.unwrap_or(
            self.spec.if_generation_match.is_some() || self.spec.if_metageneration_match.is_some(),
        );
        let throttler = self.options.retry_throttler.clone();
        let retry = Arc::new(ContinueOn308::new(self.options.retry_policy.clone()));
        let backoff = self.options.backoff_policy.clone();
        gax::retry_loop_internal::retry_loop(
            // TODO(#2044) - we need to apply any timeouts here.
            async move |_| self.single_shot_attempt(hint.clone()).await,
            async |duration| tokio::time::sleep(duration).await,
            idempotent,
            throttler,
            retry,
            backoff,
        )
        .await
    }

    async fn single_shot_attempt(&self, hint: SizeHint) -> Result<Object> {
        let builder = self.single_shot_builder(hint).await?;
        let response = builder.send().await.map_err(Self::send_err)?;
        let object = super::handle_object_response(response).await?;
        self.validate_response_object(object).await
    }

    async fn single_shot_builder(&self, hint: SizeHint) -> Result<reqwest::RequestBuilder> {
        let bucket = &self.resource().bucket;
        let bucket_id = bucket.strip_prefix("projects/_/buckets/").ok_or_else(|| {
            Error::binding(format!(
                "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
            ))
        })?;
        let object = &self.resource().name;
        let builder = self
            .inner
            .client
            .request(
                reqwest::Method::POST,
                format!("{}/upload/storage/v1/b/{bucket_id}/o", &self.inner.endpoint),
            )
            .query(&[("uploadType", "multipart")])
            .query(&[("name", object)])
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&X_GOOG_API_CLIENT_HEADER),
            );

        let builder = self.apply_preconditions(builder);
        let builder = apply_customer_supplied_encryption_headers(builder, &self.params);
        let builder = self.inner.apply_auth_headers(builder).await?;

        let metadata = reqwest::multipart::Part::text(v1::insert_body(self.resource()).to_string())
            .mime_str("application/json; charset=UTF-8")
            .map_err(Error::ser)?;
        self.payload
            .lock()
            .await
            .seek(0)
            .await
            .map_err(Error::ser)?;
        let payload = self.payload_to_body().await?;
        let form = reqwest::multipart::Form::new().part("metadata", metadata);
        let form = if let Some(exact) = hint.exact() {
            form.part(
                "media",
                reqwest::multipart::Part::stream_with_length(payload, exact),
            )
        } else {
            form.part("media", reqwest::multipart::Part::stream(payload))
        };

        let builder = builder.header(
            "content-type",
            format!("multipart/related; boundary={}", form.boundary()),
        );
        Ok(builder.body(reqwest::Body::wrap_stream(form.into_stream())))
    }

    async fn payload_to_body(&self) -> Result<reqwest::Body> {
        let payload = self.payload.clone();
        let stream = Box::pin(unfold(Some(payload), move |state| async move {
            if let Some(payload) = state {
                let mut guard = payload.lock().await;
                if let Some(next) = guard.next().await {
                    drop(guard);
                    return Some((next, Some(payload)));
                }
            }
            None
        }));
        Ok(reqwest::Body::wrap_stream(stream))
    }
}

#[cfg(test)]
mod resumable_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::storage::WriteObject;
    use crate::model_ext::{KeyAes256, tests::create_key_helper};
    use crate::storage::client::{
        Storage,
        tests::{test_builder, test_inner_client},
    };
    use crate::streaming_source::IterSource;
    use gax::retry_policy::RetryPolicyExt;
    use http_body_util::BodyExt;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::{Value, json};

    type Result = anyhow::Result<()>;

    #[tokio::test]
    async fn send_unbuffered_single_shot() -> Result {
        let payload = response_body().to_string();
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
            .times(1)
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
            .write_object("projects/_/buckets/test-bucket", "test-object", "")
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
    async fn single_shot_http_error() -> Result {
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
            .write_object("projects/_/buckets/test-bucket", "test-object", "")
            .send_unbuffered()
            .await
            .expect_err("expected a not found error");
        assert_eq!(err.http_status_code(), Some(404));

        Ok(())
    }

    #[tokio::test]
    async fn single_shot_deserialization() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
            .respond_with(status_code(200).body("bad format")),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let err = client
            .write_object("projects/_/buckets/test-bucket", "test-object", "")
            .send_unbuffered()
            .await
            .expect_err("expected a deserialization error");
        assert!(err.is_deserialization(), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn single_shot_source_error() -> Result {
        let server = Server::run();

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        use crate::streaming_source::tests::MockSeekSource;
        use std::io::{Error as IoError, ErrorKind};
        let mut source = MockSeekSource::new();
        source
            .expect_next()
            .once()
            .returning(|| Some(Err(IoError::new(ErrorKind::ConnectionAborted, "test-only"))));
        source.expect_seek().times(1..).returning(|_| Ok(()));
        source
            .expect_size_hint()
            .once()
            .returning(|| Ok(SizeHint::with_exact(1024)));
        let err = client
            .write_object("projects/_/buckets/test-bucket", "test-object", source)
            .send_unbuffered()
            .await
            .expect_err("expected a serialization error");
        assert!(err.is_serialization(), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn single_shot_seek_error() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
            .times(0)
            .respond_with(status_code(200).body("bad format")),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        use crate::streaming_source::tests::MockSeekSource;
        use std::io::{Error as IoError, ErrorKind};
        let mut source = MockSeekSource::new();
        source.expect_next().never();
        source
            .expect_seek()
            .once()
            .returning(|_| Err(IoError::new(ErrorKind::ConnectionAborted, "test-only")));
        source
            .expect_size_hint()
            .once()
            .returning(|| Ok(SizeHint::with_exact(1024_u64)));
        let err = client
            .write_object("projects/_/buckets/test-bucket", "test-object", source)
            .send_unbuffered()
            .await
            .expect_err("expected a serialization error");
        assert!(err.is_serialization(), "{err:?}");

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

    fn response_body() -> Value {
        json!({
            "name": "test-object",
            "bucket": "test-bucket",
            "metadata": {
                "is-test-object": "true",
            }
        })
    }

    #[tokio::test]
    async fn upload_object_bytes() -> Result {
        const PAYLOAD: &str = "hello";
        let inner = test_inner_client(test_builder());
        let request = WriteObject::new(inner, "projects/_/buckets/bucket", "object", PAYLOAD)
            .build()
            .single_shot_builder(SizeHint::with_exact(PAYLOAD.len() as u64))
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
        let inner = test_inner_client(test_builder());
        let request = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .set_metadata([("k0", "v0"), ("k1", "v1")])
            .build()
            .single_shot_builder(SizeHint::new())
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
        let stream = IterSource::new(
            [
                "the ", "quick ", "brown ", "fox ", "jumps ", "over ", "the ", "lazy ", "dog",
            ]
            .map(|x| bytes::Bytes::from_static(x.as_bytes())),
        );
        let inner = test_inner_client(test_builder());
        let request = WriteObject::new(inner, "projects/_/buckets/bucket", "object", stream)
            .build()
            .single_shot_builder(SizeHint::new())
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
        let inner = test_inner_client(
            test_builder().with_credentials(auth::credentials::testing::error_credentials(false)),
        );
        let _ = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .build()
            .single_shot_builder(SizeHint::new())
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_bad_bucket() -> Result {
        let inner = test_inner_client(test_builder());
        WriteObject::new(inner, "malformed", "object", "hello")
            .build()
            .single_shot_builder(SizeHint::new())
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_headers() -> Result {
        // Make a 32-byte key.
        let (key, key_base64, _, key_sha256_base64) = create_key_helper();

        let inner = test_inner_client(test_builder());
        let request = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .set_key(KeyAes256::new(&key)?)
            .build()
            .single_shot_builder(SizeHint::new())
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

    #[tokio::test]
    async fn single_shot_retry_transient_not_idempotent() -> Result {
        let server = Server::run();
        let matching = || {
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
                request::query(url_decoded(contains(("name", "object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
        };
        server.expect(
            matching()
                .times(1)
                .respond_with(cycle![status_code(503).body("try-again"),]),
        );

        let inner =
            test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
        let err = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .send_unbuffered()
            .await
            .expect_err("expected error as request is not idempotent");
        assert_eq!(err.http_status_code(), Some(503), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn single_shot_retry_transient_override_idempotency() -> Result {
        let server = Server::run();
        let matching = || {
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
                request::query(url_decoded(contains(("name", "object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
        };
        server.expect(matching().times(3).respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(429).body("try-again"),
            json_encoded(response_body()).append_header("content-type", "application/json"),
        ]));

        let inner =
            test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
        let got = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .with_idempotency(true)
            .send_unbuffered()
            .await?;
        let want = Object::from(serde_json::from_value::<v1::Object>(response_body())?);
        assert_eq!(got, want);

        Ok(())
    }

    #[tokio::test]
    async fn single_shot_retry_transient_failures_then_success() -> Result {
        let server = Server::run();
        let matching = || {
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
        };
        server.expect(matching().times(3).respond_with(cycle![
            status_code(503).body("try-again"),
            status_code(503).body("try-again"),
            json_encoded(response_body()).append_header("content-type", "application/json"),
        ]));

        let inner =
            test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
        let got = WriteObject::new(
            inner,
            "projects/_/buckets/test-bucket",
            "test-object",
            "hello",
        )
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
        let want = Object::from(serde_json::from_value::<v1::Object>(response_body())?);
        assert_eq!(got, want);

        Ok(())
    }

    #[tokio::test]
    async fn single_shot_retry_transient_failures_then_permanent() -> Result {
        let server = Server::run();
        let matching = || {
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
        };
        server.expect(matching().times(3).respond_with(cycle![
            status_code(503).body("try-again"),
            status_code(503).body("try-again"),
            status_code(403).body("uh-oh"),
        ]));

        let inner =
            test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
        let err = WriteObject::new(
            inner,
            "projects/_/buckets/test-bucket",
            "test-object",
            "hello",
        )
        .set_if_generation_match(0)
        .send_unbuffered()
        .await
        .expect_err("expected permanent error");
        assert_eq!(err.http_status_code(), Some(403), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn single_shot_retry_transient_failures_exhausted() -> Result {
        let server = Server::run();
        let matching = || {
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
        };
        server.expect(
            matching()
                .times(3)
                .respond_with(status_code(503).body("try-again")),
        );

        let inner =
            test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
        let err = WriteObject::new(
            inner,
            "projects/_/buckets/test-bucket",
            "test-object",
            "hello",
        )
        .set_if_generation_match(0)
        .with_retry_policy(crate::retry_policy::RetryableErrors.with_attempt_limit(3))
        .send_unbuffered()
        .await
        .expect_err("expected permanent error");
        assert_eq!(err.http_status_code(), Some(503), "{err:?}");

        Ok(())
    }
}
