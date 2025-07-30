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
    ChecksumEngine, ChecksummedSource, ContinueOn308, Error, IterSource, Object, PerformUpload,
    Precomputed, Result, ResumableUploadStatus, StreamingSource, X_GOOG_API_CLIENT_HEADER,
    apply_customer_supplied_encryption_headers,
};
use crate::storage::checksum::validate as validate_checksum;
use progress::InProgressUpload;
use std::sync::Arc;
use tokio::sync::Mutex;

mod progress;

impl<C, S> PerformUpload<C, S>
where
    C: ChecksumEngine + Send + Sync + 'static,
    S: StreamingSource + Send + Sync + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    pub(crate) async fn send(self) -> crate::Result<Object> {
        let hint = self
            .payload
            .lock()
            .await
            .size_hint()
            .await
            .map_err(Error::ser)?;
        let threshold = self.options.resumable_upload_threshold as u64;
        if hint.1.is_none_or(|max| max >= threshold) {
            self.send_buffered_resumable(hint).await
        } else {
            self.send_buffered_single_shot().await
        }
    }

    async fn send_buffered_resumable(self, hint: (u64, Option<u64>)) -> Result<Object> {
        let mut progress = InProgressUpload::new(self.options.resumable_upload_buffer_size, hint);
        let mut url = None;
        let throttler = self.options.retry_throttler.clone();
        let retry = Arc::new(ContinueOn308::new(self.options.retry_policy.clone()));
        let backoff = self.options.backoff_policy.clone();
        gax::retry_loop_internal::retry_loop(
            async move |_| {
                self.buffered_resumable_attempt(&mut progress, &mut url)
                    .await
            },
            async |duration| tokio::time::sleep(duration).await,
            true,
            throttler,
            retry,
            backoff,
        )
        .await
    }

    // Use separate arguments for `progress` and `url` so we can borrow them
    // separately.
    async fn buffered_resumable_attempt(
        &self,
        progress: &mut InProgressUpload,
        url: &mut Option<String>,
    ) -> Result<Object> {
        let upload_url = if let Some(u) = url.as_deref() {
            u
        } else {
            let u = self.start_resumable_upload_attempt().await?;
            url.insert(u).as_str()
        };

        if progress.needs_query() {
            match self.query_resumable_upload_attempt(upload_url).await? {
                ResumableUploadStatus::Finalized(object) => return Ok(*object),
                ResumableUploadStatus::Partial(persisted_size) => {
                    progress.handle_partial(persisted_size)?;
                }
            };
        }

        loop {
            progress
                .next_buffer(&mut *self.payload.lock().await)
                .await?;
            let builder = self.partial_upload_request(upload_url, progress).await?;
            let response = builder.send().await.map_err(super::send_err)?;
            match super::query_resumable_upload_handle_response(response).await {
                Err(e) => {
                    progress.handle_error();
                    return Err(e);
                }
                Ok(ResumableUploadStatus::Finalized(object)) => {
                    return self.validate_response_object(*object).await;
                }
                Ok(ResumableUploadStatus::Partial(persisted_size)) => {
                    progress.handle_partial(persisted_size)?;
                }
            };
        }
    }

    async fn partial_upload_request(
        &self,
        upload_url: &str,
        progress: &mut InProgressUpload,
    ) -> Result<reqwest::RequestBuilder> {
        let range = progress.range_header();
        let builder = self
            .inner
            .client
            .request(reqwest::Method::PUT, upload_url)
            .header("content-type", "application/octet-stream")
            .header("Content-Range", range)
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&X_GOOG_API_CLIENT_HEADER),
            );

        let builder = apply_customer_supplied_encryption_headers(builder, &self.params);
        let builder = self.inner.apply_auth_headers(builder).await?;
        Ok(builder.body(progress.put_body()))
    }

    async fn send_buffered_single_shot(mut self) -> Result<Object> {
        let mut stream = self.payload.lock().await;
        let mut collected = Vec::new();
        while let Some(b) = stream.next().await.transpose().map_err(Error::ser)? {
            collected.push(b);
        }
        let source = IterSource::new(collected);
        // Use the computed checksum, if any, and if the spec does not have a
        // checksum already.
        let computed = stream.final_checksum();
        let has = self
            .spec
            .resource
            .get_or_insert_default()
            .checksums
            .get_or_insert_default();
        if has.crc32c.is_none() {
            has.crc32c = computed.crc32c;
        }
        if has.md5_hash.is_empty() {
            has.md5_hash = computed.md5_hash;
        }
        let upload = PerformUpload {
            payload: Arc::new(Mutex::new(ChecksummedSource::new(Precomputed, source))),
            inner: self.inner,
            spec: self.spec,
            params: self.params,
            options: self.options,
        };
        upload.send_unbuffered_single_shot().await
    }

    pub(crate) async fn validate_response_object(&self, object: Object) -> Result<Object> {
        if let Some(pre) = self
            .spec
            .resource
            .as_ref()
            .and_then(|r| r.checksums.as_ref())
        {
            self::validate_checksum(pre, &object.checksums).map_err(Error::ser)?;
        }
        let computed = self.payload.lock().await.final_checksum();
        self::validate_checksum(&computed, &object.checksums).map_err(Error::ser)?;
        Ok(object)
    }
}

// Resumable uploads chunks (except for the last chunk) *must* be sized to a
// multiple of 256 KiB.
const RESUMABLE_UPLOAD_QUANTUM: usize = 256 * 1024;

#[cfg(test)]
mod resumable_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::client::{
        Storage,
        tests::{test_builder, test_inner_client},
    };
    use crate::storage::upload_object::UploadObject;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    // We rely on the tests from `unbuffered.rs` for coverage of other
    // single-shot upload features. Here we just want to verify the right upload
    // type is selected depending on the with_resumable_upload_threshold()
    // option.
    #[tokio::test]
    async fn upload_object_buffered_single_shot() -> Result {
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
            .with_resumable_upload_threshold(4 * RESUMABLE_UPLOAD_QUANTUM)
            .build()
            .await?;
        let response = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", "")
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
    async fn single_shot_source_error() -> Result {
        let server = Server::run();

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        use crate::upload_source::tests::MockSimpleSource;
        use std::io::{Error as IoError, ErrorKind};
        let mut source = MockSimpleSource::new();
        source
            .expect_next()
            .once()
            .returning(|| Some(Err(IoError::new(ErrorKind::ConnectionAborted, "test-only"))));
        source
            .expect_size_hint()
            .once()
            .returning(|| Ok((1024_u64, Some(1024_u64))));
        let err = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", source)
            .send()
            .await
            .expect_err("expected a serialization error");
        assert!(err.is_serialization(), "{err:?}");

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
        let inner = test_inner_client(test_builder());
        let request = UploadObject::new(inner, "projects/_/buckets/bucket", name, "hello")
            .build()
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
}
