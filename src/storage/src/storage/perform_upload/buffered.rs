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
    ChecksummedSource, ContinueOn308, Error, IterSource, Object, PerformUpload, Result,
    ResumableUploadStatus, SizeHint, StreamingSource, X_GOOG_API_CLIENT_HEADER,
    apply_customer_supplied_encryption_headers,
};
use crate::error::WriteError;
use crate::storage::checksum::details::{
    Checksum, update as checksum_update, validate as checksum_validate,
};
use progress::InProgressUpload;
use std::sync::Arc;
use tokio::sync::Mutex;

mod progress;

impl<S> PerformUpload<S>
where
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
        if hint.upper().is_none_or(|max| max >= threshold) {
            self.send_buffered_resumable(hint).await
        } else {
            self.send_buffered_single_shot().await
        }
    }

    async fn send_buffered_resumable(self, hint: SizeHint) -> Result<Object> {
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
            let response = builder.send().await.map_err(Self::send_err)?;
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
        let mut exact = 0_u64;
        while let Some(b) = stream.next().await.transpose().map_err(Error::ser)? {
            exact += b.len() as u64;
            collected.push(b);
        }
        let source = IterSource::new(collected);
        // Use the computed checksum, if any, and if the spec does not have a
        // checksum already.
        let computed = stream.final_checksum();
        let current = self
            .spec
            .resource
            .get_or_insert_default()
            .checksums
            .get_or_insert_default();
        checksum_update(current, computed);
        let upload = PerformUpload {
            payload: Arc::new(Mutex::new(ChecksummedSource::new(
                Checksum {
                    crc32c: None,
                    md5_hash: None,
                },
                source,
            ))),
            inner: self.inner,
            spec: self.spec,
            params: self.params,
            options: self.options,
        };
        upload
            .send_unbuffered_single_shot(SizeHint::with_exact(exact))
            .await
    }

    pub(crate) async fn validate_response_object(&self, object: Object) -> Result<Object> {
        let err = |mismatch, o: Object| {
            Err(Error::ser(WriteError::ChecksumMismatch {
                mismatch,
                object: o.into(),
            }))
        };
        if let Some(pre) = self
            .spec
            .resource
            .as_ref()
            .and_then(|r| r.checksums.as_ref())
        {
            if let Err(mismatch) = self::checksum_validate(pre, &object.checksums) {
                return err(mismatch, object);
            }
        }
        let computed = self.payload.lock().await.final_checksum();
        if let Err(mismatch) = self::checksum_validate(&computed, &object.checksums) {
            return err(mismatch, object);
        }
        Ok(object)
    }

    fn as_inner<E>(error: &reqwest::Error) -> Option<&E>
    where
        E: std::error::Error + 'static,
    {
        use std::error::Error as _;
        let mut e = error.source()?;
        // Prevent infinite loops due to cycles in the `source()` errors. This seems
        // unlikely, and it would require effort to create, but it is easy to
        // prevent.
        for _ in 0..32 {
            if let Some(value) = e.downcast_ref::<E>() {
                return Some(value);
            }
            e = e.source()?;
        }
        None
    }

    pub(crate) fn send_err(error: reqwest::Error) -> Error {
        if let Some(e) = Self::as_inner::<hyper::Error>(&error) {
            if e.is_user() {
                return Error::ser(error);
            }
        }
        Error::io(error)
    }
}

// Resumable uploads chunks (except for the last chunk) *must* be sized to a
// multiple of 256 KiB.
const RESUMABLE_UPLOAD_QUANTUM: usize = 256 * 1024;

#[cfg(test)]
mod resumable_tests;

#[cfg(test)]
mod single_shot_tests;

#[cfg(test)]
mod tests {
    use crate::builder::storage::WriteObject;
    use crate::storage::client::tests::{test_builder, test_inner_client};
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    #[test_case("projects/p")]
    #[test_case("kebab-case")]
    #[test_case("dot.name")]
    #[test_case("under_score")]
    #[test_case("tilde~123")]
    #[test_case("exclamation!point!")]
    #[test_case("spaces   spaces")]
    #[test_case("preserve%percent%21")]
    #[test_case("testall !#$&'()*+,/:;=?@[]")]
    #[test_case(concat!("Benjamín pidió una bebida de kiwi y fresa. ",
            "Noé, sin vergüenza, la más exquisita champaña del menú"))]
    #[tokio::test]
    async fn test_percent_encoding_object_name(want: &str) -> Result {
        let inner = test_inner_client(test_builder());
        let request = WriteObject::new(inner, "projects/_/buckets/bucket", want, "hello")
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
