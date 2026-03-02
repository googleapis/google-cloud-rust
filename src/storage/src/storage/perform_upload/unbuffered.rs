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
use gaxi::attempt_info::AttemptInfo;
use gaxi::http::HttpRequestBuilder;
use gaxi::http::reqwest::{Body, HeaderValue, Method, multipart};
use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt, ResourceName};
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
        let threshold = self.options.resumable_upload_threshold() as u64;
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
        let mut count = 0_u32;
        let inner = async move |_| {
            let previous = count;
            count += 1;
            self.resumable_attempt(&mut upload_url, hint.clone(), previous)
                .await
        };
        google_cloud_gax::retry_loop_internal::retry_loop(
            inner,
            async |duration| tokio::time::sleep(duration).await,
            true,
            throttler,
            retry,
            backoff,
        )
        .await
    }

    async fn resumable_attempt(
        &self,
        url: &mut Option<String>,
        hint: SizeHint,
        attempt_count: u32,
    ) -> Result<Object> {
        let (offset, upload_url) = if let Some(upload_url) = url.as_deref() {
            match self
                .query_resumable_upload_attempt(upload_url, attempt_count)
                .await?
            {
                ResumableUploadStatus::Finalized(object) => {
                    return Ok(*object);
                }
                ResumableUploadStatus::Partial(offset) => (offset, upload_url),
            }
        } else {
            let upload_url = self.start_resumable_upload_attempt(attempt_count).await?;
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
            .http_builder_with_url(Method::PUT, upload_url, crate::storage::DEFAULT_HOST)?
            .header("content-type", "application/octet-stream")
            .header("Content-Range", range)
            .header(
                "x-goog-api-client",
                HeaderValue::from_static(&X_GOOG_API_CLIENT_HEADER),
            );

        let builder = apply_customer_supplied_encryption_headers(builder, &self.params);

        self.payload
            .lock()
            .await
            .seek(offset)
            .await
            .map_err(Error::ser)?;
        let payload = self.payload_to_body().await?;
        let options = self
            .options
            .gax()
            .insert_extension(PathTemplate("/upload/storage/v1/b/{bucket}/o"))
            .insert_extension(ResourceName(format!(
                "//storage.googleapis.com/{}",
                self.resource().bucket
            )));
        let builder = builder.body(payload);
        // TODO(#4862) - maybe this should also use attempt_count ?
        let response = builder.send(options, AttemptInfo::new(0)).await?;
        let object = self::handle_object_response(response).await?;
        self.validate_response_object(object).await
    }

    pub(super) async fn send_unbuffered_single_shot(self, hint: SizeHint) -> Result<Object> {
        // Single shot uploads are idempotent only if they have pre-conditions.
        let idempotent = self.options.idempotency.unwrap_or(
            self.spec.if_generation_match.is_some() || self.spec.if_metageneration_match.is_some(),
        );
        let throttler = self.options.retry_throttler.clone();
        let retry = self.options.retry_policy.clone();
        let backoff = self.options.backoff_policy.clone();
        let mut count = 0;
        // TODO(#2044) - we need to apply any timeouts here.
        let inner = async move |_| {
            let previous = count;
            count += 1;
            self.single_shot_attempt(hint.clone(), previous).await
        };
        google_cloud_gax::retry_loop_internal::retry_loop(
            inner,
            async |duration| tokio::time::sleep(duration).await,
            idempotent,
            throttler,
            retry,
            backoff,
        )
        .await
    }

    async fn single_shot_attempt(&self, hint: SizeHint, attempt_count: u32) -> Result<Object> {
        let builder = self.single_shot_builder(hint).await?;
        let options = self
            .options
            .gax()
            .insert_extension(PathTemplate("/upload/storage/v1/b/{bucket}/o"))
            .insert_extension(ResourceName(format!(
                "//storage.googleapis.com/{}",
                self.resource().bucket
            )));
        let response = builder
            .send(options, AttemptInfo::new(attempt_count))
            .await?;
        let object = super::handle_object_response(response).await?;
        self.validate_response_object(object).await
    }

    async fn single_shot_builder(&self, hint: SizeHint) -> Result<HttpRequestBuilder> {
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
            .http_builder(Method::POST, &format!("/upload/storage/v1/b/{bucket_id}/o"))
            .query("uploadType", "multipart")
            .query("name", object)
            .header(
                "x-goog-api-client",
                HeaderValue::from_static(&X_GOOG_API_CLIENT_HEADER),
            );

        let builder = self.apply_preconditions(builder);
        let builder = apply_customer_supplied_encryption_headers(builder, &self.params);

        let metadata = multipart::Part::text(v1::insert_body(self.resource()).to_string())
            .mime_str("application/json; charset=UTF-8")
            .map_err(Error::ser)?;
        self.payload
            .lock()
            .await
            .seek(0)
            .await
            .map_err(Error::ser)?;
        let payload = self.payload_to_body().await?;
        let form = multipart::Form::new().part("metadata", metadata);
        let form = if let Some(exact) = hint.exact() {
            form.part("media", multipart::Part::stream_with_length(payload, exact))
        } else {
            form.part("media", multipart::Part::stream(payload))
        };

        let builder = builder.header(
            "content-type",
            format!("multipart/related; boundary={}", form.boundary()),
        );
        Ok(builder.body(Body::wrap_stream(form.into_stream())))
    }

    async fn payload_to_body(&self) -> Result<Body> {
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
        Ok(Body::wrap_stream(stream))
    }
}

#[cfg(test)]
mod resumable_tests;

#[cfg(test)]
mod single_shot_tests;
