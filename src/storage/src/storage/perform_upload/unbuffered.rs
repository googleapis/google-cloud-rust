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
use gaxi::http::map_send_error;
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
        let response = builder.send().await.map_err(map_send_error)?;
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
        let response = builder.send().await.map_err(map_send_error)?;
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
mod single_shot_tests;
