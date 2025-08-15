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

use super::client::{StorageInner, apply_customer_supplied_encryption_headers};
use crate::model::Object;
use crate::retry_policy::ContinueOn308;
use crate::storage::checksum::details::ChecksumEnum;
use crate::storage::checksum::details::{ChecksummedSource, Known};
use crate::storage::client::info::X_GOOG_API_CLIENT_HEADER;
use crate::storage::v1;
use crate::streaming_source::{IterSource, Seek, SizeHint, StreamingSource};
use crate::{Error, Result};
use std::sync::Arc;
use tokio::sync::Mutex;

mod buffered;
mod unbuffered;

/// Represents an upload constructed via `UploadObject<T>`.
///
/// Once the application has fully configured an `UploadObject<T>` it calls
/// `send()` or `send_buffered()` to initiate the upload. At that point the
/// client library creates an instance of this class. Notably, the `payload`
/// becomes `Arc<Mutex<T>>` because it needs to be reused in the retry loop.
pub struct PerformUpload<S> {
    // We need `Arc<Mutex<>>` because this is re-used in retryable uploads.
    payload: Arc<Mutex<ChecksummedSource<S>>>,
    inner: Arc<StorageInner>,
    spec: crate::model::WriteObjectSpec,
    params: Option<crate::model::CommonObjectRequestParams>,
    options: super::request_options::RequestOptions,
}

impl<S> PerformUpload<S> {
    pub(crate) fn new(
        checksum: ChecksumEnum,
        payload: S,
        inner: Arc<StorageInner>,
        spec: crate::model::WriteObjectSpec,
        params: Option<crate::model::CommonObjectRequestParams>,
        options: super::request_options::RequestOptions,
    ) -> Self {
        Self {
            payload: Arc::new(Mutex::new(ChecksummedSource::new(checksum, payload))),
            inner,
            spec,
            params,
            options,
        }
    }

    fn resource(&self) -> &crate::model::Object {
        self.spec
            .resource
            .as_ref()
            .expect("resource field initialized in `new()`")
    }

    async fn start_resumable_upload_attempt(&self) -> Result<String> {
        let builder = self.start_resumable_upload_request().await?;
        let response = builder.send().await.map_err(Error::io)?;
        self::handle_start_resumable_upload_response(response).await
    }

    async fn start_resumable_upload_request(&self) -> Result<reqwest::RequestBuilder> {
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
            .query(&[("uploadType", "resumable")])
            .query(&[("name", object)])
            .header("content-type", "application/json")
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&X_GOOG_API_CLIENT_HEADER),
            );

        let builder = self.apply_preconditions(builder);
        let builder = apply_customer_supplied_encryption_headers(builder, &self.params);
        let builder = self.inner.apply_auth_headers(builder).await?;
        let builder = builder.json(&v1::insert_body(self.resource()));
        Ok(builder)
    }

    async fn query_resumable_upload_attempt(
        &self,
        upload_url: &str,
    ) -> Result<ResumableUploadStatus> {
        let builder = self
            .inner
            .client
            .request(reqwest::Method::PUT, upload_url)
            .header("content-type", "application/octet-stream")
            .header("Content-Range", "bytes */*")
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&X_GOOG_API_CLIENT_HEADER),
            );
        let builder = self.inner.apply_auth_headers(builder).await?;
        let response = builder.send().await.map_err(Error::io)?;
        self::query_resumable_upload_handle_response(response).await
    }

    fn apply_preconditions(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let builder = self
            .spec
            .if_generation_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifGenerationMatch", v)]));
        let builder = self
            .spec
            .if_generation_not_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifGenerationNotMatch", v)]));
        let builder = self
            .spec
            .if_metageneration_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifMetagenerationMatch", v)]));
        let builder = self
            .spec
            .if_metageneration_not_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifMetagenerationNotMatch", v)]));

        [
            ("kmsKeyName", self.resource().kms_key.as_str()),
            ("predefinedAcl", self.spec.predefined_acl.as_str()),
        ]
        .into_iter()
        .fold(
            builder,
            |b, (k, v)| if v.is_empty() { b } else { b.query(&[(k, v)]) },
        )
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

async fn query_resumable_upload_handle_response(
    response: reqwest::Response,
) -> Result<ResumableUploadStatus> {
    if response.status() == RESUME_INCOMPLETE {
        return self::parse_range(response).await;
    }
    let object = handle_object_response(response).await?;
    Ok(ResumableUploadStatus::Finalized(Box::new(object)))
}

async fn handle_object_response(response: reqwest::Response) -> Result<Object> {
    if !response.status().is_success() {
        return gaxi::http::to_http_error(response).await;
    }
    let response = response.json::<v1::Object>().await.map_err(Error::deser)?;
    Ok(Object::from(response))
}

async fn parse_range(response: reqwest::Response) -> Result<ResumableUploadStatus> {
    let Some(end) = self::parse_range_end(response.headers()) else {
        return gaxi::http::to_http_error(response).await;
    };
    // The `Range` header returns an inclusive range, i.e. bytes=0-999 means "1000 bytes".
    let persisted_size = match end {
        0 => 0,
        e => e + 1,
    };
    Ok(ResumableUploadStatus::Partial(persisted_size))
}

#[derive(Debug, PartialEq)]
enum ResumableUploadStatus {
    Finalized(Box<Object>),
    Partial(u64),
}

fn parse_range_end(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let Some(range) = headers.get("range") else {
        // A missing `Range:` header indicates that no bytes are persisted.
        return Some(0_u64);
    };
    // Uploads must be sequential, so the persisted range (if present) always
    // starts at zero. This is poorly documented, but can be inferred from
    //   https://cloud.google.com/storage/docs/performing-resumable-uploads#resume-upload
    // which requires uploads to continue from the last byte persisted. It is
    // better documented in the gRPC version, where holes are explicitly
    // forbidden:
    //   https://github.com/googleapis/googleapis/blob/302273adb3293bb504ecd83be8e1467511d5c779/google/storage/v2/storage.proto#L1253-L1255
    let end = std::str::from_utf8(range.as_bytes().strip_prefix(b"bytes=0-")?).ok()?;
    end.parse::<u64>().ok()
}

const RESUME_INCOMPLETE: reqwest::StatusCode = reqwest::StatusCode::PERMANENT_REDIRECT;

#[cfg(test)]
mod tests;
