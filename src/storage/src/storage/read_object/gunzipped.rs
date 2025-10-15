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

//! Support for gunzipped downloads.
//!
//! Cloud Storage automatically [decompresses gzip-compressed][transcoding]
//! objects. Reading such objects comes with a number of restrictions:
//! - Ranged reads do not work.
//! - Consequently, it is impossible to resume an interrupted read.
//! - The size of the decompressed data is not known.
//! - Checksums do not work because the object checksums correspond to the
//!   compressed data and the client library receives the decompressed data.
//!
//! Consequently, the implementation is substantially different.
//!
//! [transcoding]: https://cloud.google.com/storage/docs/transcoding

use super::parse_http_response;
use super::{Error, Result};
use crate::model_ext::ObjectHighlights;

#[derive(Debug)]
pub struct GunzippedResponse {
    response: Option<reqwest::Response>,
    highlights: ObjectHighlights,
}

impl GunzippedResponse {
    pub(crate) fn new(response: reqwest::Response) -> Result<Self> {
        let generation =
            parse_http_response::response_generation(&response).map_err(Error::deser)?;

        let headers = response.headers();
        let get_as_i64 = |header_name: &str| -> i64 {
            headers
                .get(header_name)
                .and_then(|s| s.to_str().ok())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or_default()
        };
        let get_as_string = |header_name: &str| -> String {
            headers
                .get(header_name)
                .and_then(|sc| sc.to_str().ok())
                .map(|sc| sc.to_string())
                .unwrap_or_default()
        };
        let highlights = ObjectHighlights {
            generation,
            metageneration: get_as_i64("x-goog-metageneration"),
            size: get_as_i64("x-goog-stored-content-length"),
            content_encoding: get_as_string("x-goog-stored-content-encoding"),
            storage_class: get_as_string("x-goog-storage-class"),
            content_type: get_as_string("content-type"),
            content_language: get_as_string("content-language"),
            content_disposition: get_as_string("content-disposition"),
            etag: get_as_string("etag"),
            checksums: None,
        };

        Ok(Self {
            response: Some(response),
            highlights,
        })
    }

    async fn next_attempt(&mut self) -> Option<Result<bytes::Bytes>> {
        let response = self.response.as_mut()?;
        response.chunk().await.map_err(Error::io).transpose()
    }
}

#[async_trait::async_trait]
impl crate::read_object::dynamic::ReadObjectResponse for GunzippedResponse {
    fn object(&self) -> ObjectHighlights {
        self.highlights.clone()
    }

    async fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        match self.next_attempt().await {
            None => None,
            Some(Ok(b)) => Some(Ok(b)),
            Some(Err(e)) => {
                self.response = None;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{client::Storage, model_ext::ObjectHighlights};
    use auth::credentials::anonymous::Builder as Anonymous;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};

    type Result = anyhow::Result<()>;

    #[tokio::test]
    async fn read_object_gunzipped_metadata() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(
                status_code(200)
                    .body("hello world")
                    .append_header("warning", "214 UploadServer gunzipped")
                    .append_header("x-goog-metageneration", 123456)
                    .append_header("x-goog-stored-content-length", 42)
                    .append_header("x-goog-generation", 234567)
                    .append_header("x-goog-stored-content-encoding", "gzip")
                    .append_header("x-goog-storage-class", "STANDARD")
                    .append_header("content-type", "text/plain")
                    .append_header("content-language", "EN")
                    .append_header("content-disposition", "attachment")
                    .append_header("etag", "etag-123"),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let mut reader = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let mut got = Vec::new();
        while let Some(b) = reader.next().await.transpose()? {
            got.extend_from_slice(&b);
        }
        assert_eq!(bytes::Bytes::from_owner(got), "hello world");

        let got = reader.object();
        let want = ObjectHighlights {
            metageneration: 123456,
            size: 42,
            generation: 234567,
            content_encoding: "gzip".to_string(),
            storage_class: "STANDARD".to_string(),
            content_type: "text/plain".to_string(),
            content_language: "EN".to_string(),
            content_disposition: "attachment".to_string(),
            etag: "etag-123".to_string(),
            checksums: None,
        };
        assert_eq!(got, want);

        Ok(())
    }
}
