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

//! Support for non-resumable (e.g. gunzipped) downloads.

use super::parse_http_response;
use super::{Error, Result};
use crate::model_ext::ObjectHighlights;
use gaxi::http::reqwest::Response;

#[derive(Debug)]
pub struct NonResumableResponse {
    response: Option<Response>,
    highlights: ObjectHighlights,
}

impl NonResumableResponse {
    pub(crate) fn new(response: Response) -> Result<Self> {
        let generation =
            parse_http_response::response_generation(&response).map_err(Error::deser)?;

        let headers = response.headers();
        let highlights = super::parse_http_response::object_highlights(generation, headers)?;

        Ok(Self {
            response: Some(response),
            highlights,
        })
    }

    async fn next_attempt(&mut self) -> Option<Result<bytes::Bytes>> {
        let response = self.response.as_mut()?;
        response
            .chunk()
            .await
            .map_err(gaxi::http::map_send_error)
            .transpose()
    }
}

#[async_trait::async_trait]
impl crate::read_object::dynamic::ReadObjectResponse for NonResumableResponse {
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
    use super::*;
    use crate::{
        client::Storage, model_ext::ObjectHighlights, read_object::dynamic::ReadObjectResponse,
    };
    use bytes::Bytes;
    use gaxi::http::reqwest::Body;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
            metadata: std::collections::HashMap::new(),
        };
        assert_eq!(got, want);

        Ok(())
    }

    #[tokio::test]
    async fn gunzipped_io_error() -> Result {
        let stream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"hello")),
            Err(anyhow::Error::msg("bad stuff")),
        ]);
        let body = Body::wrap_stream(stream);
        let response = http::Response::builder()
            .status(200)
            .header("x-goog-generation", 123456)
            .body(body)?;
        let mut response = NonResumableResponse::new(Response::from(response))?;

        let chunk = response.next().await;
        assert!(matches!(&chunk, Some(Ok(b)) if b == "hello"), "{chunk:?}");
        let chunk = response.next().await;
        assert!(matches!(&chunk, Some(Err(_))), "{chunk:?}");
        let chunk = response.next().await;
        assert!(&chunk.is_none(), "{chunk:?}");
        Ok(())
    }

    #[tokio::test]
    async fn gunzipped_read_timeout_preserves_timeout_kind() -> Result {
        // Arrange
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept succeeds");
            let mut req = Vec::new();
            let mut buf = [0_u8; 1024];
            while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.expect("read succeeds");
                assert!(n > 0, "unexpected EOF reading request");
                req.extend_from_slice(&buf[..n]);
            }
            let head = concat!(
                "HTTP/1.1 200 OK\r\n",
                "warning: 214 UploadServer gunzipped\r\n",
                "x-goog-generation: 123456\r\n",
                "content-length: 10\r\n",
                "\r\n",
                "hello",
            );
            socket
                .write_all(head.as_bytes())
                .await
                .expect("write succeeds");
            let _ = done_rx.await;
        });

        let client = Storage::builder()
            .with_endpoint(format!("http://{addr}"))
            .with_credentials(Anonymous::new().build())
            .with_retry_policy(crate::retry_policy::RetryableErrors.with_attempt_limit(3))
            .with_read_timeout(Duration::from_millis(100))
            .build()
            .await?;

        // Act
        let mut reader = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let first = reader.next().await.transpose()?;
        let second = reader.next().await;
        let after_err = reader.next().await;

        // Assert
        assert_eq!(first, Some(Bytes::from_static(b"hello")));
        let err = second
            .expect("stream should yield timeout error")
            .expect_err("second chunk should time out");
        assert!(err.is_timeout(), "{err:?}");
        assert!(after_err.is_none(), "{after_err:?}");

        let _ = done_tx.send(());
        server.await?;
        Ok(())
    }
}
