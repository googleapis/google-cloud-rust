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

#[allow(dead_code)]
mod progress;

use super::*;
use crate::retry_policy::ContinueOn308;
use thiserror::Error;

impl<T> UploadObject<T>
where
    T: StreamingSource + Send + Sync + 'static,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    /// Upload an object from a streaming source without rewinds.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub async fn send(self) -> crate::Result<Object> {
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
        let mut progress = InProgressUpload::new(&self.options, hint);
        gax::retry_loop_internal::retry_loop(
            async |_| self.buffered_resumable_attempt(&mut progress).await,
            async |duration| tokio::time::sleep(duration).await,
            true,
            self.options.retry_throttler.clone(),
            Arc::new(ContinueOn308::new(self.options.retry_policy.clone())),
            self.options.backoff_policy.clone(),
        )
        .await
    }

    async fn buffered_resumable_attempt(&self, progress: &mut InProgressUpload) -> Result<Object> {
        // Cannot borrow `progress.url` because we plan to borrow `progress` as
        // mutable below.
        let upload_url = if let Some(url) = progress.url.as_ref() {
            url.clone()
        } else {
            let url = self.start_resumable_upload_attempt().await?;
            progress.url = Some(url.clone());
            progress.persisted_size = Some(0_u64);
            url
        };

        if Some(progress.offset) != progress.persisted_size {
            match self.query_resumable_upload_attempt(&upload_url).await? {
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
            let builder = self.partial_upload_request(&upload_url, progress).await?;
            let response = builder.send().await.map_err(Error::io)?;
            match super::query_resumable_upload_handle_response(response).await {
                Err(e) => {
                    progress.handle_error();
                    return Err(e);
                }
                Ok(ResumableUploadStatus::Finalized(object)) => return Ok(*object),
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
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        let builder = apply_customer_supplied_encryption_headers(builder, &self.params);
        let builder = self.inner.apply_auth_headers(builder).await?;
        let stream = unfold(Some(progress.buffer.clone()), move |state| async move {
            if let Some(mut payload) = state {
                if let Some(next) = payload.pop_front() {
                    return Some((Ok::<bytes::Bytes, Error>(next), Some(payload)));
                }
            }
            None
        });
        Ok(builder.body(reqwest::Body::wrap_stream(stream)))
    }

    async fn send_buffered_single_shot(self) -> Result<Object> {
        let mut stream = self.payload.lock().await;
        let mut collected = Vec::new();
        while let Some(b) = stream.next().await.transpose().map_err(Error::io)? {
            collected.push(b);
        }
        let upload = UploadObject {
            payload: Arc::new(Mutex::new(InsertPayload::from(collected))),
            inner: self.inner,
            spec: self.spec,
            params: self.params,
            options: self.options,
        };
        upload.send_unbuffered_single_shot().await
    }
}

// Resumable uploads chunks (except for the last chunk) *must* be sized to a
// multiple of 256 KiB.
const RESUMABLE_UPLOAD_QUANTUM: usize = 256 * 1024;

#[derive(Clone, Default)]
struct InProgressUpload {
    /// The target size for each PUT request.
    ///
    /// The last PUT request may be smaller. This must be a multiple of 256KiB
    /// and greater than 0.
    target_size: usize,
    /// The expected size for the full object.
    hint: (u64, Option<u64>),
    /// The upload session URL.
    ///
    /// Starts as `None` and is initialized before the first `PUT` request.
    url: Option<String>,
    /// The offset for the current `PUT` request.
    offset: u64,
    /// The data for the current `PUT` request.
    buffer: VecDeque<bytes::Bytes>,
    /// The size of the current `PUT` request.
    buffer_size: usize,
    /// The persisted size, if known.
    persisted_size: Option<u64>,
    /// Keep the bytes retrieved from the payload stream, that did not fit in
    /// current PUT request.
    ///
    /// When getting data from the source stream we may retrieve more data.
    remainder: VecDeque<bytes::Bytes>,
}

struct Summary<'a>(&'a VecDeque<bytes::Bytes>);
impl<'a> std::fmt::Debug for Summary<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fmt = f.debug_struct("Summary");
        fmt.field("len", &self.0.len())
            .field(
                "total_size",
                &self.0.iter().fold(0_usize, |s, b| s + b.len()),
            )
            .field(
                "contents[0..32]",
                &self
                    .0
                    .front()
                    .map(|b| b.slice(..(std::cmp::min(32, b.len())))),
            );
        fmt.finish()
    }
}

// We need a custom Debug because the buffers can be large and hard to grok.
impl std::fmt::Debug for InProgressUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fmt = f.debug_struct("InProgressUpload");
        fmt.field("target_size", &self.target_size)
            .field("hint", &self.hint)
            .field("url", &self.url)
            .field("offset", &self.offset)
            .field("buffer_size", &self.buffer_size)
            // The buffer and remainder can be rather large, just print a summary.
            .field("buffer", &Summary(&self.buffer))
            .field("remainder", &Summary(&self.remainder));
        fmt.finish()
    }
}

impl InProgressUpload {
    fn new(options: &super::request_options::RequestOptions, hint: (u64, Option<u64>)) -> Self {
        // The buffer size must be a multiple of the upload quantum. The
        // upload is finalized on the first PUT request with a size that is
        // not such.
        let target_size = options
            .resumable_upload_buffer_size
            .div_ceil(RESUMABLE_UPLOAD_QUANTUM)
            * RESUMABLE_UPLOAD_QUANTUM;
        let target_size = target_size.max(RESUMABLE_UPLOAD_QUANTUM);

        Self {
            target_size,
            hint,
            ..Default::default()
        }
    }

    async fn next_buffer<S>(&mut self, payload: &mut S) -> Result<()>
    where
        S: StreamingSource,
    {
        let mut buffer = VecDeque::new();
        let mut size = 0;
        let mut process_buffer = |mut b: bytes::Bytes| match b.len() {
            n if size + n > self.target_size => {
                let remainder = b.split_off(self.target_size - size);
                size = self.target_size;
                buffer.push_back(b);
                Some(Some(remainder))
            }
            n if size + n == self.target_size => {
                size = self.target_size;
                buffer.push_back(b);
                Some(None)
            }
            n => {
                size += n;
                buffer.push_back(b);
                None
            }
        };

        while let Some(b) = self.remainder.pop_front() {
            if let Some(r) = process_buffer(b) {
                r.into_iter().for_each(|b| self.remainder.push_front(b));
                self.buffer = buffer;
                self.buffer_size = size;
                return Ok(());
            }
        }

        while let Some(b) = payload.next().await.transpose().map_err(Error::ser)? {
            if let Some(r) = process_buffer(b) {
                r.into_iter().for_each(|b| self.remainder.push_front(b));
                self.buffer = buffer;
                self.buffer_size = size;
                return Ok(());
            }
        }
        self.buffer = buffer;
        self.buffer_size = size;
        Ok(())
    }

    fn range_header(&self) -> String {
        match (
            self.buffer_size as u64,
            self.offset,
            self.hint.0,
            self.hint.1,
        ) {
            (0, 0, min, Some(max)) if min == max => format!("bytes */{min}"),
            (0, 0, _, _) => "bytes */0".to_string(),
            (n, o, min, Some(max)) if min == max => format!("bytes {o}-{}/{min}", o + n - 1),
            (n, o, _, _) if n < self.target_size as u64 => {
                format!("bytes {o}-{}/{}", o + n - 1, o + n)
            }
            (n, o, _, _) => format!("bytes {o}-{}/*", o + n - 1),
        }
    }

    fn handle_partial(&mut self, persisted_size: u64) -> Result<()> {
        let consumed = match (self.offset, self.buffer_size as u64, persisted_size) {
            (o, _, p) if p < o => Err(ProgressError::UnexpectedRewind {
                offset: o,
                persisted: p,
            }),
            (o, n, p) if p <= o + n => Ok((p - o) as usize),
            (o, n, p) => Err(ProgressError::TooMuchProgress {
                sent: o + n,
                persisted: p,
            }),
        };
        let mut skip = consumed.map_err(Error::ser)?;
        self.persisted_size = Some(persisted_size);
        self.offset = persisted_size;
        self.remainder = self
            .buffer
            .drain(0..)
            .filter_map(|mut b| match (skip, b.len()) {
                (0, _) => Some(b),
                (s, n) if s >= n => {
                    skip -= n;
                    None
                }
                (s, n) => {
                    skip = 0;
                    Some(b.split_off(n - s))
                }
            })
            .chain(self.remainder.drain(0..))
            .collect();
        self.buffer_size = 0_usize;

        Ok(())
    }

    fn handle_error(&mut self) {
        self.persisted_size = None;
    }
}

#[derive(Error, Debug)]
enum ProgressError {
    #[error(
        "the service previously persisted {offset} bytes, but now reports only {persisted} as persisted"
    )]
    UnexpectedRewind { offset: u64, persisted: u64 },
    #[error("the service reports {persisted} bytes as persisted, but we only sent {sent} bytes")]
    TooMuchProgress { sent: u64, persisted: u64 },
}

#[cfg(test)]
mod resumable_tests;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::super::client::tests::{test_builder, test_inner_client};
    use super::upload_source::IterSource;
    use super::*;
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

    fn new_line_string(i: i32, len: usize) -> String {
        let data = String::from_iter(('a'..='z').cycle().take(len - 22 - 2));
        format!("{i:022} {data}\n")
    }

    fn new_line(i: i32, len: usize) -> bytes::Bytes {
        bytes::Bytes::from_owner(new_line_string(i, len))
    }

    fn fake_upload(target_size: usize) -> InProgressUpload {
        let mut progress =
            InProgressUpload::new(&super::request_options::RequestOptions::new(), (0, None));
        progress.target_size = target_size;
        progress
    }

    #[tokio::test]
    async fn upload_debug() -> Result {
        const LEN: usize = 1000;
        let stream = IterSource::new((0..8).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(LEN);
        upload.next_buffer(&mut payload).await?;
        let dbg = format!("{upload:?}");
        assert!(dbg.contains("buffer"), "{dbg}");
        assert!(dbg.contains("remainder"), "{dbg}");

        let want = format!("contents[0..32]: Some({:?})", new_line(0, LEN).slice(..32));
        assert!(dbg.contains(&want), "'{want}' not found in '{dbg}'");
        assert!(dbg.len() < LEN, "dbg is too long: '{dbg}'");

        Ok(())
    }

    #[tokio::test]
    async fn handle_partial_full() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..8).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(2 * LEN);
        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        upload.handle_partial(2 * LEN as u64)?;
        assert_eq!(upload.persisted_size, Some(2 * LEN as u64));
        assert_eq!(upload.offset, 2 * LEN as u64);
        assert!(upload.buffer.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer_size, 0, "{upload:?}");
        assert!(upload.remainder.is_empty(), "{upload:?}");
        Ok(())
    }

    #[tokio::test]
    async fn handle_partial_partial() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..8).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(2 * LEN);
        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        upload.handle_partial(LEN as u64)?;
        assert_eq!(upload.persisted_size, Some(LEN as u64));
        assert_eq!(upload.offset, LEN as u64);
        assert!(upload.buffer.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer_size, 0, "{upload:?}");
        assert_eq!(upload.remainder, vec![new_line(1, LEN)]);
        Ok(())
    }

    #[tokio::test]
    async fn handle_partial_partial_with_remainder() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..8).map(|i| new_line(i, 4 * LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(2 * LEN);
        upload.next_buffer(&mut payload).await?;
        assert_eq!(
            upload.remainder,
            vec![new_line(0, 4 * LEN).split_off(2 * LEN)],
            "{upload:?}"
        );
        upload.handle_partial(LEN as u64)?;
        assert_eq!(upload.persisted_size, Some(LEN as u64));
        assert_eq!(upload.offset, LEN as u64);
        assert!(upload.buffer.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer_size, 0, "{upload:?}");
        assert_eq!(
            upload.remainder,
            vec![
                new_line(0, 4 * LEN).split_to(2 * LEN).split_off(LEN),
                new_line(0, 4 * LEN).split_off(2 * LEN)
            ]
        );
        upload.next_buffer(&mut payload).await?;
        assert_eq!(
            upload.buffer,
            vec![
                new_line(0, 4 * LEN).split_to(2 * LEN).split_off(LEN),
                new_line(0, 4 * LEN).split_off(2 * LEN).split_to(LEN),
            ]
        );
        assert_eq!(
            upload.remainder,
            vec![new_line(0, 4 * LEN).split_off(3 * LEN)],
            "{upload:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_partial_too_much_progress() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..8).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(2 * LEN);
        upload.next_buffer(&mut payload).await?;
        let err = upload
            .handle_partial(4 * LEN as u64)
            .expect_err("too much progress should cause errors");
        assert!(err.is_serialization(), "{err:?}");
        let source = err
            .source()
            .and_then(|e| e.downcast_ref::<ProgressError>())
            .expect("source should be a ProgressError");
        assert!(
            matches!(source, ProgressError::TooMuchProgress { sent, persisted } if *sent == 2 * LEN as u64 && *persisted == 4 * LEN as u64 ),
            "{source:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_partial_rewind() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..8).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(2 * LEN);
        upload.next_buffer(&mut payload).await?;
        upload.handle_partial(2 * LEN as u64)?;

        upload.next_buffer(&mut payload).await?;
        let err = upload
            .handle_partial(LEN as u64)
            .expect_err("rewind should cause errors");
        assert!(err.is_serialization(), "{err:?}");
        let source = err
            .source()
            .and_then(|e| e.downcast_ref::<ProgressError>())
            .expect("source should be a ProgressError");
        assert!(
            matches!(source, ProgressError::UnexpectedRewind { offset, persisted } if *offset == 2 * LEN as u64 && *persisted == LEN as u64 ),
            "{source:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn next_buffer_success() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..5).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(LEN * 2);
        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer, vec![new_line(0, LEN), new_line(1, LEN)]);
        assert_eq!(upload.buffer_size, 2 * LEN);

        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer, vec![new_line(2, LEN), new_line(3, LEN)]);
        assert_eq!(upload.buffer_size, 2 * LEN);

        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer, vec![new_line(4, LEN)]);
        assert_eq!(upload.buffer_size, LEN);

        Ok(())
    }

    #[tokio::test]
    async fn next_buffer_split() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..5).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(LEN * 2 + LEN / 2);
        upload.next_buffer(&mut payload).await?;
        assert_eq!(upload.remainder, vec![new_line(2, LEN).split_off(LEN / 2)]);
        assert_eq!(
            upload.buffer,
            vec![
                new_line(0, LEN),
                new_line(1, LEN),
                new_line(2, LEN).split_to(LEN / 2)
            ]
        );
        assert_eq!(upload.buffer_size, 2 * LEN + LEN / 2);

        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(
            upload.buffer,
            vec![
                new_line(2, LEN).split_off(LEN / 2),
                new_line(3, LEN),
                new_line(4, LEN)
            ]
        );
        assert_eq!(upload.buffer_size, 2 * LEN + LEN / 2);

        Ok(())
    }

    #[tokio::test]
    async fn next_buffer_split_large_remainder() -> Result {
        const LEN: usize = 32;
        let buffer = (0..3)
            .map(|i| new_line_string(i, LEN))
            .collect::<Vec<_>>()
            .join("");
        let stream = IterSource::new(vec![bytes::Bytes::from_owner(buffer), new_line(3, LEN)]);
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(LEN);
        upload.next_buffer(&mut payload).await?;
        assert_eq!(upload.buffer, vec![new_line(0, LEN)]);
        assert_eq!(upload.buffer_size, LEN);

        upload.next_buffer(&mut payload).await?;
        assert!(!upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer, vec![new_line(1, LEN)]);
        assert_eq!(upload.buffer_size, LEN);

        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer, vec![new_line(2, LEN)]);
        assert_eq!(upload.buffer_size, LEN);

        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer, vec![new_line(3, LEN)]);
        assert_eq!(upload.buffer_size, LEN);

        Ok(())
    }

    #[tokio::test]
    async fn next_buffer_join_remainder() -> Result {
        const LEN: usize = 32;
        let buffer = (0..3)
            .map(|i| new_line_string(i, LEN))
            .collect::<Vec<_>>()
            .join("");
        let stream = IterSource::new(vec![
            bytes::Bytes::from_owner(buffer.clone()),
            new_line(3, LEN),
        ]);
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(2 * LEN);
        upload.next_buffer(&mut payload).await?;
        assert!(!upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(
            upload.buffer,
            vec![bytes::Bytes::from_owner(buffer.clone()).slice(0..(2 * LEN))]
        );
        assert_eq!(upload.buffer_size, 2 * LEN);

        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(
            upload.buffer,
            vec![
                bytes::Bytes::from_owner(buffer.clone()).slice((2 * LEN)..),
                new_line(3, LEN)
            ]
        );
        assert_eq!(upload.buffer_size, 2 * LEN);

        Ok(())
    }

    #[tokio::test]
    async fn next_buffer_done() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..2).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = fake_upload(4 * LEN);
        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer, vec![new_line(0, LEN), new_line(1, LEN)]);
        assert_eq!(upload.buffer_size, 2 * LEN);

        upload.next_buffer(&mut payload).await?;
        assert!(upload.remainder.is_empty(), "{upload:?}");
        assert!(upload.buffer.is_empty(), "{upload:?}");
        assert_eq!(upload.buffer_size, 0);

        Ok(())
    }
}
