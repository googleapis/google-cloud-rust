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

use super::RESUMABLE_UPLOAD_QUANTUM;
use super::StreamingSource;
use crate::Error;
use crate::Result;
use crate::storage::UploadError;
use futures::stream::unfold;
use std::collections::VecDeque;

#[derive(Clone, Default)]
pub struct InProgressUpload {
    /// The target size for each PUT request.
    ///
    /// The last PUT request may be smaller. This must be a multiple of 256KiB
    /// and greater than 0.
    target_size: usize,
    /// The expected size [minimum, maximum) for the full object.
    ///
    /// If the maximum is `None` the maximum size is not known.
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
    pub fn new(target_size: usize, hint: (u64, Option<u64>)) -> Self {
        // The buffer size must be a multiple of the upload quantum. The
        // upload is finalized on the first PUT request with a size that is
        // not such.
        let target_size = target_size.div_ceil(RESUMABLE_UPLOAD_QUANTUM) * RESUMABLE_UPLOAD_QUANTUM;
        let target_size = target_size.max(RESUMABLE_UPLOAD_QUANTUM);

        Self {
            target_size,
            hint,
            ..Default::default()
        }
    }

    // This is only used in tests.
    #[cfg(test)]
    fn fake(target_size: usize) -> Self {
        Self {
            target_size,
            hint: (0, None),
            ..Default::default()
        }
    }

    pub fn upload_session(&self) -> Option<String> {
        self.url.clone()
    }

    pub fn set_upload_session(&mut self, url: String) -> String {
        self.url = Some(url.clone());
        self.persisted_size = Some(0_u64);
        url
    }

    pub fn needs_query(&self) -> bool {
        self.persisted_size.is_none_or(|x| x != self.offset)
    }

    pub async fn next_buffer<S>(&mut self, payload: &mut S) -> Result<()>
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

    pub fn range_header(&self) -> String {
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

    pub fn put_body(&self) -> reqwest::Body {
        let stream = unfold(Some(self.buffer.clone()), move |state| async move {
            if let Some(mut payload) = state {
                if let Some(next) = payload.pop_front() {
                    return Some((Ok::<bytes::Bytes, Error>(next), Some(payload)));
                }
            }
            None
        });
        reqwest::Body::wrap_stream(stream)
    }

    pub fn handle_partial(&mut self, persisted_size: u64) -> Result<()> {
        let consumed = match (self.offset, self.buffer_size as u64, persisted_size) {
            (o, _, p) if p < o => Err(UploadError::UnexpectedRewind {
                offset: o,
                persisted: p,
            }),
            (o, n, p) if p <= o + n => Ok((p - o) as usize),
            (o, n, p) => Err(UploadError::TooMuchProgress {
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

    pub fn handle_error(&mut self) {
        self.persisted_size = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::upload_source::{InsertPayload, IterSource};
    use http_body_util::BodyExt;
    use std::error::Error as _;
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    fn new_line_string(i: i32, len: usize) -> String {
        let data = String::from_iter(('a'..='z').cycle().take(len - 22 - 2));
        format!("{i:022} {data}\n")
    }

    fn new_line(i: i32, len: usize) -> bytes::Bytes {
        bytes::Bytes::from_owner(new_line_string(i, len))
    }

    #[tokio::test]
    async fn upload_debug() -> Result {
        const LEN: usize = 1000;
        let stream = IterSource::new((0..8).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = InProgressUpload::fake(LEN);
        upload.next_buffer(&mut payload).await?;
        let dbg = format!("{upload:?}");
        assert!(dbg.contains("buffer"), "{dbg}");
        assert!(dbg.contains("remainder"), "{dbg}");

        let want = format!("contents[0..32]: Some({:?})", new_line(0, LEN).slice(..32));
        assert!(dbg.contains(&want), "'{want}' not found in '{dbg}'");
        assert!(dbg.len() < LEN, "dbg is too long: '{dbg}'");

        Ok(())
    }

    #[test_case(0, RESUMABLE_UPLOAD_QUANTUM)]
    #[test_case(RESUMABLE_UPLOAD_QUANTUM / 2, RESUMABLE_UPLOAD_QUANTUM)]
    #[test_case(RESUMABLE_UPLOAD_QUANTUM, RESUMABLE_UPLOAD_QUANTUM)]
    #[test_case(RESUMABLE_UPLOAD_QUANTUM * 2, RESUMABLE_UPLOAD_QUANTUM * 2)]
    #[test_case(RESUMABLE_UPLOAD_QUANTUM * 2 + 1, RESUMABLE_UPLOAD_QUANTUM * 3)]
    fn rounding(input: usize, want: usize) {
        let upload = InProgressUpload::new(input, (0, None));
        assert_eq!(upload.target_size, want, "{upload:?}");
    }

    #[test]
    fn upload_session() {
        let mut upload = InProgressUpload::new(0, (0, None));
        assert!(upload.upload_session().is_none(), "{upload:?}");
        assert!(upload.needs_query(), "{upload:?}");

        upload.set_upload_session("test-only-invalid".to_string());
        assert_eq!(
            upload.upload_session().as_deref(),
            Some("test-only-invalid"),
            "{upload:?}"
        );
        assert!(!upload.needs_query(), "{upload:?}");
    }

    #[tokio::test]
    async fn next_buffer_success() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..5).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = InProgressUpload::fake(LEN * 2);
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

        let mut upload = InProgressUpload::fake(LEN * 2 + LEN / 2);
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

        let mut upload = InProgressUpload::fake(LEN);
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

        let mut upload = InProgressUpload::fake(2 * LEN);
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

        let mut upload = InProgressUpload::fake(4 * LEN);
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

    #[tokio::test]
    async fn range_header_known_size() -> Result {
        let stream = IterSource::new((0..1).map(|i| new_line(i, 1024)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = InProgressUpload::new(0, (1024, Some(1024)));
        assert_eq!(upload.range_header(), "bytes */1024");

        upload.next_buffer(&mut payload).await?;
        assert_eq!(&upload.range_header(), "bytes 0-1023/1024");
        Ok(())
    }

    #[tokio::test]
    async fn range_header_empty() -> Result {
        let mut payload = InsertPayload::from("");

        let mut upload = InProgressUpload::new(0, (0, Some(0)));
        assert_eq!(upload.range_header(), "bytes */0");

        upload.next_buffer(&mut payload).await?;
        assert_eq!(&upload.range_header(), "bytes */0");
        Ok(())
    }

    #[tokio::test]
    async fn range_header_unknown_size() -> Result {
        const LINES: i32 = 257;
        let stream = IterSource::new((0..LINES).map(|i| new_line(i, 1024)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = InProgressUpload::new(0, (0, None));
        upload.next_buffer(&mut payload).await?;
        assert_eq!(
            upload.range_header(),
            format!("bytes 0-{}/*", RESUMABLE_UPLOAD_QUANTUM - 1)
        );
        upload.handle_partial(RESUMABLE_UPLOAD_QUANTUM as u64)?;

        upload.next_buffer(&mut payload).await?;
        assert_eq!(
            upload.range_header(),
            format!(
                "bytes {}-{}/{}",
                RESUMABLE_UPLOAD_QUANTUM,
                RESUMABLE_UPLOAD_QUANTUM + 1024 - 1,
                RESUMABLE_UPLOAD_QUANTUM + 1024
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn put_body() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..8).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = InProgressUpload::fake(4 * LEN);
        upload.next_buffer(&mut payload).await?;
        let want = (0..4).map(|i| new_line(i, LEN)).collect::<Vec<_>>();
        assert_eq!(upload.buffer, want);

        let body = upload.put_body();
        let got = body.collect().await?.to_bytes();
        assert_eq!(got.slice(0..LEN), new_line(0, LEN));
        assert_eq!(got.slice(LEN..2 * LEN), new_line(1, LEN));
        assert_eq!(got.slice(2 * LEN..3 * LEN), new_line(2, LEN));
        assert_eq!(got.slice(3 * LEN..), new_line(3, LEN));

        Ok(())
    }

    #[tokio::test]
    async fn handle_partial_full() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..8).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = InProgressUpload::fake(2 * LEN);
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

        let mut upload = InProgressUpload::fake(2 * LEN);
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

        let mut upload = InProgressUpload::fake(2 * LEN);
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

        let mut upload = InProgressUpload::fake(2 * LEN);
        upload.next_buffer(&mut payload).await?;
        let err = upload
            .handle_partial(4 * LEN as u64)
            .expect_err("too much progress should cause errors");
        assert!(err.is_serialization(), "{err:?}");
        let source = err
            .source()
            .and_then(|e| e.downcast_ref::<UploadError>())
            .expect("source should be a ProgressError");
        assert!(
            matches!(source, UploadError::TooMuchProgress { sent, persisted } if *sent == 2 * LEN as u64 && *persisted == 4 * LEN as u64 ),
            "{source:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_partial_rewind() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..8).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let mut upload = InProgressUpload::fake(2 * LEN);
        upload.next_buffer(&mut payload).await?;
        upload.handle_partial(2 * LEN as u64)?;

        upload.next_buffer(&mut payload).await?;
        let err = upload
            .handle_partial(LEN as u64)
            .expect_err("rewind should cause errors");
        assert!(err.is_serialization(), "{err:?}");
        let source = err
            .source()
            .and_then(|e| e.downcast_ref::<UploadError>())
            .expect("source should be a ProgressError");
        assert!(
            matches!(source, UploadError::UnexpectedRewind { offset, persisted } if *offset == 2 * LEN as u64 && *persisted == LEN as u64 ),
            "{source:?}"
        );
        Ok(())
    }
}
