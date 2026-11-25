// Copyright 2026 Google LLC
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

//! Defines multi-cursor data sources for [Storage::parallel_upload][crate::client::Storage::parallel_upload].

use crate::streaming_source::{Seek, SizeHint, StreamingSource};
use bytes::Bytes;
use std::convert::Infallible;
use std::fmt;
use std::future::ready;
use std::io::{Error as IoError, ErrorKind};

/// A data source of known size that can be sliced into independent, rewindable
/// segment readers for concurrent parallel uploads.
#[cfg_attr(docsrs, doc(cfg(google_cloud_unstable_storage_parallel_upload)))]
pub trait ParallelUploadSource: Send + Sync + 'static {
    /// The segment reader returned by [`slice`][Self::slice].
    type SegmentSource: StreamingSource<Error = Self::Error>
        + Seek<Error = Self::Error>
        + Clone
        + Send
        + Sync
        + 'static;

    /// The error type returned when querying size or reading segments.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns the total size of the upload payload in bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the total size cannot be determined.
    fn total_size(&self) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Returns an independent segment reader covering `[offset, offset + len)`.
    ///
    /// # Errors
    ///
    /// Returns an error if the requested byte range is out of bounds or cannot
    /// be created.
    fn slice(&self, offset: u64, len: u64) -> Result<Self::SegmentSource, Self::Error>;
}

/// Converts a value into a [`ParallelUploadSource`].
#[cfg_attr(docsrs, doc(cfg(google_cloud_unstable_storage_parallel_upload)))]
pub trait IntoParallelUploadSource: Send {
    /// The resulting [`ParallelUploadSource`] type.
    type Source: ParallelUploadSource;

    /// The error type returned if conversion fails.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Converts `self` into a [`ParallelUploadSource`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying source cannot be initialized.
    fn into_parallel_upload_source(
        self,
    ) -> impl Future<Output = Result<Self::Source, Self::Error>> + Send;
}

impl<S> IntoParallelUploadSource for S
where
    S: ParallelUploadSource,
{
    type Source = S;
    type Error = Infallible;

    fn into_parallel_upload_source(
        self,
    ) -> impl Future<Output = Result<Self::Source, Self::Error>> + Send {
        ready(Ok(self))
    }
}

/// An in-memory [`ParallelUploadSource`] backed by [`Bytes`].
#[cfg_attr(docsrs, doc(cfg(google_cloud_unstable_storage_parallel_upload)))]
#[derive(Clone)]
pub struct BytesParallelUploadSource {
    contents: Bytes,
}

impl BytesParallelUploadSource {
    /// Creates a new in-memory source from any value convertible to [`Bytes`].
    pub fn new(contents: impl Into<Bytes>) -> Self {
        Self {
            contents: contents.into(),
        }
    }
}

impl fmt::Debug for BytesParallelUploadSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BytesParallelUploadSource")
            .field("len", &self.contents.len())
            .finish_non_exhaustive()
    }
}

impl ParallelUploadSource for BytesParallelUploadSource {
    type SegmentSource = BytesSegmentSource;
    type Error = IoError;

    fn total_size(&self) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(Ok(self.contents.len() as u64))
    }

    fn slice(&self, offset: u64, len: u64) -> Result<Self::SegmentSource, Self::Error> {
        let total_size = self.contents.len() as u64;
        let end = offset
            .checked_add(len)
            .filter(|&end| end <= total_size)
            .ok_or_else(|| {
                IoError::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "slice range offset={offset}, len={len} exceeds total size {total_size}"
                    ),
                )
            })?;
        // `offset <= end <= self.contents.len() <= usize::MAX` holds on all platforms.
        let start = offset as usize;
        let end = end as usize;
        Ok(BytesSegmentSource::new(self.contents.slice(start..end)))
    }
}

/// A zero-copy, rewindable, cloneable segment reader backed by [`Bytes`].
///
/// Cloning preserves the current read offset while giving each clone an
/// independent cursor.
#[cfg_attr(docsrs, doc(cfg(google_cloud_unstable_storage_parallel_upload)))]
#[derive(Clone)]
pub struct BytesSegmentSource {
    contents: Bytes,
    offset: usize,
}

impl BytesSegmentSource {
    pub(crate) fn new(contents: Bytes) -> Self {
        Self {
            contents,
            offset: 0,
        }
    }
}

impl fmt::Debug for BytesSegmentSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BytesSegmentSource")
            .field("len", &self.contents.len())
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl StreamingSource for BytesSegmentSource {
    type Error = IoError;

    async fn next(&mut self) -> Option<Result<Bytes, Self::Error>> {
        if self.offset >= self.contents.len() {
            return None;
        }
        let chunk = self.contents.slice(self.offset..);
        self.offset = self.contents.len();
        Some(Ok(chunk))
    }

    async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
        Ok(SizeHint::with_exact(self.contents.len() as u64))
    }
}

impl Seek for BytesSegmentSource {
    type Error = IoError;

    async fn seek(&mut self, offset: u64) -> Result<(), Self::Error> {
        let len = self.contents.len() as u64;
        if offset > len {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("seek offset {offset} exceeds segment length {len}"),
            ));
        }
        // `offset <= self.contents.len() <= usize::MAX` holds on all platforms.
        self.offset = offset as usize;
        Ok(())
    }
}

impl IntoParallelUploadSource for Bytes {
    type Source = BytesParallelUploadSource;
    type Error = Infallible;

    fn into_parallel_upload_source(
        self,
    ) -> impl Future<Output = Result<Self::Source, Self::Error>> + Send {
        ready(Ok(BytesParallelUploadSource::new(self)))
    }
}

impl IntoParallelUploadSource for Vec<u8> {
    type Source = BytesParallelUploadSource;
    type Error = Infallible;

    fn into_parallel_upload_source(
        self,
    ) -> impl Future<Output = Result<Self::Source, Self::Error>> + Send {
        ready(Ok(BytesParallelUploadSource::new(self)))
    }
}

impl IntoParallelUploadSource for String {
    type Source = BytesParallelUploadSource;
    type Error = Infallible;

    fn into_parallel_upload_source(
        self,
    ) -> impl Future<Output = Result<Self::Source, Self::Error>> + Send {
        ready(Ok(BytesParallelUploadSource::new(self)))
    }
}

impl IntoParallelUploadSource for &'static [u8] {
    type Source = BytesParallelUploadSource;
    type Error = Infallible;

    fn into_parallel_upload_source(
        self,
    ) -> impl Future<Output = Result<Self::Source, Self::Error>> + Send {
        ready(Ok(BytesParallelUploadSource::new(Bytes::from_static(self))))
    }
}

impl<const N: usize> IntoParallelUploadSource for &'static [u8; N] {
    type Source = BytesParallelUploadSource;
    type Error = Infallible;

    fn into_parallel_upload_source(
        self,
    ) -> impl Future<Output = Result<Self::Source, Self::Error>> + Send {
        ready(Ok(BytesParallelUploadSource::new(Bytes::from_static(self))))
    }
}

impl IntoParallelUploadSource for &'static str {
    type Source = BytesParallelUploadSource;
    type Error = Infallible;

    fn into_parallel_upload_source(
        self,
    ) -> impl Future<Output = Result<Self::Source, Self::Error>> + Send {
        ready(Ok(BytesParallelUploadSource::new(Bytes::from_static(
            self.as_bytes(),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_ext::Segment;
    use crate::parallel_upload_session::{ParallelUploadSession, SegmentPayload};
    use mockall::mock;
    use static_assertions::assert_impl_all;
    use std::sync::{Arc, Mutex};
    use test_case::test_case;

    fn assert_send<T: Send>(_: &T) {}
    fn assert_static<T: 'static>(_: &T) {}

    async fn read_all<S>(mut source: S) -> anyhow::Result<Vec<u8>>
    where
        S: StreamingSource,
    {
        let mut out = Vec::new();
        while let Some(chunk) = source.next().await.transpose()? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    async fn generic_into_source<P>(payload: P) -> Result<P::Source, P::Error>
    where
        P: IntoParallelUploadSource,
    {
        payload.into_parallel_upload_source().await
    }

    async fn generic_write_first_segment<S>(
        session: &ParallelUploadSession,
        source: &S,
    ) -> anyhow::Result<Segment>
    where
        S: ParallelUploadSource,
    {
        let len = source.total_size().await?;
        let segment = source.slice(0, len)?;
        Ok(session.write_segment(1, segment).await?)
    }

    mock! {
        #[derive(Debug)]
        Session {}

        impl crate::stub::ParallelUploadSession for Session {
            async fn write_segment(
                &self,
                segment_number: u32,
                segment_payload: SegmentPayload,
            ) -> crate::Result<Segment>;
        }
    }

    #[test]
    fn traits_and_debug() {
        // Arrange.
        assert_impl_all!(BytesParallelUploadSource: Clone, fmt::Debug, Send, Sync);
        assert_impl_all!(BytesSegmentSource: Clone, fmt::Debug, Send, Sync);
        let secret_bytes = Bytes::from_static(b"sensitive-payload-content");
        let source = BytesParallelUploadSource::new(secret_bytes);
        let segment = source.slice(0, 9).expect("valid slice");
        assert_static(&source);
        assert_static(&segment);

        // Act.
        let source_debug = format!("{source:?}");
        let segment_debug = format!("{segment:?}");

        // Assert.
        assert!(source_debug.contains("BytesParallelUploadSource"));
        assert!(source_debug.contains("len: 25"));
        assert!(!source_debug.contains("sensitive-payload-content"));
        assert!(segment_debug.contains("BytesSegmentSource"));
        assert!(segment_debug.contains("len: 9"));
        assert!(segment_debug.contains("offset: 0"));
        assert!(!segment_debug.contains("sensitive"));
    }

    #[tokio::test]
    async fn futures_are_send() -> anyhow::Result<()> {
        // Arrange.
        let source = BytesParallelUploadSource::new(Bytes::from_static(b"hello"));
        let mut segment = source.slice(0, 5)?;

        // Act & Assert.
        let into_fut = generic_into_source(Bytes::from_static(b"hello"));
        assert_send(&into_fut);
        assert_static(&into_fut);
        drop(into_fut);

        let total_fut = source.total_size();
        assert_send(&total_fut);
        drop(total_fut);

        let hint_fut = segment.size_hint();
        assert_send(&hint_fut);
        drop(hint_fut);

        let next_fut = segment.next();
        assert_send(&next_fut);
        drop(next_fut);

        let seek_fut = segment.seek(0);
        assert_send(&seek_fut);
        drop(seek_fut);

        Ok(())
    }

    #[tokio::test]
    async fn into_parallel_upload_source_conversions() -> anyhow::Result<()> {
        // Arrange.
        let want = b"parallel upload payload";

        // Act.
        let from_bytes = Bytes::from_static(want)
            .into_parallel_upload_source()
            .await?;
        let from_vec = want.to_vec().into_parallel_upload_source().await?;
        let from_string = String::from_utf8(want.to_vec())?
            .into_parallel_upload_source()
            .await?;
        let from_slice = want.as_slice().into_parallel_upload_source().await?;
        let from_array = want.into_parallel_upload_source().await?;
        let from_str = std::str::from_utf8(want)?
            .into_parallel_upload_source()
            .await?;
        let from_source = BytesParallelUploadSource::new(Bytes::from_static(want))
            .into_parallel_upload_source()
            .await?;

        // Assert.
        for src in [
            from_bytes,
            from_vec,
            from_string,
            from_slice,
            from_array,
            from_str,
            from_source,
        ] {
            let total = src.total_size().await?;
            assert_eq!(total, want.len() as u64);
            let seg = src.slice(0, total)?;
            assert_eq!(read_all(seg).await?, want);
        }
        Ok(())
    }

    #[tokio::test]
    async fn zero_copy_slicing() -> anyhow::Result<()> {
        // Arrange.
        let backing = Bytes::from(b"0123456789abcdef".to_vec());
        let base_ptr = backing.as_ptr();
        let source = backing.into_parallel_upload_source().await?;

        // Act.
        let cloned_source = source.clone();
        let mut s1 = source.slice(0, 6)?;
        let mut s2 = cloned_source.slice(6, 10)?;
        let mut overlap = source.slice(4, 4)?;
        let c1 = s1.next().await.transpose()?.expect("chunk 1");
        let c2 = s2.next().await.transpose()?.expect("chunk 2");
        let c3 = overlap.next().await.transpose()?.expect("overlap chunk");

        // Assert.
        assert_eq!(c1.as_ref(), b"012345");
        assert_eq!(c2.as_ref(), b"6789abcdef");
        assert_eq!(c3.as_ref(), b"4567");
        assert_eq!(c1.as_ptr(), base_ptr);
        assert_eq!(c2.as_ptr(), base_ptr.wrapping_add(6));
        assert_eq!(c3.as_ptr(), base_ptr.wrapping_add(4));
        assert_eq!(s1.next().await.transpose()?, None);
        assert_eq!(s2.next().await.transpose()?, None);
        assert_eq!(overlap.next().await.transpose()?, None);
        Ok(())
    }

    #[tokio::test]
    async fn empty_source_and_zero_length_slices() -> anyhow::Result<()> {
        // Arrange.
        let empty = Bytes::new().into_parallel_upload_source().await?;
        let non_empty = Bytes::from_static(b"abcd")
            .into_parallel_upload_source()
            .await?;

        // Act.
        let mut s_empty = empty.slice(0, 0)?;
        let mut s_mid = non_empty.slice(2, 0)?;
        let mut s_end = non_empty.slice(4, 0)?;

        // Assert.
        assert_eq!(empty.total_size().await?, 0);
        assert_eq!(
            empty.slice(0, 1).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            empty.slice(1, 0).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        for seg in [&mut s_empty, &mut s_mid, &mut s_end] {
            assert_eq!(seg.size_hint().await?.exact(), Some(0));
            assert_eq!(seg.next().await.transpose()?, None);
            seg.seek(0).await?;
            assert_eq!(seg.next().await.transpose()?, None);
            let err = seg.seek(1).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidInput);
        }
        Ok(())
    }

    #[test_case(6, 0 ; "offset past end on non-empty")]
    #[test_case(0, 6 ; "len exceeds total")]
    #[test_case(3, 3 ; "offset plus len exceeds total")]
    #[test_case(u64::MAX, 0 ; "max offset zero len")]
    #[test_case(u64::MAX, 1 ; "offset overflow")]
    #[test_case(1, u64::MAX ; "len overflow")]
    fn slice_out_of_bounds_errors(offset: u64, len: u64) {
        // Arrange.
        let source = BytesParallelUploadSource::new(Bytes::from_static(b"12345"));

        // Act.
        let err = source.slice(offset, len).unwrap_err();

        // Assert.
        assert_eq!(err.kind(), ErrorKind::InvalidInput, "{err:?}");
    }

    #[tokio::test]
    async fn segment_seek_and_size_hint() -> anyhow::Result<()> {
        // Arrange.
        let source = BytesParallelUploadSource::new(Bytes::from_static(b"abcdefghij"));
        let mut seg = source.slice(2, 6)?; // "cdefgh"

        // Act & Assert.
        assert_eq!(seg.size_hint().await?.exact(), Some(6));
        assert_eq!(
            seg.next().await.transpose()?,
            Some(Bytes::from_static(b"cdefgh"))
        );
        assert_eq!(seg.next().await.transpose()?, None);
        // `size_hint()` stays equal to the total segment length after exhaustion.
        assert_eq!(seg.size_hint().await?.exact(), Some(6));

        // Rewind to mid-segment.
        seg.seek(2).await?;
        assert_eq!(seg.size_hint().await?.exact(), Some(6));
        assert_eq!(
            seg.next().await.transpose()?,
            Some(Bytes::from_static(b"efgh"))
        );
        assert_eq!(seg.next().await.transpose()?, None);

        // Seek to exact end yields None.
        seg.seek(6).await?;
        assert_eq!(seg.next().await.transpose()?, None);

        // Rewind to start.
        seg.seek(0).await?;
        assert_eq!(
            seg.next().await.transpose()?,
            Some(Bytes::from_static(b"cdefgh"))
        );

        // Out-of-bounds seek fails and preserves current offset.
        seg.seek(2).await?;
        assert_eq!(
            seg.seek(7).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            seg.seek(u64::MAX).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            seg.next().await.transpose()?,
            Some(Bytes::from_static(b"efgh"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn segment_clone_preserves_independent_cursors() -> anyhow::Result<()> {
        // Arrange.
        let source = BytesParallelUploadSource::new(Bytes::from_static(b"hello world"));
        let mut seg = source.slice(0, 11)?;

        // Act & Assert: clone at offset 0.
        let mut clone_at_start = seg.clone();
        assert_eq!(
            seg.next().await.transpose()?,
            Some(Bytes::from_static(b"hello world"))
        );
        assert_eq!(
            clone_at_start.next().await.transpose()?,
            Some(Bytes::from_static(b"hello world"))
        );

        // Act & Assert: clone mid-stream preserves offset 6 independently.
        seg.seek(6).await?;
        let mut clone_mid = seg.clone();
        assert_eq!(
            seg.next().await.transpose()?,
            Some(Bytes::from_static(b"world"))
        );
        assert_eq!(
            clone_mid.next().await.transpose()?,
            Some(Bytes::from_static(b"world"))
        );

        // Act & Assert: clone while exhausted stays exhausted; rewinding one leaves the other unaffected.
        let mut clone_exhausted = seg.clone();
        seg.seek(0).await?;
        assert_eq!(
            seg.next().await.transpose()?,
            Some(Bytes::from_static(b"hello world"))
        );
        assert_eq!(clone_exhausted.next().await.transpose()?, None);

        Ok(())
    }

    #[tokio::test]
    async fn works_with_parallel_upload_session_write_segment() -> anyhow::Result<()> {
        // Arrange.
        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = captured.clone();
        let mut mock = MockSession::new();
        mock.expect_write_segment()
            .times(2)
            .returning(move |num, payload| {
                captured_clone
                    .lock()
                    .expect("never poisoned")
                    .push((num, payload));
                Ok(Segment::new().set_name(format!("seg-{num}")))
            });
        let session = ParallelUploadSession::new(mock);
        let source = BytesParallelUploadSource::new(Bytes::from_static(b"hello world"));

        // Act: test both concrete `BytesSegmentSource` and generic `S: ParallelUploadSource`.
        let concrete_seg = source.slice(6, 5)?;
        let r1 = session.write_segment(2, concrete_seg).await?;
        let r2 = generic_write_first_segment(&session, &source).await?;

        // Assert.
        assert_eq!(r1.name, "seg-2");
        assert_eq!(r2.name, "seg-1");
        let mut recorded = std::mem::take(&mut *captured.lock().expect("never poisoned"));
        assert_eq!(recorded.len(), 2);

        let (n1, mut p1) = recorded.remove(0);
        assert_eq!(n1, 2);
        assert_eq!(
            p1.next().await.transpose()?,
            Some(Bytes::from_static(b"world"))
        );
        p1.seek(0).await?;
        assert_eq!(
            p1.next().await.transpose()?,
            Some(Bytes::from_static(b"world"))
        );

        let (n2, mut p2) = recorded.remove(0);
        assert_eq!(n2, 1);
        assert_eq!(
            p2.next().await.transpose()?,
            Some(Bytes::from_static(b"hello world"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn non_static_into_parallel_upload_source() -> anyhow::Result<()> {
        // Arrange: a borrowed input verifies `IntoParallelUploadSource` does not require `'static`.
        struct Borrowed<'a>(&'a [u8]);

        impl IntoParallelUploadSource for Borrowed<'_> {
            type Source = BytesParallelUploadSource;
            type Error = Infallible;

            fn into_parallel_upload_source(
                self,
            ) -> impl Future<Output = Result<Self::Source, Self::Error>> + Send {
                ready(Ok(BytesParallelUploadSource::new(self.0.to_vec())))
            }
        }

        let local = b"borrowed local payload".to_vec();

        // Act.
        let fut = generic_into_source(Borrowed(&local));
        assert_send(&fut);
        let source = fut.await?;
        let segment = source.slice(0, local.len() as u64)?;

        // Assert.
        assert_eq!(read_all(segment).await?, local);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_reads_across_tasks() -> anyhow::Result<()> {
        // Arrange.
        let data: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let source = Arc::new(BytesParallelUploadSource::new(data.clone()));
        let ranges = [(0_u64, 256_u64), (256, 256), (512, 512), (128, 384)];

        // Act.
        let mut handles = Vec::new();
        for (offset, len) in ranges {
            let src = Arc::clone(&source);
            handles.push(tokio::spawn(async move {
                let mut seg = src.slice(offset, len)?;
                let first = read_all(seg.clone()).await?;
                seg.seek(0).await?;
                let second = read_all(seg).await?;
                anyhow::Ok((offset, len, first, second))
            }));
        }

        // Assert.
        for handle in handles {
            let (offset, len, first, second) = handle.await??;
            let want = &data[offset as usize..(offset + len) as usize];
            assert_eq!(first, want);
            assert_eq!(second, want);
        }
        Ok(())
    }
}
