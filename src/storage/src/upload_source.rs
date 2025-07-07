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

//! Defines upload data sources.

/// The payload for [insert_object()][super::client::Storage::insert_object].
///
/// `insert_object()` consumes any type that can be converted to this type. That
/// includes simple buffers, and any type implementing [StreamingSource].
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::upload_source::InsertPayload;
/// use google_cloud_storage::upload_source::StreamingSource;
/// let buffer : &[u8] = b"the quick brown fox jumps over the lazy dog";
/// let mut size = 0_usize;
/// let mut payload = InsertPayload::from(buffer);
/// while let Some(bytes) = payload.next().await.transpose()? {
///     size += bytes.len();
/// }
/// assert_eq!(size, buffer.len());
/// # anyhow::Result::<()>::Ok(()) });
/// ```
pub struct InsertPayload<T> {
    payload: T,
}

impl<T> StreamingSource for InsertPayload<T>
where
    T: StreamingSource,
{
    type Error = T::Error;

    fn next(&mut self) -> impl Future<Output = Option<Result<bytes::Bytes, Self::Error>>> + Send {
        self.payload.next()
    }

    fn size_hint(&self) -> (u64, Option<u64>) {
        self.payload.size_hint()
    }
}

impl<T> Seek for InsertPayload<T>
where
    T: Seek,
{
    type Error = T::Error;

    fn seek(&mut self, offset: u64) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.payload.seek(offset)
    }
}

impl From<bytes::Bytes> for InsertPayload<BytesSource> {
    fn from(value: bytes::Bytes) -> Self {
        let payload = BytesSource::new(value);
        Self { payload }
    }
}

impl From<&'static str> for InsertPayload<BytesSource> {
    fn from(value: &'static str) -> Self {
        let b = bytes::Bytes::from_static(value.as_bytes());
        InsertPayload::from(b)
    }
}

impl From<&'static [u8]> for InsertPayload<BytesSource> {
    fn from(value: &'static [u8]) -> Self {
        let b = bytes::Bytes::from_static(value);
        InsertPayload::from(b)
    }
}

impl<S> From<S> for InsertPayload<S>
where
    S: StreamingSource + Seek,
{
    fn from(value: S) -> Self {
        Self { payload: value }
    }
}

/// Provides bytes for an upload from single-pass sources.
pub trait StreamingSource {
    /// The error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Gets the next set of data to upload.
    fn next(&mut self) -> impl Future<Output = Option<Result<bytes::Bytes, Self::Error>>> + Send;

    /// An estimate of the upload size.
    ///
    /// Returns the expected size as a [min, max) range. Where `None` represents
    /// an unknown limit for the upload.
    ///
    /// If the upper limit is known and sufficiently small, the client library
    /// may be able to use a more efficient protocol for the upload.
    fn size_hint(&self) -> (u64, Option<u64>) {
        (0_u64, None)
    }
}

/// Provides bytes for an upload from sources that support seek.
///
/// Implementations of this trait provide data for Google Cloud Storage uploads.
/// The data may be received asynchronously, such as downloads from Google Cloud
/// Storage, other remote storage systems, or the result of repeatable
/// computations.
pub trait Seek {
    /// The error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Resets the stream to start from `offset`.
    ///
    /// The client library automatically restarts uploads when the connection
    /// is reset or there is some kind of partial failure. Resuming an upload
    /// may require resetting the stream to an arbitrary point.
    ///
    /// The client library assumes that `seek(N)` followed by `next()` always
    /// returns the same data.
    fn seek(&mut self, offset: u64) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

const READ_SIZE: usize = 256 * 1024;

impl<S> StreamingSource for S
where
    S: tokio::io::AsyncRead + Unpin + Send,
{
    type Error = std::io::Error;

    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        let mut buffer = vec![0_u8; READ_SIZE];
        match tokio::io::AsyncReadExt::read(self, &mut buffer).await {
            Err(e) => Some(Err(e)),
            Ok(0) => None,
            Ok(n) => {
                buffer.resize(n, 0_u8);
                Some(Ok(bytes::Bytes::from_owner(buffer)))
            }
        }
    }
}

impl<S> Seek for S
where
    S: tokio::io::AsyncSeek + Unpin + Send,
{
    type Error = std::io::Error;

    async fn seek(&mut self, offset: u64) -> Result<(), Self::Error> {
        let _ = tokio::io::AsyncSeekExt::seek(self, std::io::SeekFrom::Start(offset)).await?;
        Ok(())
    }
}

/// Wrap a `bytes::Bytes` to support `StreamingSource`.
pub struct BytesSource {
    contents: bytes::Bytes,
    current: Option<bytes::Bytes>,
}

impl BytesSource {
    pub(crate) fn new(contents: bytes::Bytes) -> Self {
        let current = Some(contents.clone());
        Self { contents, current }
    }
}

impl StreamingSource for BytesSource {
    type Error = crate::Error;

    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        self.current.take().map(Result::Ok)
    }

    fn size_hint(&self) -> (u64, Option<u64>) {
        let s = self.contents.len() as u64;
        (s, Some(s))
    }
}

impl Seek for BytesSource {
    type Error = crate::Error;

    async fn seek(&mut self, offset: u64) -> Result<(), Self::Error> {
        let pos = std::cmp::min(offset as usize, self.contents.len());
        self.current = Some(self.contents.slice(pos..));
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use std::{collections::VecDeque, io::Write};
    use tempfile::NamedTempFile;

    type Result = anyhow::Result<()>;

    const CONTENTS: &[u8] = b"how vexingly quick daft zebras jump";

    /// A helper function to simplify the tests.
    async fn collect<S>(source: S) -> anyhow::Result<Vec<u8>>
    where
        S: StreamingSource,
    {
        let mut vec = Vec::new();
        let mut source = source;
        while let Some(bytes) = source.next().await.transpose()? {
            vec.extend_from_slice(&bytes);
        }
        Ok(vec)
    }

    #[tokio::test]
    async fn empty_bytes() -> Result {
        let buffer = InsertPayload::from(bytes::Bytes::default());
        let range = buffer.size_hint();
        assert_eq!(range, (0, Some(0)));
        let got = collect(buffer).await?;
        assert!(got.is_empty(), "{got:?}");

        Ok(())
    }

    #[tokio::test]
    async fn simple_bytes() -> Result {
        let buffer = InsertPayload::from(bytes::Bytes::from_static(CONTENTS));
        let range = buffer.size_hint();
        assert_eq!(range, (CONTENTS.len() as u64, Some(CONTENTS.len() as u64)));
        let got = collect(buffer).await?;
        assert_eq!(got[..], CONTENTS[..], "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn simple_u8() -> Result {
        let buffer = InsertPayload::from(CONTENTS);
        let range = buffer.size_hint();
        assert_eq!(range, (CONTENTS.len() as u64, Some(CONTENTS.len() as u64)));
        let got = collect(buffer).await?;
        assert_eq!(got[..], CONTENTS[..], "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn simple_str() -> Result {
        const LAZY: &str = "the quick brown fox jumps over the lazy dog";
        let buffer = InsertPayload::from(LAZY);
        let range = buffer.size_hint();
        assert_eq!(range, (LAZY.len() as u64, Some(LAZY.len() as u64)));
        let got = collect(buffer).await?;
        assert_eq!(&got, LAZY.as_bytes(), "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn seek_bytes() -> Result {
        let mut buffer = InsertPayload::from(bytes::Bytes::from_static(CONTENTS));
        buffer.seek(8).await?;
        let got = collect(buffer).await?;
        assert_eq!(got[..], CONTENTS[8..], "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn empty_stream() -> Result {
        let source = VecStream::new(vec![]);
        let payload = InsertPayload::from(source);
        let range = payload.size_hint();
        assert_eq!(range, (0, Some(0)));
        let got = collect(payload).await?;
        assert!(got.is_empty(), "{got:?}");

        Ok(())
    }

    #[tokio::test]
    async fn simple_stream() -> Result {
        let source = VecStream::new(
            ["how ", "vexingly ", "quick ", "daft ", "zebras ", "jump"]
                .map(|v| bytes::Bytes::from_static(v.as_bytes()))
                .to_vec(),
        );
        let payload = InsertPayload::from(source);
        let got = collect(payload).await?;
        assert_eq!(got[..], CONTENTS[..]);

        Ok(())
    }

    #[tokio::test]
    async fn empty_file() -> Result {
        let file = NamedTempFile::new()?;
        let read = file.reopen()?;
        let got = collect(tokio::fs::File::from(read)).await?;
        assert!(got.is_empty(), "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn small_file() -> Result {
        let mut file = NamedTempFile::new()?;
        assert_eq!(file.write(CONTENTS)?, CONTENTS.len());
        file.flush()?;
        let read = file.reopen()?;
        let got = collect(tokio::fs::File::from(read)).await?;
        assert_eq!(got[..], CONTENTS[..], "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn small_file_seek() -> Result {
        let mut file = NamedTempFile::new()?;
        assert_eq!(file.write(CONTENTS)?, CONTENTS.len());
        file.flush()?;
        let mut read = tokio::fs::File::from(file.reopen()?);
        read.seek(8).await?;
        let got = collect(read).await?;
        assert_eq!(got[..], CONTENTS[8..], "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn larger_file() -> Result {
        let mut file = NamedTempFile::new()?;
        assert_eq!(file.write(&[0_u8; READ_SIZE])?, READ_SIZE);
        assert_eq!(file.write(&[1_u8; READ_SIZE])?, READ_SIZE);
        assert_eq!(file.write(&[2_u8; READ_SIZE])?, READ_SIZE);
        assert_eq!(file.write(&[3_u8; READ_SIZE])?, READ_SIZE);
        file.flush()?;
        assert_eq!(READ_SIZE % 2, 0);
        let mut read = tokio::fs::File::from(file.reopen()?);
        read.seek((READ_SIZE + READ_SIZE / 2) as u64).await?;
        let got = collect(read).await?;
        let mut want = Vec::new();
        want.extend_from_slice(&[1_u8; READ_SIZE / 2]);
        want.extend_from_slice(&[2_u8; READ_SIZE]);
        want.extend_from_slice(&[3_u8; READ_SIZE]);
        assert_eq!(got[..], want[..], "{got:?}");
        Ok(())
    }

    pub struct VecStream {
        contents: Vec<bytes::Bytes>,
        current: VecDeque<std::io::Result<bytes::Bytes>>,
    }

    impl VecStream {
        pub fn new(contents: Vec<bytes::Bytes>) -> Self {
            let current: VecDeque<std::io::Result<_>> =
                contents.iter().map(|x| Ok(x.clone())).collect();
            Self { contents, current }
        }
    }

    impl StreamingSource for VecStream {
        type Error = std::io::Error;

        async fn next(&mut self) -> Option<std::result::Result<bytes::Bytes, Self::Error>> {
            self.current.pop_front()
        }

        fn size_hint(&self) -> (u64, Option<u64>) {
            let s = self.contents.iter().fold(0_u64, |a, i| a + i.len() as u64);
            (s, Some(s))
        }
    }

    impl Seek for VecStream {
        type Error = std::io::Error;

        async fn seek(&mut self, _offset: u64) -> std::result::Result<(), Self::Error> {
            panic!(); // The tests do not use this (yet).
        }
    }
}
