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

use std::collections::VecDeque;

/// The *total* number of bytes expected in a [StreamingSource].
pub type SizeHint = http_body::SizeHint;

/// The payload for object uploads via the [Storage][crate::client::Storage]
/// client.
///
/// The storage client functions to upload new objects consume any type that can
/// be converted to this type. That includes simple buffers, and any type
/// implementing [StreamingSource].
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::streaming_source::Payload;
/// use google_cloud_storage::streaming_source::StreamingSource;
/// let buffer : &[u8] = b"the quick brown fox jumps over the lazy dog";
/// let mut size = 0_usize;
/// let mut payload = Payload::from(bytes::Bytes::from_static(buffer));
/// while let Some(bytes) = payload.next().await.transpose()? {
///     size += bytes.len();
/// }
/// assert_eq!(size, buffer.len());
/// # anyhow::Result::<()>::Ok(()) });
/// ```
pub struct Payload<T> {
    payload: T,
}

impl<T> Payload<T>
where
    T: StreamingSource,
{
    pub fn from_stream(payload: T) -> Self {
        Self { payload }
    }
}

impl<T> StreamingSource for Payload<T>
where
    T: StreamingSource + Send + Sync,
{
    type Error = T::Error;

    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        self.payload.next().await
    }

    async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
        self.payload.size_hint().await
    }
}

impl<T> Seek for Payload<T>
where
    T: Seek,
{
    type Error = T::Error;

    fn seek(&mut self, offset: u64) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.payload.seek(offset)
    }
}

impl From<bytes::Bytes> for Payload<BytesSource> {
    fn from(value: bytes::Bytes) -> Self {
        let payload = BytesSource::new(value);
        Self { payload }
    }
}

impl From<&'static str> for Payload<BytesSource> {
    fn from(value: &'static str) -> Self {
        let b = bytes::Bytes::from_static(value.as_bytes());
        Payload::from(b)
    }
}

impl From<Vec<bytes::Bytes>> for Payload<IterSource> {
    fn from(value: Vec<bytes::Bytes>) -> Self {
        let payload = IterSource::new(value);
        Self { payload }
    }
}

impl<S> From<S> for Payload<S>
where
    S: StreamingSource,
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
    /// If the maximum size is known and sufficiently small, the client library
    /// may be able to use a more efficient protocol for the upload.
    fn size_hint(&self) -> impl Future<Output = Result<SizeHint, Self::Error>> + Send {
        std::future::ready(Ok(SizeHint::new()))
    }
}

/// Provides bytes for an upload from sources that support seek.
///
/// Implementations of this trait provide data for Google Cloud Storage uploads.
/// The data may be received asynchronously, such as reads from Google Cloud
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

impl From<tokio::fs::File> for Payload<FileSource> {
    fn from(value: tokio::fs::File) -> Self {
        Self {
            payload: FileSource::new(value),
        }
    }
}

/// Implements [StreamingSource] for a [tokio::fs::File].
///
/// # Example
/// ```
/// # use google_cloud_storage::client::Storage;
/// # async fn sample(client: &Storage) -> anyhow::Result<()> {
/// let payload = tokio::fs::File::open("my-data").await?;
/// let response = client
///     .upload_object("projects/_/buckets/my-bucket", "my-object", payload)
///     .send_unbuffered()
///     .await?;
/// println!("response details={response:?}");
/// # Ok(()) }
/// ```
pub struct FileSource {
    inner: tokio::fs::File,
}

impl FileSource {
    fn new(inner: tokio::fs::File) -> Self {
        Self { inner }
    }
}

impl StreamingSource for FileSource {
    type Error = std::io::Error;

    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        let mut buffer = vec![0_u8; READ_SIZE];
        match tokio::io::AsyncReadExt::read(&mut self.inner, &mut buffer).await {
            Err(e) => Some(Err(e)),
            Ok(0) => None,
            Ok(n) => {
                buffer.resize(n, 0_u8);
                Some(Ok(bytes::Bytes::from_owner(buffer)))
            }
        }
    }
    async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
        let m = self.inner.metadata().await?;
        Ok(SizeHint::with_exact(m.len()))
    }
}

impl Seek for FileSource {
    type Error = std::io::Error;

    async fn seek(&mut self, offset: u64) -> Result<(), Self::Error> {
        use tokio::io::AsyncSeekExt;
        let _ = self.inner.seek(std::io::SeekFrom::Start(offset)).await?;
        Ok(())
    }
}

/// Implements [StreamingSource] for [bytes::Bytes].
///
/// # Example
/// ```
/// # use google_cloud_storage::client::Storage;
/// # async fn sample(client: &Storage) -> anyhow::Result<()> {
/// let payload = bytes::Bytes::from_static(b"Hello World!");
/// let response = client
///     .upload_object("projects/_/buckets/my-bucket", "my-object", payload)
///     .send_unbuffered()
///     .await?;
/// println!("response details={response:?}");
/// # Ok(()) }
/// ```
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

    async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
        let s = self.contents.len() as u64;
        Ok(SizeHint::with_exact(s))
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

/// Implements [StreamingSource] for a sequence of [bytes::Bytes].
pub(crate) struct IterSource {
    contents: Vec<bytes::Bytes>,
    current: VecDeque<bytes::Bytes>,
}

impl IterSource {
    pub(crate) fn new<I>(iterator: I) -> Self
    where
        I: IntoIterator<Item = bytes::Bytes>,
    {
        let contents: Vec<bytes::Bytes> = iterator.into_iter().collect();
        let current: VecDeque<bytes::Bytes> = contents.iter().cloned().collect();
        Self { contents, current }
    }
}

impl StreamingSource for IterSource {
    type Error = std::io::Error;

    async fn next(&mut self) -> Option<std::result::Result<bytes::Bytes, Self::Error>> {
        self.current.pop_front().map(Ok)
    }

    async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
        let s = self.contents.iter().fold(0_u64, |a, i| a + i.len() as u64);
        Ok(SizeHint::with_exact(s))
    }
}

impl Seek for IterSource {
    type Error = std::io::Error;
    async fn seek(&mut self, offset: u64) -> std::result::Result<(), Self::Error> {
        let mut current = VecDeque::new();
        let mut offset = offset as usize;
        for b in self.contents.iter() {
            offset = match (offset, b.len()) {
                (0, _) => {
                    current.push_back(b.clone());
                    0
                }
                (o, n) if o >= n => o - n,
                (o, n) => {
                    current.push_back(b.clone().split_off(n - o));
                    0
                }
            }
        }
        self.current = current;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    type Result = anyhow::Result<()>;

    const CONTENTS: &[u8] = b"how vexingly quick daft zebras jump";

    pub(crate) struct UnknownSize {
        inner: BytesSource,
    }
    impl UnknownSize {
        pub fn new(inner: BytesSource) -> Self {
            Self { inner }
        }
    }
    impl Seek for UnknownSize {
        type Error = <BytesSource as Seek>::Error;
        async fn seek(&mut self, offset: u64) -> std::result::Result<(), Self::Error> {
            self.inner.seek(offset).await
        }
    }
    impl StreamingSource for UnknownSize {
        type Error = <BytesSource as StreamingSource>::Error;
        async fn next(&mut self) -> Option<std::result::Result<bytes::Bytes, Self::Error>> {
            self.inner.next().await
        }
        async fn size_hint(&self) -> std::result::Result<SizeHint, Self::Error> {
            let inner = self.inner.size_hint().await?;
            let mut hint = SizeHint::default();
            hint.set_lower(inner.lower());
            Ok(hint)
        }
    }

    mockall::mock! {
        pub(crate) SimpleSource {}

        impl StreamingSource for SimpleSource {
            type Error = std::io::Error;
            async fn next(&mut self) -> Option<std::result::Result<bytes::Bytes, std::io::Error>>;
            async fn size_hint(&self) -> std::result::Result<SizeHint, std::io::Error>;
        }
    }

    mockall::mock! {
        pub(crate) SeekSource {}

        impl StreamingSource for SeekSource {
            type Error = std::io::Error;
            async fn next(&mut self) -> Option<std::result::Result<bytes::Bytes, std::io::Error>>;
            async fn size_hint(&self) -> std::result::Result<SizeHint, std::io::Error>;
        }
        impl Seek for SeekSource {
            type Error = std::io::Error;
            async fn seek(&mut self, offset: u64) ->std::result::Result<(), std::io::Error>;
        }
    }

    /// A helper function to simplify the tests.
    async fn collect<S>(mut source: S) -> anyhow::Result<Vec<u8>>
    where
        S: StreamingSource,
    {
        collect_mut(&mut source).await
    }

    /// A helper function to simplify the tests.
    async fn collect_mut<S>(source: &mut S) -> anyhow::Result<Vec<u8>>
    where
        S: StreamingSource,
    {
        let mut vec = Vec::new();
        while let Some(bytes) = source.next().await.transpose()? {
            vec.extend_from_slice(&bytes);
        }
        Ok(vec)
    }

    #[tokio::test]
    async fn empty_bytes() -> Result {
        let buffer = Payload::from(bytes::Bytes::default());
        let range = buffer.size_hint().await?;
        assert_eq!(range.exact(), Some(0));
        let got = collect(buffer).await?;
        assert!(got.is_empty(), "{got:?}");

        Ok(())
    }

    #[tokio::test]
    async fn simple_bytes() -> Result {
        let buffer = Payload::from(bytes::Bytes::from_static(CONTENTS));
        let range = buffer.size_hint().await?;
        assert_eq!(range.exact(), Some(CONTENTS.len() as u64));
        let got = collect(buffer).await?;
        assert_eq!(got[..], CONTENTS[..], "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn simple_str() -> Result {
        const LAZY: &str = "the quick brown fox jumps over the lazy dog";
        let buffer = Payload::from(LAZY);
        let range = buffer.size_hint().await?;
        assert_eq!(range.exact(), Some(LAZY.len() as u64));
        let got = collect(buffer).await?;
        assert_eq!(&got, LAZY.as_bytes(), "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn seek_bytes() -> Result {
        let mut buffer = Payload::from(bytes::Bytes::from_static(CONTENTS));
        buffer.seek(8).await?;
        let got = collect(buffer).await?;
        assert_eq!(got[..], CONTENTS[8..], "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn empty_stream() -> Result {
        let source = IterSource::new(vec![]);
        let payload = Payload::from(source);
        let range = payload.size_hint().await?;
        assert_eq!(range.exact(), Some(0));
        let got = collect(payload).await?;
        assert!(got.is_empty(), "{got:?}");

        Ok(())
    }

    #[tokio::test]
    async fn simple_stream() -> Result {
        let source = IterSource::new(
            ["how ", "vexingly ", "quick ", "daft ", "zebras ", "jump"]
                .map(|v| bytes::Bytes::from_static(v.as_bytes())),
        );
        let payload = Payload::from_stream(source);
        let got = collect(payload).await?;
        assert_eq!(got[..], CONTENTS[..]);

        Ok(())
    }

    #[tokio::test]
    async fn empty_file() -> Result {
        let file = NamedTempFile::new()?;
        let read = tokio::fs::File::from(file.reopen()?);
        let payload = Payload::from(read);
        let hint = payload.size_hint().await?;
        assert_eq!(hint.exact(), Some(0));
        let got = collect(payload).await?;
        assert!(got.is_empty(), "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn small_file() -> Result {
        let mut file = NamedTempFile::new()?;
        assert_eq!(file.write(CONTENTS)?, CONTENTS.len());
        file.flush()?;
        let read = tokio::fs::File::from(file.reopen()?);
        let payload = Payload::from(read);
        let hint = payload.size_hint().await?;
        let s = CONTENTS.len() as u64;
        assert_eq!(hint.exact(), Some(s));
        let got = collect(payload).await?;
        assert_eq!(got[..], CONTENTS[..], "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn small_file_seek() -> Result {
        let mut file = NamedTempFile::new()?;
        assert_eq!(file.write(CONTENTS)?, CONTENTS.len());
        file.flush()?;
        let read = tokio::fs::File::from(file.reopen()?);
        let mut payload = Payload::from(read);
        payload.seek(8).await?;
        let got = collect(payload).await?;
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
        let read = tokio::fs::File::from(file.reopen()?);
        let mut payload = Payload::from(read);
        payload.seek((READ_SIZE + READ_SIZE / 2) as u64).await?;
        let got = collect(payload).await?;
        let mut want = Vec::new();
        want.extend_from_slice(&[1_u8; READ_SIZE / 2]);
        want.extend_from_slice(&[2_u8; READ_SIZE]);
        want.extend_from_slice(&[3_u8; READ_SIZE]);
        assert_eq!(got[..], want[..], "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn iter_source_full() -> Result {
        const N: usize = 32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&[1_u8; N]);
        buf.extend_from_slice(&[2_u8; N]);
        buf.extend_from_slice(&[3_u8; N]);
        let b = bytes::Bytes::from_owner(buf);

        let mut stream =
            IterSource::new(vec![b.slice(0..N), b.slice(N..(2 * N)), b.slice((2 * N)..)]);
        assert_eq!(stream.size_hint().await?.exact(), Some(3 * N as u64));

        // test_case() is not appropriate here: we want to verify seek() works
        // multiple times over the *same* stream.
        for offset in [0, N / 2, 0, N, 0, 2 * N + N / 2] {
            stream.seek(offset as u64).await?;
            let got = collect_mut(&mut stream).await?;
            assert_eq!(got[..], b[offset..(3 * N)]);
        }

        Ok(())
    }
}
