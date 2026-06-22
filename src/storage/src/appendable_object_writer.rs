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

//! Defines the return type for [Storage::open_appendable_object][crate::client::Storage::open_appendable_object].

use bytes::Bytes;
#[cfg(google_cloud_unstable_storage_bidi)]
use crate::storage::bidi_write::stub::dynamic::AppendableObjectWriter as AppendableObjectWriterStub;

/// An open appendable object ready to write chunks.
///
/// # Example
/// ```
/// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
/// use google_cloud_storage::client::Storage;
/// use bytes::Bytes;
/// # async fn sample() -> anyhow::Result<()> {
/// let client = Storage::builder().build().await?;
/// let open: AppendableObjectWriter = client
///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
///     .send().await?;
///
/// // Append 2000 bytes.
/// open.append(Bytes::from(vec![0u8; 2000])).await?;
/// open.finalize().await?;
/// # Ok(()) }
/// ```
///
/// This is analogous to a "file descriptor". It represents an object in Cloud
/// Storage that has been "opened" and is ready for more write operations. An
/// appendable object writer processes appends sequentially.
///
/// There are strict guarantees about the order of the appends. The client library
/// ensures data is sent to the backend in the order `append()` is called.
#[cfg(google_cloud_unstable_storage_bidi)]
#[derive(Debug)]
pub struct AppendableObjectWriter {
    inner: Box<dyn AppendableObjectWriterStub>,
}

#[cfg(google_cloud_unstable_storage_bidi)]
impl AppendableObjectWriter {
    /// Append a chunk of data to the object.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
    /// use bytes::Bytes;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let writer = open();
    /// writer.append(Bytes::from("hello ")).await?;
    /// writer.append(Bytes::from("world")).await?;
    ///
    /// fn open() -> AppendableObjectWriter {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn append(&mut self, chunk: bytes::Bytes) -> crate::Result<()> {
        self.inner.append(chunk).await
    }

    /// Flush pending chunks to the server, ensuring they are durably persisted.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
    /// use bytes::Bytes;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let mut writer = open();
    /// writer.append(Bytes::from("hello")).await?;
    /// let persisted_size = writer.flush().await?;
    /// println!("persisted {} bytes", persisted_size);
    ///
    /// fn open() -> AppendableObjectWriter {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn flush(&mut self) -> crate::Result<i64> {
        self.inner.flush().await
    }

    /// Finalize the upload, indicating no more data will be written.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
    /// use bytes::Bytes;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let mut writer = open();
    /// writer.append(Bytes::from("hello")).await?;
    /// let object = writer.finalize().await?;
    /// println!("final object size is {} bytes", object.size);
    ///
    /// fn open() -> AppendableObjectWriter {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn finalize(&mut self) -> crate::Result<crate::model::Object> {
        self.inner.finalize().await
    }

    /// Close the stream, dropping any unacknowledged or un-flushed data.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let mut writer = open();
    /// writer.close().await?;
    ///
    /// fn open() -> AppendableObjectWriter {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn close(&mut self) -> crate::Result<i64> {
        self.inner.close().await
    }

    /// Returns the generation of the object being appended to.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let writer = open();
    /// println!("generation = {}", writer.generation());
    ///
    /// fn open() -> AppendableObjectWriter {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub fn generation(&self) -> i64 {
        self.inner.generation()
    }

    /// Returns the current number of bytes the server has acknowledged as persisted.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let writer = open();
    /// println!("persisted_size = {}", writer.persisted_size());
    ///
    /// fn open() -> AppendableObjectWriter {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub fn persisted_size(&self) -> i64 {
        self.inner.persisted_size()
    }

    /// Create a new instance.
    ///
    /// Application developers should only need to create an `AppendableObjectWriter`
    /// in unit tests.
    pub fn new<T>(inner: T) -> Self
    where
        T: crate::stub::AppendableObjectWriter + 'static,
    {
        Self {
            inner: Box::new(inner),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_parts(self) -> Box<dyn AppendableObjectWriterStub> {
        self.inner
    }
}

#[cfg(test)]
#[cfg(google_cloud_unstable_storage_bidi)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::HeaderMap;

    mockall::mock! {
        #[derive(Debug)]
        Writer {}
        impl crate::stub::AppendableObjectWriter for Writer {
            async fn append(&self, chunk: Bytes) -> crate::Result<()>;
            async fn flush(&self) -> crate::Result<i64>;
            async fn finalize(&self) -> crate::Result<i64>;
            async fn close(&self) -> crate::Result<()>;
            fn headers(&self) -> HeaderMap;
        }
    }

    #[tokio::test]
    async fn test_appendable_object_writer_delegates() {
        let mut mock = MockWriter::new();
        mock.expect_append()
            .with(mockall::predicate::eq(Bytes::from("test")))
            .returning(|_| Ok(()));
        mock.expect_flush().returning(|| Ok(123));
        mock.expect_finalize().returning(|| Ok(456));
        mock.expect_close().returning(|| Ok(()));
        mock.expect_headers().returning(HeaderMap::new);

        let writer = AppendableObjectWriter::new(mock);
        assert!(writer.append(Bytes::from("test")).await.is_ok());
        assert_eq!(writer.flush().await.unwrap(), 123);
        assert_eq!(writer.finalize().await.unwrap(), 456);
        assert!(writer.close().await.is_ok());
        assert!(writer.headers().is_empty());
    }
}

