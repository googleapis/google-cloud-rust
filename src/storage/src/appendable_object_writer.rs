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

//! Defines the return type for [Storage::open_appendable_object][crate::client::Storage::open_appendable_object].

use crate::storage::bidi_write::stub::dynamic::AppendableObjectWriter as AppendableObjectWriterStub;
use bytes::Bytes;

/// An open appendable object ready to write chunks.
///
/// # Example
/// ```
/// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
/// use bytes::Bytes;
/// # async fn sample() -> anyhow::Result<()> {
/// let mut writer = open();
///
/// // Append 2000 bytes.
/// writer.append(Bytes::from(vec![0u8; 2000])).await?;
/// writer.finalize().await?;
///
/// fn open() -> AppendableObjectWriter {
/// # panic!()
/// // ... details omitted ...
/// }
/// # Ok(()) }
/// ```
///
/// This is analogous to a "file descriptor". It represents an object in Cloud
/// Storage that has been "opened" and is ready for more write operations. An
/// appendable object writer processes appends sequentially.
///
/// There are strict guarantees about the order of the appends. The client library
/// ensures data is sent to the backend in the order `append()` is called.
#[derive(Debug)]
pub struct AppendableObjectWriter {
    inner: Box<dyn AppendableObjectWriterStub>,
}

impl AppendableObjectWriter {
    /// Append a chunk of data to a local buffer.
    /// User should call flush/close/finalize to persist the bytes to the server.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
    /// use bytes::Bytes;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let mut writer = open();
    /// writer.append(Bytes::from("hello ")).await?;
    /// writer.append(Bytes::from("world")).await?;
    ///
    /// fn open() -> AppendableObjectWriter {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn append(&mut self, chunk: Bytes) -> crate::Result<()> {
        self.inner.append(chunk).await
    }

    /// Flushes the stream and blocks until the server confirms the bytes are persisted.
    /// Returns the `persisted_size`.
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

    /// Flushes the stream and finalizes the object on the server.
    /// After this call, no further appends can be done to the same object.
    /// Returns the `Object` metadata.
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
    pub async fn finalize(self) -> crate::Result<crate::model::Object> {
        self.inner.finalize().await
    }

    /// Flushes the stream and blocks until the server confirms the persisted bytes.
    /// Then closes the stream.
    /// Returns the `persisted_size`.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let mut writer = open();
    /// let persisted_size = writer.close().await?;
    /// println!("persisted {} bytes", persisted_size);
    ///
    /// fn open() -> AppendableObjectWriter {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn close(self) -> crate::Result<i64> {
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

    pub(crate) fn into_parts(self) -> Box<dyn AppendableObjectWriterStub> {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mockall::mock;

    #[tokio::test]
    async fn test_appendable_object_writer_delegates() -> crate::Result<()> {
        let mut mock = MockWriter::new();
        mock.expect_append()
            .with(mockall::predicate::eq(Bytes::from("test")))
            .returning(|_| Ok(()));
        mock.expect_flush().returning(|| Ok(123));
        mock.expect_generation().returning(|| 1);
        mock.expect_persisted_size().returning(|| 123);

        let mut writer = AppendableObjectWriter::new(mock);
        writer.append(Bytes::from("test")).await?;
        assert_eq!(writer.flush().await?, 123);
        assert_eq!(writer.generation(), 1);
        assert_eq!(writer.persisted_size(), 123);

        let mut mock = MockWriter::new();
        mock.expect_finalize().returning(|| {
            Ok(crate::model::Object {
                size: 456,
                ..Default::default()
            })
        });
        let writer = AppendableObjectWriter::new(mock);
        assert_eq!(writer.finalize().await?.size, 456);

        let mut mock = MockWriter::new();
        mock.expect_close().returning(|| Ok(789));
        let writer = AppendableObjectWriter::new(mock);
        assert_eq!(writer.close().await?, 789);

        Ok(())
    }

    mock! {
        #[derive(Debug)]
        Writer {}
        impl crate::stub::AppendableObjectWriter for Writer {
            async fn append(&mut self, chunk: Bytes) -> crate::Result<()>;
            async fn flush(&mut self) -> crate::Result<i64>;
            async fn finalize(self) -> crate::Result<crate::model::Object>;
            async fn close(self) -> crate::Result<i64>;
            fn generation(&self) -> i64;
            fn persisted_size(&self) -> i64;
        }
    }
}
