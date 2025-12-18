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

//! Defines the return type for [Storage::open_object][crate::client::Storage::open_object].

use crate::model::Object;
use crate::model_ext::ReadRange;
use crate::read_object::ReadObjectResponse;
use crate::storage::bidi::stub::dynamic::ObjectDescriptor as ObjectDescriptorStub;

/// The `open_object()` metadata attributes.
pub use http::HeaderMap;

/// An open object ready to read one or more ranges.
///
/// # Example
/// ```
/// # use google_cloud_storage::object_descriptor::ObjectDescriptor;
/// use google_cloud_storage::{client::Storage, model_ext::ReadRange};
/// # async fn sample() -> anyhow::Result<()> {
/// let client = Storage::builder().build().await?;
/// let open: ObjectDescriptor = client
///     .open_object("projects/_/buckets/my-bucket", "my-object")
///     .send().await?;
/// println!("metadata = {:?}", open.object());
/// // Read 2000 bytes starting at offset 1000.
/// let mut reader = open.read_range(ReadRange::segment(1000, 2000)).await;
/// while let Some(data) = reader.next().await.transpose()? {
///   println!("received {} bytes", data.len());
/// }
/// # Ok(()) }
/// ```
///
/// This is analogous to a "file descriptor". It represents an object in Cloud
/// Storage that has been "opened" and is ready for more read operations. An
/// object descriptor can have multiple concurrent read operations at a time.
/// You may call `read_range()` even if previous reads have not completed.
///
/// There are no guarantees about the order of the responses. All the data for
/// a `read_range()` may be returned before any data of earlier `read_range()`
/// calls, or the data may arrive in any interleaved order. Naturally, the data
/// for a single ranged read arrives in order.
///
/// You should actively read from all concurrent reads: the client library uses
/// separate buffers for each `read_range()` call, but once a buffer is full the
/// library will stop delivering data to **all** the concurrent reads.
#[derive(Debug)]
pub struct ObjectDescriptor {
    inner: Box<dyn ObjectDescriptorStub>,
}

impl ObjectDescriptor {
    /// Returns the metadata for the open object.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::object_descriptor::ObjectDescriptor;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let descriptor = open();
    /// println!("object generation = {}", descriptor.object().generation);
    ///
    /// fn open() -> ObjectDescriptor {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    ///
    pub fn object(&self) -> Object {
        self.inner.object()
    }

    /// Start reading a range.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::object_descriptor::ObjectDescriptor;
    /// use google_cloud_storage::{model_ext::ReadRange, read_object::ReadObjectResponse};
    /// # async fn sample() -> anyhow::Result<()> {
    /// const MIB: u64 = 1024 * 1024;
    /// let descriptor = open();
    /// println!("object generation = {}", descriptor.object().generation);
    ///
    /// // Read 2 MiB starting at offset 0.
    /// let read1 = descriptor.read_range(ReadRange::segment(0, 2 * MIB)).await;
    /// // Concurrently read 2 MiB starting at offset 4 MiB.
    /// let read2 = descriptor.read_range(ReadRange::segment(4 * MIB, 2 * MIB)).await;
    ///
    /// let t1 = tokio::spawn(async move { do_read(read1) });
    /// let t2 = tokio::spawn(async move { do_read(read2) });
    ///
    /// async fn do_read(mut reader: ReadObjectResponse) {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// fn open() -> ObjectDescriptor {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn read_range(&self, range: ReadRange) -> ReadObjectResponse {
        self.inner.read_range(range).await
    }

    /// Returns metadata about the original `open_object()` request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::object_descriptor::ObjectDescriptor;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let descriptor = open();
    /// // Often useful when troubleshooting problems with Google Support.
    /// let header = descriptor.headers().get("x-guploader-uploadid");
    /// println!("debugging header = {:?}", header);
    ///
    /// fn open() -> ObjectDescriptor {
    /// # panic!()
    /// // ... details omitted ...
    /// }
    /// # Ok(()) }
    /// ```
    pub fn headers(&self) -> HeaderMap {
        self.inner.headers()
    }

    /// Create a new instance.
    ///
    /// Application developers should only need to create an `ObjectDescriptor`
    /// in unit tests.
    pub fn new<T>(inner: T) -> Self
    where
        T: crate::stub::ObjectDescriptor + 'static,
    {
        Self {
            inner: Box::new(inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_ext::ObjectHighlights;
    use crate::read_object::ReadObjectResponse;
    use http::{HeaderName, HeaderValue};
    use mockall::mock;

    // Verify this can be mocked inside the crate.
    // TODO(#3838) - support mocking outside the crate too.
    #[tokio::test]
    async fn can_be_mocked() -> anyhow::Result<()> {
        let object = Object::new().set_name("test-object").set_generation(123456);
        let headers = HeaderMap::from_iter(
            [
                ("content-type", "application/octet-stream"),
                ("x-guploader-uploadid", "abc-123"),
            ]
            .map(|(k, v)| (HeaderName::from_static(k), HeaderValue::from_static(v))),
        );
        let mut mock = MockDescriptor::new();
        mock.expect_object().times(1).return_const(object.clone());
        mock.expect_read_range()
            .times(1)
            .withf(|range| range.0 == ReadRange::segment(100, 200).0)
            .returning(|_| ReadObjectResponse::new(Box::new(MockResponse::new())));
        mock.expect_headers().times(1).return_const(headers.clone());

        let descriptor = ObjectDescriptor::new(mock);
        assert_eq!(descriptor.object(), object);
        assert_eq!(descriptor.headers(), headers);

        let _reader = descriptor.read_range(ReadRange::segment(100, 200)).await;
        Ok(())
    }

    mock! {
        #[derive(Debug)]
        Descriptor {}

        impl crate::stub::ObjectDescriptor for Descriptor {
            fn object(&self) -> Object;
            async fn read_range(&self, range: ReadRange) -> ReadObjectResponse;
            fn headers(&self) -> HeaderMap;
        }
    }

    mock! {
        #[derive(Debug)]
        Response {}

        #[async_trait::async_trait]
        impl crate::read_object::dynamic::ReadObjectResponse for Response {
            fn object(&self) -> ObjectHighlights;
            async fn next(&mut self) -> Option<crate::Result<bytes::Bytes>>;
        }
    }
}
