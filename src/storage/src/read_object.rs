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

//! Defines the return interface for [Storage::read_object][crate::client::Storage::read_object]

use crate::Result;
use crate::model_ext::ObjectHighlights;
#[cfg(feature = "unstable-stream")]
use futures::Stream;

mod sealed {
    pub trait ReadObjectResponse {}
}

impl<T> sealed::ReadObjectResponse for T where T: ReadObjectResponse {}

/// A trait representing the interface to read an object
pub trait ReadObjectResponse: sealed::ReadObjectResponse + std::fmt::Debug {
    /// Get the highlights of the object metadata included in the
    /// response.
    ///
    /// To get full metadata about this object, use [crate::client::StorageControl::get_object].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::read_object::ReadObjectResponse;
    /// let object = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?
    ///     .object();
    /// println!("object generation={}", object.generation);
    /// println!("object metageneration={}", object.metageneration);
    /// println!("object size={}", object.size);
    /// println!("object content encoding={}", object.content_encoding);
    /// # Ok(()) }
    /// ```
    fn object(&self) -> ObjectHighlights;

    /// Stream the next bytes of the object.
    ///
    /// When the response has been exhausted, this will return None.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::read_object::ReadObjectResponse;
    /// let mut resp = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    /// while let Some(next) = resp.next().await {
    ///     println!("next={:?}", next?);
    /// }
    /// # Ok(()) }
    /// ```
    fn next(&mut self) -> impl Future<Output = Option<Result<bytes::Bytes>>> + Send;

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Convert the response to a [Stream].
    fn into_stream(self) -> impl Stream<Item = Result<bytes::Bytes>> + Unpin;
}
