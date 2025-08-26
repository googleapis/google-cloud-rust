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

/// The result of a `ReadObject` request.
///
/// Objects can be large, and must be returned as a stream of bytes. This struct
/// also provides an accessor to retrieve the object's metadata.
#[derive(Debug)]
pub struct ReadObjectResponse {
    inner: Box<dyn dynamic::ReadObjectResponse>,
}

impl ReadObjectResponse {
    pub(crate) fn new<T>(inner: Box<T>) -> Self
    where
        T: dynamic::ReadObjectResponse + 'static,
    {
        Self { inner }
    }

    /// Get the highlights of the object metadata included in the
    /// response.
    ///
    /// To get full metadata about this object, use [crate::client::StorageControl::get_object].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
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
    pub fn object(&self) -> ObjectHighlights {
        self.inner.object()
    }

    /// Stream the next bytes of the object.
    ///
    /// When the response has been exhausted, this will return None.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut resp = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    /// while let Some(next) = resp.next().await {
    ///     println!("next={:?}", next?);
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        self.inner.next().await
    }

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Convert the response to a [Stream].
    pub fn into_stream(self) -> impl Stream<Item = Result<bytes::Bytes>> + Unpin {
        use futures::stream::unfold;
        Box::pin(unfold(Some(self), move |state| async move {
            if let Some(mut this) = state {
                if let Some(chunk) = this.next().await {
                    return Some((chunk, Some(this)));
                }
            };
            None
        }))
    }
}

pub(crate) mod dynamic {
    use crate::Result;
    use crate::model_ext::ObjectHighlights;

    /// A trait representing the interface to read an object
    #[async_trait::async_trait]
    pub trait ReadObjectResponse: std::fmt::Debug {
        fn object(&self) -> ObjectHighlights;
        async fn next(&mut self) -> Option<Result<bytes::Bytes>>;
    }
}
