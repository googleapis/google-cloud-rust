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
use crate::streaming_source::{Payload, StreamingSource};
#[cfg(feature = "unstable-stream")]
use futures::Stream;

/// The result of a `ReadObject` request.
///
/// Objects can be large, and must be returned as a stream of bytes. This struct
/// also provides an accessor to retrieve the object's metadata.
#[derive(Debug)]
pub struct ReadObjectResponse {
    inner: Box<dyn dynamic::ReadObjectResponse + Send>,
}

impl ReadObjectResponse {
    pub(crate) fn new<T>(inner: Box<T>) -> Self
    where
        T: dynamic::ReadObjectResponse + Send + 'static,
    {
        Self { inner }
    }

    #[cfg(google_cloud_unstable_storage_bidi)]
    pub(crate) fn from_dyn(inner: Box<dyn dynamic::ReadObjectResponse + Send>) -> Self {
        Self { inner }
    }

    /// Create a ReadObjectResponse, given a data source.
    ///
    /// Use this method to mock the return type of
    /// [Storage::read_object][crate::client::Storage::read_object].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::model_ext::ObjectHighlights;
    /// # use google_cloud_storage::read_object::ReadObjectResponse;
    /// let object = ObjectHighlights::default();
    /// let response = ReadObjectResponse::from_source(object, "payload");
    /// ```
    pub fn from_source<T, S>(object: ObjectHighlights, source: T) -> Self
    where
        T: Into<Payload<S>> + Send + Sync + 'static,
        S: StreamingSource + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(FakeReadObjectResponse::<S> {
                object,
                source: source.into(),
            }),
        }
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

struct FakeReadObjectResponse<T>
where
    T: StreamingSource + Send + Sync + 'static,
{
    object: ObjectHighlights,
    source: Payload<T>,
}

impl<T> std::fmt::Debug for FakeReadObjectResponse<T>
where
    T: StreamingSource + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeReadObjectResponse")
            .field("object", &self.object)
            // skip source, as it is not `Debug`
            .finish()
    }
}

#[async_trait::async_trait]
impl<T> dynamic::ReadObjectResponse for FakeReadObjectResponse<T>
where
    T: StreamingSource + Send + Sync + 'static,
{
    fn object(&self) -> ObjectHighlights {
        self.object.clone()
    }

    async fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        self.source
            .next()
            .await
            .map(|r| r.map_err(gax::error::Error::io))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn from_source() -> anyhow::Result<()> {
        const LAZY: &str = "the quick brown fox jumps over the lazy dog";
        let object = ObjectHighlights {
            etag: "custom-etag".to_string(),
            ..Default::default()
        };

        let mut response = ReadObjectResponse::from_source(object.clone(), LAZY);
        assert_eq!(&object, &response.object());
        let mut contents = Vec::new();
        while let Some(chunk) = response.next().await.transpose()? {
            contents.extend_from_slice(&chunk);
        }
        let contents = bytes::Bytes::from_owner(contents);
        assert_eq!(contents, LAZY);
        Ok(())
    }
}
