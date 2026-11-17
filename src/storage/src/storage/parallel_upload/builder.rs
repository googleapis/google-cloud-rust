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

use crate::Result;
use crate::model::Object;
use crate::model_ext::CreateObjectWriteSessionRequest;
use crate::parallel_upload_session::ParallelUploadSession;
use crate::request_options::RequestOptions;
use std::sync::Arc;

/// A request builder for [Storage::parallel_upload][crate::client::Storage::parallel_upload].
#[cfg_attr(docsrs, doc(cfg(google_cloud_unstable_storage_parallel_upload)))]
#[derive(Clone, Debug)]
pub struct ParallelUpload<S = crate::storage::transport::Storage> {
    pub(crate) stub: Arc<S>,
    pub(crate) request: CreateObjectWriteSessionRequest,
    pub(crate) max_concurrency: Option<usize>,
    pub(crate) enable_hedging: bool,
    pub(crate) options: RequestOptions,
}

impl<S> ParallelUpload<S> {
    pub(crate) fn new<B, O>(stub: Arc<S>, bucket: B, object: O, options: RequestOptions) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        let request = CreateObjectWriteSessionRequest {
            resource: Some(
                Object::new()
                    .set_bucket(bucket.into())
                    .set_name(object.into()),
            ),
            ..CreateObjectWriteSessionRequest::default()
        };
        Self {
            stub,
            request,
            max_concurrency: None,
            enable_hedging: false,
            options,
        }
    }

    /// Sets the maximum number of concurrent segment uploads.
    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = Some(max_concurrency);
        self
    }

    /// Enables or disables hedging for segment uploads.
    pub fn with_enable_hedging(mut self, enable_hedging: bool) -> Self {
        self.enable_hedging = enable_hedging;
        self
    }
}

impl<S> ParallelUpload<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    /// Creates a new parallel upload session without uploading payload bytes yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the session creation RPC fails.
    pub async fn create_session(self) -> Result<ParallelUploadSession> {
        let _ = (
            self.stub,
            self.request,
            self.max_concurrency,
            self.enable_hedging,
            self.options,
        );
        unimplemented!("{}", gaxi::unimplemented::UNIMPLEMENTED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Storage;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use static_assertions::assert_impl_all;

    const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";
    const OBJECT_NAME: &str = "test-object";

    #[tokio::test]
    async fn traits_and_defaults() -> anyhow::Result<()> {
        fn assert_send<T: Send>(_: &T) {}
        fn assert_static<T: 'static>(_: &T) {}

        assert_impl_all!(ParallelUpload: Clone, std::fmt::Debug, Send, Sync);

        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let builder = client.parallel_upload(BUCKET_NAME, OBJECT_NAME);
        assert_static(&builder);

        let resource = builder.request.resource.as_ref().expect("resource set");
        assert_eq!(resource.bucket, BUCKET_NAME);
        assert_eq!(resource.name, OBJECT_NAME);
        assert!(builder.request.predefined_acl.is_empty());
        assert_eq!(builder.request.params, None);
        assert_eq!(builder.request.preferred_segment_size_bytes, 0);
        assert_eq!(builder.max_concurrency, None);
        assert!(!builder.enable_hedging);

        let create_fut = builder.create_session();
        assert_send(&create_fut);
        assert_static(&create_fut);
        drop(create_fut);

        Ok(())
    }

    #[derive(Debug)]
    struct StorageStub;
    impl crate::stub::Storage for StorageStub {}

    #[test]
    fn options() {
        let builder = ParallelUpload::new(
            Arc::new(StorageStub),
            BUCKET_NAME,
            OBJECT_NAME,
            RequestOptions::new(),
        )
        .with_max_concurrency(8)
        .with_enable_hedging(true);

        assert_eq!(builder.max_concurrency, Some(8));
        assert!(builder.enable_hedging);
    }
}
