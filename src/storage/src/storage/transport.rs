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

use crate::Result;
use crate::model::{Object, ReadObjectRequest};
use crate::model_ext::WriteObjectRequest;
use crate::read_object::ReadObjectResponse;
use crate::storage::client::StorageInner;
use crate::storage::perform_upload::PerformUpload;
use crate::storage::read_object::{ReadObjectResponseImpl, Reader};
use crate::storage::request_options::RequestOptions;
use crate::storage::streaming_source::{Seek, StreamingSource};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

impl super::stub::Storage for Storage {
    async fn read_object(
        &self,
        req: ReadObjectRequest,
        options: RequestOptions,
    ) -> Result<ReadObjectResponse> {
        let reader = Reader {
            inner: self.inner.clone(),
            request: req,
            options,
        };
        let inner = ReadObjectResponseImpl::new(reader).await?;
        Ok(ReadObjectResponse::new(Box::new(inner)))
    }

    /// Implements [crate::client::Storage::write_object].
    async fn write_object_buffered<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        PerformUpload::new(payload, self.inner.clone(), req.spec, req.params, options)
            .send()
            .await
    }
    /// Implements [crate::client::Storage::write_object].
    async fn write_object_unbuffered<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Seek + Send + Sync + 'static,
    {
        PerformUpload::new(payload, self.inner.clone(), req.spec, req.params, options)
            .send_unbuffered()
            .await
    }
}

// This is the actual class impl
impl Storage {
    pub fn new(inner: Arc<StorageInner>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}
