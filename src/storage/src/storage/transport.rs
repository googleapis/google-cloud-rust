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
use crate::model::ReadObjectRequest;
use crate::read_object::ReadObjectResponse;
use crate::storage::client::StorageInner;
use crate::storage::read_object::{ReadObjectResponseImpl, Reader};
use crate::storage::request_options::RequestOptions;
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
}

// This is the actual class impl
impl Storage {
    pub fn new(inner: Arc<StorageInner>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}
