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
use crate::model::{MoveObjectRequest, Object};
use crate::storage::request_options::RequestOptions;
use std::sync::Arc;

/// Request builder for [Storage::move_object][crate::client::Storage::move_object] calls.
#[derive(Clone, Debug)]
pub struct MoveObject<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    stub: Arc<S>,
    request: MoveObjectRequest,
    options: RequestOptions,
}

impl<S> MoveObject<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    pub(crate) fn new<B, Src, D>(
        stub: Arc<S>,
        bucket: B,
        source_object: Src,
        destination_object: D,
        options: RequestOptions,
    ) -> Self
    where
        B: Into<String>,
        Src: Into<String>,
        D: Into<String>,
    {
        let mut request = MoveObjectRequest::default();
        request.bucket = bucket.into();
        request.source_object = source_object.into();
        request.destination_object = destination_object.into();
        Self {
            stub,
            request,
            options,
        }
    }

    // Preconditions
    pub fn if_source_generation_match(mut self, v: i64) -> Self {
        self.request.if_source_generation_match = Some(v);
        self
    }

    pub fn if_source_generation_not_match(mut self, v: i64) -> Self {
        self.request.if_source_generation_not_match = Some(v);
        self
    }

    pub fn if_source_metageneration_match(mut self, v: i64) -> Self {
        self.request.if_source_metageneration_match = Some(v);
        self
    }

    pub fn if_source_metageneration_not_match(mut self, v: i64) -> Self {
        self.request.if_source_metageneration_not_match = Some(v);
        self
    }

    pub fn if_generation_match(mut self, v: i64) -> Self {
        self.request.if_generation_match = Some(v);
        self
    }

    pub fn if_generation_not_match(mut self, v: i64) -> Self {
        self.request.if_generation_not_match = Some(v);
        self
    }

    pub fn if_metageneration_match(mut self, v: i64) -> Self {
        self.request.if_metageneration_match = Some(v);
        self
    }

    pub fn if_metageneration_not_match(mut self, v: i64) -> Self {
        self.request.if_metageneration_not_match = Some(v);
        self
    }

    // Common options
    pub fn with_options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self
    }

    pub async fn send(self) -> Result<Object> {
        self.stub.move_object(self.request, self.options).await
    }
}
