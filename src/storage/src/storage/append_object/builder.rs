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
use crate::model::{Object, WriteObjectSpec};
use crate::model_ext::OpenAppendableObjectRequest;
use crate::storage::append_object::writer::AppendableObjectWriter;
use crate::storage::request_options::RequestOptions;
use std::sync::Arc;

/// A builder for configuring and initiating an appendable object upload.
#[derive(Debug)]
pub struct OpenAppendableObject<S = crate::storage::transport::Storage> {
    stub: Arc<S>,
    request: OpenAppendableObjectRequest,
    options: RequestOptions,
}

impl<S> OpenAppendableObject<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    pub(crate) fn new(
        stub: Arc<S>,
        bucket: impl Into<String>,
        object: impl Into<String>,
        options: RequestOptions,
    ) -> Self {
        let resource = Object::new().set_bucket(bucket).set_name(object);

        let spec = WriteObjectSpec::new()
            .set_resource(resource)
            .set_appendable(true);

        Self {
            stub,
            request: OpenAppendableObjectRequest { spec, params: None },
            options,
        }
    }

    /// Opens the stream to append data.
    pub async fn send(self) -> Result<AppendableObjectWriter> {
        // TODO: Add a test that verifies this builder sets up the request correctly and calls send.
        self.stub
            .open_appendable_object(self.request, self.options)
            .await
    }
}
