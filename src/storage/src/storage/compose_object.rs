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
use crate::model::Object;
use crate::request_options::RequestOptions;
use std::sync::Arc;

/// A request builder for [Storage::compose_object][crate::client::Storage::compose_object].
///
/// # Example
/// ```
/// use google_cloud_storage::client::Storage;
/// # use google_cloud_storage::builder::storage::ComposeObject;
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     let response = client
///         .compose_object("projects/_/buckets/my-bucket", "composite-object")
///         .add_source("part-1")
///         .add_source_with_generation("part-2", 123456)
///         .set_content_type("application/json")
///         .send()
///         .await?;
///     println!("composite object details={response:?}");
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ComposeObject<S = crate::storage::transport::Storage> {
    stub: Arc<S>,
    request: crate::model::ComposeObjectRequest,
    options: RequestOptions,
}

impl<S> ComposeObject<S> {
    pub(crate) fn new<B, D>(
        stub: Arc<S>,
        bucket: B,
        destination: D,
        options: RequestOptions,
    ) -> Self
    where
        B: Into<String>,
        D: Into<String>,
    {
        let mut request = crate::model::ComposeObjectRequest::default();
        request.destination = Some(crate::model::Object {
            bucket: bucket.into(),
            name: destination.into(),
            ..Default::default()
        });
        Self {
            stub,
            request,
            options,
        }
    }
}

impl<S> ComposeObject<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    /// Appends a source object name to the compose request.
    pub fn add_source<T: Into<String>>(mut self, name: T) -> Self {
        let mut source = crate::model::compose_object_request::SourceObject::default();
        source.name = name.into();
        self.request.source_objects.push(source);
        self
    }

    /// Appends a source object name and its expected generation to the compose request.
    pub fn add_source_with_generation<T: Into<String>>(mut self, name: T, generation: i64) -> Self {
        let mut source = crate::model::compose_object_request::SourceObject::default();
        source.name = name.into();
        source.generation = generation;
        self.request.source_objects.push(source);
        self
    }

    /// Appends a source object name, generation, and generation match precondition.
    pub fn add_source_with_preconditions<T: Into<String>>(
        mut self,
        name: T,
        generation: i64,
        if_generation_match: i64,
    ) -> Self {
        let mut source = crate::model::compose_object_request::SourceObject::default();
        source.name = name.into();
        source.generation = generation;
        let mut preconditions = crate::model::compose_object_request::source_object::ObjectPreconditions::default();
        preconditions.if_generation_match = Some(if_generation_match);
        source.object_preconditions = Some(preconditions);
        self.request.source_objects.push(source);
        self
    }

    /// Sets the generation match precondition for the destination object.
    pub fn set_if_generation_match(mut self, v: i64) -> Self {
        self.request.if_generation_match = Some(v);
        self
    }

    /// Sets the metageneration match precondition for the destination object.
    pub fn set_if_metageneration_match(mut self, v: i64) -> Self {
        self.request.if_metageneration_match = Some(v);
        self
    }

    /// Sets the content type for the destination composite object.
    pub fn set_content_type<T: Into<String>>(mut self, v: T) -> Self {
        if let Some(ref mut dest) = self.request.destination {
            dest.content_type = v.into();
        }
        self
    }

    /// Sets custom metadata attributes for the destination composite object.
    pub fn set_metadata(mut self, metadata: std::collections::HashMap<String, String>) -> Self {
        if let Some(ref mut dest) = self.request.destination {
            dest.metadata = metadata;
        }
        self
    }

    /// Configures a custom retry policy for this compose request.
    pub fn with_retry_policy<V: Into<google_cloud_gax::retry_policy::RetryPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_policy = v.into().into();
        self
    }

    /// Sends the compose request to the GCS service and returns the created composite object.
    pub async fn send(self) -> Result<Object> {
        self.stub.compose_object(self.request, self.options).await
    }
}
