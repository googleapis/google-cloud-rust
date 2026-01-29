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

// Model the sidekick-generated builders for a service using LROs.

use super::*;
use gaxi::http::ReqwestClient;

#[derive(Clone, Debug)]
pub struct CreateResource {
    stub: ReqwestClient,
    request: model::CreateResourceRequest,
    options: gax::options::RequestOptions,
}

impl CreateResource {
    pub fn new(stub: ReqwestClient) -> Self {
        Self {
            stub,
            request: model::CreateResourceRequest::default(),
            options: gax::options::RequestOptions::default(),
        }
    }

    pub fn set_parent(mut self, v: impl Into<String>) -> Self {
        self.request.parent = v.into();
        self
    }

    pub fn set_id(mut self, v: impl Into<String>) -> Self {
        self.request.id = v.into();
        self
    }

    pub async fn send(self) -> gax::Result<google_cloud_longrunning::model::Operation> {
        let builder = self
            .stub
            .builder(reqwest::Method::POST, "/create".to_string())
            .query(&[("alt", "json")]);
        let r = self
            .stub
            .execute(
                builder,
                None::<gaxi::http::NoBody>,
                gax::options::RequestOptions::default(),
            )
            .await?;
        Ok(r.into_body())
    }

    pub fn poller(
        self,
    ) -> impl google_cloud_lro::Poller<super::model::Resource, super::model::CreateResourceMetadata>
    {
        type Operation = google_cloud_lro::internal::Operation<
            super::model::Resource,
            super::model::CreateResourceMetadata,
        >;

        let polling_error_policy = self.stub.get_polling_error_policy(&self.options);
        let polling_backoff_policy = self.stub.get_polling_backoff_policy(&self.options);
        let stub = self.stub.clone();
        let mut options = self.options.clone();
        options.set_retry_policy(gax::retry_policy::NeverRetry);
        let query = move |name| {
            let stub = stub.clone();
            let options = options.clone();
            async {
                let op = super::builders::GetOperation::new(stub)
                    .set_name(name)
                    .with_request_options(options)
                    .send()
                    .await?;
                Ok(Operation::new(op))
            }
        };

        let start = move || async {
            let op = self.send().await?;
            Ok(Operation::new(op))
        };
        google_cloud_lro::internal::new_poller(
            polling_error_policy,
            polling_backoff_policy,
            start,
            query,
        )
    }
}

pub struct GetOperation {
    inner: ReqwestClient,
    request: google_cloud_longrunning::model::GetOperationRequest,
    options: gax::options::RequestOptions,
}

impl GetOperation {
    pub fn new(inner: ReqwestClient) -> Self {
        Self {
            inner,
            request: google_cloud_longrunning::model::GetOperationRequest::default(),
            options: gax::options::RequestOptions::default(),
        }
    }

    pub fn with_request_options<V: Into<gax::options::RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    pub fn set_name(mut self, v: impl Into<String>) -> Self {
        self.request.name = v.into();
        self
    }

    pub async fn send(self) -> gax::Result<google_cloud_longrunning::model::Operation> {
        let builder = self
            .inner
            .builder(reqwest::Method::GET, "/poll".to_string())
            .query(&[("alt", "json")]);
        let r = self
            .inner
            .execute(
                builder,
                None::<gaxi::http::NoBody>,
                gax::options::RequestOptions::default(),
            )
            .await?;
        Ok(r.into_body())
    }
}
