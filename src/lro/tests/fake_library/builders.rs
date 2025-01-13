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
use gax::http_client::ReqwestClient;

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

    pub async fn send(self) -> gax::Result<longrunning::model::Operation> {
        let builder = self
            .stub
            .builder(reqwest::Method::POST, "/create".to_string())
            .query(&[("alt", "json")]);
        let r = self
            .stub
            .execute(
                builder,
                None::<gax::http_client::NoBody>,
                gax::options::RequestOptions::default(),
            )
            .await?;
        Ok(r)
    }

    pub fn poller(
        self,
    ) -> impl gcp_sdk_lro::Poller<super::model::Resource, super::model::CreateResourceMetadata>
    {
        type Operation =
            gcp_sdk_lro::Operation<super::model::Resource, super::model::CreateResourceMetadata>;

        let stub = self.stub.clone();
        let options = self.options.clone()
            // TODO(684) - use NoRetries policy here.
            ;
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
        gcp_sdk_lro::new_poller(start, query)
    }

    pub async fn until_done(self) -> Result<super::model::Resource> {
        use gcp_sdk_lro::Poller;
        use gcp_sdk_lro::PollingResult;
        use std::time::Duration;
        let duration = Duration::from_secs(1);
        let mut poller = self.poller();
        while let Some(p) = poller.poll().await {
            match p {
                PollingResult::Completed(result) => {
                    return result;
                }
                PollingResult::PollingError(e) => {
                    // TODO: use policy...
                    if let Some(svc) = e.as_inner::<gax::error::ServiceError>() {
                        if svc.status().code == gax::error::rpc::Code::Unavailable as i32 {
                            continue;
                        }
                    }
                    return Err(e);
                }
                PollingResult::InProgress(_) => {}
            }

            // TODO: use policy
            let duration = if duration > Duration::from_secs(60) {
                duration
            } else {
                duration.saturating_mul(2)
            };
            tokio::time::sleep(duration).await;
        }
        return Err(gax::error::Error::other("no response"));
    }
}

pub struct GetOperation {
    inner: ReqwestClient,
    request: longrunning::model::GetOperationRequest,
    options: gax::options::RequestOptions,
}

impl GetOperation {
    pub fn new(inner: ReqwestClient) -> Self {
        Self {
            inner,
            request: longrunning::model::GetOperationRequest::default(),
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

    pub async fn send(self) -> gax::Result<longrunning::model::Operation> {
        let builder = self
            .inner
            .builder(reqwest::Method::GET, "/poll".to_string())
            .query(&[("alt", "json")]);
        let r = self
            .inner
            .execute(
                builder,
                None::<gax::http_client::NoBody>,
                gax::options::RequestOptions::default(),
            )
            .await?;
        Ok(r)
    }
}
