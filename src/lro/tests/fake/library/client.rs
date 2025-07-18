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

use super::*;
use gaxi::http::ReqwestClient;
pub struct Client {
    // A sidekick-generated client contains a `Arc<dyn T>`. The code
    // in this test skips some layers of abstraction.
    inner: ReqwestClient,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        gax::client_builder::internal::new_builder(Factory)
    }

    pub async fn new(config: gaxi::options::ClientConfig) -> gax::client_builder::Result<Self> {
        let inner = ReqwestClient::new(config, "http://127.0.0.1:1").await?;
        Ok(Self { inner })
    }

    pub fn create_resource(
        &self,
        parent: impl Into<String>,
        id: impl Into<String>,
    ) -> builders::CreateResource {
        builders::CreateResource::new(self.inner.clone())
            .set_parent(parent)
            .set_id(id)
    }

    pub fn get_operation(&self, name: impl Into<String>) -> builders::GetOperation {
        builders::GetOperation::new(self.inner.clone()).set_name(name)
    }
}

pub type ClientBuilder =
    gax::client_builder::ClientBuilder<Factory, auth::credentials::Credentials>;
pub struct Factory;
impl gax::client_builder::internal::ClientFactory for Factory {
    type Client = Client;
    type Credentials = auth::credentials::Credentials;
    async fn build(
        self,
        config: gaxi::options::ClientConfig,
    ) -> gax::client_builder::Result<Self::Client> {
        Self::Client::new(config).await
    }
}
