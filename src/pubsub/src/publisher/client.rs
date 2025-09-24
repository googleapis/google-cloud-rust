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

use crate::client::TopicAdmin;
use crate::model::PubsubMessage;

#[derive(Clone, Debug)]
pub struct Publisher {
    client: TopicAdmin,
}

impl Publisher {
    // TODO: change to builder.
    pub async fn new() -> crate::Result<Self> {
        let client = TopicAdmin::builder()
            .build()
            .await
            .map_err(crate::Error::io)?;
        Ok(Self { client })
    }

    pub async fn publish(
        &self,
        topic: &str,
        msg: PubsubMessage,
    ) -> crate::Result<crate::model::PublishResponse> {
        self.client
            .publish()
            .set_messages([msg])
            .set_topic(topic)
            .send()
            .await
    }
}
