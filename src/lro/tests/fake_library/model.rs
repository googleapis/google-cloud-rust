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

//! Partially implement the types from a service that uses LROs.

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateResourceRequest {
    pub parent: String,
    pub id: String,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    pub name: String,
}

impl wkt::message::Message for Resource {
    fn typename() -> &'static str {
        "test-only/Resource"
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateResourceMetadata {
    pub percent: u32,
}

impl wkt::message::Message for CreateResourceMetadata {
    fn typename() -> &'static str {
        "test-only/CreateResourceMetadata"
    }
}
