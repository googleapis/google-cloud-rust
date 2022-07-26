// Copyright 2022 Google LLC
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

//! This module contains the mappings for a JSON Discovery document.

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Document {
    pub id: String,
    pub name: String,
    pub version: String,
    pub title: String,
    pub root_url: String,
    pub mtls_root_url: String,
    pub service_path: String,
    pub base_path: String,
    pub documentation_link: String,
    pub auth: Auth,
    #[serde(default = "Vec::new")]
    pub features: Vec<String>,
    #[serde(default = "BTreeMap::new")]
    pub methods: BTreeMap<String, Method>,
    #[serde(default = "BTreeMap::new")]
    pub schemas: BTreeMap<String, Schema>,
    #[serde(default = "BTreeMap::new")]
    pub resources: BTreeMap<String, Resource>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Auth {
    pub oauth2: OAuth2,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuth2 {
    #[serde(default = "BTreeMap::new")]
    pub scopes: BTreeMap<String, ScopeDesc>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeDesc {
    pub description: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Method {
    pub name: Option<String>,
    pub id: Option<String>,
    pub path: Option<String>,
    pub http_method: Option<String>,
    pub description: Option<String>,
    #[serde(default = "BTreeMap::new")]
    pub parameters: BTreeMap<String, Parameter>,
    #[serde(default = "Vec::new")]
    pub parameter_order: Vec<String>,
    pub request: Option<Schema>,
    pub response: Option<Schema>,
    #[serde(default = "Vec::new")]
    pub scopes: Vec<String>,
    pub media_upload: Option<MediaUpload>,
    pub supports_media_download: Option<bool>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parameter {
    #[serde(flatten)]
    pub schema: Schema,
    pub required: Option<bool>,
    pub repeated: Option<bool>,
    pub location: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaUpload {
    #[serde(default = "Vec::new")]
    pub accept: Vec<String>,
    pub max_size: Option<String>,
    #[serde(default = "BTreeMap::new")]
    pub protocols: BTreeMap<String, Protocol>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    pub multipart: bool,
    pub path: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub schema_type: Option<String>,
    pub format: Option<String>,
    pub description: Option<String>,
    #[serde(default = "BTreeMap::new")]
    pub properties: BTreeMap<String, Schema>,
    pub items: Option<Box<Schema>>,
    pub additional_properties: Option<Box<Schema>>,
    #[serde(rename = "$ref")]
    pub schema_ref: Option<String>,
    pub default: Option<String>,
    pub pattern: Option<String>,
    #[serde(default = "Vec::new")]
    #[serde(rename = "enum")]
    pub schema_enum: Vec<String>,
    #[serde(default = "Vec::new")]
    #[serde(rename = "enumDescriptions")]
    pub schema_enum_desc: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    pub name: Option<String>,
    pub full_name: Option<String>,
    #[serde(default = "BTreeMap::new")]
    pub methods: BTreeMap<String, Method>,
    #[serde(default = "BTreeMap::new")]
    pub resources: BTreeMap<String, Resource>,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn it_works() {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test/secret-manager-api.json");

        let contents = fs::read(d).unwrap();
        let _: Document = serde_json::from_slice(&contents).unwrap();
    }
}
