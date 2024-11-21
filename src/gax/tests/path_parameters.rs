// Copyright 2024 Google LLC
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

use gcp_sdk_gax::path_parameter::PathParameter;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

// We use this to simulate a request and how it is used in the client.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct FakeRequest {
    // Typically the struct would have a required path parameter.
    pub parent: String,
    // Sometimes there is a required parameter inside another struct.
    pub payload: Option<FakePayload>,
}

impl FakeRequest {
    pub fn set_parent(mut self, v: impl Into<String>) -> Self {
        self.parent = v.into();
        self
    }
    pub fn set_payload(mut self, v: impl Into<Option<FakePayload>>) -> Self {
        self.payload = v.into();
        self
    }
}

// We use this to simulate a request and how it is used in the client.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct FakePayload {
    // This may be one of the fields used in the request.
    pub id: String,
}

impl FakePayload {
    pub fn set_id(mut self, v: impl Into<String>) -> Self {
        self.id = v.into();
        self
    }
}

#[test]
fn make_reqwest_with_optional_path_parameter() -> Result {
    let client = reqwest::Client::builder().build()?;
    let request = FakeRequest::default()
        .set_parent("projects/test-only")
        .set_payload(FakePayload::default().set_id("abc"));
    let builder = client.get(format!(
        "https://test.googleapis.com/v1/{}/foos/{}",
        &request.parent,
        PathParameter::required(&request.payload, "payload")?.id
    ));

    let r = builder.build()?;
    assert_eq!("test.googleapis.com", r.url().authority());
    assert_eq!("/v1/projects/test-only/foos/abc", r.url().path());

    Ok(())
}

#[test]
fn make_reqwest_with_missing_optional_path() -> Result {
    let client = reqwest::Client::builder().build()?;
    let request = FakeRequest::default().set_parent("projects/test-only");
    let result = || -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _builder = client.get(format!(
            "https://test.googleapis.com/v1/{}/foos/{}",
            &request.parent,
            PathParameter::required(&request.payload, "payload")?.id
        ));
        Ok(())
    }();

    assert!(result.is_err());
    if let Err(e) = result {
        assert!(
            format!("{e:?}").contains("payload"),
            "expected the field name (payload) in the error message {:?}",
            e
        );
    }

    Ok(())
}
