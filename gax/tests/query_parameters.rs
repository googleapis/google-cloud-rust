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

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

// We use this to simulate a request and how it is used in
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FakeRequest {
    // Typically the struct would have a
    pub parent: String,
    // Most query parameter fields are optional.
    pub count: Option<i32>,
    pub filter_expression: Option<String>,
    pub get_mask: Option<types::FieldMask>,
    pub ttl: Option<types::Duration>,
    pub expiration: Option<types::Timestamp>,
    // Some query parameter fields are required.
    pub required: String,
}

#[test]
fn make_reqwest_no_query() -> Result {
    let client = reqwest::Client::builder().build()?;
    let empty : [Option<(&str, String)>;0] = [];
    let builder = client.get("https://test.googleapis.com/v1/unused").query(
        &empty.into_iter()
            .flatten()
            .collect::<Vec<(&str, String)>>(),
    );

    let r = builder.build()?;
    assert_eq!(None, r.url().query());

    Ok(())
}

#[test]
fn basic_query() -> Result {
    // Create a basic request.
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        filter_expression: Some("only-the-good-stuff".into()),
        ..Default::default()
    };
    let client = reqwest::Client::builder().build()?;
    let builder = client.get("https://test.googleapis.com/v1/unused").query(
        &[
            gax::query_parameter::format("count", &request.count)?,
            gax::query_parameter::format("filterExpression", &request.filter_expression)?,
            gax::query_parameter::format("getMask", &request.get_mask)?,
            gax::query_parameter::format("ttl", &request.ttl)?,
            gax::query_parameter::format("expiration", &request.expiration)?,
            gax::query_parameter::format("required", &request.required)?,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<(&str, String)>>(),
    );

    let r = builder.build()?;
    assert_eq!(
        Some("filterExpression=only-the-good-stuff&required="),
        r.url().query()
    );

    Ok(())
}

#[test]
fn with_fieldmask() -> Result {
    // Create a basic request.
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        get_mask: Some(
            types::FieldMask::default().set_paths(["f0", "f1"].map(str::to_string).to_vec()),
        ),
        ..Default::default()
    };
    let client = reqwest::Client::builder().build()?;
    let builder = client.get("https://test.googleapis.com/v1/unused").query(
        &[
            gax::query_parameter::format("count", &request.count)?,
            gax::query_parameter::format("filterExpression", &request.filter_expression)?,
            gax::query_parameter::format("getMask", &request.get_mask)?,
            gax::query_parameter::format("ttl", &request.ttl)?,
            gax::query_parameter::format("expiration", &request.expiration)?,
            gax::query_parameter::format("required", &request.required)?,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<(&str, String)>>(),
    );

    let r = builder.build()?;
    // %2C is the URL-safe encoding for comma (`,`)
    assert_eq!(Some("getMask=f0%2Cf1&required="), r.url().query());

    Ok(())
}

#[test]
fn with_duration() -> Result {
    // Create a basic request.
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        ttl: Some(types::Duration::new(12, 345_678_900)),
        ..Default::default()
    };
    let client = reqwest::Client::builder().build()?;
    let builder = client.get("https://test.googleapis.com/v1/unused").query(
        &[
            gax::query_parameter::format("count", &request.count)?,
            gax::query_parameter::format("filterExpression", &request.filter_expression)?,
            gax::query_parameter::format("getMask", &request.get_mask)?,
            gax::query_parameter::format("ttl", &request.ttl)?,
            gax::query_parameter::format("expiration", &request.expiration)?,
            gax::query_parameter::format("required", &request.required)?,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<(&str, String)>>(),
    );

    let r = builder.build()?;
    // %2C is the URL-safe encoding for comma (`,`)
    assert_eq!(Some("ttl=12.345678900s&required="), r.url().query());

    Ok(())
}

#[test]
fn with_timestamp() -> Result {
    // Create a basic request.
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        expiration: Some(
            types::Timestamp::default()
                .set_seconds(12)
                .set_nanos(345_678_900),
        ),
        ..Default::default()
    };
    let client = reqwest::Client::builder().build()?;
    let builder = client.get("https://test.googleapis.com/v1/unused").query(
        &[
            gax::query_parameter::format("count", &request.count)?,
            gax::query_parameter::format("filterExpression", &request.filter_expression)?,
            gax::query_parameter::format("getMask", &request.get_mask)?,
            gax::query_parameter::format("ttl", &request.ttl)?,
            gax::query_parameter::format("expiration", &request.expiration)?,
            gax::query_parameter::format("required", &request.required)?,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<(&str, String)>>(),
    );

    let r = builder.build()?;
    // %3A is the URL-safe encoding for colon (`:`)
    assert_eq!(
        Some("expiration=1970-01-01T00%3A00%3A12.3456789Z&required="),
        r.url().query()
    );

    Ok(())
}
