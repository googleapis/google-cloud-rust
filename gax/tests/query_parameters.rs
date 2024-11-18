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

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

// We use this to simulate a request and how it is used in query parameters.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FakeRequest {
    // Typically the struct would have at least one path parameter.
    pub parent: String,
    // Most query parameter fields are optional.
    pub count: Option<i32>,
    pub filter_expression: Option<String>,
    pub get_mask: Option<types::FieldMask>,
    pub ttl: Option<types::Duration>,
    pub expiration: Option<types::Timestamp>,
    // Some query parameter fields are required.
    pub required: String,

    pub repeated_int32: Vec<i32>,
    pub repeated_duration: Vec<types::Duration>,

    pub nested: Option<NestedOptions>,
}

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NestedOptions {
    pub repeated_nested_string: Vec<String>,
    pub repeated_double_nested: Vec<DoubleNestedOptions>,
}

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DoubleNestedOptions {
    pub repeated_string: Vec<String>,
}

fn with_query_parameters(request: &FakeRequest) -> Result<reqwest::RequestBuilder> {
    let client = reqwest::Client::builder().build()?;
    let builder = client.get("https://test.googleapis.com/v1/unused");
    let builder = gax::query_parameter::add(builder, "count", &request.count)?;
    let builder =
        gax::query_parameter::add(builder, "filterExpression", &request.filter_expression)?;
    let builder = gax::query_parameter::add(builder, "getMask", &request.get_mask)?;
    let builder = gax::query_parameter::add(builder, "ttl", &request.ttl)?;
    let builder = gax::query_parameter::add(builder, "expiration", &request.expiration)?;
    let builder = gax::query_parameter::add(builder, "required", &request.required)?;
    let builder = gax::query_parameter::add(builder, "repeatedInt32", &request.repeated_int32)?;
    let builder =
        gax::query_parameter::add(builder, "repeatedDuration", &request.repeated_duration)?;
    let builder =
        gax::query_parameter::add(builder, "nested", &serde_json::to_value(&request.nested)?)?;
    Ok(builder)
}

#[test]
fn make_reqwest_no_query() -> Result<()> {
    let client = reqwest::Client::builder().build()?;
    let empty: [Option<(&str, String)>; 0] = [];
    let builder = client
        .get("https://test.googleapis.com/v1/unused")
        .query(&empty.into_iter().flatten().collect::<Vec<(&str, String)>>());

    let r = builder.build()?;
    assert_eq!(None, r.url().query());

    Ok(())
}

#[test]
fn basic_query() -> Result<()> {
    // Create a basic request.
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        filter_expression: Some("only-the-good-stuff".into()),
        ..Default::default()
    };
    let builder = with_query_parameters(&request)?;

    let r = builder.build()?;
    assert_eq!(
        Some("filterExpression=only-the-good-stuff&required="),
        r.url().query()
    );

    Ok(())
}

#[test]
fn with_fieldmask() -> Result<()> {
    // Create a basic request.
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        get_mask: Some(
            types::FieldMask::default().set_paths(["f0", "f1"].map(str::to_string).to_vec()),
        ),
        ..Default::default()
    };
    let builder = with_query_parameters(&request)?;

    let r = builder.build()?;
    // %2C is the URL-safe encoding for comma (`,`)
    assert_eq!(Some("getMask=f0%2Cf1&required="), r.url().query());

    Ok(())
}

#[test]
fn with_duration() -> Result<()> {
    // Create a basic request.
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        ttl: Some(types::Duration::new(12, 345_678_900)),
        ..Default::default()
    };
    let builder = with_query_parameters(&request)?;

    let r = builder.build()?;
    // %2C is the URL-safe encoding for comma (`,`)
    assert_eq!(Some("ttl=12.345678900s&required="), r.url().query());

    Ok(())
}

#[test]
fn with_timestamp() -> Result<()> {
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
    let builder = with_query_parameters(&request)?;

    let r = builder.build()?;
    // %3A is the URL-safe encoding for colon (`:`)
    assert_eq!(
        Some("expiration=1970-01-01T00%3A00%3A12.3456789Z&required="),
        r.url().query()
    );

    Ok(())
}

#[test]
fn with_repeated_int32() -> Result<()> {
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        repeated_int32: vec![2_i32, 3_i32, 5_i32],
        ..Default::default()
    };
    let builder = with_query_parameters(&request)?;

    let r = builder.build()?;
    // %3A is the URL-safe encoding for colon (`:`)
    assert_eq!(
        Some("required=&repeatedInt32=2&repeatedInt32=3&repeatedInt32=5"),
        r.url().query()
    );

    Ok(())
}

#[test]
fn with_repeated_duration() -> Result<()> {
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        repeated_duration: [2, 3, 5].map(types::Duration::from_seconds).to_vec(),
        ..Default::default()
    };
    let builder = with_query_parameters(&request)?;

    let r = builder.build()?;
    assert_eq!(
        Some("required=&repeatedDuration=2s&repeatedDuration=3s&repeatedDuration=5s"),
        r.url().query()
    );

    Ok(())
}

#[test]
fn with_nested() -> Result<()> {
    let request = FakeRequest {
        parent: "projects/test-only-invalid".into(),
        nested: Some(NestedOptions {
            repeated_nested_string: ["a", "b", "c"].map(str::to_string).to_vec(),
            repeated_double_nested: vec![
                DoubleNestedOptions {
                    repeated_string: ["e", "d"].map(str::to_string).to_vec(),
                },
                DoubleNestedOptions {
                    repeated_string: ["f", "g"].map(str::to_string).to_vec(),
                },
            ],
        }),
        ..Default::default()
    };
    let builder = with_query_parameters(&request)?;

    let r = builder.build()?;
    let got = r.url().query().unwrap();
    let mut got = got.split('&').map(str::to_string).collect::<Vec<_>>();
    got.sort();
    let got = got;
    let mut want = vec![
        "required=",
        "nested.repeatedNestedString=a",
        "nested.repeatedNestedString=b",
        "nested.repeatedNestedString=c",
        "nested.repeatedDoubleNested.repeatedString=e",
        "nested.repeatedDoubleNested.repeatedString=d",
        "nested.repeatedDoubleNested.repeatedString=f",
        "nested.repeatedDoubleNested.repeatedString=g",
    ];
    want.sort();
    let want = want;

    assert_eq!(got, want);

    Ok(())
}
