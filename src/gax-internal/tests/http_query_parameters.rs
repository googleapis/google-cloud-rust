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

#[cfg(all(test, feature = "_internal-http-client"))]
mod tests {
    use google_cloud_gax_internal as gaxi;
    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    // We use this to simulate a request and how it is used in query parameters.
    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct FakeRequest {
        // Typically the struct would have at least one path parameter.
        pub parent: String,

        // Most query parameter fields are optional.
        pub boolean: Option<bool>,
        pub filter_expression: Option<String>,

        // Some query parameter fields are required or repeated. Use String and
        // int32 because those are the most common and good representatives for
        // all primitive types.
        pub required_string: String,
        pub optional_string: Option<String>,
        pub repeated_string: Vec<String>,

        pub required_int32: i32,
        pub optional_int32: Option<i32>,
        pub repeated_int32: Vec<i32>,

        // Enums may also appear as well query parameters.
        pub required_enum_value: State,
        pub optional_enum_value: Option<State>,
        pub repeated_enum_value: Vec<State>,

        // Messages (including well-known-types) are always optional or
        // repeated. However, the specification says:
        //
        // > Note that fields which are mapped to URL query parameters must
        // > have a primitive type or a repeated primitive type or a
        // > **non-repeated** message type.
        //
        // https://github.com/googleapis/googleapis/blob/3776db131e34e42ec8d287203020cb4282166aa5/google/api/http.proto#L114-L119
        //
        pub duration: Option<wkt::Duration>,
        pub timestamp: Option<wkt::Timestamp>,

        // FieldMask is different from `Duration` and `Timestamp`. Message types
        // are mapped according to:
        //
        // > In the case of a message type, each field of the message is mapped
        // > to a separate parameter,
        //
        // The JSON representation of `Duration` and `Timestamp` are simply
        // strings.  FieldMask is mapped to a JSON object with a single `paths`
        // field.
        pub field_mask: Option<wkt::FieldMask>,

        // In OpenAPI-derived client libraries this appears as a required field.
        // This may be a bug in the OpenAPI parser, but let's make sure the code
        // compiles and works until that is fixed.
        pub required_field_mask: wkt::FieldMask,

        // Sometimes query parameters appear inside fields that are object
        // fields.
        pub optional_nested: Option<NestedOptions>,
    }

    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct NestedOptions {
        pub required_int32: i32,
        pub optional_int32: Option<i32>,
        pub repeated_int32: Vec<i32>,
        pub double_nested: Option<DoubleNestedOptions>,
    }

    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DoubleNestedOptions {
        pub repeated_string: Vec<String>,
    }

    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    pub struct State(String);

    impl State {
        /// Sets the enum value.
        pub fn set_value<T: Into<String>>(mut self, v: T) -> Self {
            self.0 = v.into();
            self
        }

        /// Gets the enum value.
        pub fn value(&self) -> &str {
            &self.0
        }
    }

    pub mod state {
        pub const DISABLED: &str = "DISABLED";
        pub const DESTROYED: &str = "DESTROYED";
    }

    // A new version of the generator will generate code as follows for query parameters.
    fn add_query_parameters(request: &FakeRequest) -> Result<reqwest::RequestBuilder> {
        use gax::error::Error;
        let client = reqwest::Client::builder().build()?;
        let builder = client.get("https://test.googleapis.com/v1/unused");

        let builder = request
            .boolean
            .iter()
            .fold(builder, |builder, p| builder.query(&[("boolean", p)]));
        let builder = request
            .filter_expression
            .iter()
            .fold(builder, |builder, p| {
                builder.query(&[("filterExpression", p)])
            });

        let builder = builder.query(&[("requiredString", &request.required_string)]);
        let builder = request.optional_string.iter().fold(builder, |builder, p| {
            builder.query(&[("optionalString", p)])
        });
        let builder = request.repeated_string.iter().fold(builder, |builder, p| {
            builder.query(&[("repeatedString", p)])
        });

        let builder = builder.query(&[("requiredInt32", &request.required_int32)]);
        let builder = request
            .optional_int32
            .iter()
            .fold(builder, |builder, p| builder.query(&[("optionalInt32", p)]));
        let builder = request
            .repeated_int32
            .iter()
            .fold(builder, |builder, p| builder.query(&[("repeatedInt32", p)]));

        let builder = builder.query(&[("requiredEnumValue", &request.required_enum_value.value())]);
        let builder = request
            .optional_enum_value
            .iter()
            .fold(builder, |builder, p| {
                builder.query(&[("optionalEnumValue", &p.value())])
            });
        let builder = request
            .repeated_enum_value
            .iter()
            .fold(builder, |builder, p| {
                builder.query(&[("repeatedEnumValue", &p.value())])
            });

        let builder = request
            .duration
            .as_ref()
            .map(|p| serde_json::to_value(p).map_err(Error::ser))
            .transpose()?
            .into_iter()
            .fold(builder, |builder, v| {
                use gaxi::query_parameter::QueryParameter;
                v.add(builder, "optionalDuration")
            });
        let builder = request
            .field_mask
            .as_ref()
            .map(|p| serde_json::to_value(p).map_err(Error::ser))
            .transpose()?
            .into_iter()
            .fold(builder, |builder, v| {
                use gaxi::query_parameter::QueryParameter;
                v.add(builder, "fieldMask")
            });
        let builder = {
            use gaxi::query_parameter::QueryParameter;
            serde_json::to_value(&request.required_field_mask)
                .map_err(Error::ser)?
                .add(builder, "requiredFieldMask")
        };

        let builder = request
            .timestamp
            .as_ref()
            .map(|p| serde_json::to_value(p).map_err(Error::ser))
            .transpose()?
            .into_iter()
            .fold(builder, |builder, v| {
                use gaxi::query_parameter::QueryParameter;
                v.add(builder, "expiration")
            });
        let builder = request
            .optional_nested
            .as_ref()
            .map(|p| serde_json::to_value(p).map_err(Error::ser))
            .transpose()?
            .into_iter()
            .fold(builder, |builder, v| {
                use gaxi::query_parameter::QueryParameter;
                v.add(builder, "optionalNested")
            });

        Ok(builder)
    }

    fn split_query(r: &reqwest::Request) -> Vec<&str> {
        r.url()
            .query()
            .unwrap_or_default()
            .split("&")
            // Remove repetitive elements to make the tests cleaner.
            .filter(|p| {
                *p != "requiredString="
                    && *p != "requiredInt32=0"
                    && *p != "requiredEnumValue="
                    && *p != "requiredFieldMask="
            })
            .collect()
    }

    #[test]
    fn default() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), Vec::<&str>::new());

        Ok(())
    }

    #[test]
    fn boolean() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            boolean: Some(true),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["boolean=true"]);

        Ok(())
    }

    #[test]
    fn filter_expression() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            filter_expression: Some("goodies".into()),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["filterExpression=goodies"]);

        Ok(())
    }

    #[test]
    fn required_string() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            required_string: "value".into(),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["requiredString=value"]);

        Ok(())
    }

    #[test]
    fn optional_string() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            optional_string: Some("value".into()),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["optionalString=value"]);

        Ok(())
    }

    #[test]
    fn repeated_string() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            repeated_string: vec!["s0".into(), "s1".into()],
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(
            split_query(&r),
            vec!["repeatedString=s0", "repeatedString=s1"]
        );

        Ok(())
    }

    #[test]
    fn required_int32() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            required_int32: 123,
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["requiredInt32=123"]);

        Ok(())
    }

    #[test]
    fn optional_int32() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            optional_int32: Some(234),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["optionalInt32=234"]);

        Ok(())
    }

    #[test]
    fn repeated_int32() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            repeated_int32: vec![123, 345, 567],
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(
            split_query(&r),
            vec![
                "repeatedInt32=123",
                "repeatedInt32=345",
                "repeatedInt32=567",
            ]
        );

        Ok(())
    }

    #[test]
    fn required_enum_value() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            required_enum_value: State::default().set_value(state::DESTROYED),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["requiredEnumValue=DESTROYED"]);

        Ok(())
    }

    #[test]
    fn optional_enum_value() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            optional_enum_value: State::default().set_value(state::DISABLED).into(),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["optionalEnumValue=DISABLED"]);

        Ok(())
    }

    #[test]
    fn repeated_enum_value() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            repeated_enum_value: vec![
                State::default().set_value(state::DISABLED),
                State::default().set_value(state::DESTROYED),
            ],
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(
            split_query(&r),
            vec!["repeatedEnumValue=DISABLED", "repeatedEnumValue=DESTROYED",]
        );

        Ok(())
    }

    #[test]
    fn optional_duration() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            duration: Some(wkt::Duration::clamp(123, 0)),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["optionalDuration=123s"]);

        Ok(())
    }

    #[test]
    fn timestamp() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            timestamp: Some(wkt::Timestamp::clamp(3654, 0)),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["expiration=1970-01-01T01%3A00%3A54Z"]);

        Ok(())
    }

    #[test]
    fn field_mask() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            field_mask: Some(
                wkt::FieldMask::default().set_paths(vec!["a".to_string(), "b".to_string()]),
            ),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["fieldMask=a%2Cb"]);

        Ok(())
    }

    #[test]
    fn required_field_mask() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            required_field_mask: wkt::FieldMask::default()
                .set_paths(vec!["a".to_string(), "b".to_string()]),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["requiredFieldMask=a%2Cb"]);

        Ok(())
    }

    #[test]
    fn optional_nested_required_int32() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            optional_nested: Some(NestedOptions {
                required_int32: 123,
                ..Default::default()
            }),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        assert_eq!(split_query(&r), vec!["optionalNested.requiredInt32=123"]);

        Ok(())
    }

    #[test]
    fn optional_nested_optional_int32() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            optional_nested: Some(NestedOptions {
                optional_int32: Some(123),
                ..Default::default()
            }),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        let mut got = split_query(&r);
        got.sort();
        assert_eq!(
            got,
            vec![
                "optionalNested.optionalInt32=123",
                "optionalNested.requiredInt32=0"
            ]
        );

        Ok(())
    }

    #[test]
    fn optional_nested_repeated_int32() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            optional_nested: Some(NestedOptions {
                repeated_int32: vec![1, 3, 5, 7],
                ..Default::default()
            }),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        let mut got = split_query(&r);
        got.sort();
        assert_eq!(
            got,
            vec![
                "optionalNested.repeatedInt32=1",
                "optionalNested.repeatedInt32=3",
                "optionalNested.repeatedInt32=5",
                "optionalNested.repeatedInt32=7",
                "optionalNested.requiredInt32=0",
            ]
        );

        Ok(())
    }

    #[test]
    fn optional_nested_double_nested() -> Result<()> {
        let request = FakeRequest {
            parent: "projects/test-only-invalid".into(),
            optional_nested: Some(NestedOptions {
                double_nested: Some(DoubleNestedOptions {
                    repeated_string: ["a", "b"].map(str::to_string).to_vec(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let builder = add_query_parameters(&request)?;

        let r = builder.build()?;
        let mut got = split_query(&r);
        got.sort();
        assert_eq!(
            got,
            vec![
                "optionalNested.doubleNested.repeatedString=a",
                "optionalNested.doubleNested.repeatedString=b",
                "optionalNested.requiredInt32=0",
            ]
        );

        Ok(())
    }
}
