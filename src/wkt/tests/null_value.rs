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

//! Test serialization and deserialization of `NullValue`.
//!
//! `NullValue` is an well-known type that represents a null JSON value. We need
//! to verify it works well as part of a larger message or in enums.

#[cfg(test)]
mod test {
    use google_cloud_wkt as wkt;
    use serde_json::json;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_serialize() -> TestResult {
        let input = Value::new()
            .set_test_name("test_serialize")
            .set_null_value(wkt::NullValue);
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "testName": "test_serialize",
            "nullValue": null
        });
        assert_eq!(got, want);

        let input = Value::new()
            .set_test_name("test_serialize")
            .set_duration(wkt::Duration::clamp(123, 456));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "testName": "test_serialize",
            "duration": "123.000000456s",
        });
        assert_eq!(got, want);

        let input = Value::new()
            .set_test_name("test_serialize")
            .set_number(123.5);
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "testName": "test_serialize",
            "number": 123.5,
        });
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn test_deserialize() -> TestResult {
        let input = json!({
            "testName": "test_serialize",
            "nullValue": null
        });
        let got = serde_json::from_value::<Value>(input)?;
        let want = Value::new()
            .set_test_name("test_serialize")
            .set_null_value(wkt::NullValue);
        assert_eq!(got, want);

        let input = json!({
            "testName": "test_serialize",
            "duration": "123.000000456s",
        });
        let got = serde_json::from_value::<Value>(input)?;
        let want = Value::new()
            .set_test_name("test_serialize")
            .set_duration(wkt::Duration::clamp(123, 456));
        assert_eq!(got, want);

        let input = json!({
            "testName": "test_serialize",
            "number": 123.5,
        });
        let got = serde_json::from_value::<Value>(input)?;
        let want = Value::new()
            .set_test_name("test_serialize")
            .set_number(123.5);
        assert_eq!(got, want);
        Ok(())
    }

    // A test message, inspired by `google.firestore.v1.ValueType`.
    #[serde_with::serde_as]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(default, rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct Value {
        #[serde(skip_serializing_if = "std::string::String::is_empty")]
        pub test_name: String,

        #[serde(flatten, skip_serializing_if = "Option::is_none")]
        pub value: Option<value::ValueType>,
    }

    impl Value {
        pub fn new() -> Self {
            Self {
                test_name: String::new(),
                value: None,
            }
        }

        pub fn set_test_name<T: Into<String>>(mut self, v: T) -> Self {
            self.test_name = v.into();
            self
        }

        pub fn set_null_value<T: Into<wkt::NullValue>>(mut self, v: T) -> Self {
            self.value = Some(value::ValueType::NullValue(v.into()));
            self
        }

        pub fn set_duration<T: Into<Box<wkt::Duration>>>(mut self, v: T) -> Self {
            self.value = Some(value::ValueType::Duration(v.into()));
            self
        }

        pub fn set_number<T: Into<f64>>(mut self, v: T) -> Self {
            self.value = Some(value::ValueType::Number(v.into()));
            self
        }
    }

    mod value {
        #[allow(unused_imports)]
        use super::*;

        #[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        #[non_exhaustive]
        pub enum ValueType {
            NullValue(wkt::NullValue),
            Duration(Box<wkt::Duration>),
            Number(f64),
        }
    }
}
