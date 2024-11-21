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

type Result<T> = std::result::Result<T, crate::request_parameter::Error>;

/// Adds a query parameter to a builder.
///
/// Google APIs use [gRPC Transcoding](https://google.aip.dev/127). Some request
/// fields are sent as query parameters and may need special formatting:
/// - Simple scalars are formatted as usual.
/// - Fields of well-known types are formatted as strings. These include
///   [Duration](types::Duration), [FieldMask](types::FieldMask), and
///   [Timestamp](types::Timestamp).
/// - [Option] fields that do not contain a value are not included in the HTTP
///   query.
/// - Repeated fields are formatted as repeated query parameters.
/// - Object fields use `field.subfield` format, and may (but rarely do)
///   recurse.
///
/// This function is called from the generated code. It is not intended for
/// general use. The goal  
pub fn add<T>(
    builder: reqwest::RequestBuilder,
    name: &str,
    parameter: &T,
) -> Result<reqwest::RequestBuilder>
where
    T: QueryParameter,
{
    QueryParameter::add(parameter, builder, name)
}

/// [QueryParameter] is a trait representing types that can be used as a query
/// parameter.
pub trait QueryParameter {
    fn add(&self, builder: reqwest::RequestBuilder, name: &str) -> Result<reqwest::RequestBuilder>;
}

impl<T: QueryParameter> QueryParameter for Option<T> {
    fn add(&self, builder: reqwest::RequestBuilder, name: &str) -> Result<reqwest::RequestBuilder> {
        match &self {
            None => Ok(builder),
            Some(t) => t.add(builder, name),
        }
    }
}

impl<T: QueryParameter> QueryParameter for Vec<T> {
    fn add(&self, builder: reqwest::RequestBuilder, name: &str) -> Result<reqwest::RequestBuilder> {
        let mut builder = builder;
        for e in self.iter() {
            builder = e.add(builder, name)?;
        }
        Ok(builder)
    }
}

impl<T: crate::request_parameter::RequestParameter> QueryParameter for T {
    fn add(&self, builder: reqwest::RequestBuilder, name: &str) -> Result<reqwest::RequestBuilder> {
        let s = self.format()?;
        Ok(builder.query(&[(name, s)]))
    }
}

impl QueryParameter for serde_json::Value {
    fn add(&self, builder: reqwest::RequestBuilder, name: &str) -> Result<reqwest::RequestBuilder> {
        let mut builder = builder;
        match &self {
            Self::Object(object) => {
                for (k, v) in object {
                    builder = v.add(builder, format!("{name}.{k}").as_str())?;
                }
            }
            Self::Array(array) => {
                for v in array {
                    builder = v.add(builder, name)?;
                }
            }
            Self::Null => {}
            Self::String(s) => {
                builder = builder.query(&[(name, s)]);
            }
            Self::Number(n) => {
                builder = builder.query(&[(name, format!("{n}"))]);
            }
            Self::Bool(b) => {
                builder = builder.query(&[(name, b)]);
            }
        };
        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn none() -> Result {
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = QueryParameter::add(&None::<i32>, builder, "test")?;
        let builder = QueryParameter::add(&None::<i64>, builder, "test")?;
        let builder = QueryParameter::add(&None::<u32>, builder, "test")?;
        let builder = QueryParameter::add(&None::<u64>, builder, "test")?;
        let builder = QueryParameter::add(&None::<f32>, builder, "test")?;
        let builder = QueryParameter::add(&None::<f64>, builder, "test")?;
        let r = builder.build()?;
        assert_eq!(None, r.url().query());

        Ok(())
    }

    #[test]
    fn with_value() -> Result {
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = QueryParameter::add(&Some(42_i32), builder, "i32")?;
        let builder = QueryParameter::add(&Some(42_i64), builder, "i64")?;
        let builder = QueryParameter::add(&Some(42_u32), builder, "u32")?;
        let builder = QueryParameter::add(&Some(42_u64), builder, "u64")?;
        let builder = QueryParameter::add(&Some(42_f32), builder, "f32")?;
        let builder = QueryParameter::add(&Some(42_f64), builder, "f64")?;
        let r = builder.build()?;
        assert_eq!(
            Some(
                ["i32=42", "i64=42", "u32=42", "u64=42", "f32=42", "f64=42",]
                    .join("&")
                    .as_str()
            ),
            r.url().query()
        );
        Ok(())
    }

    #[test]
    fn duration() -> Result {
        let d = wkt::Duration::new(12, 345_678_900);
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = QueryParameter::add(&d, builder, "duration")?;
        let r = builder.build()?;
        assert_eq!(Some("duration=12.345678900s"), r.url().query());
        Ok(())
    }

    #[test]
    fn field_mask() -> Result {
        let fm = wkt::FieldMask::default().set_paths(["a", "b"].map(str::to_string).to_vec());
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = QueryParameter::add(&fm, builder, "fieldMask")?;
        let r = builder.build()?;
        // %2C is the URL encoding for `,`
        assert_eq!(Some("fieldMask=a%2Cb"), r.url().query());
        Ok(())
    }

    #[test]
    fn timestamp() -> Result {
        let ts = wkt::Timestamp::default();
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = QueryParameter::add(&ts, builder, "timestamp")?;
        let r = builder.build()?;
        // %3A is the URL encoding for `:`
        assert_eq!(Some("timestamp=1970-01-01T00%3A00%3A00Z"), r.url().query());
        Ok(())
    }
}
