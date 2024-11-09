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

/// Formats a query parameter.
/// 
/// Google APIs use [gRPC Transcoding](https://google.aip.dev/127). Some request
/// fields are sent as query parameters and may need special formatting:
/// - [Option] fields that do not contain a value are not included in the HTTP
///   query.
/// - Fields of well-known types are formatted as strings. These include
///   [Duration](types::Duration), [FieldMask](types::FieldMask), and
///   [Timestamp](types::Timestamp).
/// - Simple scalars are formatted as usual.
/// 
/// This function is called from the generated code. It is not intended for
/// general use. The goal  
pub fn format<T>(
    name: &'static str,
    parameter: &T,
) -> Result<Option<(&'static str, String)>>
where
    T: QueryParameter,
{
    QueryParameter::format(parameter)
        .map(|result| result.map(|s| (name, s)))
        .transpose()
}

/// [QueryParameter] is a trait representing types that can be used as a query
/// parameter.
/// 
pub trait QueryParameter {
    fn format(&self) -> Option<Result<String>>;
}

impl<T: QueryParameter> QueryParameter for Option<T> {
    fn format(&self) -> Option<Result<String>> {
        self.as_ref().and_then(|v| QueryParameter::format(v))
    }
}

impl<T: crate::request_parameter::RequestParameter> QueryParameter for T {
    fn format(&self) -> Option<Result<String>> {
        Some(self.format())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn none() -> Result {
        assert_eq!(None, QueryParameter::format(&None::<i32>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<i64>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<u32>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<u64>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<f32>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<f64>).transpose()?);
        Ok(())
    }

    #[test]
    fn with_value() -> Result {
        let want = Some("42".to_string());
        assert_eq!(want, QueryParameter::format(&Some(42_i32)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42_i64)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42_u32)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42_u64)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42_f32)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42_f64)).transpose()?);
        Ok(())
    }

    #[test]
    fn duration() -> Result {
        let d = types::Duration::new(12, 345_678_900);
        let f = QueryParameter::format(&d).transpose()?;
        assert_eq!(Some("12.345678900s".to_string()), f);
        Ok(())
    }

    #[test]
    fn field_mask() -> Result {
        let fm = types::FieldMask::default().set_paths(["a", "b"].map(str::to_string).to_vec());
        let f = QueryParameter::format(&fm).transpose()?;
        assert_eq!(Some("a,b".to_string()), f);
        Ok(())
    }

    #[test]
    fn timestamp() -> Result {
        let ts = types::Timestamp::default();
        let f = QueryParameter::format(&ts).transpose()?;
        assert_eq!(Some("1970-01-01T00:00:00Z".to_string()), f);
        Ok(())
    }
}
