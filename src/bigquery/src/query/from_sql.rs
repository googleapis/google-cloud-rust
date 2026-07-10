// Copyright 2026 Google LLC
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

use crate::error::ConvertError;

/// Converts BigQuery internal [wkt::Value] to Rust types.
pub trait FromSql: Sized {
    /// Converts a BigQuery `wkt::Value` into the implementing type.
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError>;
}

impl FromSql for wkt::Value {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        Ok(value)
    }
}

impl FromSql for String {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::String(s) => Ok(s),
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

impl FromSql for i64 {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::Number(n) => n
                .as_i64()
                .ok_or_else(|| ConvertError::Convert("number is not a valid i64".into())),
            wkt::Value::String(s) => s
                .parse::<i64>()
                .map_err(|e| ConvertError::Convert(Box::new(e))),
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "number or string",
                got: other,
            }),
        }
    }
}

impl FromSql for f64 {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::Number(n) => n
                .as_f64()
                .ok_or_else(|| ConvertError::Convert("invalid f64 number".into())),
            wkt::Value::String(s) => s
                .parse::<f64>()
                .map_err(|e| ConvertError::Convert(Box::new(e))),
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "number or string",
                got: other,
            }),
        }
    }
}

impl FromSql for bool {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::Bool(b) => Ok(b),
            wkt::Value::String(s) => s
                .parse::<bool>()
                .map_err(|e| ConvertError::Convert(Box::new(e))),
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "bool or string",
                got: other,
            }),
        }
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::Null => Ok(None),
            other => T::from_sql(other).map(Some),
        }
    }
}

impl<T: FromSql> FromSql for Vec<T> {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::Array(arr) => arr.into_iter().map(T::from_sql).collect(),
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "array",
                got: other,
            }),
        }
    }
}

impl FromSql for wkt::Struct {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::Object(obj) => Ok(obj),
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "object",
                got: other,
            }),
        }
    }
}

// TODO(#5592): implement for more Rust
// types: f32, i32, Decimal, wkt::Timestamp, etc.

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    // Test-only representation of `ConvertError` that implements `PartialEq`.
    // This allows testing error outcomes using `test_case` assertions without
    // implementing `PartialEq` on the production `ConvertError`.
    #[derive(Debug, PartialEq)]
    enum TestConvertError {
        NotNull,
        TypeMismatch(&'static str),
        Convert(String),
    }

    impl From<ConvertError> for TestConvertError {
        fn from(err: ConvertError) -> Self {
            match err {
                ConvertError::NotNull => Self::NotNull,
                ConvertError::TypeMismatch { expected, .. } => Self::TypeMismatch(expected),
                ConvertError::Convert(e) => Self::Convert(e.to_string()),
            }
        }
    }

    #[test_case(wkt::Value::String("hello".to_string()) => Ok(wkt::Value::String("hello".to_string())) ; "value string")]
    fn test_from_sql_value(value: wkt::Value) -> Result<wkt::Value, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::String("hello".to_string()) => Ok("hello".to_string()) ; "string")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null string")]
    #[test_case(wkt::Value::Number(123.into()) => Err(TestConvertError::TypeMismatch("string")) ; "type mismatch string")]
    fn test_from_sql_string(value: wkt::Value) -> Result<String, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::Number(123.into()) => Ok(123) ; "i64 from number")]
    #[test_case(wkt::Value::String("123".to_string()) => Ok(123) ; "i64 from string")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null i64")]
    #[test_case(wkt::Value::Bool(true) => Err(TestConvertError::TypeMismatch("number or string")) ; "try bool as i64")]
    #[test_case(wkt::Value::String("hello".to_string()) => Err(TestConvertError::Convert("invalid digit found in string".to_string())) ; "invalid string as i64")]
    fn test_from_sql_i64(value: wkt::Value) -> Result<i64, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::Number(serde_json::Number::from_f64(123.45).unwrap()) => Ok(123.45) ; "f64 from number")]
    #[test_case(wkt::Value::String("123.45".to_string()) => Ok(123.45) ; "f64 from string")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null f64")]
    #[test_case(wkt::Value::Bool(true) => Err(TestConvertError::TypeMismatch("number or string")) ; "try bool as f64")]
    #[test_case(wkt::Value::String("hello".to_string()) => Err(TestConvertError::Convert("invalid float literal".to_string())) ; "invalid string as f64")]
    fn test_from_sql_f64(value: wkt::Value) -> Result<f64, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::Bool(true) => Ok(true) ; "bool true")]
    #[test_case(wkt::Value::Bool(false) => Ok(false) ; "bool false")]
    #[test_case(wkt::Value::String("true".to_string()) => Ok(true) ; "bool from string true")]
    #[test_case(wkt::Value::String("false".to_string()) => Ok(false) ; "bool from string false")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null bool")]
    #[test_case(wkt::Value::Number(1.into()) => Err(TestConvertError::TypeMismatch("bool or string")) ; "try number as bool")]
    #[test_case(wkt::Value::String("hello".to_string()) => Err(TestConvertError::Convert("provided string was not `true` or `false`".to_string())) ; "invalid string as bool")]
    fn test_from_sql_bool(value: wkt::Value) -> Result<bool, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::Null => Ok(None) ; "option null")]
    #[test_case(wkt::Value::Number(123.into()) => Ok(Some(123)) ; "option some i64")]
    #[test_case(wkt::Value::String("hello".to_string()) => Err(TestConvertError::Convert("invalid digit found in string".to_string())) ; "option error i64")]
    fn test_from_sql_option(value: wkt::Value) -> Result<Option<i64>, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::Array(vec![wkt::Value::Number(1.into()), wkt::Value::Number(2.into())]) => Ok(vec![1, 2]) ; "vec i64")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "vec null")]
    #[test_case(wkt::Value::String("hello".to_string()) => Err(TestConvertError::TypeMismatch("array")) ; "vec type mismatch")]
    #[test_case(wkt::Value::Array(vec![wkt::Value::String("invalid".to_string())]) => Err(TestConvertError::Convert("invalid digit found in string".to_string())) ; "vec element convert error")]
    fn test_from_sql_vec(value: wkt::Value) -> Result<Vec<i64>, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::Object(wkt::Struct::from_iter([("a".to_string(), wkt::Value::Number(1.into()))])) => Ok(wkt::Struct::from_iter([("a".to_string(), wkt::Value::Number(1.into()))])) ; "struct ok")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "struct null")]
    #[test_case(wkt::Value::String("hello".to_string()) => Err(TestConvertError::TypeMismatch("object")) ; "struct type mismatch")]
    fn test_from_sql_struct(value: wkt::Value) -> Result<wkt::Struct, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }
}
