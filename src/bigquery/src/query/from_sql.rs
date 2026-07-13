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

pub(crate) const BIGQUERY_DATE_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day]");
pub(crate) const BIGQUERY_TIME_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[hour]:[minute]:[second]");
pub(crate) const BIGQUERY_TIME_SUBSEC_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[hour]:[minute]:[second].[subsecond]");
pub(crate) const BIGQUERY_DATETIME_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
pub(crate) const BIGQUERY_DATETIME_SUBSEC_FORMAT: &[time::format_description::FormatItem<
    'static,
>] = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]");

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

impl FromSql for i32 {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::Number(n) => n
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .ok_or_else(|| ConvertError::Convert("number is not a valid i32".into())),
            wkt::Value::String(s) => s
                .parse::<i32>()
                .map_err(|e| ConvertError::Convert(Box::new(e))),
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "number or string",
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

impl FromSql for f32 {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::Number(n) => n
                .as_f64()
                .map(|v| v as f32)
                .ok_or_else(|| ConvertError::Convert("invalid f32 number".into())),
            wkt::Value::String(s) => s
                .parse::<f32>()
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

impl FromSql for wkt::Timestamp {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::String(s) => {
                let micros = s
                    .parse::<i64>()
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?;
                timestamp_from_micros(micros)
            }
            wkt::Value::Number(n) => {
                let micros = n.as_i64().ok_or_else(|| {
                    ConvertError::Convert("timestamp number is not valid i64".into())
                })?;
                timestamp_from_micros(micros)
            }
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "string or number",
                got: other,
            }),
        }
    }
}

fn timestamp_from_micros(micros: i64) -> Result<wkt::Timestamp, ConvertError> {
    wkt::Timestamp::new(
        micros.div_euclid(1_000_000),
        (micros.rem_euclid(1_000_000) * 1_000) as i32,
    )
    .map_err(|e| ConvertError::Convert(Box::new(e)))
}

impl FromSql for google_cloud_type::model::Date {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::String(s) => {
                let date = time::Date::parse(s.as_str(), BIGQUERY_DATE_FORMAT)
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?;
                Ok(google_cloud_type::model::Date::new()
                    .set_year(date.year())
                    .set_month(u8::from(date.month()) as i32)
                    .set_day(date.day() as i32))
            }
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

impl FromSql for google_cloud_type::model::TimeOfDay {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::String(s) => {
                let format = if s.contains('.') {
                    BIGQUERY_TIME_SUBSEC_FORMAT
                } else {
                    BIGQUERY_TIME_FORMAT
                };
                let t = time::Time::parse(s.as_str(), format)
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?;
                Ok(google_cloud_type::model::TimeOfDay::new()
                    .set_hours(t.hour() as i32)
                    .set_minutes(t.minute() as i32)
                    .set_seconds(t.second() as i32)
                    .set_nanos(t.nanosecond() as i32))
            }
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

impl FromSql for google_cloud_type::model::DateTime {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::String(s) => {
                let format = if s.contains('.') {
                    BIGQUERY_DATETIME_SUBSEC_FORMAT
                } else {
                    BIGQUERY_DATETIME_FORMAT
                };
                let dt = time::PrimitiveDateTime::parse(s.as_str(), format)
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?;
                Ok(google_cloud_type::model::DateTime::new()
                    .set_year(dt.year())
                    .set_month(u8::from(dt.month()) as i32)
                    .set_day(dt.day() as i32)
                    .set_hours(dt.hour() as i32)
                    .set_minutes(dt.minute() as i32)
                    .set_seconds(dt.second() as i32)
                    .set_nanos(dt.nanosecond() as i32))
            }
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

impl FromSql for google_cloud_type::model::Decimal {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::String(s) => Ok(google_cloud_type::model::Decimal::new().set_value(s)),
            wkt::Value::Number(n) => {
                Ok(google_cloud_type::model::Decimal::new().set_value(n.to_string()))
            }
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "string or number",
                got: other,
            }),
        }
    }
}

impl FromSql for rust_decimal::Decimal {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::String(s) => rust_decimal::Decimal::from_str_exact(&s)
                .map_err(|e| ConvertError::Convert(Box::new(e))),
            wkt::Value::Number(n) => rust_decimal::Decimal::from_str_exact(&n.to_string())
                .map_err(|e| ConvertError::Convert(Box::new(e))),
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "string or number",
                got: other,
            }),
        }
    }
}

// TODO(#5592): implement for more BigQuery types
// types: Range, Interval, etc.

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_type::model::Decimal;
    use rust_decimal::Decimal as RustDecimal;
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

    #[test_case(wkt::Value::String("1779982200000000".to_string()) => Ok(wkt::Timestamp::new(1779982200, 0).unwrap()) ; "timestamp micro integer string")]
    #[test_case(wkt::Value::Number(1779982200000000i64.into()) => Ok(wkt::Timestamp::new(1779982200, 0).unwrap()) ; "timestamp micro integer number")]
    #[test_case(wkt::Value::String("2026-05-28T15:30:00Z".to_string()) => Err(TestConvertError::Convert("invalid digit found in string".to_string())) ; "timestamp rfc3339 string fails")]
    #[test_case(wkt::Value::Number(serde_json::Number::from_f64(1779982200.5).unwrap()) => Err(TestConvertError::Convert("timestamp number is not valid i64".to_string())) ; "timestamp f64 number fails")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "timestamp null")]
    #[test_case(wkt::Value::Bool(true) => Err(TestConvertError::TypeMismatch("string or number")) ; "timestamp type mismatch")]
    fn test_from_sql_timestamp(value: wkt::Value) -> Result<wkt::Timestamp, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::String("2026-05-28".to_string()) => Ok(google_cloud_type::model::Date::new().set_year(2026).set_month(5).set_day(28)) ; "date valid")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "date null")]
    #[test_case(wkt::Value::Number(123.into()) => Err(TestConvertError::TypeMismatch("string")) ; "date type mismatch")]
    #[test_case(wkt::Value::String("invalid-date".to_string()) => Err(TestConvertError::Convert("the 'year' component could not be parsed".to_string())) ; "date invalid format")]
    #[test_case(wkt::Value::String("2026-abc-28".to_string()) => Err(TestConvertError::Convert("the 'month' component could not be parsed".to_string())) ; "date invalid digits")]
    fn test_from_sql_date(
        value: wkt::Value,
    ) -> Result<google_cloud_type::model::Date, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::String("15:30:00".to_string()) => Ok(google_cloud_type::model::TimeOfDay::new().set_hours(15).set_minutes(30).set_seconds(0).set_nanos(0)) ; "time of day valid")]
    #[test_case(wkt::Value::String("15:30:00.123456".to_string()) => Ok(google_cloud_type::model::TimeOfDay::new().set_hours(15).set_minutes(30).set_seconds(0).set_nanos(123_456_000)) ; "time of day fractional")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "time of day null")]
    #[test_case(wkt::Value::Number(123.into()) => Err(TestConvertError::TypeMismatch("string")) ; "time of day type mismatch")]
    fn test_from_sql_time_of_day(
        value: wkt::Value,
    ) -> Result<google_cloud_type::model::TimeOfDay, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::String("2026-05-28T15:30:00".to_string()) => Ok(google_cloud_type::model::DateTime::new().set_year(2026).set_month(5).set_day(28).set_hours(15).set_minutes(30).set_seconds(0).set_nanos(0)) ; "datetime without subseconds")]
    #[test_case(wkt::Value::String("2026-05-28T15:30:00.123456".to_string()) => Ok(google_cloud_type::model::DateTime::new().set_year(2026).set_month(5).set_day(28).set_hours(15).set_minutes(30).set_seconds(0).set_nanos(123_456_000)) ; "datetime with subseconds")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "datetime null")]
    #[test_case(wkt::Value::Number(123.into()) => Err(TestConvertError::TypeMismatch("string")) ; "datetime type mismatch")]
    fn test_from_sql_datetime(
        value: wkt::Value,
    ) -> Result<google_cloud_type::model::DateTime, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::Number(123.into()) => Ok(123) ; "i32 from number")]
    #[test_case(wkt::Value::String("123".to_string()) => Ok(123) ; "i32 from string")]
    #[test_case(wkt::Value::Number(3_000_000_000i64.into()) => Err(TestConvertError::Convert("number is not a valid i32".to_string())) ; "i32 overflow from number")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null i32")]
    #[test_case(wkt::Value::Bool(true) => Err(TestConvertError::TypeMismatch("number or string")) ; "try bool as i32")]
    #[test_case(wkt::Value::String("hello".to_string()) => Err(TestConvertError::Convert("invalid digit found in string".to_string())) ; "invalid string as i32")]
    fn test_from_sql_i32(value: wkt::Value) -> Result<i32, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::Number(serde_json::Number::from_f64(123.45).unwrap()) => Ok(123.45) ; "f32 from number")]
    #[test_case(wkt::Value::String("123.45".to_string()) => Ok(123.45) ; "f32 from string")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null f32")]
    #[test_case(wkt::Value::Bool(true) => Err(TestConvertError::TypeMismatch("number or string")) ; "try bool as f32")]
    #[test_case(wkt::Value::String("hello".to_string()) => Err(TestConvertError::Convert("invalid float literal".to_string())) ; "invalid string as f32")]
    fn test_from_sql_f32(value: wkt::Value) -> Result<f32, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::String("123.456".to_string()) => Ok(Decimal::new().set_value("123.456")) ; "decimal from string")]
    #[test_case(wkt::Value::Number(serde_json::Number::from_f64(123.456).unwrap()) => Ok(Decimal::new().set_value("123.456")) ; "decimal from number")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null decimal")]
    #[test_case(wkt::Value::Bool(true) => Err(TestConvertError::TypeMismatch("string or number")) ; "try bool as decimal")]
    fn test_from_sql_decimal(value: wkt::Value) -> Result<Decimal, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::String("123.456".to_string()) => Ok(RustDecimal::from_str_exact("123.456").unwrap()) ; "rust_decimal from string")]
    #[test_case(wkt::Value::Number(serde_json::Number::from_f64(123.456).unwrap()) => Ok(RustDecimal::from_str_exact("123.456").unwrap()) ; "rust_decimal from number")]
    #[test_case(wkt::Value::String("99999999999999999999999999999999.123".to_string()) => Err(TestConvertError::Convert("Invalid decimal: overflow from too many digits".to_string())) ; "rust_decimal overflow")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null rust_decimal")]
    #[test_case(wkt::Value::Bool(true) => Err(TestConvertError::TypeMismatch("string or number")) ; "try bool as rust_decimal")]
    fn test_from_sql_rust_decimal(value: wkt::Value) -> Result<RustDecimal, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }
}
