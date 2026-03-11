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

pub use crate::types::{Type, TypeCode};
use crate::value::Kind;
use crate::value::Value;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use rust_decimal::Decimal;
use std::time::SystemTime;
use time::{Date, OffsetDateTime};

/// Represent failures in converting a Spanner Value to a Rust type.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ConvertError {
    /// The value kind is not what we expected.
    #[error("expected {want:?}, got {got:?}")]
    KindMismatch { want: Kind, got: Kind },

    /// The value is null, but the target type does not support nulls.
    #[error("expected non-null value, got null")]
    NotNull,

    /// There was a problem during conversion.
    #[error("cannot convert value, source={0}")]
    Convert(#[source] BoxedError),
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Converts Spanner [Value] to Rust types.
pub trait FromValue: Sized {
    fn from_value(value: &Value, type_: &Type) -> Result<Self, ConvertError>;
}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_value(value: &Value, type_: &Type) -> Result<Self, ConvertError> {
        match &value.0.kind {
            Some(prost_types::value::Kind::NullValue(_)) => Ok(None),
            _ => T::from_value(value, type_).map(Some),
        }
    }
}

impl FromValue for Value {
    fn from_value(value: &Value, _type: &Type) -> Result<Self, ConvertError> {
        Ok(value.clone())
    }
}

impl FromValue for String {
    fn from_value(value: &Value, _type: &Type) -> Result<Self, ConvertError> {
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => Ok(s.clone()),
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value, _type: &Type) -> Result<Self, ConvertError> {
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                s.parse().map_err(|e| ConvertError::Convert(Box::new(e)))
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for i32 {
    fn from_value(value: &Value, _type: &Type) -> Result<Self, ConvertError> {
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                s.parse().map_err(|e| ConvertError::Convert(Box::new(e)))
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for Decimal {
    fn from_value(value: &Value, type_: &Type) -> Result<Self, ConvertError> {
        if type_.code() != TypeCode::Numeric {
            return Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            });
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                Decimal::from_str_exact(s).map_err(|e| ConvertError::Convert(Box::new(e)))
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for SystemTime {
    fn from_value(value: &Value, type_: &Type) -> Result<Self, ConvertError> {
        if type_.code() != TypeCode::Timestamp {
            return Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            });
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                let dt = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?;
                Ok(dt.into())
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for OffsetDateTime {
    fn from_value(value: &Value, type_: &Type) -> Result<Self, ConvertError> {
        if type_.code() != TypeCode::Timestamp {
            return Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            });
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                let dt = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?;
                Ok(dt)
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for Date {
    fn from_value(value: &Value, type_: &Type) -> Result<Self, ConvertError> {
        if type_.code() != TypeCode::Date {
            return Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            });
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                let date = Date::parse(s, crate::value::SPANNER_DATE_FORMAT)
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?;
                Ok(date)
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value, _type: &Type) -> Result<Self, ConvertError> {
        match &value.0.kind {
            Some(prost_types::value::Kind::BoolValue(b)) => Ok(*b),
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::Bool,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value, _type: &Type) -> Result<Self, ConvertError> {
        match &value.0.kind {
            Some(prost_types::value::Kind::NumberValue(n)) => Ok(*n),
            Some(prost_types::value::Kind::StringValue(s)) => {
                s.parse().map_err(|e| ConvertError::Convert(Box::new(e)))
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::Number,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for f32 {
    fn from_value(value: &Value, _type: &Type) -> Result<Self, ConvertError> {
        match &value.0.kind {
            Some(prost_types::value::Kind::NumberValue(n)) => Ok(*n as f32),
            Some(prost_types::value::Kind::StringValue(s)) => {
                s.parse().map_err(|e| ConvertError::Convert(Box::new(e)))
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::Number,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value, type_: &Type) -> Result<Self, ConvertError> {
        if type_.code() != TypeCode::Bytes && type_.code() != TypeCode::Proto {
            return Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            });
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => BASE64_STANDARD
                .decode(s)
                .map_err(|e| ConvertError::Convert(Box::new(e))),
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::String,
                got: value.kind(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::to_value::ToValue;
    use crate::types;

    #[test]
    fn test_from_value_string() {
        let v = "hello".to_value();
        let s = String::from_value(&v, &types::string()).unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_from_value_int() {
        let v = 42i64.to_value();
        let i = i64::from_value(&v, &types::int64()).unwrap();
        assert_eq!(i, 42);

        let v = 42i32.to_value();
        let i = i32::from_value(&v, &types::int64()).unwrap();
        assert_eq!(i, 42);

        // Negative tests
        let v = "not an int".to_value();
        let err = i64::from_value(&v, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));

        let v = "not an int".to_value();
        let err = i32::from_value(&v, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_float() {
        let v = 42.5f64.to_value();
        let f = f64::from_value(&v, &types::float64()).unwrap();
        assert_eq!(f, 42.5);

        let v = "Infinity".to_string().to_value();
        let f = f64::from_value(&v, &types::float64()).unwrap();
        assert_eq!(f, f64::INFINITY);

        let v = "invalid float".to_string().to_value();
        let err = f64::from_value(&v, &types::float64()).unwrap_err();
        assert!(format!("{}", err).contains("invalid float literal"));
    }

    #[test]
    fn test_from_value_bool() {
        let v = true.to_value();
        let b = bool::from_value(&v, &types::bool()).unwrap();
        assert!(b);
    }

    #[test]
    fn test_from_value_bytes() {
        let bytes = vec![1, 2, 3];
        let v = bytes.to_value();
        let b = Vec::<u8>::from_value(&v, &types::bytes()).unwrap();
        assert_eq!(b, bytes);

        let v = "invalid base64".to_string().to_value();
        let err = Vec::<u8>::from_value(&v, &types::bytes()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_decimal() {
        let d = Decimal::from_str_exact("123.456").unwrap();
        let v = d.to_value();
        let res = Decimal::from_value(&v, &types::numeric()).unwrap();
        assert_eq!(res, d);

        let v = "invalid decimal".to_string().to_value();
        let err = Decimal::from_value(&v, &types::numeric()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_date() {
        let d = Date::from_calendar_date(2023, time::Month::October, 27).unwrap();
        let v = d.to_value();
        let res = Date::from_value(&v, &types::date()).unwrap();
        assert_eq!(res, d);

        let v = "invalid date".to_string().to_value();
        let err = Date::from_value(&v, &types::date()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_timestamp() {
        let dt = OffsetDateTime::parse(
            "2023-10-27T10:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let v = dt.to_value();
        let res = OffsetDateTime::from_value(&v, &types::timestamp()).unwrap();
        assert_eq!(res, dt);

        let v = "invalid timestamp".to_string().to_value();
        let err = OffsetDateTime::from_value(&v, &types::timestamp()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_null() {
        let v = Option::<i32>::None.to_value();
        let res = Option::<i32>::from_value(&v, &types::int64()).unwrap();
        assert_eq!(res, None);

        let v = Option::<i32>::None.to_value();
        let err = i32::from_value(&v, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));
    }
    #[test]
    fn test_from_value_system_time() {
        let dt = OffsetDateTime::parse(
            "2023-10-27T10:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let system_time: SystemTime = dt.into();
        let v = system_time.to_value();
        let res = SystemTime::from_value(&v, &types::timestamp()).unwrap();
        let res_dt: OffsetDateTime = res.into();
        assert_eq!(res_dt, dt);

        let v = "invalid timestamp".to_string().to_value();
        let err = SystemTime::from_value(&v, &types::timestamp()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_type_mismatch() {
        let v = Decimal::from(42).to_value();
        let err = Decimal::from_value(&v, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("expected String, got String"));

        let v = SystemTime::now().to_value();
        let err = SystemTime::from_value(&v, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected String, got String")); // This might require adjustment as logic changed. In `SystemTime::from_value`, we check TypeCode first.

        let v = OffsetDateTime::now_utc().to_value();
        let err = OffsetDateTime::from_value(&v, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected String, got String"));

        let v = Date::from_calendar_date(2023, time::Month::October, 27)
            .unwrap()
            .to_value();
        let err = Date::from_value(&v, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected String, got String"));

        let v = vec![1u8].to_value();
        let err = Vec::<u8>::from_value(&v, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected String, got String"));
    }

    #[test]
    fn test_from_value_wrong_kind() {
        let v_bool = true.to_value();
        let err = String::from_value(&v_bool, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected String, got Bool"));

        let v_string = "hello".to_value();
        let err = i64::from_value(&v_string, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));

        let v_struct = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(
                prost_types::Struct::default(),
            )),
        });
        let err = i64::from_value(&v_struct, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("expected String, got Struct"));

        let err = f64::from_value(&v_bool, &types::float64()).unwrap_err();
        assert!(format!("{}", err).contains("expected Number, got Bool"));

        let err = bool::from_value(&v_string, &types::bool()).unwrap_err();
        assert!(format!("{}", err).contains("expected Bool, got String"));
    }

    #[test]
    fn test_from_value_null_errors() {
        let v_null = Option::<i32>::None.to_value();

        let err = String::from_value(&v_null, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = i64::from_value(&v_null, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = f64::from_value(&v_null, &types::float64()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = f32::from_value(&v_null, &types::float32()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = bool::from_value(&v_null, &types::bool()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = Decimal::from_value(&v_null, &types::numeric()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = SystemTime::from_value(&v_null, &types::timestamp()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = OffsetDateTime::from_value(&v_null, &types::timestamp()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = Date::from_value(&v_null, &types::date()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = Vec::<u8>::from_value(&v_null, &types::bytes()).unwrap_err();
        assert!(format!("{}", err).contains("expected non-null value, got null"));
    }

    #[test]
    fn test_from_value_option_missing_kind() {
        let v = crate::value::Value(prost_types::Value { kind: None });
        let err = Option::<i32>::from_value(&v, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("expected String, got Null"));
    }
}
