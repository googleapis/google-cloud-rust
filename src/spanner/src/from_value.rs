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
use crate::value::Value;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use google_cloud_gax::error::rpc::{Code, Status};
use rust_decimal::Decimal;
use std::time::SystemTime;

/// A trait for converting a Spanner Value to a Rust type.
pub trait FromValue: Sized {
    fn from_value(value: &Value, type_: &Type) -> crate::Result<Self>;
}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_value(value: &Value, type_: &Type) -> crate::Result<Self> {
        match &value.0.kind {
            Some(prost_types::value::Kind::NullValue(_)) => Ok(None),
            None => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("unexpected missing value kind"),
            )),
            _ => T::from_value(value, type_).map(Some),
        }
    }
}

impl FromValue for Value {
    fn from_value(value: &Value, _type: &Type) -> crate::Result<Self> {
        Ok(value.clone())
    }
}

impl FromValue for String {
    fn from_value(value: &Value, _type: &Type) -> crate::Result<Self> {
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => Ok(s.clone()),
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional String field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected String, got {:?}", value)),
            )),
        }
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value, _type: &Type) -> crate::Result<Self> {
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => s.parse().map_err(|e| {
                crate::Error::service(
                    Status::default()
                        .set_code(Code::InvalidArgument)
                        .set_message(format!("invalid int64 value '{}': {}", s, e)),
                )
            }),
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional i64 field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected String (int64), got {:?}", value)),
            )),
        }
    }
}

impl FromValue for i32 {
    fn from_value(value: &Value, _type: &Type) -> crate::Result<Self> {
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => s.parse().map_err(|e| {
                crate::Error::service(
                    Status::default()
                        .set_code(Code::InvalidArgument)
                        .set_message(format!("invalid int32 value '{}': {}", s, e)),
                )
            }),
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional i32 field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected String (int32), got {:?}", value)),
            )),
        }
    }
}

impl FromValue for Decimal {
    fn from_value(value: &Value, type_: &Type) -> crate::Result<Self> {
        if type_.code() != TypeCode::Numeric {
            return Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!(
                        "expected NUMERIC type code, got {:?}",
                        type_.code()
                    )),
            ));
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                Decimal::from_str_exact(s).map_err(|e| {
                    crate::Error::service(
                        Status::default()
                            .set_code(Code::InvalidArgument)
                            .set_message(format!("invalid Decimal value '{}': {}", s, e)),
                    )
                })
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional Decimal field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected String (Decimal), got {:?}", value)),
            )),
        }
    }
}

impl FromValue for SystemTime {
    fn from_value(value: &Value, type_: &Type) -> crate::Result<Self> {
        if type_.code() != TypeCode::Timestamp {
            return Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!(
                        "expected TIMESTAMP type code, got {:?}",
                        type_.code()
                    )),
            ));
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                let dt = chrono::DateTime::parse_from_rfc3339(s).map_err(|e| {
                    crate::Error::service(
                        Status::default()
                            .set_code(Code::InvalidArgument)
                            .set_message(format!("invalid TIMESTAMP value '{}': {}", s, e)),
                    )
                })?;
                Ok(SystemTime::from(dt.with_timezone(&chrono::Utc)))
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional SystemTime field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected String (TIMESTAMP), got {:?}", value)),
            )),
        }
    }
}

impl FromValue for chrono::DateTime<chrono::Utc> {
    fn from_value(value: &Value, type_: &Type) -> crate::Result<Self> {
        if type_.code() != TypeCode::Timestamp {
            return Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!(
                        "expected TIMESTAMP type code, got {:?}",
                        type_.code()
                    )),
            ));
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                let dt = chrono::DateTime::parse_from_rfc3339(s).map_err(|e| {
                    crate::Error::service(
                        Status::default()
                            .set_code(Code::InvalidArgument)
                            .set_message(format!("invalid TIMESTAMP value '{}': {}", s, e)),
                    )
                })?;
                Ok(dt.with_timezone(&chrono::Utc))
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional DateTime<Utc> field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected String (TIMESTAMP), got {:?}", value)),
            )),
        }
    }
}

impl FromValue for chrono::NaiveDate {
    fn from_value(value: &Value, type_: &Type) -> crate::Result<Self> {
        if type_.code() != TypeCode::Date {
            return Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected DATE type code, got {:?}", type_.code())),
            ));
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| {
                    crate::Error::service(
                        Status::default()
                            .set_code(Code::InvalidArgument)
                            .set_message(format!("invalid DATE value '{}': {}", s, e)),
                    )
                })?;
                Ok(date)
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional NaiveDate field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected String (DATE), got {:?}", value)),
            )),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value, _type: &Type) -> crate::Result<Self> {
        match &value.0.kind {
            Some(prost_types::value::Kind::BoolValue(b)) => Ok(*b),
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional bool field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected Bool, got {:?}", value)),
            )),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value, _type: &Type) -> crate::Result<Self> {
        match &value.0.kind {
            Some(prost_types::value::Kind::NumberValue(n)) => Ok(*n),
            Some(prost_types::value::Kind::StringValue(s)) => match s.as_str() {
                "Infinity" => Ok(f64::INFINITY),
                "-Infinity" => Ok(f64::NEG_INFINITY),
                "NaN" => Ok(f64::NAN),
                _ => Err(crate::Error::service(
                    Status::default()
                        .set_code(Code::InvalidArgument)
                        .set_message(format!("invalid f64 string '{}'", s)),
                )),
            },
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional f64 field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected Number or String (f64), got {:?}", value)),
            )),
        }
    }
}

impl FromValue for f32 {
    fn from_value(value: &Value, _type: &Type) -> crate::Result<Self> {
        match &value.0.kind {
            Some(prost_types::value::Kind::NumberValue(n)) => Ok(*n as f32),
            Some(prost_types::value::Kind::StringValue(s)) => match s.as_str() {
                "Infinity" => Ok(f32::INFINITY),
                "-Infinity" => Ok(f32::NEG_INFINITY),
                "NaN" => Ok(f32::NAN),
                _ => Err(crate::Error::service(
                    Status::default()
                        .set_code(Code::InvalidArgument)
                        .set_message(format!("invalid f32 string '{}'", s)),
                )),
            },
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional f32 field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected Number or String (f32), got {:?}", value)),
            )),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value, type_: &Type) -> crate::Result<Self> {
        if type_.code() != TypeCode::Bytes && type_.code() != TypeCode::Proto {
            return Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!(
                        "expected BYTES or PROTO type code, got {:?}",
                        type_.code()
                    )),
            ));
        }
        match &value.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => {
                BASE64_STANDARD.decode(s).map_err(|e| {
                    crate::Error::service(
                        Status::default()
                            .set_code(Code::InvalidArgument)
                            .set_message(format!("invalid base64 string '{}': {}", s, e)),
                    )
                })
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional Vec<u8> field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected String (base64), got {:?}", value)),
            )),
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
        assert!(format!("{}", err).contains("invalid int64 value 'not an int'"));

        let v = "not an int".to_value();
        let err = i32::from_value(&v, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("invalid int32 value 'not an int'"));
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
        assert!(format!("{}", err).contains("invalid f64 string 'invalid float'"));
    }

    #[test]
    fn test_from_value_bool() {
        let v = true.to_value();
        let b = bool::from_value(&v, &types::bool()).unwrap();
        assert_eq!(b, true);
    }

    #[test]
    fn test_from_value_bytes() {
        let bytes = vec![1, 2, 3];
        let v = bytes.to_value();
        let b = Vec::<u8>::from_value(&v, &types::bytes()).unwrap();
        assert_eq!(b, bytes);

        let v = "invalid base64".to_string().to_value();
        let err = Vec::<u8>::from_value(&v, &types::bytes()).unwrap_err();
        assert!(format!("{}", err).contains("invalid base64 string 'invalid base64'"));
    }

    #[test]
    fn test_from_value_decimal() {
        let d = Decimal::from_str_exact("123.456").unwrap();
        let v = d.to_value();
        let res = Decimal::from_value(&v, &types::numeric()).unwrap();
        assert_eq!(res, d);

        let v = "invalid decimal".to_string().to_value();
        let err = Decimal::from_value(&v, &types::numeric()).unwrap_err();
        assert!(format!("{}", err).contains("invalid Decimal value 'invalid decimal'"));
    }

    #[test]
    fn test_from_value_date() {
        let d = chrono::NaiveDate::from_ymd_opt(2023, 10, 27).unwrap();
        let v = d.to_value();
        let res = chrono::NaiveDate::from_value(&v, &types::date()).unwrap();
        assert_eq!(res, d);

        let v = "invalid date".to_string().to_value();
        let err = chrono::NaiveDate::from_value(&v, &types::date()).unwrap_err();
        assert!(format!("{}", err).contains("invalid DATE value 'invalid date'"));
    }

    #[test]
    fn test_from_value_timestamp() {
        let dt = chrono::DateTime::parse_from_rfc3339("2023-10-27T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let v = dt.to_value();
        let res = chrono::DateTime::<chrono::Utc>::from_value(&v, &types::timestamp()).unwrap();
        assert_eq!(res, dt);

        let v = "invalid timestamp".to_string().to_value();
        let err = chrono::DateTime::<chrono::Utc>::from_value(&v, &types::timestamp()).unwrap_err();
        assert!(format!("{}", err).contains("invalid TIMESTAMP value 'invalid timestamp'"));
    }

    #[test]
    fn test_from_value_null() {
        let v = Option::<i32>::None.to_value();
        let res = Option::<i32>::from_value(&v, &types::int64()).unwrap();
        assert_eq!(res, None);

        let v = Option::<i32>::None.to_value();
        let err = i32::from_value(&v, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional i32 field"));
    }
    #[test]
    fn test_from_value_system_time() {
        let dt = chrono::DateTime::parse_from_rfc3339("2023-10-27T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let system_time: SystemTime = dt.into();
        let v = system_time.to_value();
        let res = SystemTime::from_value(&v, &types::timestamp()).unwrap();
        let res_dt: chrono::DateTime<chrono::Utc> = res.into();
        assert_eq!(res_dt, dt);

        let v = "invalid timestamp".to_string().to_value();
        let err = SystemTime::from_value(&v, &types::timestamp()).unwrap_err();
        assert!(format!("{}", err).contains("invalid TIMESTAMP value 'invalid timestamp'"));
    }

    #[test]
    fn test_from_value_type_mismatch() {
        let v = Decimal::from(42).to_value();
        let err = Decimal::from_value(&v, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("expected NUMERIC type code, got Int64"));

        let v = SystemTime::now().to_value();
        let err = SystemTime::from_value(&v, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected TIMESTAMP type code, got String"));

        let v = chrono::Utc::now().to_value();
        let err = chrono::DateTime::<chrono::Utc>::from_value(&v, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected TIMESTAMP type code, got String"));

        let v = chrono::NaiveDate::from_ymd_opt(2023, 10, 27)
            .unwrap()
            .to_value();
        let err = chrono::NaiveDate::from_value(&v, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected DATE type code, got String"));

        let v = vec![1u8].to_value();
        let err = Vec::<u8>::from_value(&v, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected BYTES or PROTO type code, got String"));
    }

    #[test]
    fn test_from_value_wrong_kind() {
        let v_bool = true.to_value();
        let err = String::from_value(&v_bool, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("expected String, got"));

        let v_string = "hello".to_value();
        let err = i64::from_value(&v_string, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("invalid int64 value 'hello'")); // This is actually parsed, not wrong kind, but "not a number string"

        let v_struct = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(
                prost_types::Struct::default(),
            )),
        });
        let err = i64::from_value(&v_struct, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("expected String (int64), got"));

        let err = f64::from_value(&v_bool, &types::float64()).unwrap_err();
        assert!(format!("{}", err).contains("expected Number or String (f64), got"));

        let err = bool::from_value(&v_string, &types::bool()).unwrap_err();
        assert!(format!("{}", err).contains("expected Bool, got"));
    }

    #[test]
    fn test_from_value_null_errors() {
        let v_null = Option::<i32>::None.to_value();

        let err = String::from_value(&v_null, &types::string()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional String field"));

        let err = i64::from_value(&v_null, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional i64 field"));

        let err = f64::from_value(&v_null, &types::float64()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional f64 field"));

        let err = f32::from_value(&v_null, &types::float32()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional f32 field"));

        let err = bool::from_value(&v_null, &types::bool()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional bool field"));

        let err = Decimal::from_value(&v_null, &types::numeric()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional Decimal field"));

        let err = SystemTime::from_value(&v_null, &types::timestamp()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional SystemTime field"));

        let err =
            chrono::DateTime::<chrono::Utc>::from_value(&v_null, &types::timestamp()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional DateTime<Utc> field"));

        let err = chrono::NaiveDate::from_value(&v_null, &types::date()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional NaiveDate field"));

        let err = Vec::<u8>::from_value(&v_null, &types::bytes()).unwrap_err();
        assert!(format!("{}", err).contains("got null for non-optional Vec<u8> field"));
    }

    #[test]
    fn test_from_value_option_missing_kind() {
        let v = crate::value::Value(prost_types::Value { kind: None });
        let err = Option::<i32>::from_value(&v, &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("unexpected missing value kind"));
    }
}
