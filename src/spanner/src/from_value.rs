use crate::types::{Type, TypeCode};
use crate::value::Value;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bigdecimal::BigDecimal;
use google_cloud_gax::error::rpc::{Code, Status};
use std::str::FromStr;
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
                        .set_message(format!("invalid int64: {}", e)),
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
                        .set_message(format!("invalid int32: {}", e)),
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

impl FromValue for BigDecimal {
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
                BigDecimal::from_str(s).map_err(|e| {
                    crate::Error::service(
                        Status::default()
                            .set_code(Code::InvalidArgument)
                            .set_message(format!("invalid BigDecimal: {}", e)),
                    )
                })
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::Internal)
                    .set_message("got null for non-optional BigDecimal field"),
            )),
            _ => Err(crate::Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(format!("expected String (BigDecimal), got {:?}", value)),
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
                            .set_message(format!("invalid TIMESTAMP: {}", e)),
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
                            .set_message(format!("invalid TIMESTAMP: {}", e)),
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
                            .set_message(format!("invalid DATE: {}", e)),
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
                        .set_message(format!("invalid f64 string: {}", s)),
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
                        .set_message(format!("invalid f32 string: {}", s)),
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
                            .set_message(format!("invalid base64 string: {}", e)),
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
