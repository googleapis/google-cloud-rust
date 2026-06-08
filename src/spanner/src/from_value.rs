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
use serde_json::Value as JsonValue;
use std::time::SystemTime;
use time::{Date, OffsetDateTime};

/// Represent failures in converting a Spanner Value to a Rust type.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ConvertError {
    /// The value kind is not as expected.
    #[error("expected {want:?}, got {got:?}")]
    KindMismatch {
        /// The expected Spanner value kind.
        want: Kind,
        /// The actual Spanner value kind.
        got: Kind,
    },

    /// The value is null, but the target type does not support nulls.
    #[error("expected non-null value, got null")]
    NotNull,

    /// There was a problem during conversion.
    #[error("cannot convert value, source={0}")]
    Convert(#[source] BoxedError),
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Converts a Spanner [Value] into a Rust type.
///
/// Implementations are provided for all standard types like `String`, primitive integer
/// and float types, decimals, timestamps, dates, vectors, and options for nullable fields.
pub trait FromValue: Sized {
    /// Converts a Spanner value into the target Rust type, using the provided
    /// Spanner `Type` metadata for compatibility checks.
    ///
    /// # Errors
    ///
    /// Returns a [`ConvertError`] if the kind of the value does not match the expected kind,
    /// if the value is null but the target type is not optional (e.g., `Option<T>`), or if
    /// parsing or decoding the inner value format fails.
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

/// Converts any Spanner [`Value`] into a [`serde_json::Value`] using runtime [`Type`] metadata.
///
/// This enables dynamic deserialization of STRUCT, ARRAY, and nested
/// `ARRAY<STRUCT<ARRAY<...>>>` columns without requiring predefined Rust types.
///
/// # Type mapping
///
/// | Spanner wire format | JSON output |
/// |---------------------|-------------|
/// | `NullValue` | `null` |
/// | `BoolValue` | `true` / `false` |
/// | `NumberValue` (finite) | JSON number |
/// | `NumberValue` (NaN/±Infinity) | `null` (matches SDK's `into_serde_value`) |
/// | `StringValue` | JSON string (or parsed JSON if `TypeCode::Json`) |
/// | `ListValue` + `TypeCode::Struct` | JSON object (positional → named via metadata) |
/// | `ListValue` + `TypeCode::Array` | JSON array |
/// | `StructValue` (named wire format) | JSON object |
///
/// # Known limitations
///
/// - **NaN/Infinity → null**: Non-finite floats become `null` since JSON has no
///   representation. Callers cannot distinguish these from genuine SQL NULLs.
/// - **Duplicate field names**: Spanner allows structs with duplicate field names
///   (e.g., unnamed columns). Since JSON objects require unique keys, last-write-wins
///   applies via `serde_json::Map::insert`.
/// - **Missing struct metadata**: When `TypeCode::Struct` is indicated but no
///   `struct_type` metadata is available, positional values are returned as a plain
///   JSON array to avoid silent data loss.
impl FromValue for JsonValue {
    fn from_value(value: &Value, type_: &Type) -> Result<Self, ConvertError> {
        from_value_recursive(value, type_, 0)
    }
}

const MAX_RECURSION_DEPTH: usize = 64;

fn from_value_recursive(
    value: &Value,
    type_: &Type,
    depth: usize,
) -> Result<JsonValue, ConvertError> {
    if depth > MAX_RECURSION_DEPTH {
        return Err(ConvertError::Convert(
            "maximum nesting depth exceeded".into(),
        ));
    }

    let default_type = Type::default();

    match &value.0.kind {
        Some(prost_types::value::Kind::NullValue(_)) => Ok(JsonValue::Null),

        Some(prost_types::value::Kind::NumberValue(n)) => {
            if let Some(num) = serde_json::Number::from_f64(*n) {
                Ok(JsonValue::Number(num))
            } else {
                Ok(JsonValue::Null)
            }
        }

        Some(prost_types::value::Kind::StringValue(s)) => {
            if type_.code() == TypeCode::Json {
                serde_json::from_str(s).map_err(|e| ConvertError::Convert(Box::new(e)))
            } else {
                Ok(JsonValue::String(s.clone()))
            }
        }

        Some(prost_types::value::Kind::BoolValue(b)) => Ok(JsonValue::Bool(*b)),

        Some(prost_types::value::Kind::StructValue(s)) => {
            let s = crate::value::Struct::from_ref(s);
            let mut map = serde_json::Map::new();

            if let Some(struct_type) = type_.struct_type() {
                for field in &struct_type.fields {
                    let field_type = field
                        .r#type
                        .as_deref()
                        .map(Type::from_ref)
                        .unwrap_or(&default_type);
                    let value = if let Some(v) = s.get(&field.name) {
                        from_value_recursive(v, field_type, depth + 1)?
                    } else {
                        JsonValue::Null
                    };
                    map.insert(field.name.clone(), value);
                }
            } else {
                for (k, v) in s.fields() {
                    map.insert(
                        k.to_string(),
                        from_value_recursive(v, &default_type, depth + 1)?,
                    );
                }
            }

            Ok(JsonValue::Object(map))
        }

        Some(prost_types::value::Kind::ListValue(list)) => match type_.code() {
            TypeCode::Struct => {
                if let Some(struct_type) = type_.struct_type() {
                    let mut map = serde_json::Map::new();
                    for (i, field) in struct_type.fields.iter().enumerate() {
                        let field_type = field
                            .r#type
                            .as_deref()
                            .map(Type::from_ref)
                            .unwrap_or(&default_type);
                        let value = if let Some(v) = list.values.get(i) {
                            let val = Value::from_ref(v);
                            from_value_recursive(val, field_type, depth + 1)?
                        } else {
                            JsonValue::Null
                        };
                        map.insert(field.name.clone(), value);
                    }
                    Ok(JsonValue::Object(map))
                } else {
                    let mut arr = Vec::with_capacity(list.values.len());
                    for v in &list.values {
                        let val = Value::from_ref(v);
                        arr.push(from_value_recursive(val, &default_type, depth + 1)?);
                    }
                    Ok(JsonValue::Array(arr))
                }
            }

            TypeCode::Array => {
                let element_type = type_.array_element_type().unwrap_or_default();
                let mut arr = Vec::with_capacity(list.values.len());
                for v in &list.values {
                    let val = Value::from_ref(v);
                    arr.push(from_value_recursive(val, &element_type, depth + 1)?);
                }
                Ok(JsonValue::Array(arr))
            }

            _ => {
                let mut arr = Vec::with_capacity(list.values.len());
                for v in &list.values {
                    let val = Value::from_ref(v);
                    arr.push(from_value_recursive(val, &default_type, depth + 1)?);
                }
                Ok(JsonValue::Array(arr))
            }
        },

        None => Ok(JsonValue::Null),
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

impl FromValue for wkt::Timestamp {
    fn from_value(value: &Value, type_: &Type) -> Result<Self, ConvertError> {
        let dt = OffsetDateTime::from_value(value, type_)?;
        wkt::Timestamp::try_from(dt).map_err(|e| ConvertError::Convert(Box::new(e)))
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

impl<T> FromValue for Vec<T>
where
    T: FromValue,
{
    fn from_value(value: &Value, r#type: &Type) -> Result<Self, ConvertError> {
        if r#type.code() != TypeCode::Array {
            return Err(ConvertError::KindMismatch {
                want: crate::value::Kind::List,
                got: value.kind(),
            });
        }
        let element_type = r#type
            .array_element_type()
            .ok_or_else(|| ConvertError::Convert("Array type missing element type".into()))?;

        match &value.0.kind {
            Some(prost_types::value::Kind::ListValue(list)) => {
                let mut vec = Vec::with_capacity(list.values.len());
                for v in &list.values {
                    // `Value` is a `#[repr(transparent)]` wrapper around `ProtoValue`.
                    // We use `from_ref` to safely cast the pointer and avoid cloning elements.
                    let val = crate::value::Value::from_ref(v);
                    vec.push(T::from_value(val, &element_type)?);
                }
                Ok(vec)
            }
            Some(prost_types::value::Kind::NullValue(_)) => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: crate::value::Kind::List,
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
    fn test_from_value_array() {
        // String array
        let str_array = vec!["one".to_string(), "two".to_string()];
        let v = str_array.to_value();
        let res = Vec::<String>::from_value(&v, &types::array(types::string()))
            .expect("parsed string array");
        assert_eq!(res, str_array);

        // Int array
        let int_array = vec![42i64, 100i64];
        let v = int_array.to_value();
        let res =
            Vec::<i64>::from_value(&v, &types::array(types::int64())).expect("parsed int array");
        assert_eq!(res, int_array);

        // Bool array
        let bool_array = vec![true, false];
        let v = bool_array.to_value();
        let res =
            Vec::<bool>::from_value(&v, &types::array(types::bool())).expect("parsed bool array");
        assert_eq!(res, bool_array);

        // Float array
        let float_array = vec![9.9f64, -2.5f64];
        let v = float_array.to_value();
        let res = Vec::<f64>::from_value(&v, &types::array(types::float64()))
            .expect("parsed float array");
        assert_eq!(res, float_array);

        // Empty array
        let empty_array: Vec<f64> = vec![];
        let v = empty_array.to_value();
        let res = Vec::<f64>::from_value(&v, &types::array(types::float64()))
            .expect("parsed empty array");
        assert_eq!(res, empty_array);

        // Array with nulls
        let opt_array: Vec<Option<i64>> = vec![Some(42), None, Some(100)];
        let v = opt_array.to_value();
        let res = Vec::<Option<i64>>::from_value(&v, &types::array(types::int64()))
            .expect("parsed optional array");
        assert_eq!(res, opt_array);

        // Null array entirely
        let null_array: Option<Vec<i64>> = None;
        let v = null_array.to_value();
        let res = Option::<Vec<i64>>::from_value(&v, &types::array(types::int64()))
            .expect("parsed null array");
        assert_eq!(res, null_array);

        // Wrong TypeCode test
        let err = Vec::<i64>::from_value(&int_array.to_value(), &types::int64()).unwrap_err();
        assert!(format!("{}", err).contains("expected List"));

        // Invalid array element values
        let err = Vec::<i64>::from_value(&str_array.to_value(), &types::array(types::int64()))
            .unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value, source="));
    }

    #[test]
    fn test_from_value_bytes() {
        let bytes: Vec<u8> = vec![1, 2, 3];
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
    fn test_from_value_wkt_timestamp() {
        let dt = OffsetDateTime::parse(
            "2023-10-27T10:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("valid date time parsing");
        let wkt_ts = wkt::Timestamp::try_from(dt).expect("valid wkt timestamp conversion");
        let v = dt.to_value();
        let res = wkt::Timestamp::from_value(&v, &types::timestamp())
            .expect("valid wkt timestamp decoding");
        assert_eq!(res, wkt_ts);

        let v = "invalid timestamp".to_string().to_value();
        let err = wkt::Timestamp::from_value(&v, &types::timestamp()).unwrap_err();
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

    // ── JSON value conversion tests ──────────────────────────────────────

    #[test]
    fn test_from_value_json_primitives() {
        use serde_json::Value as JsonValue;

        // String → JSON string
        let v = "hello".to_value();
        let j = JsonValue::from_value(&v, &types::string()).unwrap();
        assert_eq!(j, JsonValue::String("hello".to_string()));

        // INT64 is string-encoded on the wire → stays as JSON string
        // (preserves full i64 range without precision loss)
        let v = 42i64.to_value();
        let j = JsonValue::from_value(&v, &types::int64()).unwrap();
        assert_eq!(j, JsonValue::String("42".to_string()));

        // Bool → JSON bool
        let v = true.to_value();
        let j = JsonValue::from_value(&v, &types::bool()).unwrap();
        assert_eq!(j, JsonValue::Bool(true));

        // Float64 → JSON number
        let v = 3.14f64.to_value();
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, serde_json::json!(3.14));

        // Null → JSON null
        let v: Option<i64> = None;
        let j = JsonValue::from_value(&v.to_value(), &types::int64()).unwrap();
        assert_eq!(j, JsonValue::Null);

        // Missing kind → JSON null
        let v = crate::value::Value(prost_types::Value { kind: None });
        let j = JsonValue::from_value(&v, &types::string()).unwrap();
        assert_eq!(j, JsonValue::Null);
    }

    #[test]
    fn test_from_value_json_string_array() {
        use serde_json::Value as JsonValue;

        let str_array = vec!["a".to_string(), "b".to_string()];
        let v = str_array.to_value();
        let j = JsonValue::from_value(&v, &types::array(types::string())).unwrap();
        assert_eq!(j, serde_json::json!(["a", "b"]));
    }

    #[test]
    fn test_from_value_json_int_array() {
        use serde_json::Value as JsonValue;

        // INT64 array — values are string-encoded
        let int_array = vec![10i64, 20i64];
        let v = int_array.to_value();
        let j = JsonValue::from_value(&v, &types::array(types::int64())).unwrap();
        assert_eq!(j, serde_json::json!(["10", "20"]));
    }

    #[test]
    fn test_from_value_json_positional_struct() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type: STRUCT<name STRING, age INT64>
        let struct_type = types::create_type(TypeCode::Struct);
        let mut inner: mdl::Type = struct_type.0;
        inner.struct_type = Some(Box::new(mdl::StructType {
            fields: vec![
                mdl::struct_type::Field::new()
                    .set_name("name")
                    .set_type(mdl::Type {
                        code: mdl::TypeCode::String,
                        ..Default::default()
                    }),
                mdl::struct_type::Field::new()
                    .set_name("age")
                    .set_type(mdl::Type {
                        code: mdl::TypeCode::Int64,
                        ..Default::default()
                    }),
            ],
            _unknown_fields: Default::default(),
        }));
        let spanner_type = Type(inner);

        // Wire value: positional ListValue ["Alice", "30"]
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("Alice".to_string())),
                    },
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("30".to_string())),
                    },
                ],
            })),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"name": "Alice", "age": "30"}));
    }

    #[test]
    fn test_from_value_json_array_of_structs() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type: ARRAY<STRUCT<a STRING, b INT64>>
        let elem_struct = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![
                    mdl::struct_type::Field::new()
                        .set_name("a")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::String,
                            ..Default::default()
                        }),
                    mdl::struct_type::Field::new()
                        .set_name("b")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Int64,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let array_type = mdl::Type {
            code: mdl::TypeCode::Array,
            array_element_type: Some(Box::new(elem_struct)),
            ..Default::default()
        };
        let spanner_type = Type(array_type);

        // Wire: [[x, 1], [y, 2]] — positional structs inside an array
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                            values: vec![
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "x".to_string(),
                                    )),
                                },
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "1".to_string(),
                                    )),
                                },
                            ],
                        })),
                    },
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                            values: vec![
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "y".to_string(),
                                    )),
                                },
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "2".to_string(),
                                    )),
                                },
                            ],
                        })),
                    },
                ],
            })),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(
            j,
            serde_json::json!([
                {"a": "x", "b": "1"},
                {"a": "y", "b": "2"},
            ])
        );
    }

    #[test]
    fn test_from_value_json_named_struct() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type: STRUCT<name STRING>
        let struct_type_mdl = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![mdl::struct_type::Field::new()
                    .set_name("name")
                    .set_type(mdl::Type {
                        code: mdl::TypeCode::String,
                        ..Default::default()
                    })],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_mdl);

        // Wire: named StructValue (rare but valid)
        let mut s = prost_types::Struct::default();
        s.fields.insert(
            "name".to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("Alice".to_string())),
            },
        );
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(s)),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"name": "Alice"}));
    }

    #[test]
    fn test_from_value_json_named_struct_missing_fields_become_null() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type declares 2 fields but wire StructValue only has 1
        let struct_type_mdl = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![
                    mdl::struct_type::Field::new()
                        .set_name("present")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::String,
                            ..Default::default()
                        }),
                    mdl::struct_type::Field::new()
                        .set_name("absent")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Int64,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_mdl);

        // Wire: named StructValue with only "present" field
        let mut s = prost_types::Struct::default();
        s.fields.insert(
            "present".to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("here".to_string())),
            },
        );
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(s)),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"present": "here", "absent": null}));
    }

    #[test]
    fn test_from_value_json_spanner_json_column() {
        use serde_json::Value as JsonValue;

        // Spanner JSON column: value arrives as a StringValue containing JSON text
        let json_str = r#"{"key": "value", "nested": [1, 2, 3]}"#;
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue(json_str.to_string())),
        });

        let j = JsonValue::from_value(&v, &types::json()).unwrap();
        assert_eq!(
            j,
            serde_json::json!({"key": "value", "nested": [1, 2, 3]})
        );
    }

    #[test]
    fn test_from_value_json_nested_struct_in_struct() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type: STRUCT<outer_field STRING, inner STRUCT<x INT64, y BOOL>>
        let inner_struct_type = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![
                    mdl::struct_type::Field::new()
                        .set_name("x")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Int64,
                            ..Default::default()
                        }),
                    mdl::struct_type::Field::new()
                        .set_name("y")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Bool,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };

        let outer_type = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![
                    mdl::struct_type::Field::new()
                        .set_name("outer_field")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::String,
                            ..Default::default()
                        }),
                    mdl::struct_type::Field::new()
                        .set_name("inner")
                        .set_type(inner_struct_type),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(outer_type);

        // Wire: positional list ["hello", ["42", true]]
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("hello".to_string())),
                    },
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                            values: vec![
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "42".to_string(),
                                    )),
                                },
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::BoolValue(true)),
                                },
                            ],
                        })),
                    },
                ],
            })),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(
            j,
            serde_json::json!({"outer_field": "hello", "inner": {"x": "42", "y": true}})
        );
    }

    #[test]
    fn test_from_value_json_null_in_struct() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type: STRUCT<name STRING, value INT64>
        let struct_type_mdl = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![
                    mdl::struct_type::Field::new()
                        .set_name("name")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::String,
                            ..Default::default()
                        }),
                    mdl::struct_type::Field::new()
                        .set_name("value")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Int64,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_mdl);

        // Wire: ["test", null]
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("test".to_string())),
                    },
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::NullValue(0)),
                    },
                ],
            })),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"name": "test", "value": null}));
    }

    #[test]
    fn test_from_value_json_nan_becomes_null() {
        use serde_json::Value as JsonValue;

        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::NumberValue(f64::NAN)),
        });
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, JsonValue::Null);

        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::NumberValue(f64::INFINITY)),
        });
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, JsonValue::Null);

        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::NumberValue(f64::NEG_INFINITY)),
        });
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, JsonValue::Null);
    }

    #[test]
    fn test_from_value_json_option_wrapping() {
        use serde_json::Value as JsonValue;

        // Option<JsonValue> for a non-null value
        let v = "hello".to_value();
        let j = Option::<JsonValue>::from_value(&v, &types::string()).unwrap();
        assert_eq!(j, Some(JsonValue::String("hello".to_string())));

        // Option<JsonValue> for a null value
        let v: Option<String> = None;
        let j = Option::<JsonValue>::from_value(&v.to_value(), &types::string()).unwrap();
        assert_eq!(j, None);
    }

    #[test]
    fn test_from_value_json_empty_struct() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type: STRUCT<> (zero fields)
        let struct_type_mdl = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_mdl);

        // Wire: empty positional list
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![],
            })),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({}));
    }

    #[test]
    fn test_from_value_json_unnamed_fields() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type: STRUCT with unnamed fields (empty string names)
        // This is valid in Spanner for SELECT expressions without aliases.
        let struct_type_mdl = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![
                    mdl::struct_type::Field::new()
                        .set_name("")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::String,
                            ..Default::default()
                        }),
                    mdl::struct_type::Field::new()
                        .set_name("")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Int64,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_mdl);

        // Wire: positional values
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("val".to_string())),
                    },
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("99".to_string())),
                    },
                ],
            })),
        });

        // Unnamed fields map to empty-string keys; last write wins for duplicates.
        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert!(j.is_object());
        // With duplicate empty keys, the map retains the last inserted value
        assert_eq!(j.as_object().unwrap().get("").unwrap(), &serde_json::json!("99"));
    }

    #[test]
    fn test_from_value_json_vec_composition() {
        use serde_json::Value as JsonValue;

        // Vec<JsonValue> uses the existing Vec<T>: FromValue impl
        let str_array = vec!["one".to_string(), "two".to_string()];
        let v = str_array.to_value();
        let res =
            Vec::<JsonValue>::from_value(&v, &types::array(types::string())).unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], JsonValue::String("one".to_string()));
        assert_eq!(res[1], JsonValue::String("two".to_string()));
    }

    #[test]
    fn test_from_value_json_struct_without_metadata() {
        use serde_json::Value as JsonValue;

        // StructValue wire format without type metadata — preserves raw field names
        let mut s = prost_types::Struct::default();
        s.fields.insert(
            "raw_field".to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::BoolValue(true)),
            },
        );
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(s)),
        });

        // Pass default type (no struct_type metadata)
        let j = JsonValue::from_value(&v, &Type::default()).unwrap();
        assert_eq!(j, serde_json::json!({"raw_field": true}));
    }

    #[test]
    fn test_from_value_json_list_without_type_info() {
        use serde_json::Value as JsonValue;

        // ListValue with no type context — falls back to plain array
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("a".to_string())),
                    },
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::BoolValue(false)),
                    },
                ],
            })),
        });

        let j = JsonValue::from_value(&v, &Type::default()).unwrap();
        assert_eq!(j, serde_json::json!(["a", false]));
    }

    #[test]
    fn test_from_value_json_invalid_json_column() {
        use serde_json::Value as JsonValue;

        // Spanner JSON column with invalid JSON text → error
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue("not valid json{".to_string())),
        });

        let err = JsonValue::from_value(&v, &types::json()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_json_missing_positional_fields_become_null() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type declares 3 fields but wire only has 1 value
        let struct_type_mdl = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![
                    mdl::struct_type::Field::new()
                        .set_name("a")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::String,
                            ..Default::default()
                        }),
                    mdl::struct_type::Field::new()
                        .set_name("b")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Int64,
                            ..Default::default()
                        }),
                    mdl::struct_type::Field::new()
                        .set_name("c")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Bool,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_mdl);

        // Wire: only one value present
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue("only_a".to_string())),
                }],
            })),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(
            j,
            serde_json::json!({"a": "only_a", "b": null, "c": null})
        );
    }

    #[test]
    fn test_from_value_json_array_of_json_columns() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type: ARRAY<JSON>
        let json_type = mdl::Type {
            code: mdl::TypeCode::Json,
            ..Default::default()
        };
        let array_type = mdl::Type {
            code: mdl::TypeCode::Array,
            array_element_type: Some(Box::new(json_type)),
            ..Default::default()
        };
        let spanner_type = Type(array_type);

        // Wire: array of JSON strings
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            r#"{"x":1}"#.to_string(),
                        )),
                    },
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            r#"{"y":2}"#.to_string(),
                        )),
                    },
                ],
            })),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!([{"x": 1}, {"y": 2}]));
    }

    #[test]
    fn test_from_value_json_nested_array_in_struct() {
        use crate::generated::gapic_dataplane::model as mdl;
        use serde_json::Value as JsonValue;

        // Type: STRUCT<tags ARRAY<STRING>, id INT64>
        let struct_type_mdl = mdl::Type {
            code: mdl::TypeCode::Struct,
            struct_type: Some(Box::new(mdl::StructType {
                fields: vec![
                    mdl::struct_type::Field::new()
                        .set_name("tags")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Array,
                            array_element_type: Some(Box::new(mdl::Type {
                                code: mdl::TypeCode::String,
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                    mdl::struct_type::Field::new()
                        .set_name("id")
                        .set_type(mdl::Type {
                            code: mdl::TypeCode::Int64,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_mdl);

        // Wire: [["foo", "bar"], "42"]
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: vec![
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                            values: vec![
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "foo".to_string(),
                                    )),
                                },
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "bar".to_string(),
                                    )),
                                },
                            ],
                        })),
                    },
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("42".to_string())),
                    },
                ],
            })),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"tags": ["foo", "bar"], "id": "42"}));
    }

    #[test]
    fn test_from_value_json_recursion_depth_limit() {
        use serde_json::Value as JsonValue;

        // Build a deeply nested ListValue (65 levels deep) to exceed MAX_RECURSION_DEPTH
        let mut inner = prost_types::Value {
            kind: Some(prost_types::value::Kind::BoolValue(true)),
        };
        for _ in 0..65 {
            inner = prost_types::Value {
                kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue {
                    values: vec![inner],
                })),
            };
        }
        let v = crate::value::Value(inner);

        let err = JsonValue::from_value(&v, &Type::default()).unwrap_err();
        assert!(format!("{}", err).contains("nesting depth exceeded"));
    }
}
