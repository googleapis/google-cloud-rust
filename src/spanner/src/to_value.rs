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

#[cfg(feature = "time")]
use crate::value::SPANNER_DATE_FORMAT;
use crate::value::SPANNER_TIMESTAMP_FORMAT;
pub use crate::value::Value;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use google_cloud_type::model::Date;
use prost_types::Value as ProtoValue;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use std::time::SystemTime;
#[cfg(feature = "time")]
use time::Date as TimeDate;
use time::OffsetDateTime;

/// Converts Rust types to Spanner [Value].
///
/// This trait is used to encode native Rust types into the generic `Value`
/// representation suitable for transmission to Cloud Spanner (such as in query parameters
/// or mutation values).
///
/// Implementations are provided for standard Rust types, mapping them to their appropriate
/// Spanner values. For example, optional types naturally map to `Value::Null` when they are `None`.
pub trait ToValue {
    /// Encodes this Rust type as a Spanner `Value`.
    ///
    /// Implementations are responsible for using the correct value kind for the
    /// corresponding data type in Spanner.
    fn to_value(&self) -> Value;
}

impl<T> ToValue for Option<T>
where
    T: ToValue,
{
    fn to_value(&self) -> Value {
        match self {
            Some(v) => v.to_value(),
            None => Value::null(),
        }
    }
}

/// Converts an optional type into a [Value].
impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => Value::null(),
        }
    }
}

impl ToValue for () {
    fn to_value(&self) -> Value {
        Value::null()
    }
}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Value::null()
    }
}

impl ToValue for Value {
    fn to_value(&self) -> Value {
        self.clone()
    }
}

impl ToValue for JsonValue {
    fn to_value(&self) -> Value {
        self.clone().into()
    }
}

impl From<JsonValue> for Value {
    fn from(json_value: JsonValue) -> Self {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                json_value.to_string(),
            )),
        })
    }
}

impl ToValue for String {
    fn to_value(&self) -> Value {
        self.as_str().to_value()
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(s)),
        })
    }
}

impl ToValue for str {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(self.to_string())),
        })
    }
}

impl ToValue for &str {
    fn to_value(&self) -> Value {
        <str as ToValue>::to_value(*self)
    }
}

impl ToValue for i64 {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(i.to_string())),
        })
    }
}

impl ToValue for i32 {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(i.to_string())),
        })
    }
}

impl ToValue for Decimal {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

impl From<Decimal> for Value {
    fn from(d: Decimal) -> Self {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(d.to_string())),
        })
    }
}

fn format_timestamp(date_time: OffsetDateTime) -> Value {
    let string_value = date_time
        .format(SPANNER_TIMESTAMP_FORMAT)
        .unwrap_or_else(|_| "invalid timestamp".to_string());
    Value(ProtoValue {
        kind: Some(prost_types::value::Kind::StringValue(string_value)),
    })
}

impl ToValue for SystemTime {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

impl From<SystemTime> for Value {
    fn from(system_time: SystemTime) -> Self {
        format_timestamp(OffsetDateTime::from(system_time))
    }
}

#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
impl ToValue for OffsetDateTime {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
impl From<OffsetDateTime> for Value {
    fn from(date_time: OffsetDateTime) -> Self {
        format_timestamp(date_time)
    }
}

impl ToValue for wkt::Timestamp {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

impl From<wkt::Timestamp> for Value {
    fn from(timestamp: wkt::Timestamp) -> Self {
        let nanos = timestamp.seconds() as i128 * 1_000_000_000 + timestamp.nanos() as i128;
        if let Ok(date_time) = OffsetDateTime::from_unix_timestamp_nanos(nanos) {
            return format_timestamp(date_time);
        }
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(String::from(
                timestamp,
            ))),
        })
    }
}

impl ToValue for Date {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(format!(
                "{:04}-{:02}-{:02}",
                self.year, self.month, self.day
            ))),
        })
    }
}

impl From<Date> for Value {
    fn from(date: Date) -> Self {
        date.to_value()
    }
}

#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
impl ToValue for TimeDate {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
impl From<TimeDate> for Value {
    fn from(date: TimeDate) -> Self {
        let string_value = date
            .format(SPANNER_DATE_FORMAT)
            .unwrap_or_else(|_| "invalid date".to_string());
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(string_value)),
        })
    }
}

impl ToValue for bool {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::BoolValue(b)),
        })
    }
}

impl ToValue for f64 {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

impl From<f64> for Value {
    fn from(float_value: f64) -> Self {
        let kind = if float_value.is_finite() {
            prost_types::value::Kind::NumberValue(float_value)
        } else if float_value.is_nan() {
            prost_types::value::Kind::StringValue("NaN".to_string())
        } else if float_value.is_sign_positive() {
            prost_types::value::Kind::StringValue("Infinity".to_string())
        } else {
            prost_types::value::Kind::StringValue("-Infinity".to_string())
        };
        Value(ProtoValue { kind: Some(kind) })
    }
}

impl ToValue for f32 {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

impl From<f32> for Value {
    fn from(float_value: f32) -> Self {
        let kind = if float_value.is_finite() {
            prost_types::value::Kind::NumberValue(float_value as f64)
        } else if float_value.is_nan() {
            prost_types::value::Kind::StringValue("NaN".to_string())
        } else if float_value.is_sign_positive() {
            prost_types::value::Kind::StringValue("Infinity".to_string())
        } else {
            prost_types::value::Kind::StringValue("-Infinity".to_string())
        };
        Value(ProtoValue { kind: Some(kind) })
    }
}

impl ToValue for Vec<u8> {
    fn to_value(&self) -> Value {
        self.as_slice().to_value()
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                BASE64_STANDARD.encode(v),
            )),
        })
    }
}

impl ToValue for [u8] {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                BASE64_STANDARD.encode(self),
            )),
        })
    }
}

impl ToValue for &[u8] {
    fn to_value(&self) -> Value {
        <[u8] as ToValue>::to_value(*self)
    }
}

impl<T> ToValue for Vec<T>
where
    T: ToValue,
{
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: self.iter().map(|v| v.to_value().0).collect(),
                },
            )),
        })
    }
}

/// Converts a vector of values into a List [Value].
impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: v.into_iter().map(|item| item.into().0).collect(),
                },
            )),
        })
    }
}

/// Converts a reference to any [ToValue] type into a [Value] by calling
/// [ToValue::to_value], which copies the referenced data.
impl<T: ToValue + ?Sized> From<&T> for Value {
    fn from(t: &T) -> Self {
        t.to_value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Kind;
    use std::str::FromStr;
    use std::time::Duration;
    #[cfg(feature = "time")]
    use time::Month;
    #[cfg(feature = "time")]
    use time::format_description::well_known::Rfc3339;

    #[test]
    fn test_null_value_conversions() {
        let null_val = Value::null();
        assert_eq!(null_val.kind(), Kind::Null);

        let unit_val = ().to_value();
        assert_eq!(unit_val, null_val);

        let unit_ref_into: Value = (&()).into();
        assert_eq!(unit_ref_into, null_val);

        let some_unit = Some(()).to_value();
        assert_eq!(some_unit, null_val);

        let opt_unit: Value = None::<()>.into();
        assert_eq!(opt_unit, null_val);

        let opt_val: Value = None::<Value>.into();
        assert_eq!(opt_val, null_val);

        let opt_i64: Value = None::<i64>.into();
        assert_eq!(opt_i64, null_val);
    }

    #[test]
    fn test_from_value_conversions() {
        let v: Value = "hello".to_string().into();
        assert_eq!(v.as_string(), "hello");

        let v: Value = "world".into();
        assert_eq!(v.as_string(), "world");

        let v: Value = 42i64.into();
        assert_eq!(v.as_string(), "42");

        let v: Value = 42i32.into();
        assert_eq!(v.as_string(), "42");

        let v: Value = true.into();
        assert!(v.as_bool());

        let v: Value = 42.5f64.into();
        assert_eq!(v.as_f64(), 42.5);

        let v: Value = 42.5f32.into();
        assert_eq!(v.as_f64(), 42.5);

        let v: Value = vec![1u8, 2, 3].into();
        assert_eq!(v.as_string(), "AQID");

        let v: Value = (&[1u8, 2, 3][..]).into();
        assert_eq!(v.as_string(), "AQID");

        let d = Decimal::from_str("123.456").expect("valid decimal");
        let v: Value = d.into();
        assert_eq!(v.as_string(), "123.456");

        #[cfg(feature = "time")]
        {
            let date_time = OffsetDateTime::parse("2023-10-27T10:00:00Z", &Rfc3339)
                .expect("valid RFC 3339 format");
            let v: Value = date_time.into();
            assert_eq!(v.as_string(), "2023-10-27T10:00:00.000000000Z");

            let time_date = TimeDate::from_calendar_date(2023, Month::October, 27)
                .expect("valid calendar date");
            let v: Value = time_date.into();
            assert_eq!(v.as_string(), "2023-10-27");
        }

        let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1698400800);
        let v: Value = system_time.into();
        assert_eq!(v.as_string(), "2023-10-27T10:00:00.000000000Z");

        let timestamp = wkt::Timestamp::clamp(1698400800, 0);
        let v: Value = timestamp.into();
        assert_eq!(v.as_string(), "2023-10-27T10:00:00.000000000Z");

        let google_date = Date::new().set_year(2023).set_month(10).set_day(27);
        let v: Value = (&google_date).into();
        assert_eq!(v.as_string(), "2023-10-27");

        let json_val = serde_json::json!({"key": "value"});
        let v: Value = json_val.into();
        assert_eq!(v.as_string(), "{\"key\":\"value\"}");

        let list: Value = vec![1i64, 2i64].into();
        assert_eq!(list.kind(), Kind::List);
        assert_eq!(list.as_list().len(), 2);

        let opt_some: Value = Some(42i64).into();
        assert_eq!(opt_some.as_string(), "42");
        let opt_none: Value = None::<i64>.into();
        assert_eq!(opt_none.kind(), Kind::Null);

        let unit: Value = ().into();
        assert_eq!(unit.kind(), Kind::Null);

        let borrowed_i64: Value = (&42i64).into();
        assert_eq!(borrowed_i64.as_string(), "42");
        let borrowed_bool: Value = (&true).into();
        assert!(borrowed_bool.as_bool());
        let borrowed_float: Value = (&42.5f64).into();
        assert_eq!(borrowed_float.as_f64(), 42.5);
        let borrowed_date: Value = google_date.into();
        assert_eq!(borrowed_date.as_string(), "2023-10-27");

        let vec_opt: Value = vec![Some(1_i64), None].into();
        assert_eq!(vec_opt.kind(), Kind::List);
        assert_eq!(vec_opt.as_list().len(), 2);
    }

    #[test]
    fn test_to_value_string() {
        let v = "hello".to_string().to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "hello");

        let v = "world".to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "world");
    }

    #[test]
    fn test_to_value_str_trait_bound() {
        fn bind_param<T: ToValue + ?Sized>(val: &T) -> Value {
            val.to_value()
        }

        let v = bind_param("hello");
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "hello");

        let owned: String = "hello".to_string();
        assert_eq!(bind_param(&owned), v);
        let borrowed: &str = &owned;
        assert_eq!(bind_param(&borrowed), v);
    }

    #[test]
    fn test_to_value_int() {
        let v = 42i64.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "42");

        let v = 42i32.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "42");
    }

    #[test]
    fn test_to_value_float() {
        let v = 42.5f64.to_value();
        assert_eq!(v.kind(), Kind::Number);
        assert_eq!(v.as_f64(), 42.5);

        let v = 42.5f32.to_value();
        assert_eq!(v.kind(), Kind::Number);
        assert_eq!(v.as_f64(), 42.5);
    }

    #[test]
    fn test_to_value_bool() {
        let v = true.to_value();
        assert_eq!(v.kind(), Kind::Bool);
        assert!(v.as_bool());

        let v = false.to_value();
        assert_eq!(v.kind(), Kind::Bool);
        assert!(!v.as_bool());
    }

    #[test]
    fn test_to_value_bytes() {
        let bytes: Vec<u8> = vec![1, 2, 3];
        let v = bytes.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "AQID"); // Base64 encoded

        let slice_val: Value = (&[1u8, 2, 3][..]).into();
        assert_eq!(slice_val.kind(), Kind::String);
        assert_eq!(slice_val.as_string(), "AQID");
    }

    #[test]
    fn test_to_value_slice_trait_bound() {
        fn bind_param<T: ToValue + ?Sized>(val: &T) -> Value {
            val.to_value()
        }

        let slice: &[u8] = &[1, 2, 3];
        let v = bind_param(slice);
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "AQID");
    }

    #[test]
    fn test_to_value_decimal() {
        let d = Decimal::from_str("123.456").unwrap();
        let v = d.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "123.456");
    }

    #[test]
    fn test_to_value_google_date() {
        let date = Date::new().set_year(2023).set_month(10).set_day(27);
        let value = date.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_string(), "2023-10-27");
    }

    #[cfg(feature = "time")]
    #[test]
    fn test_to_value_time_date() {
        let date =
            TimeDate::from_calendar_date(2023, Month::October, 27).expect("valid calendar date");
        let value = date.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_string(), "2023-10-27");
    }

    #[test]
    fn test_to_value_system_time() {
        let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1698400800);
        let value = system_time.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_string(), "2023-10-27T10:00:00.000000000Z");
    }

    #[cfg(feature = "time")]
    #[test]
    fn test_to_value_offset_date_time() {
        let date_time = OffsetDateTime::parse("2023-10-27T10:00:00Z", &Rfc3339)
            .expect("valid date time parsing");
        let value = date_time.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_string(), "2023-10-27T10:00:00.000000000Z");
    }

    #[test]
    fn test_to_value_wkt_timestamp() {
        let timestamp = wkt::Timestamp::clamp(1698400800, 0);
        let value = timestamp.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_string(), "2023-10-27T10:00:00.000000000Z");
    }

    #[test]
    fn test_to_value_json() {
        let json_value = serde_json::json!({"test": 123});
        let value = json_value.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_string(), "{\"test\":123}");
    }

    #[test]
    fn test_to_value_option() {
        let some_val: Option<i32> = Some(42);
        let v = some_val.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "42");

        let none_val: Option<i32> = None;
        let v = none_val.to_value();
        assert_eq!(v.kind(), Kind::Null);
    }

    #[test]
    fn test_to_value_value() {
        let v_original = 42i32.to_value();
        let v = v_original.to_value();
        assert_eq!(v, v_original);
    }

    #[test]
    fn test_to_value_array() {
        let str_array = vec!["one".to_string(), "two".to_string()];
        let v = str_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 2);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_string(),
            "one"
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").as_string(),
            "two"
        );

        let int_array = vec![42i64, 100i64];
        let v = int_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 2);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_string(),
            "42"
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").as_string(),
            "100"
        );

        let bool_array = vec![true, false];
        let v = bool_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 2);
        assert!(list.get(0).expect("element 0 should exist").as_bool());
        assert!(!list.get(1).expect("element 1 should exist").as_bool());

        let float_array = vec![9.9f64, -2.5f64];
        let v = float_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 2);
        assert_eq!(list.get(0).expect("element 0 should exist").as_f64(), 9.9);
        assert_eq!(list.get(1).expect("element 1 should exist").as_f64(), -2.5);

        let empty_array: Vec<f64> = vec![];
        let v = empty_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        assert_eq!(v.as_list().len(), 0);

        let null_array: Option<Vec<i64>> = None;
        let v = null_array.to_value();
        assert_eq!(v.kind(), Kind::Null);

        let opt_array: Vec<Option<i64>> = vec![Some(42), None, Some(100)];
        let v = opt_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 3);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_string(),
            "42"
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").kind(),
            Kind::Null
        );
        assert_eq!(
            list.get(2).expect("element 2 should exist").as_string(),
            "100"
        );
    }

    #[test]
    fn to_value_non_finite_floats() {
        let f64_nan_val = f64::NAN.to_value();
        assert_eq!(f64_nan_val.kind(), Kind::String);
        assert_eq!(f64_nan_val.as_string(), "NaN");

        let f64_inf_val = f64::INFINITY.to_value();
        assert_eq!(f64_inf_val.kind(), Kind::String);
        assert_eq!(f64_inf_val.as_string(), "Infinity");

        let f64_neginf_val = f64::NEG_INFINITY.to_value();
        assert_eq!(f64_neginf_val.kind(), Kind::String);
        assert_eq!(f64_neginf_val.as_string(), "-Infinity");

        let f64_finite_val = 42.5f64.to_value();
        assert_eq!(f64_finite_val.kind(), Kind::Number);
        assert_eq!(f64_finite_val.as_f64(), 42.5);

        let f32_nan_val = f32::NAN.to_value();
        assert_eq!(f32_nan_val.kind(), Kind::String);
        assert_eq!(f32_nan_val.as_string(), "NaN");

        let f32_inf_val = f32::INFINITY.to_value();
        assert_eq!(f32_inf_val.kind(), Kind::String);
        assert_eq!(f32_inf_val.as_string(), "Infinity");

        let f32_neginf_val = f32::NEG_INFINITY.to_value();
        assert_eq!(f32_neginf_val.kind(), Kind::String);
        assert_eq!(f32_neginf_val.as_string(), "-Infinity");

        let f32_finite_val = 12.5f32.to_value();
        assert_eq!(f32_finite_val.kind(), Kind::Number);
        assert_eq!(f32_finite_val.as_f64(), 12.5);
    }
}
