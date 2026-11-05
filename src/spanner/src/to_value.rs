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

#[cfg(feature = "unstable-time")]
use crate::value::SPANNER_DATE_FORMAT;
use crate::value::SPANNER_TIMESTAMP_FORMAT;
pub use crate::value::Value;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use google_cloud_type::model::Date;
use prost_types::Value as ProtoValue;
use prost_types::value::Kind as ProtoKind;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use std::time::SystemTime;
#[cfg(feature = "unstable-time")]
use time::Date as TimeDate;
use time::OffsetDateTime;
use time::UtcOffset;
use uuid::Uuid;
use wkt::Timestamp;

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

const SENTINEL_MAX_TIMESTAMP: &str = "999999-12-31T23:59:59Z";
const SENTINEL_MIN_TIMESTAMP: &str = "-999999-01-01T00:00:00Z";

/// Returns a sentinel RFC 3339 timestamp string for values that cannot be represented
/// or converted to UTC within the supported calendar range.
///
/// Cloud Spanner rejects timestamps outside `0001-01-01T00:00:00Z..=9999-12-31T23:59:59.999999999Z`
/// with an `INVALID_ARGUMENT` error. Emitting an out-of-range sentinel ensures the client
/// never panics, while allowing the backend to reject invalid data with a descriptive error.
fn out_of_range_sentinel(is_non_negative: bool) -> &'static str {
    if is_non_negative {
        SENTINEL_MAX_TIMESTAMP
    } else {
        SENTINEL_MIN_TIMESTAMP
    }
}

/// Formats an [`OffsetDateTime`] into a Spanner-compatible RFC 3339 UTC timestamp string.
///
/// Cloud Spanner requires timestamps to be formatted as RFC 3339 strings with nanosecond
/// resolution and a UTC `Z` suffix. Non-UTC offsets are normalized to UTC before formatting
/// so that the exact instant in time is preserved. If normalizing to UTC would overflow
/// the representable year range `[-9999, 9999]`, an out-of-range sentinel string is returned
/// so Spanner can reject it.
fn format_offset_date_time(date_time: OffsetDateTime) -> String {
    date_time
        .checked_to_offset(UtcOffset::UTC)
        .and_then(|utc_date_time| utc_date_time.format(SPANNER_TIMESTAMP_FORMAT).ok())
        .unwrap_or_else(|| out_of_range_sentinel(date_time.year() >= 0).to_string())
}

/// Formats calendar date components into a Spanner-compatible `YYYY-MM-DD` string.
///
/// Preserves negative years with an explicit minus sign and 4-digit zero-padding.
fn format_date(year: i32, month: i32, day: i32) -> String {
    if year < 0 {
        format!("-{:04}-{:02}-{:02}", year.unsigned_abs(), month, day)
    } else {
        format!("{:04}-{:02}-{:02}", year, month, day)
    }
}

impl ToValue for SystemTime {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

impl From<SystemTime> for Value {
    fn from(system_time: SystemTime) -> Self {
        // Compute signed nanoseconds relative to Unix epoch.
        let nanos = match system_time.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => i128::try_from(duration.as_nanos()).ok(),
            Err(error) => i128::try_from(error.duration().as_nanos())
                .ok()
                .map(|nanos| -nanos),
        };

        // If nanoseconds fit within OffsetDateTime's [-9999, 9999] range (e.g. year 0 or negative
        // years), format the exact calendar date so Spanner returns an informative error message
        // displaying the user's actual input. If out of range even for OffsetDateTime (e.g. year 10000+
        // or Duration::MAX), fall back safely to a sentinel string so the client never panics.
        let string_value = nanos
            .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
            .map(format_offset_date_time)
            .unwrap_or_else(|| {
                out_of_range_sentinel(system_time >= SystemTime::UNIX_EPOCH).to_string()
            });

        Value(ProtoValue {
            kind: Some(ProtoKind::StringValue(string_value)),
        })
    }
}

#[cfg(feature = "unstable-time")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-time")))]
impl ToValue for OffsetDateTime {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

#[cfg(feature = "unstable-time")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-time")))]
impl From<OffsetDateTime> for Value {
    fn from(date_time: OffsetDateTime) -> Self {
        Value(ProtoValue {
            kind: Some(ProtoKind::StringValue(format_offset_date_time(date_time))),
        })
    }
}

impl ToValue for Timestamp {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

// Timestamp is strictly bounded to [0001-01-01, 9999-12-31], which is
// entirely within OffsetDateTime's range ([-9999, 9999]). This conversion
// and formatting are guaranteed panic-free.
impl From<Timestamp> for Value {
    fn from(timestamp: Timestamp) -> Self {
        let nanos = timestamp.seconds() as i128 * 1_000_000_000 + timestamp.nanos() as i128;
        let string_value = OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .map(format_offset_date_time)
            .unwrap_or_else(|_| out_of_range_sentinel(timestamp.seconds() >= 0).to_string());
        Value(ProtoValue {
            kind: Some(ProtoKind::StringValue(string_value)),
        })
    }
}

impl ToValue for Date {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(ProtoKind::StringValue(format_date(
                self.year, self.month, self.day,
            ))),
        })
    }
}

impl From<Date> for Value {
    fn from(date: Date) -> Self {
        date.to_value()
    }
}

#[cfg(feature = "unstable-time")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-time")))]
impl ToValue for TimeDate {
    fn to_value(&self) -> Value {
        (*self).into()
    }
}

#[cfg(feature = "unstable-time")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-time")))]
impl From<TimeDate> for Value {
    fn from(date: TimeDate) -> Self {
        // Formats time::Date using SPANNER_DATE_FORMAT ("[year]-[month]-[day]").
        let string_value = date
            .format(SPANNER_DATE_FORMAT)
            .unwrap_or_else(|_| format_date(date.year(), date.month() as i32, date.day() as i32));
        Value(ProtoValue {
            kind: Some(ProtoKind::StringValue(string_value)),
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

/// Encodes a fixed-size byte array as a Spanner [`Value`].
///
/// Encodes the binary payload as a base64 string matching Spanner's `BYTES` wire format.
impl<const N: usize> ToValue for [u8; N] {
    fn to_value(&self) -> Value {
        self.as_slice().to_value()
    }
}

/// Converts a fixed-size byte array into a Spanner [`Value`].
impl<const N: usize> From<[u8; N]> for Value {
    fn from(bytes_array: [u8; N]) -> Self {
        bytes_array.as_slice().to_value()
    }
}

/// Encodes a [`Bytes`] buffer as a Spanner [`Value`].
///
/// Encodes the binary payload as a base64 string matching Spanner's `BYTES` wire format.
impl ToValue for Bytes {
    fn to_value(&self) -> Value {
        (**self).to_value()
    }
}

/// Converts a [`Bytes`] buffer into a Spanner [`Value`].
impl From<Bytes> for Value {
    fn from(byte_buffer: Bytes) -> Self {
        byte_buffer.to_value()
    }
}

/// Encodes a [`Uuid`] as a Spanner [`Value`].
///
/// Formats the UUID as a lowercase hyphenated string matching Spanner's `UUID` wire format.
impl ToValue for Uuid {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(ProtoKind::StringValue(self.to_string())),
        })
    }
}

/// Converts a [`Uuid`] into a Spanner [`Value`].
impl From<Uuid> for Value {
    fn from(uuid: Uuid) -> Self {
        uuid.to_value()
    }
}

/// Encodes a [`serde_json::Value`] as a Spanner [`Value`].
///
/// Formats the JSON document as a standard RFC 7159 JSON string wrapped in a protobuf
/// string value (`string_value`), matching the wire encoding expected by Cloud Spanner
/// for `JSON` and `PG_JSONB` columns.
///
/// # SQL NULL vs. JSON null
///
/// Cloud Spanner distinguishes between a SQL `NULL` and a JSON `null` literal (`JSON 'null'`):
/// - A JSON `null` document is represented by `serde_json::Value::Null` and serializes to the
///   string `"null"` (`SELECT JSON 'null' IS NULL` evaluates to `false`).
/// - A SQL `NULL` is represented by `Option::<serde_json::Value>::None`, which converts to
///   [`Value::null`].
impl ToValue for JsonValue {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(ProtoKind::StringValue(self.to_string())),
        })
    }
}

/// Converts a [`serde_json::Value`] into a Spanner [`Value`].
///
/// Formats the JSON document as a standard RFC 7159 JSON string wrapped in a protobuf
/// string value (`string_value`), matching the wire encoding expected by Cloud Spanner
/// for `JSON` and `PG_JSONB` columns.
impl From<JsonValue> for Value {
    fn from(json: JsonValue) -> Self {
        json.to_value()
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

/// Encodes a shared reference to a [`ToValue`] type as a Spanner [`Value`].
impl<T: ToValue + ?Sized> ToValue for &T {
    fn to_value(&self) -> Value {
        T::to_value(*self)
    }
}

/// Encodes a mutable reference to a [`ToValue`] type as a Spanner [`Value`].
impl<T: ToValue + ?Sized> ToValue for &mut T {
    fn to_value(&self) -> Value {
        T::to_value(*self)
    }
}

/// Converts a reference to any [`ToValue`] type into a [`Value`] by calling
/// [`ToValue::to_value`], which copies the referenced data.
impl<T: ToValue + ?Sized> From<&T> for Value {
    fn from(reference: &T) -> Self {
        reference.to_value()
    }
}

/// Converts a mutable reference to any [`ToValue`] type into a [`Value`] by calling
/// [`ToValue::to_value`], which copies the referenced data.
impl<T: ToValue + ?Sized> From<&mut T> for Value {
    fn from(reference: &mut T) -> Self {
        reference.to_value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::from_value::{ConvertError, FromValue};
    use crate::key;
    use crate::mutation::Mutation;
    use crate::statement::Statement;
    use crate::types;
    use crate::value::Kind;
    use serde_json::json;
    use static_assertions::assert_impl_all;
    use std::str::FromStr;
    use std::time::Duration;
    #[cfg(feature = "unstable-time")]
    use time::Month;
    #[cfg(feature = "unstable-time")]
    use time::format_description::well_known::Rfc3339;

    #[test]
    fn null_value_conversions() {
        let null_value = Value::null();
        assert_eq!(null_value.kind(), Kind::Null);

        let unit_value = ().to_value();
        assert_eq!(unit_value, null_value);

        let unit_ref_value: Value = (&()).into();
        assert_eq!(unit_ref_value, null_value);

        let some_unit_value = Some(()).to_value();
        assert_eq!(some_unit_value, null_value);

        let opt_unit_value: Value = None::<()>.into();
        assert_eq!(opt_unit_value, null_value);

        let opt_value: Value = None::<Value>.into();
        assert_eq!(opt_value, null_value);

        let opt_i64_value: Value = None::<i64>.into();
        assert_eq!(opt_i64_value, null_value);
    }

    #[test]
    fn from_value_conversions() {
        let value: Value = "hello".to_string().into();
        assert_eq!(value.as_str(), Some("hello"));

        let value: Value = "world".into();
        assert_eq!(value.as_str(), Some("world"));

        let value: Value = 42i64.into();
        assert_eq!(value.as_str(), Some("42"));

        let value: Value = 42i32.into();
        assert_eq!(value.as_str(), Some("42"));

        let value: Value = true.into();
        assert_eq!(value.as_bool(), Some(true));

        let value: Value = 42.5f64.into();
        assert_eq!(value.as_f64(), Some(42.5));

        let value: Value = 42.5f32.into();
        assert_eq!(value.as_f64(), Some(42.5));

        let value: Value = vec![1u8, 2, 3].into();
        assert_eq!(value.as_str(), Some("AQID"));

        let value: Value = (&[1u8, 2, 3][..]).into();
        assert_eq!(value.as_str(), Some("AQID"));

        let decimal = Decimal::from_str("123.456").expect("valid decimal");
        let value: Value = decimal.into();
        assert_eq!(value.as_str(), Some("123.456"));

        #[cfg(feature = "unstable-time")]
        {
            let date_time = OffsetDateTime::parse("2023-10-27T10:00:00Z", &Rfc3339)
                .expect("valid RFC 3339 format");
            let value: Value = date_time.into();
            assert_eq!(value.as_str(), Some("2023-10-27T10:00:00.000000000Z"));

            let time_date = TimeDate::from_calendar_date(2023, Month::October, 27)
                .expect("valid calendar date");
            let value: Value = time_date.into();
            assert_eq!(value.as_str(), Some("2023-10-27"));
        }

        let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1698400800);
        let value: Value = system_time.into();
        assert_eq!(value.as_str(), Some("2023-10-27T10:00:00.000000000Z"));

        let timestamp = Timestamp::clamp(1698400800, 0);
        let value: Value = timestamp.into();
        assert_eq!(value.as_str(), Some("2023-10-27T10:00:00.000000000Z"));

        let google_date = Date::new().set_year(2023).set_month(10).set_day(27);
        let value: Value = (&google_date).into();
        assert_eq!(value.as_str(), Some("2023-10-27"));

        let list: Value = vec![1i64, 2i64].into();
        assert_eq!(list.kind(), Kind::List);
        assert_eq!(list.as_list().expect("list should exist").len(), 2);

        let opt_some_value: Value = Some(42i64).into();
        assert_eq!(opt_some_value.as_str(), Some("42"));
        let opt_none_value: Value = None::<i64>.into();
        assert_eq!(opt_none_value.kind(), Kind::Null);

        let unit_value: Value = ().into();
        assert_eq!(unit_value.kind(), Kind::Null);

        let borrowed_i64: Value = (&42i64).into();
        assert_eq!(borrowed_i64.as_str(), Some("42"));
        let borrowed_bool: Value = (&true).into();
        assert_eq!(borrowed_bool.as_bool(), Some(true));
        let borrowed_float: Value = (&42.5f64).into();
        assert_eq!(borrowed_float.as_f64(), Some(42.5));
        let borrowed_date: Value = google_date.into();
        assert_eq!(borrowed_date.as_str(), Some("2023-10-27"));

        let vec_opt_value: Value = vec![Some(1_i64), None].into();
        assert_eq!(vec_opt_value.kind(), Kind::List);
        assert_eq!(vec_opt_value.as_list().expect("list should exist").len(), 2);
    }

    #[test]
    fn to_value_string() {
        let value = "hello".to_string().to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("hello"));

        let value = "world".to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("world"));
    }

    #[test]
    fn to_value_str_trait_bound() {
        fn bind_param<T: ToValue + ?Sized>(val: &T) -> Value {
            val.to_value()
        }

        let value = bind_param("hello");
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("hello"));

        let owned: String = "hello".to_string();
        assert_eq!(bind_param(&owned), value);
        let borrowed: &str = &owned;
        assert_eq!(bind_param(&borrowed), value);
    }

    #[test]
    fn to_value_reference_types() {
        let integer_value: i64 = 42;
        let integer_reference: &i64 = &integer_value;
        let integer_double_reference: &&i64 = &integer_reference;
        assert_eq!(
            integer_reference.to_value(),
            Value::from(42_i64),
            "single reference to integer must encode correctly"
        );
        assert_eq!(
            integer_double_reference.to_value(),
            Value::from(42_i64),
            "double reference to integer must encode correctly"
        );

        let mut mutable_integer: i64 = 99;
        let mutable_reference: &mut i64 = &mut mutable_integer;
        assert_eq!(
            mutable_reference.to_value(),
            Value::from(99_i64),
            "mutable reference to integer must encode correctly"
        );

        let string_value: String = "test".to_string();
        let string_reference: &String = &string_value;
        let string_double_reference: &&String = &string_reference;
        assert_eq!(
            string_reference.to_value(),
            Value::from("test"),
            "single reference to string must encode correctly"
        );
        assert_eq!(
            string_double_reference.to_value(),
            Value::from("test"),
            "double reference to string must encode correctly"
        );

        let str_slice: &str = "slice";
        let str_reference: &&str = &str_slice;
        assert_eq!(
            str_slice.to_value(),
            Value::from("slice"),
            "str slice must encode correctly"
        );
        assert_eq!(
            str_reference.to_value(),
            Value::from("slice"),
            "reference to str slice must encode correctly"
        );

        let mut mutable_string: String = "mutable".to_string();
        let mutable_str: &mut str = mutable_string.as_mut_str();
        assert_eq!(
            mutable_str.to_value(),
            Value::from("mutable"),
            "mutable str slice must encode correctly"
        );

        let mut mutable_byte_array = [1_u8, 2, 3];
        let mutable_byte_slice: &mut [u8] = &mut mutable_byte_array;
        assert_eq!(
            mutable_byte_slice.to_value(),
            Value::from(&[1_u8, 2, 3][..]),
            "mutable byte slice must encode correctly"
        );

        let fixed_bytes_array = [10_u8, 20, 30];
        assert_eq!(
            fixed_bytes_array.to_value(),
            Value::from(&[10_u8, 20, 30][..]),
            "owned fixed-size byte array must encode correctly"
        );
        assert_impl_all!(&i64: ToValue);
        assert_impl_all!(&&i64: ToValue);
        assert_impl_all!(&mut i64: ToValue);
        assert_impl_all!(&bool: ToValue);
        assert_impl_all!(&f64: ToValue);
        assert_impl_all!(&str: ToValue);
        assert_impl_all!(&mut str: ToValue);
        assert_impl_all!(&[u8]: ToValue);
        assert_impl_all!(&mut [u8]: ToValue);
        assert_impl_all!([u8; 3]: ToValue);
        assert_impl_all!(&[u8; 3]: ToValue);
        assert_impl_all!(&mut [u8; 3]: ToValue);
        assert_impl_all!(&String: ToValue);
        assert_impl_all!(&Decimal: ToValue);
        assert_impl_all!(&Timestamp: ToValue);
        assert_impl_all!(&Date: ToValue);
        assert_impl_all!(&Uuid: ToValue);
        assert_impl_all!(JsonValue: ToValue);
        assert_impl_all!(&JsonValue: ToValue);
        assert_impl_all!(&mut JsonValue: ToValue);
        assert_impl_all!(Option<JsonValue>: ToValue);
        assert_impl_all!(Option<&JsonValue>: ToValue);
        assert_impl_all!(Vec<JsonValue>: ToValue);
        assert_impl_all!(Vec<&JsonValue>: ToValue);
        assert_impl_all!(&Option<i64>: ToValue);
        assert_impl_all!(Option<&str>: ToValue);

        let from_array_value: Value = fixed_bytes_array.into();
        assert_eq!(
            from_array_value,
            Value::from(&[10_u8, 20, 30][..]),
            "From<[u8; N]> for Value must encode correctly"
        );

        let from_array_ref_value = Value::from(&fixed_bytes_array);
        assert_eq!(
            from_array_ref_value,
            Value::from(&[10_u8, 20, 30][..]),
            "From<&[u8; N]> for Value must encode correctly"
        );

        let mut mutable_bytes_array = [10_u8, 20, 30];
        let from_mut_array_value = Value::from(&mut mutable_bytes_array);
        assert_eq!(
            from_mut_array_value,
            Value::from(&[10_u8, 20, 30][..]),
            "From<&mut [u8; N]> for Value must encode correctly"
        );

        let empty_bytes_array: [u8; 0] = [];
        assert_eq!(
            empty_bytes_array.to_value(),
            Value::from(&[][..]),
            "empty fixed-size byte array must encode as empty bytes"
        );

        let mut mutable_for_from = 55_i64;
        let from_mut_value = Value::from(&mut mutable_for_from);
        assert_eq!(
            from_mut_value,
            Value::from(55_i64),
            "From<&mut T> for Value via Value::from must encode correctly"
        );

        let optional_str_none: Option<&str> = None;
        assert_eq!(
            optional_str_none.to_value(),
            Value::null(),
            "Option<&str>::None must encode as null"
        );

        let optional_str_some: Option<&str> = Some("hello");
        assert_eq!(
            optional_str_some.to_value(),
            Value::from("hello"),
            "Option<&str>::Some must encode correctly"
        );
    }

    #[test]
    fn to_value_int() {
        let value = 42i64.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("42"));

        let value = 42i32.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("42"));
    }

    #[test]
    fn to_value_float() {
        let value = 42.5f64.to_value();
        assert_eq!(value.kind(), Kind::Number);
        assert_eq!(value.as_f64(), Some(42.5));

        let value = 42.5f32.to_value();
        assert_eq!(value.kind(), Kind::Number);
        assert_eq!(value.as_f64(), Some(42.5));
    }

    #[test]
    fn to_value_bool() {
        let value = true.to_value();
        assert_eq!(value.kind(), Kind::Bool);
        assert_eq!(value.as_bool(), Some(true));

        let value = false.to_value();
        assert_eq!(value.kind(), Kind::Bool);
        assert_eq!(value.as_bool(), Some(false));
    }

    #[test]
    fn to_value_bytes() {
        let bytes: Vec<u8> = vec![1, 2, 3];
        let value = bytes.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("AQID")); // Base64 encoded

        let slice_value: Value = (&[1u8, 2, 3][..]).into();
        assert_eq!(slice_value.kind(), Kind::String);
        assert_eq!(slice_value.as_str(), Some("AQID"));
    }

    #[test]
    fn to_value_slice_trait_bound() {
        fn bind_param<T: ToValue + ?Sized>(val: &T) -> Value {
            val.to_value()
        }

        let slice: &[u8] = &[1, 2, 3];
        let value = bind_param(slice);
        assert_eq!(value.kind(), Kind::String, "slice must serialize to string");
        assert_eq!(value.as_str(), Some("AQID"), "expected base64 encoding");
    }

    #[test]
    fn to_value_bytes_crate() {
        let byte_buffer = Bytes::from_static(b"hello world");
        let value = byte_buffer.to_value();
        assert_eq!(value.kind(), Kind::String, "bytes must serialize to string");
        assert_eq!(
            value.as_str(),
            Some("aGVsbG8gd29ybGQ="),
            "expected base64 representation"
        );

        let into_value: Value = byte_buffer.clone().into();
        assert_eq!(into_value, value, "Into<Value> must match to_value");

        let reference_value: Value = (&byte_buffer).into();
        assert_eq!(
            reference_value, value,
            "&Bytes Into<Value> must match to_value"
        );
    }

    #[test]
    fn to_value_uuid() {
        let uuid_string = "123e4567-e89b-12d3-a456-426614174000";
        let uuid = Uuid::parse_str(uuid_string).expect("valid uuid string");
        let value = uuid.to_value();
        assert_eq!(value.kind(), Kind::String, "uuid must serialize to string");
        assert_eq!(
            value.as_str(),
            Some(uuid_string),
            "expected uuid string representation"
        );

        let into_value: Value = uuid.into();
        assert_eq!(into_value, value, "Into<Value> must match to_value");

        let reference_value: Value = (&uuid).into();
        assert_eq!(
            reference_value, value,
            "&Uuid Into<Value> must match to_value"
        );
    }

    #[test]
    fn to_value_decimal() {
        let decimal = Decimal::from_str("123.456").expect("valid decimal");
        let value = decimal.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("123.456"));
    }

    #[test]
    fn to_value_google_date() {
        let date = Date::new().set_year(2023).set_month(10).set_day(27);
        let value = date.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("2023-10-27"));
    }

    #[cfg(feature = "unstable-time")]
    #[test]
    fn to_value_time_date() {
        let date =
            TimeDate::from_calendar_date(2023, Month::October, 27).expect("valid calendar date");
        let value = date.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("2023-10-27"));
    }

    #[test]
    fn to_value_system_time() {
        let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1698400800);
        let value = system_time.to_value();
        assert_eq!(
            value.kind(),
            Kind::String,
            "expected SystemTime to encode as String"
        );
        assert_eq!(
            value.as_str(),
            Some("2023-10-27T10:00:00.000000000Z"),
            "expected valid RFC 3339 timestamp"
        );
    }

    #[test]
    fn to_value_system_time_out_of_range() {
        // An extreme future SystemTime must never panic and must produce an out-of-range string.
        if let Some(extreme_future) = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(i64::MAX as u64))
            .or_else(|| SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(300_000_000_000)))
        {
            let max_value = extreme_future.to_value();
            assert_eq!(
                max_value.kind(),
                Kind::String,
                "expected extreme future SystemTime to encode as String"
            );
            let max_str = max_value
                .as_str()
                .expect("expected non-empty string for extreme future SystemTime");
            assert_eq!(
                max_str, "999999-12-31T23:59:59Z",
                "expected extreme future timestamp to produce sentinel out-of-range timestamp"
            );
        }

        // An extreme past SystemTime must never panic and must produce the negative sentinel.
        if let Some(extreme_past) = SystemTime::UNIX_EPOCH
            .checked_sub(Duration::from_secs(i64::MAX as u64))
            .or_else(|| SystemTime::UNIX_EPOCH.checked_sub(Duration::from_secs(400_000_000_000)))
        {
            let min_value = extreme_past.to_value();
            assert_eq!(
                min_value.kind(),
                Kind::String,
                "expected extreme past SystemTime to encode as String"
            );
            let min_str = min_value
                .as_str()
                .expect("expected non-empty string for extreme past SystemTime");
            assert_eq!(
                min_str, "-999999-01-01T00:00:00Z",
                "expected extreme past timestamp to produce negative sentinel out-of-range timestamp"
            );
        }

        // Year 10000 (post-9999 out of range)
        // 253402300800 seconds corresponds to 10000-01-01T00:00:00Z.
        let post_9999_system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(253_402_300_800);
        let post_9999_value = post_9999_system_time.to_value();
        assert_eq!(
            post_9999_value.kind(),
            Kind::String,
            "expected post-9999 SystemTime to encode as String"
        );
        assert_eq!(
            post_9999_value.as_str(),
            Some("999999-12-31T23:59:59Z"),
            "expected post-9999 timestamp to emit sentinel out-of-range timestamp"
        );

        // Pre-0001 out of range
        // 62135596801 seconds before epoch corresponds to 0000-12-31T23:59:59Z.
        if let Some(pre_0001_system_time) =
            SystemTime::UNIX_EPOCH.checked_sub(Duration::from_secs(62_135_596_801))
        {
            let pre_0001_value = pre_0001_system_time.to_value();
            assert_eq!(
                pre_0001_value.kind(),
                Kind::String,
                "expected pre-0001 SystemTime to encode as String"
            );
            assert_eq!(
                pre_0001_value.as_str(),
                Some("0000-12-31T23:59:59.000000000Z"),
                "expected pre-0001 timestamp to preserve exact calendar components"
            );
        }

        // Pre-0001 with non-zero subsecond nanoseconds.
        if let Some(pre_epoch_nanos) =
            SystemTime::UNIX_EPOCH.checked_sub(Duration::new(62_135_596_801, 500_000_000))
        {
            let pre_epoch_value = pre_epoch_nanos.to_value();
            assert_eq!(
                pre_epoch_value.as_str(),
                Some("0000-12-31T23:59:58.500000000Z"),
                "expected pre-0001 SystemTime with subseconds to format accurately"
            );
        }
    }

    #[cfg(feature = "unstable-time")]
    #[test]
    fn to_value_offset_date_time() {
        let date_time = OffsetDateTime::parse("2023-10-27T10:00:00Z", &Rfc3339)
            .expect("valid date time parsing");
        let value = date_time.to_value();
        assert_eq!(
            value.kind(),
            Kind::String,
            "expected OffsetDateTime to encode as String"
        );
        assert_eq!(
            value.as_str(),
            Some("2023-10-27T10:00:00.000000000Z"),
            "expected valid RFC 3339 timestamp"
        );

        // Non-UTC offset must normalize to UTC instant rather than outputting local components + 'Z'.
        let non_utc = OffsetDateTime::parse("2023-10-27T12:00:00+02:00", &Rfc3339)
            .expect("valid non-utc date time");
        let non_utc_value = non_utc.to_value();
        assert_eq!(
            non_utc_value.kind(),
            Kind::String,
            "expected non-UTC OffsetDateTime to encode as String"
        );
        assert_eq!(
            non_utc_value.as_str(),
            Some("2023-10-27T10:00:00.000000000Z"),
            "expected non-UTC timestamp to normalize to UTC instant"
        );

        // Pre-0001 year must format accurately without panicking or returning 'invalid timestamp'.
        let year_zero = TimeDate::from_calendar_date(0, Month::January, 1)
            .expect("valid year 0")
            .with_hms(0, 0, 0)
            .expect("valid time")
            .assume_utc();
        let year_zero_value = year_zero.to_value();
        assert_eq!(
            year_zero_value.kind(),
            Kind::String,
            "expected year 0 OffsetDateTime to encode as String"
        );
        assert_eq!(
            year_zero_value.as_str(),
            Some("0000-01-01T00:00:00.000000000Z"),
            "expected year 0 to be preserved for Spanner to reject"
        );

        let negative_year = TimeDate::from_calendar_date(-50, Month::January, 1)
            .expect("valid negative year")
            .with_hms(0, 0, 0)
            .expect("valid time")
            .assume_utc();
        let negative_value = negative_year.to_value();
        assert_eq!(
            negative_value.kind(),
            Kind::String,
            "expected negative year OffsetDateTime to encode as String"
        );
        assert_eq!(
            negative_value.as_str(),
            Some("-0050-01-01T00:00:00.000000000Z"),
            "expected negative year to format with proper zero padding"
        );

        // Boundary 4-digit years (9999 and -9999) format with full precision.
        let max_date_time = TimeDate::from_calendar_date(9999, Month::December, 31)
            .expect("valid year 9999 date")
            .with_hms_nano(23, 59, 59, 999_999_999)
            .expect("valid max time")
            .assume_utc();
        let max_value = max_date_time.to_value();
        assert_eq!(
            max_value.as_str(),
            Some("9999-12-31T23:59:59.999999999Z"),
            "expected year 9999 to format with 4-digit year"
        );

        let min_date_time = TimeDate::from_calendar_date(-9999, Month::January, 1)
            .expect("valid year -9999 date")
            .with_hms_nano(0, 0, 0, 0)
            .expect("valid min time")
            .assume_utc();
        let min_value = min_date_time.to_value();
        assert_eq!(
            min_value.as_str(),
            Some("-9999-01-01T00:00:00.000000000Z"),
            "expected year -9999 to format with negative 4-digit year"
        );

        // When shifting to UTC exceeds representable OffsetDateTime range, emits sentinel.
        let offset = UtcOffset::from_hms(-12, 0, 0).expect("valid negative UTC offset");
        let overflow_date_time = TimeDate::MAX
            .with_hms_nano(23, 59, 59, 999_999_999)
            .expect("valid max time")
            .assume_offset(offset);
        let overflow_value = overflow_date_time.to_value();
        assert_eq!(
            overflow_value.as_str(),
            Some("999999-12-31T23:59:59Z"),
            "expected boundary overflow on UTC conversion to emit out-of-range sentinel"
        );

        let underflow_offset = UtcOffset::from_hms(12, 0, 0).expect("valid positive UTC offset");
        let underflow_date_time = TimeDate::MIN
            .with_hms_nano(0, 0, 0, 0)
            .expect("valid min time")
            .assume_offset(underflow_offset);
        let underflow_value = underflow_date_time.to_value();
        assert_eq!(
            underflow_value.as_str(),
            Some("-999999-01-01T00:00:00Z"),
            "expected boundary underflow on UTC conversion to emit negative out-of-range sentinel"
        );
    }

    #[test]
    fn to_value_timestamp() {
        let timestamp = Timestamp::clamp(1698400800, 0);
        let value = timestamp.to_value();
        assert_eq!(
            value.kind(),
            Kind::String,
            "expected Timestamp to encode as String"
        );
        assert_eq!(
            value.as_str(),
            Some("2023-10-27T10:00:00.000000000Z"),
            "expected valid RFC 3339 timestamp"
        );

        // Verify boundary conditions (MIN_SECONDS and MAX_SECONDS) convert cleanly without panicking.
        let min_timestamp = Timestamp::clamp(Timestamp::MIN_SECONDS, 0);
        let min_value = min_timestamp.to_value();
        assert_eq!(
            min_value.kind(),
            Kind::String,
            "expected min Timestamp to encode as String"
        );
        assert_eq!(
            min_value.as_str(),
            Some("0001-01-01T00:00:00.000000000Z"),
            "expected 0001-01-01T00:00:00.000000000Z"
        );

        let max_timestamp = Timestamp::clamp(Timestamp::MAX_SECONDS, 999_999_999);
        let max_value = max_timestamp.to_value();
        assert_eq!(
            max_value.kind(),
            Kind::String,
            "expected max Timestamp to encode as String"
        );
        assert_eq!(
            max_value.as_str(),
            Some("9999-12-31T23:59:59.999999999Z"),
            "expected 9999-12-31T23:59:59.999999999Z"
        );
    }

    #[test]
    fn to_value_google_date_out_of_range() {
        let default_date = Date::default();
        let value = default_date.to_value();
        assert_eq!(
            value.kind(),
            Kind::String,
            "expected Date::default() to encode as String"
        );
        assert_eq!(
            value.as_str(),
            Some("0000-00-00"),
            "expected Date::default() to format as 0000-00-00 for Spanner to reject"
        );

        let post_9999_date = Date::new().set_year(10000).set_month(1).set_day(1);
        let value = post_9999_date.to_value();
        assert_eq!(
            value.kind(),
            Kind::String,
            "expected post-9999 Date to encode as String"
        );
        assert_eq!(
            value.as_str(),
            Some("10000-01-01"),
            "expected year 10000 date to format accurately for Spanner to reject"
        );

        let negative_year_date = Date::new().set_year(-50).set_month(1).set_day(1);
        let value = negative_year_date.to_value();
        assert_eq!(
            value.kind(),
            Kind::String,
            "expected negative year Date to encode as String"
        );
        assert_eq!(
            value.as_str(),
            Some("-0050-01-01"),
            "expected negative year Date to format with 4-digit zero-padding"
        );

        let min_year_date = Date::new().set_year(i32::MIN).set_month(1).set_day(1);
        let min_year_value = min_year_date.to_value();
        assert_eq!(
            min_year_value.as_str(),
            Some("-2147483648-01-01"),
            "expected i32::MIN year Date to format without integer overflow"
        );
    }

    #[cfg(feature = "unstable-time")]
    #[test]
    fn to_value_time_date_out_of_range() {
        let year_zero_date =
            TimeDate::from_calendar_date(0, Month::January, 1).expect("valid year 0 date");
        let value = year_zero_date.to_value();
        assert_eq!(
            value.kind(),
            Kind::String,
            "expected year 0 TimeDate to encode as String"
        );
        assert_eq!(
            value.as_str(),
            Some("0000-01-01"),
            "expected year 0 to format accurately without 'invalid date'"
        );

        let negative_date =
            TimeDate::from_calendar_date(-50, Month::January, 1).expect("valid negative date");
        let value = negative_date.to_value();
        assert_eq!(
            value.kind(),
            Kind::String,
            "expected negative TimeDate to encode as String"
        );
        assert_eq!(
            value.as_str(),
            Some("-0050-01-01"),
            "expected negative year to format with proper zero padding"
        );

        let min_time_date = TimeDate::MIN;
        let min_value = min_time_date.to_value();
        assert_eq!(
            min_value.as_str(),
            Some("-9999-01-01"),
            "expected TimeDate::MIN to format as -9999-01-01"
        );

        let max_time_date = TimeDate::MAX;
        let max_value = max_time_date.to_value();
        assert_eq!(
            max_value.as_str(),
            Some("9999-12-31"),
            "expected TimeDate::MAX to format as 9999-12-31"
        );
    }

    #[test]
    fn to_value_option() {
        let some_value: Option<i32> = Some(42);
        let value = some_value.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("42"));

        let none_value: Option<i32> = None;
        let value = none_value.to_value();
        assert_eq!(value.kind(), Kind::Null);
    }

    #[test]
    fn to_value_value() {
        let original_value = 42i32.to_value();
        let value = original_value.to_value();
        assert_eq!(value, original_value);
    }

    #[test]
    fn to_value_array() {
        let string_array = vec!["one".to_string(), "two".to_string()];
        let value = string_array.to_value();
        assert_eq!(value.kind(), Kind::List);
        let list = value.as_list().expect("list should exist");
        assert_eq!(list.len(), 2);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_str(),
            Some("one")
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").as_str(),
            Some("two")
        );

        let int_array = vec![42i64, 100i64];
        let value = int_array.to_value();
        assert_eq!(value.kind(), Kind::List);
        let list = value.as_list().expect("list should exist");
        assert_eq!(list.len(), 2);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_str(),
            Some("42")
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").as_str(),
            Some("100")
        );

        let bool_array = vec![true, false];
        let value = bool_array.to_value();
        assert_eq!(value.kind(), Kind::List);
        let list = value.as_list().expect("list should exist");
        assert_eq!(list.len(), 2);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_bool(),
            Some(true)
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").as_bool(),
            Some(false)
        );

        let float_array = vec![9.9f64, -2.5f64];
        let value = float_array.to_value();
        assert_eq!(value.kind(), Kind::List);
        let list = value.as_list().expect("list should exist");
        assert_eq!(list.len(), 2);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_f64(),
            Some(9.9)
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").as_f64(),
            Some(-2.5)
        );

        let empty_array: Vec<f64> = vec![];
        let value = empty_array.to_value();
        assert_eq!(value.kind(), Kind::List);
        assert_eq!(value.as_list().expect("list should exist").len(), 0);

        let null_array: Option<Vec<i64>> = None;
        let value = null_array.to_value();
        assert_eq!(value.kind(), Kind::Null);

        let opt_array: Vec<Option<i64>> = vec![Some(42), None, Some(100)];
        let value = opt_array.to_value();
        assert_eq!(value.kind(), Kind::List);
        let list = value.as_list().expect("list should exist");
        assert_eq!(list.len(), 3);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_str(),
            Some("42")
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").kind(),
            Kind::Null
        );
        assert_eq!(
            list.get(2).expect("element 2 should exist").as_str(),
            Some("100")
        );
    }

    #[test]
    fn to_value_non_finite_floats() {
        let f64_nan_value = f64::NAN.to_value();
        assert_eq!(f64_nan_value.kind(), Kind::String);
        assert_eq!(f64_nan_value.as_str(), Some("NaN"));

        let f64_infinity_value = f64::INFINITY.to_value();
        assert_eq!(f64_infinity_value.kind(), Kind::String);
        assert_eq!(f64_infinity_value.as_str(), Some("Infinity"));

        let f64_neg_infinity_value = f64::NEG_INFINITY.to_value();
        assert_eq!(f64_neg_infinity_value.kind(), Kind::String);
        assert_eq!(f64_neg_infinity_value.as_str(), Some("-Infinity"));

        let f64_finite_value = 42.5f64.to_value();
        assert_eq!(f64_finite_value.kind(), Kind::Number);
        assert_eq!(f64_finite_value.as_f64(), Some(42.5));

        let f32_nan_value = f32::NAN.to_value();
        assert_eq!(f32_nan_value.kind(), Kind::String);
        assert_eq!(f32_nan_value.as_str(), Some("NaN"));

        let f32_infinity_value = f32::INFINITY.to_value();
        assert_eq!(f32_infinity_value.kind(), Kind::String);
        assert_eq!(f32_infinity_value.as_str(), Some("Infinity"));

        let f32_neg_infinity_value = f32::NEG_INFINITY.to_value();
        assert_eq!(f32_neg_infinity_value.kind(), Kind::String);
        assert_eq!(f32_neg_infinity_value.as_str(), Some("-Infinity"));

        let f32_finite_value = 12.5f32.to_value();
        assert_eq!(f32_finite_value.kind(), Kind::Number);
        assert_eq!(f32_finite_value.as_f64(), Some(12.5));
    }

    #[test]
    fn to_value_json() {
        let object_json = json!({"color": "red", "value": "#f00"});
        let object_value = object_json.to_value();
        assert_eq!(
            object_value.kind(),
            Kind::String,
            "json object must encode as string"
        );
        assert_eq!(
            object_value.as_str(),
            Some(r##"{"color":"red","value":"#f00"}"##),
            "serialized json object string must match"
        );

        let array_json = json!([1, 2, 3]);
        let array_value = array_json.to_value();
        assert_eq!(
            array_value.kind(),
            Kind::String,
            "json array must encode as string"
        );
        assert_eq!(
            array_value.as_str(),
            Some("[1,2,3]"),
            "serialized json array string must match"
        );

        let string_json = json!("hello world");
        let string_value = string_json.to_value();
        assert_eq!(
            string_value.kind(),
            Kind::String,
            "json string must encode as string"
        );
        assert_eq!(
            string_value.as_str(),
            Some(r#""hello world""#),
            "serialized json string must match"
        );

        let number_json = json!(42);
        let number_value = number_json.to_value();
        assert_eq!(
            number_value.kind(),
            Kind::String,
            "json number must encode as string"
        );
        assert_eq!(
            number_value.as_str(),
            Some("42"),
            "serialized json number string must match"
        );

        let bool_json = json!(true);
        let bool_value = bool_json.to_value();
        assert_eq!(
            bool_value.kind(),
            Kind::String,
            "json boolean must encode as string"
        );
        assert_eq!(
            bool_value.as_str(),
            Some("true"),
            "serialized json boolean string must match"
        );

        let null_json = json!(null);
        let null_json_value = null_json.to_value();
        assert_eq!(
            null_json_value.kind(),
            Kind::String,
            "json null literal must encode as string 'null'"
        );
        assert_eq!(
            null_json_value.as_str(),
            Some("null"),
            "json null literal string must be 'null'"
        );

        let borrowed_json: &JsonValue = &object_json;
        let borrowed_object_value = Value::from(borrowed_json);
        assert_eq!(
            borrowed_object_value, object_value,
            "borrowed json reference must encode identically to owned json"
        );

        let mut mutable_object_json = json!({"mutable": true});
        let mutable_ref_value = Value::from(&mut mutable_object_json);
        assert_eq!(
            mutable_ref_value.as_str(),
            Some(r#"{"mutable":true}"#),
            "mutable json reference must encode correctly"
        );

        let none_json: Option<JsonValue> = None;
        let none_value = none_json.to_value();
        assert_eq!(
            none_value.kind(),
            Kind::Null,
            "Option::<JsonValue>::None must encode as SQL null"
        );
        assert!(none_value.is_null(), "None must be null");

        let some_null_json = Some(json!(null)).to_value();
        assert_eq!(
            some_null_json.kind(),
            Kind::String,
            "Some(json!(null)) must encode as JSON null string"
        );
        assert_eq!(
            some_null_json.as_str(),
            Some("null"),
            "Some(json!(null)) string must be 'null'"
        );

        let some_object_json = Some(json!({"x": 1})).to_value();
        assert_eq!(
            some_object_json.kind(),
            Kind::String,
            "Some(json!(...)) must encode as JSON string"
        );
        assert_eq!(
            some_object_json.as_str(),
            Some(r#"{"x":1}"#),
            "Some(json!(...)) string must match"
        );
    }

    #[test]
    fn from_json_for_value() {
        let payload = json!({"id": "item-123", "tags": ["a", "b"]});

        let owned_value: Value = payload.clone().into();
        assert_eq!(
            owned_value.kind(),
            Kind::String,
            "From<JsonValue> must produce a String value"
        );
        assert_eq!(
            owned_value.as_str(),
            Some(r#"{"id":"item-123","tags":["a","b"]}"#),
            "owned json string must match"
        );

        let borrowed_value: Value = (&payload).into();
        assert_eq!(
            borrowed_value, owned_value,
            "From<&JsonValue> must produce identical value to owned"
        );

        let mut mutable_payload = payload.clone();
        let mutable_value: Value = (&mut mutable_payload).into();
        assert_eq!(
            mutable_value, owned_value,
            "From<&mut JsonValue> must produce identical value to owned"
        );

        let none_value: Value = Option::<JsonValue>::None.into();
        assert_eq!(
            none_value.kind(),
            Kind::Null,
            "From<Option::None> must produce SQL null"
        );

        let some_value: Value = Some(payload.clone()).into();
        assert_eq!(
            some_value, owned_value,
            "From<Option::Some> must produce JSON value"
        );

        let array_payload = vec![json!({"x": 1}), json!({"y": 2})];
        let list_value: Value = array_payload.into();
        assert_eq!(
            list_value.kind(),
            Kind::List,
            "From<Vec<JsonValue>> must produce a List value"
        );
        let list = list_value.as_list().expect("list should be present");
        assert_eq!(list.len(), 2, "array list must contain 2 elements");
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_str(),
            Some(r#"{"x":1}"#),
            "first element must be serialized json string"
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").as_str(),
            Some(r#"{"y":2}"#),
            "second element must be serialized json string"
        );
    }

    #[test]
    fn json_in_statements_mutations_and_keys() {
        let profile = json!({"user": "alice", "roles": ["admin"]});

        // 1. Statement add_param with borrowed and owned JsonValue
        let borrowed_statement = Statement::builder("SELECT * FROM users WHERE profile = @profile")
            .add_param("profile", &profile)
            .build();
        assert_eq!(
            borrowed_statement.params.get("profile"),
            Some(&Value::from(&profile)),
            "statement params must contain profile parameter from borrowed json"
        );

        let owned_statement = Statement::builder("SELECT * FROM users WHERE profile = @profile")
            .add_param("profile", profile.clone())
            .build();
        assert_eq!(
            owned_statement.params.get("profile"),
            Some(&Value::from(&profile)),
            "statement params must contain profile parameter from owned json"
        );

        // 2. Statement add_typed_param with JsonValue and types::json()
        let typed_statement = Statement::builder("SELECT * FROM users WHERE profile = @profile")
            .add_typed_param("profile", &profile, types::json())
            .build();
        assert_eq!(
            typed_statement.params.get("profile"),
            Some(&Value::from(&profile)),
            "typed statement params must contain profile"
        );
        assert_eq!(
            typed_statement.param_types.get("profile"),
            Some(&types::json()),
            "typed statement param_types must contain TypeCode::Json"
        );

        // 3. Mutation ValueBinder::to with borrowed and owned JsonValue
        let borrowed_mutation = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(1i64)
            .set("Profile")
            .to(&profile)
            .build();
        let owned_mutation = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(1i64)
            .set("Profile")
            .to(profile.clone())
            .build();
        assert_eq!(
            borrowed_mutation, owned_mutation,
            "borrowed and owned mutations with json must be identical"
        );

        // 4. key! macro with borrowed and owned JsonValue
        let owned_key = key![profile.clone()];
        let borrowed_key = key![&profile];
        assert_eq!(
            owned_key, borrowed_key,
            "owned and borrowed key macro expansions with json must match"
        );
        assert_eq!(
            owned_key.values,
            vec![Value::from(&profile)],
            "key values must match json value"
        );
    }

    #[test]
    fn to_value_json_all_permutations() {
        let test_cases = [
            // Booleans
            json!(true),
            json!(false),
            // Integers
            json!(0),
            json!(42),
            json!(-42),
            json!(i64::MIN),
            json!(i64::MAX),
            // Floating-point numbers
            json!(0.0),
            json!(123.456),
            json!(-99.99),
            // Strings
            json!(""),
            json!("hello world"),
            json!("quotes \" and \n and \t and \\"),
            json!("unicode: こんにちは 🌍"),
            // Arrays
            json!([]),
            json!([1, 2, 3]),
            json!([1, "two", true, null, [2], {"k": "v"}]),
            json!([[1, 2], [3, 4]]),
            // Objects
            json!({}),
            json!({"color": "red", "value": "#f00"}),
            json!({"nested": {"id": 1, "items": ["a", "b"]}}),
            json!({"field_with_null": null}),
            json!({"キー": "値"}),
            // Null literal
            json!(null),
        ];

        let json_type = types::json();

        for original_json in test_cases {
            let encoded_value = original_json.to_value();
            assert_eq!(
                encoded_value.kind(),
                Kind::String,
                "encoded JSON must have Kind::String for: {original_json}"
            );
            assert_eq!(
                encoded_value.as_str(),
                Some(original_json.to_string().as_str()),
                "encoded string must match to_string() for: {original_json}"
            );

            // From owned JsonValue
            let from_owned_value = Value::from(original_json.clone());
            assert_eq!(
                from_owned_value, encoded_value,
                "Value::from(owned) must match to_value() for: {original_json}"
            );

            // From borrowed &JsonValue
            let from_borrowed_value = Value::from(&original_json);
            assert_eq!(
                from_borrowed_value, encoded_value,
                "Value::from(&borrowed) must match to_value() for: {original_json}"
            );

            // Round-trip deserialization via borrowed Value
            let roundtripped_borrowed = JsonValue::from_value(&encoded_value, &json_type)
                .expect("borrowed roundtrip deserialization must succeed");
            assert_eq!(
                roundtripped_borrowed, original_json,
                "borrowed roundtrip must preserve exact JSON structure for: {original_json}"
            );

            // Round-trip deserialization via owned Value
            let roundtripped_owned = JsonValue::from_owned_value(encoded_value, &json_type)
                .expect("owned roundtrip deserialization must succeed");
            assert_eq!(
                roundtripped_owned, original_json,
                "owned roundtrip must preserve exact JSON structure for: {original_json}"
            );
        }
    }

    #[test]
    fn to_value_json_container_permutations() {
        let json_type = types::json();

        // 1. Option<JsonValue>
        let none_json: Option<JsonValue> = None;
        let none_value = none_json.to_value();
        assert_eq!(
            none_value.kind(),
            Kind::Null,
            "Option::<JsonValue>::None must encode as SQL null"
        );
        let roundtrip_none = <Option<JsonValue>>::from_value(&none_value, &json_type)
            .expect("Option::from_value for SQL null must succeed");
        assert_eq!(roundtrip_none, None, "SQL null must deserialize as None");

        let some_null_json = Some(json!(null));
        let some_null_value = some_null_json.to_value();
        assert_eq!(
            some_null_value.kind(),
            Kind::String,
            "Some(json!(null)) must encode as JSON null string"
        );
        let roundtrip_some_null = <Option<JsonValue>>::from_value(&some_null_value, &json_type)
            .expect("Option::from_value for JSON null must succeed");
        assert_eq!(
            roundtrip_some_null,
            Some(JsonValue::Null),
            "JSON null must deserialize as Some(JsonValue::Null)"
        );

        let some_object_json = Some(json!({"id": 100}));
        let some_object_value = some_object_json.to_value();
        let roundtrip_some_object = <Option<JsonValue>>::from_value(&some_object_value, &json_type)
            .expect("Option::from_value for JSON object must succeed");
        assert_eq!(
            roundtrip_some_object,
            Some(json!({"id": 100})),
            "JSON object must deserialize as Some(object)"
        );

        // 2. Option<&JsonValue>
        let payload = json!({"key": "value"});
        let none_ref: Option<&JsonValue> = None;
        assert_eq!(
            none_ref.to_value().kind(),
            Kind::Null,
            "Option::<&JsonValue>::None must encode as SQL null"
        );
        let some_ref: Option<&JsonValue> = Some(&payload);
        assert_eq!(
            some_ref.to_value().as_str(),
            Some(r#"{"key":"value"}"#),
            "Option::<&JsonValue>::Some must encode as JSON string"
        );

        // 3. Empty Vec<JsonValue> (ARRAY<JSON>)
        let empty_vec: Vec<JsonValue> = Vec::new();
        let empty_list_value = empty_vec.to_value();
        assert_eq!(
            empty_list_value.kind(),
            Kind::List,
            "empty Vec<JsonValue> must encode as Kind::List"
        );
        let empty_list = empty_list_value
            .as_list()
            .expect("empty list must be present");
        assert_eq!(empty_list.len(), 0, "empty list must have length 0");

        // 4. Vec<&JsonValue>
        let json_a = json!({"a": 1});
        let json_b = json!({"b": 2});
        let borrowed_vec: Vec<&JsonValue> = vec![&json_a, &json_b];
        let borrowed_list_value = borrowed_vec.to_value();
        assert_eq!(
            borrowed_list_value.kind(),
            Kind::List,
            "Vec<&JsonValue> must encode as Kind::List"
        );
        let borrowed_list = borrowed_list_value
            .as_list()
            .expect("borrowed list must be present");
        assert_eq!(borrowed_list.len(), 2, "borrowed list must have 2 elements");

        // 5. Vec<Option<JsonValue>>: Testing array with SQL NULL and JSON null elements
        let array_with_nulls: Vec<Option<JsonValue>> =
            vec![Some(json!({"x": 1})), None, Some(json!(null))];
        let array_with_nulls_value = array_with_nulls.to_value();
        assert_eq!(
            array_with_nulls_value.kind(),
            Kind::List,
            "Vec<Option<JsonValue>> must encode as Kind::List"
        );
        let list_with_nulls = array_with_nulls_value
            .as_list()
            .expect("list with nulls must be present");
        assert_eq!(
            list_with_nulls.len(),
            3,
            "list with nulls must have 3 elements"
        );
        assert_eq!(
            list_with_nulls
                .get(0)
                .expect("element 0 should exist")
                .kind(),
            Kind::String,
            "element 0 must be Kind::String"
        );
        assert_eq!(
            list_with_nulls
                .get(0)
                .expect("element 0 should exist")
                .as_str(),
            Some(r#"{"x":1}"#),
            "element 0 string must match"
        );
        assert_eq!(
            list_with_nulls
                .get(1)
                .expect("element 1 should exist")
                .kind(),
            Kind::Null,
            "element 1 must be SQL NULL (Kind::Null)"
        );
        assert_eq!(
            list_with_nulls
                .get(2)
                .expect("element 2 should exist")
                .kind(),
            Kind::String,
            "element 2 must be JSON null (Kind::String)"
        );
        assert_eq!(
            list_with_nulls
                .get(2)
                .expect("element 2 should exist")
                .as_str(),
            Some("null"),
            "element 2 string must be 'null'"
        );
    }

    #[test]
    fn from_json_unhappy_paths() {
        let json_type = types::json();

        // 1. Malformed JSON string
        let malformed_value = Value::from("{not valid json");
        let malformed_error = JsonValue::from_value(&malformed_value, &json_type)
            .expect_err("malformed JSON string must fail to deserialize");
        assert!(
            matches!(malformed_error, ConvertError::Convert(_)),
            "expected ConvertError::Convert for malformed JSON string"
        );

        // 2. Unterminated JSON string
        let unterminated_value = Value::from("{\"key\":");
        let unterminated_error = JsonValue::from_value(&unterminated_value, &json_type)
            .expect_err("unterminated JSON must fail to deserialize");
        assert!(
            matches!(unterminated_error, ConvertError::Convert(_)),
            "expected ConvertError::Convert for unterminated JSON"
        );

        // 3. Empty string on a JSON column (empty string is not valid JSON)
        let empty_str_value = Value::from("");
        let empty_str_error = JsonValue::from_value(&empty_str_value, &json_type)
            .expect_err("empty string must fail to deserialize as JSON");
        assert!(
            matches!(empty_str_error, ConvertError::Convert(_)),
            "expected ConvertError::Convert for empty string"
        );

        // 4. Protobuf StructValue on wire (Spanner never accepts or sends StructValue)
        let struct_wire_value = Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(
                prost_types::Struct::default(),
            )),
        });
        let struct_wire_error = JsonValue::from_value(&struct_wire_value, &json_type)
            .expect_err("unexpected StructValue on wire must be rejected");
        assert!(
            matches!(
                &struct_wire_error,
                ConvertError::Convert(source_error)
                    if source_error.to_string().contains("unexpected protobuf StructValue")
            ),
            "expected ConvertError::Convert mentioning 'unexpected protobuf StructValue', got: {struct_wire_error:?}"
        );

        // 5. Incompatible wire type inside an array
        let bad_array_value = Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            "{invalid".to_string(),
                        )),
                    }],
                },
            )),
        });
        let array_type = types::array(types::json());
        let bad_array_error = <Vec<JsonValue>>::from_value(&bad_array_value, &array_type)
            .expect_err("array with invalid JSON element must fail");
        assert!(
            matches!(bad_array_error, ConvertError::Convert(_)),
            "expected ConvertError::Convert for bad element in array"
        );
    }
}
