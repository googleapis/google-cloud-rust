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

impl ToValue for JsonValue {
    fn to_value(&self) -> Value {
        self.to_string().into()
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

        let json_value = serde_json::json!({"key": "value"});
        let value: Value = json_value.into();
        assert_eq!(value.as_str(), Some("{\"key\":\"value\"}"));

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
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("AQID"));
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
    fn to_value_json() {
        let json_value = serde_json::json!({"test": 123});
        let value = json_value.to_value();
        assert_eq!(value.kind(), Kind::String);
        assert_eq!(value.as_str(), Some("{\"test\":123}"));
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
}
