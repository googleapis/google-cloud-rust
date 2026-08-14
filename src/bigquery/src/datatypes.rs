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

//! Custom data types for BigQuery.
//!
//! This module provides Rust representations of BigQuery data types such as
//! [`Interval`] and [`Range`].

use crate::error::ConvertError;
use crate::query::FromSql;
use crate::query::from_sql::parse_time;

/// Represents a BigQuery time [INTERVAL] value.
///
/// [INTERVAL]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#interval_type
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// use google_cloud_bigquery::client::BigQuery;
/// use google_cloud_bigquery::datatypes::Interval;
///
/// let client = BigQuery::builder()
///     .with_project_id("my-project-id")
///     .build()
///     .await?;
/// let mut rows = client
///     .query("SELECT INTERVAL '1-2 15 5:30:00' YEAR TO SECOND AS duration")
///     .until_done()
///     .await?
///     .read();
///
/// if let Some(row) = rows.next().await.transpose()? {
///     let interval: Interval = row.get("duration");
///     println!("{} years, {} months, {} days", interval.years, interval.months, interval.days);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Interval {
    /// Years component.
    pub years: i32,
    /// Months component.
    pub months: i32,
    /// Days component.
    pub days: i32,
    /// Hours component.
    pub hours: i32,
    /// Minutes component.
    pub minutes: i32,
    /// Seconds component.
    pub seconds: i32,
    /// Nanoseconds component.
    pub nanos: i32,
}

impl FromSql for Interval {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::String(s) => {
                let mut parts = s.split_whitespace();
                let ym_str = parts.next();
                let days_str = parts.next();
                let time_str = parts.next();
                let extra = parts.next();

                let (ym_str, days_str, time_str) = match (ym_str, days_str, time_str, extra) {
                    (Some(ym), Some(d), Some(t), None) => (ym, d, t),
                    _ => {
                        return Err(ConvertError::Convert(
                            format!("invalid interval format: expected 3 parts, got `{s}`").into(),
                        ));
                    }
                };

                // Parse Y-M
                let ym_neg = ym_str.starts_with('-');
                let ym_content = if ym_neg { &ym_str[1..] } else { ym_str };
                let mut ym_parts = ym_content.split('-');
                let y_str = ym_parts.next();
                let m_str = ym_parts.next();
                let ym_extra = ym_parts.next();

                let (y_str, m_str) = match (y_str, m_str, ym_extra) {
                    (Some(y), Some(m), None) => (y, m),
                    _ => {
                        return Err(ConvertError::Convert(
                            "invalid interval year-month format".into(),
                        ));
                    }
                };
                let ym_sign = if ym_neg { -1 } else { 1 };
                let years = y_str
                    .parse::<i32>()
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?
                    * ym_sign;
                let months = m_str
                    .parse::<i32>()
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?
                    * ym_sign;

                // Parse Days
                let days = days_str
                    .parse::<i32>()
                    .map_err(|e| ConvertError::Convert(Box::new(e)))?;

                // Parse H:M:S.F
                let time_neg = time_str.starts_with('-');
                let time_content = if time_neg { &time_str[1..] } else { time_str };
                let t = parse_time(time_content)?;
                let time_sign = if time_neg { -1 } else { 1 };
                let hours = t.hour() as i32 * time_sign;
                let minutes = t.minute() as i32 * time_sign;
                let seconds = t.second() as i32 * time_sign;
                let nanos = t.nanosecond() as i32 * time_sign;

                Ok(Interval {
                    years,
                    months,
                    days,
                    hours,
                    minutes,
                    seconds,
                    nanos,
                })
            }
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

/// Represents a BigQuery [RANGE] value.
///
/// [RANGE]: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types#range_type
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// use google_cloud_bigquery::client::BigQuery;
/// use google_cloud_bigquery::datatypes::Range;
/// use google_cloud_type::model::Date;
///
/// let client = BigQuery::builder()
///     .with_project_id("my-project-id")
///     .build()
///     .await?;
/// let mut rows = client
///     .query("SELECT RANGE(DATE '2024-01-01', DATE '2024-12-31') AS date_range")
///     .until_done()
///     .await?
///     .read();
///
/// if let Some(row) = rows.next().await.transpose()? {
///     let date_range: Range<Date> = row.get("date_range");
///     println!("Start: {:?}, End: {:?}", date_range.start, date_range.end);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Range<T> {
    /// The inclusive start of the range (or None if unbounded).
    pub start: Option<T>,
    /// The exclusive end of the range (or None if unbounded).
    pub end: Option<T>,
}

impl<T: FromSql> FromSql for Range<T> {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        match value {
            wkt::Value::String(s) => {
                let trimmed = s.trim();
                // Strip leading [ and trailing )
                let content = trimmed
                    .strip_prefix('[')
                    .and_then(|c| c.strip_suffix(')'))
                    .ok_or_else(|| {
                        ConvertError::Convert(
                            "invalid range format: missing enclosing brackets".into(),
                        )
                    })?;

                // Split on the comma
                let parts: Vec<&str> = content.split(',').collect();
                if parts.len() != 2 {
                    return Err(ConvertError::Convert(
                        format!(
                            "invalid range format: expected 2 parts, got {}",
                            parts.len()
                        )
                        .into(),
                    ));
                }

                let start_str = parts[0].trim();
                let end_str = parts[1].trim();

                let start = if start_str.is_empty() || start_str == "UNBOUNDED" {
                    None
                } else {
                    Some(T::from_sql(wkt::Value::String(start_str.to_string()))?)
                };

                let end = if end_str.is_empty() || end_str == "UNBOUNDED" {
                    None
                } else {
                    Some(T::from_sql(wkt::Value::String(end_str.to_string()))?)
                };

                Ok(Range { start, end })
            }
            wkt::Value::Null => Err(ConvertError::NotNull),
            other => Err(ConvertError::TypeMismatch {
                expected: "string",
                got: other,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

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
                ConvertError::MissingField(f) => Self::Convert(format!("missing field: {f}")),
            }
        }
    }

    #[test_case(wkt::Value::String("1-2 3 4:05:06.789123456".to_string()) => Ok(Interval { years: 1, months: 2, days: 3, hours: 4, minutes: 5, seconds: 6, nanos: 789_123_456 }) ; "valid interval with nanos")]
    #[test_case(wkt::Value::String("0-0 0 0:00:00".to_string()) => Ok(Interval { years: 0, months: 0, days: 0, hours: 0, minutes: 0, seconds: 0, nanos: 0 }) ; "zero interval")]
    #[test_case(wkt::Value::String("0-0 1 2:30:45.123456".to_string()) => Ok(Interval { years: 0, months: 0, days: 1, hours: 2, minutes: 30, seconds: 45, nanos: 123_456_000 }) ; "valid interval from integration test")]
    #[test_case(wkt::Value::String("1-2 3 4:5:6".to_string()) => Ok(Interval { years: 1, months: 2, days: 3, hours: 4, minutes: 5, seconds: 6, nanos: 0 }) ; "unpadded time without subseconds")]
    #[test_case(wkt::Value::String("1-2 3 4:5:6.5".to_string()) => Ok(Interval { years: 1, months: 2, days: 3, hours: 4, minutes: 5, seconds: 6, nanos: 500_000_000 }) ; "unpadded time with short subsecond")]
    #[test_case(wkt::Value::String("-1-2 3 -4:5:6.123".to_string()) => Ok(Interval { years: -1, months: -2, days: 3, hours: -4, minutes: -5, seconds: -6, nanos: -123_000_000 }) ; "mixed signs interval")]
    #[test_case(wkt::Value::String("0-0 0 1:1:1.000000001".to_string()) => Ok(Interval { years: 0, months: 0, days: 0, hours: 1, minutes: 1, seconds: 1, nanos: 1 }) ; "single nanosecond")]
    #[test_case(wkt::Value::String("-1-2 -3 -4:05:06.123".to_string()) => Ok(Interval { years: -1, months: -2, days: -3, hours: -4, minutes: -5, seconds: -6, nanos: -123_000_000 }) ; "all negative interval")]
    #[test_case(wkt::Value::String("0-0 0 0:00:00.1234567899".to_string()) => Ok(Interval { years: 0, months: 0, days: 0, hours: 0, minutes: 0, seconds: 0, nanos: 123_456_789 }) ; "truncated nanos")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null interval")]
    #[test_case(wkt::Value::Number(123.into()) => Err(TestConvertError::TypeMismatch("string")) ; "type mismatch interval")]
    #[test_case(wkt::Value::String("".to_string()) => Err(TestConvertError::Convert("invalid interval format: expected 3 parts, got ``".to_string())) ; "empty interval string")]
    #[test_case(wkt::Value::String("1-2 3".to_string()) => Err(TestConvertError::Convert("invalid interval format: expected 3 parts, got `1-2 3`".to_string())) ; "invalid interval parts count")]
    #[test_case(wkt::Value::String("1 3 4:05:06".to_string()) => Err(TestConvertError::Convert("invalid interval year-month format".to_string())) ; "invalid year-month format")]
    #[test_case(wkt::Value::String("1-2 3 4:05".to_string()) => Err(TestConvertError::Convert("a character literal was not valid".to_string())) ; "invalid time format")]
    fn test_from_sql_interval(value: wkt::Value) -> Result<Interval, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }

    #[test_case(wkt::Value::String("[2026-05-28, 2026-05-29)".to_string()) => Ok(Range { start: Some(google_cloud_type::model::Date::new().set_year(2026).set_month(5).set_day(28)), end: Some(google_cloud_type::model::Date::new().set_year(2026).set_month(5).set_day(29)) }) ; "date range bounded")]
    #[test_case(wkt::Value::String("[2026-05-28, UNBOUNDED)".to_string()) => Ok(Range { start: Some(google_cloud_type::model::Date::new().set_year(2026).set_month(5).set_day(28)), end: None }) ; "date range unbounded end")]
    #[test_case(wkt::Value::String("[UNBOUNDED, 2026-05-29)".to_string()) => Ok(Range { start: None, end: Some(google_cloud_type::model::Date::new().set_year(2026).set_month(5).set_day(29)) }) ; "date range unbounded start")]
    #[test_case(wkt::Value::String("[UNBOUNDED, UNBOUNDED)".to_string()) => Ok(Range { start: None, end: None }) ; "date range unbounded both")]
    #[test_case(wkt::Value::Null => Err(TestConvertError::NotNull) ; "null range")]
    #[test_case(wkt::Value::Number(123.into()) => Err(TestConvertError::TypeMismatch("string")) ; "range type mismatch")]
    #[test_case(wkt::Value::String("[2026-05-28)".to_string()) => Err(TestConvertError::Convert("invalid range format: expected 2 parts, got 1".to_string())) ; "range invalid format one part")]
    #[test_case(wkt::Value::String("[2026-05-28, 2026-05-29, 2026-05-30)".to_string()) => Err(TestConvertError::Convert("invalid range format: expected 2 parts, got 3".to_string())) ; "range invalid format three parts")]
    #[test_case(wkt::Value::String("[".to_string()) => Err(TestConvertError::Convert("invalid range format: missing enclosing brackets".to_string())) ; "range too short")]
    #[test_case(wkt::Value::String("2026-05-28, 2026-05-29".to_string()) => Err(TestConvertError::Convert("invalid range format: missing enclosing brackets".to_string())) ; "range missing brackets")]
    #[test_case(wkt::Value::String("(2026-05-28, 2026-05-29)".to_string()) => Err(TestConvertError::Convert("invalid range format: missing enclosing brackets".to_string())) ; "range invalid leading parenthesis")]
    #[test_case(wkt::Value::String("[2026-05-28, 2026-05-29]".to_string()) => Err(TestConvertError::Convert("invalid range format: missing enclosing brackets".to_string())) ; "range invalid trailing square bracket")]
    fn test_from_sql_range(
        value: wkt::Value,
    ) -> Result<Range<google_cloud_type::model::Date>, TestConvertError> {
        FromSql::from_sql(value).map_err(TestConvertError::from)
    }
}
