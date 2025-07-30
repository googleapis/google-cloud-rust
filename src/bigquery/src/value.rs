// Copyright 2025 Google LLC
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

use base64::Engine;
use serde::{Deserialize, Deserializer};
use std::str::FromStr;
use wkt::Value;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("type mismatch, expected {expected}, got {got:?}")]
    TypeMismatch {
        expected: &'static str,
        got: Option<Value>,
    },
    #[error("missing field {0}")]
    MissingField(String),
    #[error("invalid value: {0}")]
    InvalidValue(String),
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("chrono parse error: {0}")]
    Chrono(String),
    #[error("numeric parse error: {0}")]
    Parse(String),
}

pub trait FromSql: Sized {
    fn from_sql(value: Value) -> Result<Self, Error>;
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql(value: Value) -> Result<Self, Error> {
        match value {
            Value::Null => Ok(None),
            v => T::from_sql(v).map(Some),
        }
    }
}

impl FromSql for i64 {
    fn from_sql(value: Value) -> Result<Self, Error> {
        match value {
            Value::String(s) => s.parse::<i64>().map_err(|e| Error::Parse(e.to_string())),
            Value::Number(n) => Ok(n.as_i64().unwrap()),
            other => Err(Error::TypeMismatch {
                expected: "string or number",
                got: other.into(),
            }),
        }
    }
}

impl FromSql for f64 {
    fn from_sql(value: Value) -> Result<Self, Error> {
        match value {
            Value::String(s) => s.parse::<f64>().map_err(|e| Error::Parse(e.to_string())),
            Value::Number(n) => Ok(n.as_f64().unwrap()),
            other => Err(Error::TypeMismatch {
                expected: "string or number",
                got: other.into(),
            }),
        }
    }
}

impl FromSql for bool {
    fn from_sql(value: Value) -> Result<Self, Error> {
        match value {
            Value::String(s) => s.parse::<bool>().map_err(|e| Error::Parse(e.to_string())),
            Value::Bool(b) => Ok(b),
            other => Err(Error::TypeMismatch {
                expected: "string or bool",
                got: other.into(),
            }),
        }
    }
}

impl FromSql for String {
    fn from_sql(value: Value) -> Result<Self, Error> {
        match value {
            Value::String(s) => Ok(s.clone()),
            other => Err(Error::TypeMismatch {
                expected: "string",
                got: other.into(),
            }),
        }
    }
}

impl FromSql for serde_json::Map<String, Value> {
    fn from_sql(value: Value) -> Result<Self, Error> {
        match value {
            Value::Object(o) => Ok(o.clone()),
            other => Err(Error::TypeMismatch {
                expected: "object",
                got: other.into(),
            }),
        }
    }
}

impl FromSql for Vec<Value> {
    fn from_sql(value: Value) -> Result<Self, Error> {
        match value {
            Value::Array(arr) => Ok(arr.clone()),
            other => Err(Error::TypeMismatch {
                expected: "array",
                got: other.into(),
            }),
        }
    }
}

impl FromSql for Vec<u8> {
    fn from_sql(value: Value) -> Result<Self, Error> {
        let s = String::from_sql(value)?;
        Ok(base64::prelude::BASE64_STANDARD.decode(s)?)
    }
}

impl FromSql for chrono::DateTime<chrono::Utc> {
    fn from_sql(value: Value) -> Result<Self, Error> {
        let s = String::from_sql(value)?;
        // BigQuery returns timestamps as a string representing a unix timestamp with microsecond precision.
        let micros: i64 = s.parse::<i64>().map_err(|e| Error::Parse(e.to_string()))?;
        chrono::DateTime::from_timestamp_micros(micros)
            .ok_or_else(|| Error::InvalidValue(format!("invalid timestamp value: {}", micros)))
    }
}

impl FromSql for chrono::NaiveDate {
    fn from_sql(value: Value) -> Result<Self, Error> {
        let s = String::from_sql(value)?;
        Ok(chrono::NaiveDate::from_str(&s)
            .map_err(|e| Error::Chrono(format!("invalid date value: {}", e)))?)
    }
}

impl FromSql for chrono::NaiveTime {
    fn from_sql(value: Value) -> Result<Self, Error> {
        let s = String::from_sql(value)?;
        Ok(chrono::NaiveTime::from_str(&s)
            .map_err(|e| Error::Chrono(format!("invalid date value: {}", e)))?)
    }
}

impl FromSql for chrono::NaiveDateTime {
    fn from_sql(value: Value) -> Result<Self, Error> {
        let s = String::from_sql(value)?;
        Ok(chrono::NaiveDateTime::from_str(&s)
            .map_err(|e| Error::Chrono(format!("invalid date value: {}", e)))?)
    }
}

/// Implements deserialized_with trait
/// See: https://serde.rs/custom-date-format.html
pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromSql,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    T::from_sql(value).map_err(|e| serde::de::Error::custom(e.to_string()))
}
