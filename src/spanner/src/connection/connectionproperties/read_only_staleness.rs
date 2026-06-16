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

use crate::Error;
use crate::connection::ConnectionError;
use crate::connection::Dialect;
use crate::connection::connectionstate::{ConnectionProperty, ConnectionState, Context};
use crate::timestamp_bound::TimestampBound;
use crate::to_value::ToValue;
use crate::types::TypeCode;
use crate::value::Value;

/// Spanner connection property struct for specifying read-only staleness.
pub struct ReadOnlyStalenessProperty;

impl ReadOnlyStalenessProperty {
    /// Retrieve the parsed read-only staleness from connection state.
    pub fn get_value(&self, state: &ConnectionState) -> Option<TimestampBound> {
        state
            .get(self.name())
            .and_then(|val| self.parse_timestamp_bound(&val))
    }

    /// Retrieve and construct current read-only staleness.
    pub fn parse_timestamp_bound(&self, val: &str) -> Option<TimestampBound> {
        let val_lower = val.trim().to_ascii_lowercase();

        if val_lower == "strong" {
            Some(TimestampBound::strong())
        } else if let Some(stripped) = val_lower.strip_prefix("exact_staleness ") {
            let dur = parse_duration(stripped).ok()?;
            Some(TimestampBound::exact_staleness(dur))
        } else if let Some(stripped) = val_lower.strip_prefix("max_staleness ") {
            let dur = parse_duration(stripped).ok()?;
            Some(TimestampBound::max_staleness(dur))
        } else if let Some(stripped) = val_lower.strip_prefix("read_timestamp ") {
            let ts = parse_timestamp(stripped).ok()?;
            Some(TimestampBound::read_timestamp(ts))
        } else if let Some(stripped) = val_lower.strip_prefix("min_read_timestamp ") {
            let ts = parse_timestamp(stripped).ok()?;
            Some(TimestampBound::min_read_timestamp(ts))
        } else {
            None
        }
    }
}

fn parse_duration(val: &str) -> Result<wkt::Duration, Error> {
    let val = val.trim();
    if let Some(stripped) = val.strip_suffix('s') {
        let secs = stripped
            .parse::<i64>()
            .map_err(|e| Error::deser(e.to_string()))?;
        wkt::Duration::new(secs, 0).map_err(|e| Error::deser(e.to_string()))
    } else {
        Err(Error::deser(format!("Unsupported duration: {}", val)))
    }
}

fn parse_timestamp(val: &str) -> Result<wkt::Timestamp, Error> {
    wkt::Timestamp::try_from(val.trim()).map_err(|e| Error::deser(e.to_string()))
}

impl ConnectionProperty for ReadOnlyStalenessProperty {
    fn name(&self) -> &str {
        "read_only_staleness"
    }
    fn description(&self) -> &str {
        "The read-only staleness to use for read-only transactions and single-use queries. \
         Format is 'strong', or 'exact_staleness <duration>', 'max_staleness <duration>', \
         'read_timestamp <timestamp>', 'min_read_timestamp <timestamp>'."
    }
    fn context(&self) -> Context {
        Context::User
    }
    fn default_value(&self) -> Option<String> {
        Some("strong".to_string())
    }
    fn validate_and_convert(&self, value: &str, _dialect: Dialect) -> Result<String, Error> {
        let val = value.trim();
        let val_lower = val.to_ascii_lowercase();
        if val_lower == "strong" {
            return Ok(val_lower);
        }
        if val_lower.starts_with("exact_staleness") {
            let rest = val_lower.strip_prefix("exact_staleness").unwrap().trim();
            if !rest.is_empty() {
                return Ok(format!("exact_staleness {}", rest));
            }
        }
        if val_lower.starts_with("max_staleness") {
            let rest = val_lower.strip_prefix("max_staleness").unwrap().trim();
            if !rest.is_empty() {
                return Ok(format!("max_staleness {}", rest));
            }
        }
        if val_lower.starts_with("read_timestamp") {
            let rest = val_lower.strip_prefix("read_timestamp").unwrap().trim();
            if !rest.is_empty() {
                return Ok(format!("read_timestamp {}", rest));
            }
        }
        if val_lower.starts_with("min_read_timestamp") {
            let rest = val_lower.strip_prefix("min_read_timestamp").unwrap().trim();
            if !rest.is_empty() {
                return Ok(format!("min_read_timestamp {}", rest));
            }
        }
        Err(Error::deser(ConnectionError::InvalidOption(format!(
            "Invalid read_only_staleness value: {}",
            value
        ))))
    }
    fn type_code(&self) -> TypeCode {
        TypeCode::String
    }
    fn to_value(&self, value: &str, _dialect: Dialect) -> Value {
        value.to_value()
    }
}
