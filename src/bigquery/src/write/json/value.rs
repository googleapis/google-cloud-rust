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

//! Encodes individual column JSON values into [`prost_reflect::Value`]s.

use super::Column;
use crate::model::table_field_schema::Type;
use prost_reflect::Value;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::fmt;

/// Encodes a single JSON value for a [`Column`] into a [`prost_reflect::Value`],
/// returning `Ok(None)` when the JSON token is `null`.
///
/// Every `serde_json` token callback is overridden so type mismatches report
/// only the field name and expected BigQuery type, never quoting input values
/// via `serde`'s default visitor errors.
#[derive(Debug)]
pub(super) struct ValueEncoder<'a>(pub(super) &'a Column);

impl ValueEncoder<'_> {
    fn invalid_value<E: de::Error>(self) -> Result<Option<Value>, E> {
        Err(de::Error::custom(format!(
            "invalid value for field '{}': expected {}",
            self.0.proto.name(),
            self.0.r#type
        )))
    }
}

impl<'de> DeserializeSeed<'de> for ValueEncoder<'_> {
    type Value = Option<Value>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        match self.0.r#type {
            Type::Bool
            | Type::Int64
            | Type::Date
            | Type::Datetime
            | Type::Time
            | Type::Geography
            | Type::Interval => deserializer.deserialize_any(self),
            _ => Err(de::Error::custom(format!(
                "unsupported type '{}' for field '{}'",
                self.0.r#type,
                self.0.proto.name()
            ))),
        }
    }
}

impl<'de> Visitor<'de> for ValueEncoder<'_> {
    type Value = Option<Value>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a {} value or null", self.0.r#type)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
        if self.0.r#type == Type::Bool {
            return Ok(Some(Value::Bool(v)));
        }
        self.invalid_value()
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        match self.0.r#type {
            Type::Bool if v.eq_ignore_ascii_case("true") => Ok(Some(Value::Bool(true))),
            Type::Bool if v.eq_ignore_ascii_case("false") => Ok(Some(Value::Bool(false))),
            Type::Int64 => match v.parse::<i64>() {
                Ok(n) => Ok(Some(Value::I64(n))),
                Err(_) => self.invalid_value(),
            },
            Type::Date | Type::Datetime | Type::Time | Type::Geography | Type::Interval => {
                Ok(Some(Value::String(v.to_owned())))
            }
            _ => self.invalid_value(),
        }
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        match self.0.r#type {
            Type::Int64 => Ok(Some(Value::I64(v))),
            _ => self.invalid_value(),
        }
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        match self.0.r#type {
            Type::Int64 => match i64::try_from(v) {
                Ok(n) => Ok(Some(Value::I64(n))),
                Err(_) => self.invalid_value(),
            },
            _ => self.invalid_value(),
        }
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
        self.invalid_value()
    }

    // Drain nested tokens first so broken JSON inside `[...]` or `{...}` still
    // surfaces as a syntax/EOF error rather than a type mismatch.
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        while seq.next_element::<de::IgnoredAny>()?.is_some() {}
        self.invalid_value()
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        while map
            .next_entry::<de::IgnoredAny, de::IgnoredAny>()?
            .is_some()
        {}
        self.invalid_value()
    }
}

#[cfg(test)]
mod tests {
    use super::super::ColumnMap;
    use super::*;
    use crate::model::{TableFieldSchema, TableSchema};
    use crate::write::schema::{build_message_descriptor, table_schema_to_descriptor};
    use anyhow::Context;
    use test_case::test_case;

    fn column_map(name: &str, r#type: Type) -> anyhow::Result<ColumnMap> {
        let schema = TableSchema::new()
            .set_fields([TableFieldSchema::new().set_name(name).set_type(r#type)]);
        let proto = table_schema_to_descriptor(&schema)?;
        let root = build_message_descriptor(proto)?;
        Ok(ColumnMap::new(&root, &schema.fields))
    }

    fn encode(col: &Column, json: &str) -> Result<Option<Value>, serde_json::Error> {
        let mut de = serde_json::Deserializer::from_str(json);
        let v = ValueEncoder(col).deserialize(&mut de)?;
        de.end()?;
        Ok(v)
    }

    #[test]
    fn bool_values() -> anyhow::Result<()> {
        let map = column_map("flag", Type::Bool)?;
        let col = map.get("flag").context("column present")?;
        for (json, want) in [
            ("true", Some(Value::Bool(true))),
            ("false", Some(Value::Bool(false))),
            (r#""true""#, Some(Value::Bool(true))),
            (r#""false""#, Some(Value::Bool(false))),
            (r#""TRUE""#, Some(Value::Bool(true))),
            (r#""False""#, Some(Value::Bool(false))),
            (r#""\u0074rue""#, Some(Value::Bool(true))),
            ("null", None),
        ] {
            let got = encode(col, json).context(json)?;
            assert_eq!(got, want, "{json}");
        }
        Ok(())
    }

    #[test]
    fn bool_rejects_invalid_values() -> anyhow::Result<()> {
        let map = column_map("flag", Type::Bool)?;
        let col = map.get("flag").context("column present")?;
        for json in [
            r#""secret_pii""#,
            r#""fal\u017fe""#,
            r#""0""#,
            "1",
            "-1",
            "1.5",
            r#"["secret_pii"]"#,
            r#"{"secret_pii": true}"#,
        ] {
            let err = encode(col, json).expect_err(json);
            assert!(err.is_data(), "{json}: {err}");
            let msg = err.to_string();
            assert!(
                msg.starts_with("invalid value for field 'flag': expected BOOL"),
                "{json}: {msg}"
            );
            assert!(!msg.contains("secret_pii"), "{msg}");
        }

        for bad_syntax in ["{bad}", "[1,"] {
            let err = encode(col, bad_syntax).expect_err(bad_syntax);
            assert!(!err.is_data(), "{bad_syntax}: {err}");
        }
        Ok(())
    }

    #[test]
    fn unsupported_type() -> anyhow::Result<()> {
        let map = column_map("ts", Type::Timestamp)?;
        let col = map.get("ts").context("column present")?;
        let err = encode(col, "42").expect_err("unsupported type should fail");
        assert!(
            err.to_string()
                .starts_with("unsupported type 'TIMESTAMP' for field 'ts'"),
            "{err}"
        );
        Ok(())
    }

    #[test_case("0", Some(0))]
    #[test_case("9223372036854775807", Some(i64::MAX))]
    #[test_case("-9223372036854775808", Some(i64::MIN))]
    #[test_case(r#""42""#, Some(42))]
    #[test_case(r#""\u00342""#, Some(42))]
    #[test_case(r#""9223372036854775807""#, Some(i64::MAX))]
    #[test_case(r#""-9223372036854775808""#, Some(i64::MIN))]
    #[test_case("null", None)]
    fn int64_values(json: &str, want: Option<i64>) -> anyhow::Result<()> {
        let map = column_map("n", Type::Int64)?;
        let col = map.get("n").context("column present")?;
        assert_eq!(encode(col, json)?, want.map(Value::I64));
        Ok(())
    }

    #[test_case("9223372036854775808")]
    #[test_case("-9223372036854775809")]
    #[test_case("1.0")]
    #[test_case("1e3")]
    #[test_case("true")]
    #[test_case(r#""""#)]
    #[test_case(r#"" 42 ""#)]
    #[test_case(r#""9223372036854775808""#)]
    #[test_case(r#""-9223372036854775809""#)]
    #[test_case(r#""1e3""#)]
    #[test_case(r#""secret_pii""#)]
    #[test_case(r#"["secret_pii"]"#)]
    #[test_case(r#"{"k": "secret_pii"}"#)]
    fn int64_rejects_invalid_values(json: &str) -> anyhow::Result<()> {
        let map = column_map("n", Type::Int64)?;
        let col = map.get("n").context("column present")?;
        let err = encode(col, json).expect_err(json);
        assert!(err.is_data(), "{json}: {err}");
        let msg = err.to_string();
        assert!(
            msg.starts_with("invalid value for field 'n': expected INT64 at line 1 column "),
            "{json}: {msg}"
        );
        assert!(!msg.contains("secret_pii"), "{msg}");
        Ok(())
    }

    #[test_case(Type::Date, r#""2026-11-19""#, Some("2026-11-19"))]
    #[test_case(
        Type::Datetime,
        r#""2026-11-19 12:34:56""#,
        Some("2026-11-19 12:34:56")
    )]
    #[test_case(Type::Time, r#""12:34:56.789""#, Some("12:34:56.789"))]
    #[test_case(Type::Geography, r#""POINT(1 2)""#, Some("POINT(1 2)"))]
    #[test_case(Type::Interval, r#""1-2 3 4:5:6""#, Some("1-2 3 4:5:6"))]
    #[test_case(Type::Date, r#""""#, Some(""))]
    #[test_case(Type::Date, r#""2026\u002d11\u002d19""#, Some("2026-11-19"))]
    #[test_case(Type::Date, "null", None)]
    fn passthrough_string_values(
        r#type: Type,
        json: &str,
        want: Option<&str>,
    ) -> anyhow::Result<()> {
        let map = column_map("col", r#type)?;
        let col = map.get("col").context("column present")?;
        assert_eq!(
            encode(col, json)?,
            want.map(|s| Value::String(s.to_owned()))
        );
        Ok(())
    }

    #[test_case(Type::Date, "true", "DATE")]
    #[test_case(Type::Date, "false", "DATE")]
    #[test_case(Type::Datetime, "0", "DATETIME")]
    #[test_case(Type::Time, "-1", "TIME")]
    #[test_case(Type::Geography, "1.5", "GEOGRAPHY")]
    #[test_case(Type::Interval, r#"["secret_pii"]"#, "INTERVAL")]
    #[test_case(Type::Date, r#"{"secret_pii": 1}"#, "DATE")]
    fn passthrough_string_rejects_non_strings(
        r#type: Type,
        json: &str,
        want_type: &str,
    ) -> anyhow::Result<()> {
        let map = column_map("col", r#type)?;
        let col = map.get("col").context("column present")?;
        let err = encode(col, json).expect_err("non-string should fail");
        assert!(err.is_data(), "{err}");
        let msg = err.to_string();
        assert!(
            msg.starts_with(&format!(
                "invalid value for field 'col': expected {want_type}"
            )),
            "{msg}"
        );
        assert!(!msg.contains("secret_pii"), "{msg}");
        Ok(())
    }
}
