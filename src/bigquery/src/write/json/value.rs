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
            Type::Bool => deserializer.deserialize_any(self),
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
        Ok(Some(Value::Bool(v)))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        if v.eq_ignore_ascii_case("true") {
            return Ok(Some(Value::Bool(true)));
        }
        if v.eq_ignore_ascii_case("false") {
            return Ok(Some(Value::Bool(false)));
        }
        self.invalid_value()
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
        self.invalid_value()
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
        self.invalid_value()
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
        let map = column_map("id", Type::Int64)?;
        let col = map.get("id").context("column present")?;
        let err = encode(col, "42").expect_err("unsupported type should fail");
        assert!(
            err.to_string()
                .starts_with("unsupported type 'INT64' for field 'id'"),
            "{err}"
        );
        Ok(())
    }
}
