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

//! Encodes flat JSON row objects into [`prost_reflect::DynamicMessage`]s.

use super::ColumnMap;
use super::value::ValueEncoder;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;

/// Borrows unescaped JSON object keys from the input slice and allocates only
/// when `serde_json` unescapes escape sequences.
#[derive(Debug, serde::Deserialize)]
struct ObjectKey<'a>(#[serde(borrow)] Cow<'a, str>);

/// Encodes a single JSON row object into a [`DynamicMessage`].
///
/// Every `serde_json` token callback is overridden so non-object inputs and
/// field errors never quote customer values via `serde`'s default errors.
#[derive(Debug)]
pub(super) struct RowEncoder<'a> {
    pub(super) descriptor: &'a MessageDescriptor,
    pub(super) columns: &'a ColumnMap,
}

impl RowEncoder<'_> {
    fn expected_object<E: de::Error>(self) -> Result<DynamicMessage, E> {
        Err(de::Error::custom("expected a JSON object"))
    }
}

impl<'de> DeserializeSeed<'de> for RowEncoder<'_> {
    type Value = DynamicMessage;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for RowEncoder<'_> {
    type Value = DynamicMessage;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut message = DynamicMessage::new(self.descriptor.clone());
        // Track seen fields separately because `null` leaves fields unset on `message`.
        let mut seen_fields = HashSet::new();
        while let Some(ObjectKey(key)) = map.next_key()? {
            let Some(column) = self.columns.get(&key) else {
                // Drain the value first so malformed JSON inside it still
                // fails as a syntax/EOF error rather than an unknown field.
                map.next_value::<de::IgnoredAny>()?;
                return Err(de::Error::custom(format!("unknown field '{key}'")));
            };
            if !seen_fields.insert(column.proto.number()) {
                map.next_value::<de::IgnoredAny>()?;
                return Err(de::Error::custom(format!(
                    "duplicate field '{}'",
                    column.proto.name()
                )));
            }
            // Repeated columns require `Value::List` (handled in a follow-up PR).
            if column.proto.is_list() {
                map.next_value::<de::IgnoredAny>()?;
                return Err(de::Error::custom(format!(
                    "unsupported repeated field '{}'",
                    column.proto.name()
                )));
            }
            if let Some(value) = map.next_value_seed(ValueEncoder(column))? {
                message.set_field(&column.proto, value);
            }
        }
        Ok(message)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        self.expected_object()
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
        self.expected_object()
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
        self.expected_object()
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
        self.expected_object()
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
        self.expected_object()
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
        self.expected_object()
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        while seq.next_element::<de::IgnoredAny>()?.is_some() {}
        self.expected_object()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::table_field_schema::{Mode, Type};
    use crate::model::{TableFieldSchema, TableSchema};
    use crate::write::schema::{build_message_descriptor, table_schema_to_descriptor};
    use prost_reflect::Value;
    use static_assertions::assert_impl_all;
    use test_case::test_case;

    assert_impl_all!(RowEncoder<'static>: std::fmt::Debug, Send, Sync);

    fn col(name: &str, r#type: Type) -> TableFieldSchema {
        TableFieldSchema::new().set_name(name).set_type(r#type)
    }

    fn encode(
        fields: &[TableFieldSchema],
        row: &str,
    ) -> anyhow::Result<Result<DynamicMessage, serde_json::Error>> {
        let schema = TableSchema::new().set_fields(fields.to_vec());
        let proto = table_schema_to_descriptor(&schema)?;
        let descriptor = build_message_descriptor(proto)?;
        let columns = ColumnMap::new(&descriptor, &schema.fields);
        let mut de = serde_json::Deserializer::from_str(row);
        Ok(RowEncoder {
            descriptor: &descriptor,
            columns: &columns,
        }
        .deserialize(&mut de)
        .and_then(|msg| de.end().map(|()| msg)))
    }

    #[test]
    fn empty_object_leaves_all_fields_unset() -> anyhow::Result<()> {
        for fields in [&[][..], &[col("active", Type::Bool)][..]] {
            for row in ["{}", "  { }  "] {
                let msg = encode(fields, row)??;
                assert!(msg.fields().next().is_none(), "{row}: {msg:?}");
            }
        }
        Ok(())
    }

    #[test]
    fn encodes_flat_rows_and_preserves_presence() -> anyhow::Result<()> {
        let fields = [
            col("active", Type::Bool).set_mode(Mode::Required),
            col("CreatedDate", Type::Date),
            col("span", Type::Interval),
            col("omitted_req", Type::Bool).set_mode(Mode::Required),
        ];

        let mut row0 = encode(
            &fields,
            r#"{"active": false, "createddate": "", "span": null}"#,
        )??;
        assert_eq!(row0.take_field_by_name("active"), Some(Value::Bool(false)));
        assert_eq!(
            row0.take_field_by_name("CreatedDate"),
            Some(Value::String(String::new()))
        );
        assert_eq!(row0.take_field_by_name("span"), None);
        assert_eq!(row0.take_field_by_name("omitted_req"), None);

        let mut row1 = encode(
            &fields,
            r#"{"\u0061ctive": "TRUE", "CreatedDate": "2026-11-23", "span": "1-2 3 4:5:6"}"#,
        )??;
        assert_eq!(row1.take_field_by_name("active"), Some(Value::Bool(true)));
        assert_eq!(
            row1.take_field_by_name("CreatedDate"),
            Some(Value::String("2026-11-23".to_owned()))
        );
        assert_eq!(
            row1.take_field_by_name("span"),
            Some(Value::String("1-2 3 4:5:6".to_owned()))
        );
        assert_eq!(row1.take_field_by_name("omitted_req"), None);
        Ok(())
    }

    #[test]
    fn rejects_invalid_rows_and_fields_without_leaking_values() -> anyhow::Result<()> {
        let fields = [
            col("active", Type::Bool),
            col("tags", Type::Bool).set_mode(Mode::Repeated),
        ];

        for (row, want_prefix) in [
            (r#"{"unknown": "secret_pii"}"#, "unknown field 'unknown'"),
            (
                r#"{"active": true, "unknown": {"nested": "secret_pii"}}"#,
                "unknown field 'unknown'",
            ),
            (
                r#"{"active": "secret_pii"}"#,
                "invalid value for field 'active': expected BOOL",
            ),
            (
                r#"{"tags": ["secret_pii"]}"#,
                "unsupported repeated field 'tags'",
            ),
            ("null", "expected a JSON object"),
            ("true", "expected a JSON object"),
            ("false", "expected a JSON object"),
            ("12345", "expected a JSON object"),
            ("-12345", "expected a JSON object"),
            ("123.45", "expected a JSON object"),
            (r#""secret_pii""#, "expected a JSON object"),
            (r#"["secret_pii"]"#, "expected a JSON object"),
        ] {
            let err = encode(&fields, row)?.expect_err(row);
            assert!(err.is_data(), "{row}: {err}");
            let msg = err.to_string();
            assert!(msg.starts_with(want_prefix), "{row}: {msg}");
            assert!(!msg.contains("secret_pii"), "{row}: {msg}");
            assert!(!msg.contains("12345"), "{row}: {msg}");
        }

        for bad_json in [
            "",
            "   ",
            "{",
            r#"{"active": true"#,
            "{1: true}",
            r#"{"active": t}"#,
            r#"{"unknown": ["secret_pii","#,
            r#"{"active": true, "ACTIVE": ["secret_pii","#,
            r#"{"tags": ["secret_pii","#,
            r#"["secret_pii","#,
            r#"{} trailing"#,
        ] {
            let err = encode(&fields, bad_json)?.expect_err(bad_json);
            assert!(!err.is_data(), "{bad_json}: {err}");
            assert!(!err.to_string().contains("secret_pii"), "{bad_json}: {err}");
        }
        Ok(())
    }

    #[test_case(r#"{"Date": "2026-11-23", "Date": "2026-11-24"}"#, "Date")]
    #[test_case(r#"{"date": "2026-11-23", "DATE": "pii"}"#, "Date")]
    #[test_case(r#"{"flag": null, "FLAG": true}"#, "flag")]
    #[test_case(r#"{"flag": false, "Date": "", "FLAG": {"k": "pii"}}"#, "flag")]
    fn rejects_duplicate_fields(row: &str, want_field: &str) -> anyhow::Result<()> {
        let fields = [col("flag", Type::Bool), col("Date", Type::Date)];
        let err = encode(&fields, row)?.expect_err(row);
        assert!(err.is_data(), "{row}: {err}");
        let msg = err.to_string();
        assert!(
            msg.starts_with(&format!("duplicate field '{want_field}'")),
            "{row}: {msg}"
        );
        assert!(!msg.contains("pii"), "{row}: {msg}");
        Ok(())
    }
}
