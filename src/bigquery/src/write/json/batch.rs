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

use super::ColumnMap;
use super::row::RowEncoder;
use crate::model::row_error::RowErrorCode;
use crate::model::{ProtoRows, RowError, TableFieldSchema};
use bytes::Bytes;
use prost::Message;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde::de::DeserializeSeed;

/// Encodes batches of JSON row strings into [`ProtoRows`].
#[derive(Debug)]
pub(super) struct BatchEncoder {
    descriptor: MessageDescriptor,
    columns: ColumnMap,
}

impl BatchEncoder {
    pub(super) fn new(descriptor: MessageDescriptor, fields: &[TableFieldSchema]) -> Self {
        let columns = ColumnMap::new(&descriptor, fields);
        Self {
            descriptor,
            columns,
        }
    }

    /// Encodes `rows` into [`ProtoRows`], or returns a [`RowError`] for every
    /// invalid row in the batch.
    pub(super) fn encode_rows<S: AsRef<str>>(
        &self,
        rows: &[S],
    ) -> Result<ProtoRows, Vec<RowError>> {
        let mut serialized_rows = Vec::new();
        let mut row_errors = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let row = row.as_ref();
            match self.decode_row(row) {
                Ok(message) if row_errors.is_empty() => {
                    if serialized_rows.is_empty() {
                        serialized_rows.reserve(rows.len());
                    }
                    serialized_rows.push(Bytes::from(message.encode_to_vec()));
                }
                // Earlier row failed; keep checking remaining rows without serializing.
                Ok(_) => {}
                Err(err) => row_errors.push(to_row_error(index, row, err)),
            }
        }
        if !row_errors.is_empty() {
            return Err(row_errors);
        }
        Ok(ProtoRows::new().set_serialized_rows(serialized_rows))
    }

    fn decode_row(&self, row: &str) -> Result<DynamicMessage, serde_json::Error> {
        let mut de = serde_json::Deserializer::from_str(row);
        let msg = RowEncoder {
            descriptor: &self.descriptor,
            columns: &self.columns,
        }
        .deserialize(&mut de)?;
        // Reject trailing tokens after the root JSON value.
        de.end()?;
        Ok(msg)
    }
}

// Column errors inside `{...}` use `FieldsError`; syntax/EOF errors
// (`!err.is_data()`) and non-object inputs like `[1, 2]` (`!is_object`)
// use `Unspecified`.
fn to_row_error(index: usize, row: &str, err: serde_json::Error) -> RowError {
    let is_object = row.trim_ascii_start().starts_with('{');
    let code = if is_object && err.is_data() {
        RowErrorCode::FieldsError
    } else {
        RowErrorCode::Unspecified
    };
    let row_index = i64::try_from(index).unwrap_or(i64::MAX);
    RowError::new()
        .set_index(row_index)
        .set_code(code)
        .set_message(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TableSchema;
    use crate::model::table_field_schema::Type;
    use crate::write::schema::{build_message_descriptor, table_schema_to_descriptor};
    use RowErrorCode::{FieldsError, Unspecified};
    use prost_reflect::Value;
    use static_assertions::assert_impl_all;
    use test_case::test_case;

    assert_impl_all!(BatchEncoder: std::fmt::Debug, Send, Sync);

    fn col(name: &str, r#type: Type) -> TableFieldSchema {
        TableFieldSchema::new().set_name(name).set_type(r#type)
    }

    fn build_encoder(
        fields: impl IntoIterator<Item = TableFieldSchema>,
    ) -> anyhow::Result<(BatchEncoder, MessageDescriptor)> {
        let schema = TableSchema::new().set_fields(fields);
        let proto = table_schema_to_descriptor(&schema)?;
        let root = build_message_descriptor(proto)?;
        let encoder = BatchEncoder::new(root.clone(), &schema.fields);
        Ok((encoder, root))
    }

    #[test]
    fn empty_batch() -> anyhow::Result<()> {
        let (encoder, _) = build_encoder([col("flag", Type::Bool)])?;
        let rows = encoder
            .encode_rows::<&str>(&[])
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert!(rows.serialized_rows.is_empty());
        Ok(())
    }

    #[test]
    fn encodes_batch_in_order() -> anyhow::Result<()> {
        let (encoder, root) = build_encoder([col("flag", Type::Bool), col("date", Type::Date)])?;

        let batch = encoder
            .encode_rows(&[
                String::from(r#"{"flag": false, "date": null}"#),
                String::from("{}"),
                String::from(r#"{"flag": true, "date": "2026-11-24"}"#),
            ])
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(batch.serialized_rows.len(), 3);

        let mut row0 = DynamicMessage::decode(root.clone(), batch.serialized_rows[0].clone())?;
        assert_eq!(row0.take_field_by_name("flag"), Some(Value::Bool(false)));
        assert_eq!(row0.take_field_by_name("date"), None);

        assert_eq!(batch.serialized_rows[1], Bytes::new());

        let mut row2 = DynamicMessage::decode(root, batch.serialized_rows[2].clone())?;
        assert_eq!(row2.take_field_by_name("flag"), Some(Value::Bool(true)));
        assert_eq!(
            row2.take_field_by_name("date"),
            Some(Value::String("2026-11-24".to_owned()))
        );
        Ok(())
    }

    #[test]
    fn collects_all_batch_errors_in_order() -> anyhow::Result<()> {
        let (encoder, _) = build_encoder([col("flag", Type::Bool)])?;

        let errs = encoder
            .encode_rows(&[
                r#"{"flag": true}"#,
                r#"{"unknown": "pii"}"#,
                r#"{}"#,
                r#"{"flag": t}"#,
                r#"{"flag": false}"#,
            ])
            .expect_err("batch with invalid rows should fail");
        let got: Vec<_> = errs.iter().map(|e| (e.index, e.code.clone())).collect();
        assert_eq!(got, vec![(1, FieldsError), (3, Unspecified)]);

        let errs = encoder
            .encode_rows(&[r#""pii""#, r#"{"flag": true}"#, r#"{"flag": "pii"}"#])
            .expect_err("batch starting with invalid row should fail");
        let got: Vec<_> = errs.iter().map(|e| (e.index, e.code.clone())).collect();
        assert_eq!(got, vec![(0, Unspecified), (2, FieldsError)]);
        Ok(())
    }

    #[test_case(r#"{"flag": "pii"}"#, FieldsError, "invalid value for field 'flag'")]
    #[test_case("  {\"unknown\": \"pii\"}", FieldsError, "unknown field 'unknown'")]
    #[test_case(r#"["pii"]"#, Unspecified, "expected a JSON object")]
    #[test_case(r#"{"flag": t}"#, Unspecified, "")]
    #[test_case(r#"{} "pii""#, Unspecified, "")]
    #[test_case("", Unspecified, "")]
    fn row_error_classification(
        row: &str,
        want_code: RowErrorCode,
        want_prefix: &str,
    ) -> anyhow::Result<()> {
        let (encoder, _) = build_encoder([col("flag", Type::Bool)])?;
        let errs = encoder
            .encode_rows(&[row])
            .expect_err("invalid row should fail");
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        assert_eq!(err.index, 0);
        assert_eq!(err.code, want_code, "{row}: {err:?}");
        assert!(!err.message.is_empty(), "{row}: {err:?}");
        assert!(err.message.starts_with(want_prefix), "{row}: {err:?}");
        assert!(!err.message.contains("pii"), "{row}: {err:?}");
        Ok(())
    }
}
