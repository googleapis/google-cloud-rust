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

use crate::model::{TableFieldSchema, TableSchema};
use arrow::ipc::reader::StreamReader;
use std::io::Cursor;
/// Schema of a table.
#[derive(Clone, Debug)]
pub(crate) struct Schema(TableSchema);

impl Schema {
    pub(crate) fn new(schema: TableSchema) -> Self {
        Self(schema)
    }

    pub(crate) fn new_from_field(field: TableFieldSchema) -> Self {
        Self(TableSchema::new().set_fields(field.fields))
    }

    pub(crate) fn get_field_index_by_name(&self, name: &str) -> Option<usize> {
        self.0.fields.iter().position(|f| f.name == name)
    }

    pub(crate) fn get_field_by_index(&self, index: usize) -> Option<&TableFieldSchema> {
        self.0.fields.get(index)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.fields.len()
    }

    pub(crate) fn try_from_arrow_ipc(
        serialized_schema: &[u8],
    ) -> Result<Self, crate::error::RowError> {
        let reader = StreamReader::try_new(Cursor::new(serialized_schema), None).map_err(|e| {
            crate::error::RowError::InvalidRowFormat(format!("failed to parse arrow schema: {e}"))
        })?;
        let table_schema = table_schema_from_arrow_schema(&reader.schema());
        Ok(Self(table_schema))
    }
}

fn table_schema_from_arrow_schema(arrow_schema: &arrow::datatypes::Schema) -> TableSchema {
    let fields: Vec<TableFieldSchema> = arrow_schema
        .fields()
        .iter()
        .map(|f| arrow_field_to_table_field(f.as_ref()))
        .collect();
    TableSchema::new().set_fields(fields)
}

fn arrow_field_to_table_field(field: &arrow::datatypes::Field) -> TableFieldSchema {
    use arrow::datatypes::DataType;

    let mode = match field.data_type() {
        DataType::List(_) | DataType::LargeList(_) => "REPEATED",
        _ if !field.is_nullable() => "REQUIRED",
        _ => "NULLABLE",
    };

    let (r#type, nested_fields) = match field.data_type() {
        DataType::Null => ("INTEGER", vec![]),
        DataType::Boolean => ("BOOLEAN", vec![]),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => ("INTEGER", vec![]),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            ("INTEGER", vec![])
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => ("FLOAT", vec![]),
        DataType::Utf8 | DataType::LargeUtf8 => ("STRING", vec![]),
        DataType::Binary | DataType::LargeBinary => ("BYTES", vec![]),
        DataType::Date32 | DataType::Date64 => ("DATE", vec![]),
        DataType::Time32(_) | DataType::Time64(_) => ("TIME", vec![]),
        DataType::Timestamp(_, Some(_)) => ("TIMESTAMP", vec![]),
        DataType::Timestamp(_, None) => ("DATETIME", vec![]),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => ("NUMERIC", vec![]),
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|f| arrow_field_to_table_field(f.as_ref()))
                .collect();
            ("RECORD", children)
        }
        DataType::List(inner) | DataType::LargeList(inner) => {
            let inner_schema = arrow_field_to_table_field(inner.as_ref());
            return TableFieldSchema::new()
                .set_name(field.name().clone())
                .set_type(inner_schema.r#type)
                .set_mode("REPEATED")
                .set_fields(inner_schema.fields);
        }
        _ => ("STRING", vec![]),
    };

    TableFieldSchema::new()
        .set_name(field.name().clone())
        .set_type(r#type)
        .set_mode(mode)
        .set_fields(nested_fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    #[test]
    fn test_from_arrow_schema() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true),
            Field::new(
                "tags",
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                true,
            ),
            Field::new(
                "created",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]);

        let table_schema = table_schema_from_arrow_schema(&arrow_schema);
        let schema = Schema::new(table_schema);
        assert_eq!(schema.len(), 4);
        assert_eq!(schema.get_field_index_by_name("name"), Some(0));
        assert_eq!(schema.get_field_index_by_name("age"), Some(1));
        assert_eq!(schema.get_field_index_by_name("tags"), Some(2));
        assert_eq!(schema.get_field_index_by_name("created"), Some(3));

        let f0 = schema.get_field_by_index(0).unwrap();
        assert_eq!(f0.name, "name");
        assert_eq!(f0.r#type, "STRING");
        assert_eq!(f0.mode, "REQUIRED");

        let f2 = schema.get_field_by_index(2).unwrap();
        assert_eq!(f2.name, "tags");
        assert_eq!(f2.r#type, "STRING");
        assert_eq!(f2.mode, "REPEATED");
    }
}
