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

use arrow::datatypes::DataType;
use arrow::ipc::reader::StreamReader;
use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};
use std::io::Cursor;

/// Schema of a table.
#[derive(Clone, Debug)]
pub(crate) struct Schema(TableSchema);

impl Schema {
    pub(crate) fn new(schema: TableSchema) -> Self {
        Self(schema)
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

    pub(crate) fn fields(&self) -> &[TableFieldSchema] {
        &self.0.fields
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
        .map(|f| arrow_field_to_table_field(f))
        .collect();
    TableSchema::new().set_fields(fields)
}

fn arrow_field_to_table_field(field: &arrow::datatypes::Field) -> TableFieldSchema {
    let tf = TableFieldSchema::new().set_name(field.name().clone());
    let mode = if field.is_nullable() {
        "NULLABLE"
    } else {
        "REQUIRED"
    };

    match field.data_type() {
        DataType::Boolean => tf.set_type("BOOLEAN").set_mode(mode),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => tf.set_type("INTEGER").set_mode(mode),
        DataType::Float32 | DataType::Float64 => tf.set_type("FLOAT64").set_mode(mode),
        DataType::Utf8 | DataType::LargeUtf8 => tf.set_type("STRING").set_mode(mode),
        DataType::Binary | DataType::LargeBinary => tf.set_type("BYTES").set_mode(mode),
        DataType::Date32 | DataType::Date64 => tf.set_type("DATE").set_mode(mode),
        DataType::Time32(_) | DataType::Time64(_) => tf.set_type("TIME").set_mode(mode),
        DataType::Timestamp(_, _) => tf.set_type("TIMESTAMP").set_mode(mode),
        DataType::Interval(_) => tf.set_type("INTERVAL").set_mode(mode),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            tf.set_type("NUMERIC").set_mode(mode)
        }
        DataType::Struct(fields) => {
            let sub_fields: Vec<TableFieldSchema> = fields
                .iter()
                .map(|f| arrow_field_to_table_field(f))
                .collect();
            tf.set_type("RECORD").set_mode(mode).set_fields(sub_fields)
        }
        DataType::List(sub_field) | DataType::LargeList(sub_field) => {
            let mut sub = arrow_field_to_table_field(sub_field);
            sub.name = field.name().clone();
            sub.mode = "REPEATED".to_string();
            sub
        }
        _ => tf.set_type("STRING").set_mode(mode),
    }
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

        let f2 = schema.get_field_by_index(2).unwrap();
        assert_eq!(f2.name, "tags");
    }
}
