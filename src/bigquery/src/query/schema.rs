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
        .map(|f| TableFieldSchema::new().set_name(f.name().clone()))
        .collect();
    TableSchema::new().set_fields(fields)
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
