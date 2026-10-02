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
use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};
use std::collections::HashMap;

/// Schema of a table.
#[derive(Clone, Debug)]
pub(crate) struct Schema {
    schema: TableSchema,
    field_indices: HashMap<String, usize>,
}

impl Schema {
    pub(crate) fn new(schema: TableSchema) -> Self {
        let field_indices = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        Self {
            schema,
            field_indices,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_arrow_schema(arrow_schema: &arrow::datatypes::Schema) -> Self {
        Self::new(table_schema_from_arrow_schema(arrow_schema))
    }

    pub(crate) fn get_field_index_by_name(&self, name: &str) -> Option<usize> {
        self.field_indices.get(name).copied()
    }

    pub(crate) fn get_field_by_index(&self, index: usize) -> Option<&TableFieldSchema> {
        self.schema.fields.get(index)
    }

    pub(crate) fn len(&self) -> usize {
        self.schema.fields.len()
    }

    pub(crate) fn fields(&self) -> &[TableFieldSchema] {
        &self.schema.fields
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn table_schema_from_arrow_schema(arrow_schema: &arrow::datatypes::Schema) -> TableSchema {
    let fields: Vec<TableFieldSchema> = arrow_schema
        .fields()
        .iter()
        .map(|f| arrow_field_to_table_field(f))
        .collect();
    TableSchema::new().set_fields(fields)
}

#[cfg_attr(not(test), allow(dead_code))]
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
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            tf.set_type("FLOAT64").set_mode(mode)
        }
        DataType::Utf8 | DataType::LargeUtf8 => tf.set_type("STRING").set_mode(mode),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            tf.set_type("BYTES").set_mode(mode)
        }
        DataType::Date32 | DataType::Date64 => tf.set_type("DATE").set_mode(mode),
        DataType::Time32(_) | DataType::Time64(_) => tf.set_type("TIME").set_mode(mode),
        DataType::Timestamp(_, tz) => {
            let bq_type = if tz.is_some() {
                "TIMESTAMP"
            } else {
                "DATETIME"
            };
            tf.set_type(bq_type).set_mode(mode)
        }
        DataType::Interval(_) => tf.set_type("INTERVAL").set_mode(mode),
        DataType::Decimal128(_, _) => tf.set_type("NUMERIC").set_mode(mode),
        DataType::Decimal256(_, _) => tf.set_type("BIGNUMERIC").set_mode(mode),
        DataType::Struct(fields) => {
            let sub_fields: Vec<TableFieldSchema> = fields
                .iter()
                .map(|f| arrow_field_to_table_field(f))
                .collect();
            tf.set_type("RECORD").set_mode(mode).set_fields(sub_fields)
        }
        DataType::List(sub_field)
        | DataType::LargeList(sub_field)
        | DataType::FixedSizeList(sub_field, _) => {
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
    use arrow::datatypes::{
        DataType, Field, Fields, IntervalUnit, Schema as ArrowSchema, TimeUnit,
    };
    use std::sync::Arc;

    #[test]
    fn test_from_arrow_schema() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "created",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("active", DataType::Boolean, false),
            Field::new("score", DataType::Float64, true),
            Field::new("payload", DataType::Binary, true),
            Field::new("birth_date", DataType::Date32, true),
            Field::new("alarm_time", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new(
                "duration",
                DataType::Interval(IntervalUnit::MonthDayNano),
                true,
            ),
            Field::new("amount", DataType::Decimal128(38, 9), true),
            Field::new(
                "profile",
                DataType::Struct(Fields::from(vec![
                    Field::new("bio", DataType::LargeUtf8, true),
                    Field::new("level", DataType::Int32, false),
                ])),
                true,
            ),
            Field::new("fallback", DataType::Null, true),
            Field::new("half_score", DataType::Float16, true),
            Field::new("fixed_bytes", DataType::FixedSizeBinary(16), false),
            Field::new(
                "local_dt",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("big_amount", DataType::Decimal256(76, 38), true),
            Field::new(
                "fixed_list",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 3),
                true,
            ),
        ]);

        let schema = Schema::from_arrow_schema(&arrow_schema);
        assert_eq!(schema.len(), 18);
        assert_eq!(schema.fields().len(), 18);
        assert_eq!(schema.get_field_index_by_name("name"), Some(0));
        assert_eq!(schema.get_field_index_by_name("age"), Some(1));
        assert_eq!(schema.get_field_index_by_name("tags"), Some(2));
        assert_eq!(schema.get_field_index_by_name("created"), Some(3));
        assert_eq!(schema.get_field_index_by_name("profile"), Some(11));
        assert_eq!(schema.get_field_index_by_name("missing"), None);

        let f0 = schema.get_field_by_index(0).unwrap();
        assert_eq!(f0.name, "name");
        assert_eq!(f0.r#type, "STRING");
        assert_eq!(f0.mode, "REQUIRED");

        let f1 = schema.get_field_by_index(1).unwrap();
        assert_eq!(f1.name, "age");
        assert_eq!(f1.r#type, "INTEGER");
        assert_eq!(f1.mode, "NULLABLE");

        let f2 = schema.get_field_by_index(2).unwrap();
        assert_eq!(f2.name, "tags");
        assert_eq!(f2.r#type, "STRING");
        assert_eq!(f2.mode, "REPEATED");

        let f3 = schema.get_field_by_index(3).unwrap();
        assert_eq!(f3.name, "created");
        assert_eq!(f3.r#type, "TIMESTAMP");
        assert_eq!(f3.mode, "REQUIRED");

        assert_eq!(schema.get_field_by_index(4).unwrap().r#type, "BOOLEAN");
        assert_eq!(schema.get_field_by_index(5).unwrap().r#type, "FLOAT64");
        assert_eq!(schema.get_field_by_index(6).unwrap().r#type, "BYTES");
        assert_eq!(schema.get_field_by_index(7).unwrap().r#type, "DATE");
        assert_eq!(schema.get_field_by_index(8).unwrap().r#type, "TIME");
        assert_eq!(schema.get_field_by_index(9).unwrap().r#type, "INTERVAL");
        assert_eq!(schema.get_field_by_index(10).unwrap().r#type, "NUMERIC");

        let f11 = schema.get_field_by_index(11).unwrap();
        assert_eq!(f11.name, "profile");
        assert_eq!(f11.r#type, "RECORD");
        assert_eq!(f11.mode, "NULLABLE");
        assert_eq!(f11.fields.len(), 2);
        assert_eq!(f11.fields[0].name, "bio");
        assert_eq!(f11.fields[0].r#type, "STRING");
        assert_eq!(f11.fields[0].mode, "NULLABLE");
        assert_eq!(f11.fields[1].name, "level");
        assert_eq!(f11.fields[1].r#type, "INTEGER");
        assert_eq!(f11.fields[1].mode, "REQUIRED");

        let f12 = schema.get_field_by_index(12).unwrap();
        assert_eq!(f12.name, "fallback");
        assert_eq!(f12.r#type, "STRING");
        assert_eq!(f12.mode, "NULLABLE");

        assert_eq!(schema.get_field_by_index(13).unwrap().r#type, "FLOAT64");
        assert_eq!(schema.get_field_by_index(14).unwrap().r#type, "BYTES");
        assert_eq!(schema.get_field_by_index(14).unwrap().mode, "REQUIRED");
        assert_eq!(schema.get_field_by_index(15).unwrap().r#type, "DATETIME");
        assert_eq!(schema.get_field_by_index(16).unwrap().r#type, "BIGNUMERIC");

        let f17 = schema.get_field_by_index(17).unwrap();
        assert_eq!(f17.name, "fixed_list");
        assert_eq!(f17.r#type, "INTEGER");
        assert_eq!(f17.mode, "REPEATED");
    }
}
