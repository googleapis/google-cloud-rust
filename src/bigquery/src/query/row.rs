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

use crate::error::{ConvertError, RowError};
use crate::query::{FromSql, Schema};
use std::sync::Arc;
use wkt::{ListValue, Struct, Value};

pub type Result<T> = std::result::Result<T, RowError>;

/// A container for a single row within a query result set.
#[derive(Clone, Debug)]
pub struct Row {
    pub(crate) values: Value,
    pub(crate) schema: Arc<Schema>,
}

pub(crate) mod private {
    /// A sealed trait to prevent external implementation of `ColumnIndex`.
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for &str {}
    impl Sealed for String {}
}

/// A trait for types that can be used to index into a [`Row`].
///
/// This trait is sealed and cannot be implemented for types outside of this crate.
pub trait ColumnIndex: private::Sealed + std::fmt::Debug {
    /// Returns the index of the column in the given row, if it exists.
    fn index(&self, row: &Row) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn index(&self, row: &Row) -> Option<usize> {
        row.schema.get_field_by_index(*self).map(|_| *self)
    }
}

impl ColumnIndex for &str {
    fn index(&self, row: &Row) -> Option<usize> {
        row.schema.get_field_index_by_name(self)
    }
}

impl ColumnIndex for String {
    fn index(&self, row: &Row) -> Option<usize> {
        self.as_str().index(row)
    }
}

impl Row {
    pub(crate) fn try_new(row: Struct, schema: &Arc<Schema>) -> Result<Self> {
        let field_list = get_field_list(row)?;

        if field_list.len() != schema.len() {
            return Err(RowError::InvalidRowFormat(format!(
                "schema and row cell mismatch (expected {}, got {})",
                schema.len(),
                field_list.len()
            )));
        }

        let mut values = ListValue::new();
        for (i, cell) in field_list.into_iter().enumerate() {
            let value = get_field_value(cell)?;
            match schema.get_field_by_index(i) {
                Some(f) => {
                    let field_name = &f.name;
                    let field_type = &f.r#type;
                    let schema = Arc::new(Schema::new_from_field(f.clone()));
                    let value = convert_value(value, field_name, field_type, &schema)?;
                    values.push(value);
                }
                None => continue,
            }
        }

        Ok(Self {
            values: Value::Array(values),
            schema: schema.clone(),
        })
    }

    /// Retrieves a value from the row by column name or zero-based index.
    pub fn try_get<T: FromSql, I: ColumnIndex>(&self, index: I) -> Result<T> {
        let idx = index
            .index(self)
            .ok_or_else(|| RowError::ColumnNotFound(format!("{:?}", index)))?;

        let val = self
            .values
            .get(idx)
            .ok_or_else(|| RowError::IndexOutOfRange {
                index: idx,
                len: self.schema.len(),
            })?;

        T::from_sql(val.clone()).map_err(|e| {
            let field_name = self
                .schema
                .get_field_by_index(idx)
                .map(|f| f.name.clone())
                .unwrap_or_else(|| idx.to_string());
            RowError::TypeConversion {
                column: field_name,
                source: e,
            }
        })
    }

    /// Retrieves a value from the row by column name or zero-based index, panicking on error.
    pub fn get<T: FromSql, I: ColumnIndex>(&self, index: I) -> T {
        self.try_get(index).unwrap()
    }
}

fn get_field_list(mut row: Struct) -> Result<Vec<Value>> {
    match row.remove("f") {
        Some(Value::Array(arr)) => Ok(arr),
        Some(_) => Err(RowError::InvalidRowFormat("invalid field values".into())),
        None => Err(RowError::InvalidRowFormat("missing field values".into())),
    }
}

fn get_field_value(value: Value) -> Result<Value> {
    match value {
        Value::Object(mut obj) => match obj.remove("v") {
            Some(val) => Ok(val),
            None => Err(RowError::InvalidRowFormat("missing field value".into())),
        },
        _ => Err(RowError::InvalidRowFormat("invalid field value".into())),
    }
}

fn convert_value(
    value: Value,
    field_name: &str,
    field_type: &str,
    _schema: &Arc<Schema>,
) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::String(v) => convert_basic_type(v, field_name, field_type),
        Value::Object(_) => unimplemented!("TODO(#5592): nested records not implemented"),
        Value::Array(_) => unimplemented!("TODO(#5592): repeated fields not implemented"),
        _ => Err(RowError::InvalidRowFormat(format!(
            "cell value is not an object: value={:?}, field_type={:?}",
            value, field_type
        ))),
    }
}

fn convert_basic_type(value: String, field_name: &str, field_type: &str) -> Result<Value> {
    match field_type {
        "STRING" => Ok(Value::String(value)),
        "BYTES" => Ok(Value::String(value)),
        "TIMESTAMP" => Ok(Value::String(value)),
        "DATE" => Ok(Value::String(value)),
        "TIME" => Ok(Value::String(value)),
        "DATETIME" => Ok(Value::String(value)),
        "NUMERIC" | "BIGNUMERIC" => Ok(Value::String(value)),
        "BIGINT" => Ok(Value::String(value)),
        "GEOGRAPHY" => Ok(Value::String(value)),
        "JSON" => Ok(Value::String(value)),
        "INTERVAL" => Ok(Value::String(value)),
        "RANGE" => Ok(Value::String(value)),
        "INTEGER" | "INT64" => {
            let num = value.parse::<i64>().map_err(|e| RowError::TypeConversion {
                column: field_name.to_string(),
                source: ConvertError::Convert(Box::new(e)),
            })?;
            Ok(Value::Number(serde_json::Number::from(num)))
        }
        "FLOAT" | "FLOAT64" => {
            let num = value.parse::<f64>().map_err(|e| RowError::TypeConversion {
                column: field_name.to_string(),
                source: ConvertError::Convert(Box::new(e)),
            })?;
            match serde_json::Number::from_f64(num) {
                Some(n) => Ok(Value::Number(n)),
                None => Ok(Value::String(value)),
            }
        }
        "BOOLEAN" | "BOOL" => {
            let b = value
                .to_lowercase()
                .parse::<bool>()
                .map_err(|e| RowError::TypeConversion {
                    column: field_name.to_string(),
                    source: ConvertError::Convert(Box::new(e)),
                })?;
            Ok(Value::Bool(b))
        }
        _ => Err(RowError::InvalidRowFormat(format!(
            "unknown field type: {} at column {}",
            field_type, field_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    async fn convert_basic_types_from_row() -> TestResult {
        let raw_row = serde_json::json!({
            "f": [
                { "v": "James" },
                { "v": "272793" },
                { "v": "TRUE" },
                { "v": null },
                { "v": "64.0" },
            ]
        })
        .as_object()
        .expect("row is an object")
        .clone();
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("name")
                .set_type("STRING")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("some_int")
                .set_type("INTEGER")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("some_bool")
                .set_type("BOOLEAN")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("some_null")
                .set_type("INTEGER")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("some_float")
                .set_type("FLOAT64")
                .set_mode("NULLABLE"),
        ]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        assert_eq!(row.get::<String, _>(0), "James");
        assert_eq!(row.get::<String, _>("name"), "James");

        assert_eq!(row.get::<i64, _>(1), 272793);
        assert_eq!(row.get::<i64, _>("some_int"), 272793);

        assert!(row.get::<bool, _>(2));
        assert!(row.get::<bool, _>("some_bool"));

        assert_eq!(row.get::<Option<i64>, _>(3), None);
        assert_eq!(row.get::<Option<i64>, _>("some_null"), None);

        assert_eq!(row.get::<f64, _>(4), 64.0);
        assert_eq!(row.get::<f64, _>("some_float"), 64.0);

        Ok(())
    }

    #[test_case("INTEGER", "123", Some(Value::Number(123.into())); "integer positive")]
    #[test_case("INTEGER", "-456", Some(Value::Number((-456).into())); "integer negative")]
    #[test_case("INTEGER", "abc", None; "integer invalid")]
    #[test_case("INT64", "9223372036854775807", Some(Value::Number(9223372036854775807_i64.into())); "int64 max")]
    #[test_case("INT64", "9223372036854775808", None; "int64 overflow")]
    #[test_case("FLOAT", "123.45", Some(Value::Number(serde_json::Number::from_f64(123.45).unwrap())); "float success")]
    #[test_case("FLOAT64", "NaN", Some(Value::String("NaN".to_string())); "float NaN")]
    #[test_case("FLOAT64", "+inf", Some(Value::String("+inf".to_string())); "float positive infinity")]
    #[test_case("FLOAT64", "-inf", Some(Value::String("-inf".to_string())); "float negative infinity")]
    #[test_case("FLOAT", "abc", None; "float invalid")]
    #[test_case("BOOLEAN", "true", Some(Value::Bool(true)); "boolean true lowercase")]
    #[test_case("BOOLEAN", "TRUE", Some(Value::Bool(true)); "boolean true uppercase")]
    #[test_case("BOOL", "false", Some(Value::Bool(false)); "bool false")]
    #[test_case("BOOL", "invalid", None; "bool invalid")]
    #[test_case("UNKNOWN", "value", None; "unknown type")]
    fn convert_basic_type_cases(field_type: &str, value: &str, expected: Option<Value>) {
        let res = convert_basic_type(value.to_string(), "test_col", field_type);
        match expected {
            Some(expected_val) => {
                assert_eq!(res.expect("should succeed"), expected_val);
            }
            None => {
                let err = res.expect_err("should fail");
                if field_type == "UNKNOWN" {
                    assert!(matches!(err, RowError::InvalidRowFormat(_)));
                } else {
                    assert!(matches!(err, RowError::TypeConversion { .. }));
                }
            }
        }
    }
}
