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

mod sealed {
    /// A sealed trait to prevent external implementation of `ColumnIndex`.
    pub trait ColumnIndex {}
    impl ColumnIndex for usize {}
    impl ColumnIndex for &str {}
    impl ColumnIndex for String {}
}

/// A trait for types that can be used to index into a [`Row`].
///
/// This trait is sealed and cannot be implemented for types outside of this crate.
pub trait ColumnIndex: sealed::ColumnIndex + std::fmt::Debug {
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

/// A trait for types that can be created from a BigQuery [`Row`].
pub trait FromRow: Sized {
    /// Creates an instance of this type by consuming the given [`Row`].
    fn from_row(row: Row) -> Result<Self>;
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

    fn resolve_index<I: ColumnIndex>(&self, col: &I) -> Result<usize> {
        col.index(self)
            .ok_or_else(|| RowError::ColumnNotFound(format!("{:?}", col)))
    }

    fn convert_value_at<T: FromSql>(&self, idx: usize, val: Value) -> Result<T> {
        T::from_sql(val).map_err(|e| {
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

    /// Retrieves a value from the row by column name or zero-based index.
    pub fn try_get<T: FromSql, I: ColumnIndex>(&self, index: I) -> Result<T> {
        let idx = self.resolve_index(&index)?;
        let val = self
            .values
            .get(idx)
            .ok_or_else(|| RowError::IndexOutOfRange {
                index: idx,
                len: self.schema.len(),
            })?;

        self.convert_value_at(idx, val.clone())
    }

    /// Takes ownership of a value from the row by column name or zero-based index.
    /// The value in the row is replaced with `Value::Null` in-place to avoid cloning.
    pub fn take<T: FromSql, I: ColumnIndex>(&mut self, index: I) -> Result<T> {
        let idx = self.resolve_index(&index)?;

        let val = self
            .values
            .get_mut(idx)
            .ok_or_else(|| RowError::IndexOutOfRange {
                index: idx,
                len: self.schema.len(),
            })?;

        // swap out the value in-place to avoid clones
        let owned_val = std::mem::replace(val, Value::Null);
        self.convert_value_at(idx, owned_val)
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
    schema: &Arc<Schema>,
) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::String(v) => convert_basic_type(v, field_name, field_type),
        Value::Object(v) => convert_nested(v, schema),
        Value::Array(v) => convert_repeated(v, field_name, field_type, schema),
        _ => Err(RowError::InvalidRowFormat(format!(
            "cell value is not an object: value={:?}, field_type={:?}",
            value, field_type
        ))),
    }
}

fn convert_repeated(
    value: ListValue,
    field_name: &str,
    field_type: &str,
    schema: &Arc<Schema>,
) -> Result<Value> {
    let mut values = ListValue::new();
    for cell in value {
        // each cell contains a single entry, keyed by "v"
        let val = get_field_value(cell)?;
        let v = convert_value(val, field_name, field_type, schema)?;
        values.push(v);
    }
    Ok(Value::Array(values))
}

fn convert_nested(value: Struct, schema: &Arc<Schema>) -> Result<Value> {
    let row = Row::try_new(value, schema)?;
    let mut obj = Struct::new();
    if let Value::Array(list) = row.values {
        for (i, val) in list.into_iter().enumerate() {
            if let Some(field) = schema.get_field_by_index(i) {
                obj.insert(field.name.clone(), val);
            }
        }
    }
    Ok(Value::Object(obj))
}

fn convert_basic_type(value: String, field_name: &str, field_type: &str) -> Result<Value> {
    match field_type {
        "STRING" | "BYTES" | "TIMESTAMP" | "DATE" | "TIME" | "DATETIME" | "NUMERIC"
        | "BIGNUMERIC" | "BIGINT" | "GEOGRAPHY" | "JSON" | "INTERVAL" | "RANGE" => {
            Ok(Value::String(value))
        }
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
    use crate as google_cloud_bigquery;
    use crate::FromRow;
    use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};
    use google_cloud_type::model::Decimal;
    use rust_decimal::Decimal as RustDecimal;
    use serde_json::{Map, json};
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    async fn convert_basic_types_from_row() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": "James" },
                { "v": "272793" },
                { "v": "TRUE" },
                { "v": null },
                { "v": "64.0" },
            ]),
        )]);
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
        let mut row = Row::try_new(raw_row, &schema)?;

        assert_eq!(row.get::<String, _>(0), "James");
        assert_eq!(row.get::<String, _>("name"), "James");

        assert_eq!(row.get::<i32, _>(1), 272793);
        assert_eq!(row.get::<i32, _>("some_int"), 272793);
        assert_eq!(row.get::<i64, _>(1), 272793);
        assert_eq!(row.get::<i64, _>("some_int"), 272793);

        assert!(row.get::<bool, _>(2));
        assert!(row.get::<bool, _>("some_bool"));

        assert_eq!(row.get::<Option<i64>, _>(3), None);
        assert_eq!(row.get::<Option<i64>, _>("some_null"), None);

        assert_eq!(row.get::<f32, _>(4), 64.0);
        assert_eq!(row.get::<f32, _>("some_float"), 64.0);
        assert_eq!(row.get::<f64, _>(4), 64.0);
        assert_eq!(row.get::<f64, _>("some_float"), 64.0);

        assert_eq!(row.take::<String, _>(0)?, "James");
        assert_eq!(row.try_get::<Option<String>, _>(0)?, None);

        assert_eq!(row.take::<i32, _>(1)?, 272793);
        assert_eq!(row.try_get::<Option<i32>, _>(1)?, None);

        assert!(row.take::<bool, _>(2)?);
        assert_eq!(row.try_get::<Option<bool>, _>(2)?, None);

        assert_eq!(row.take::<Option<i64>, _>(3)?, None);
        assert_eq!(row.try_get::<Option<i64>, _>(3)?, None);

        assert_eq!(row.take::<f32, _>(4)?, 64.0);
        assert_eq!(row.try_get::<Option<f32>, _>(4)?, None);

        Ok(())
    }

    #[tokio::test]
    async fn convert_numeric_from_row() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": "123.456" },
                { "v": "99999999999999999999.123456789" },
                { "v": "99999999999999999999999999999999.123" },
            ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("price")
                .set_type("NUMERIC")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("big_amount")
                .set_type("BIGNUMERIC")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("overflow_amount")
                .set_type("BIGNUMERIC")
                .set_mode("NULLABLE"),
        ]);
        let schema = Arc::new(Schema::new(schema));
        let mut row = Row::try_new(raw_row, &schema)?;

        assert_eq!(
            row.get::<Decimal, _>(0),
            Decimal::new().set_value("123.456")
        );
        assert_eq!(
            row.get::<Decimal, _>("price"),
            Decimal::new().set_value("123.456")
        );

        assert_eq!(
            row.get::<Decimal, _>(1),
            Decimal::new().set_value("99999999999999999999.123456789")
        );
        assert_eq!(
            row.get::<Decimal, _>("big_amount"),
            Decimal::new().set_value("99999999999999999999.123456789")
        );

        assert_eq!(
            row.get::<RustDecimal, _>(0),
            "123.456".parse().expect("valid decimal")
        );
        assert_eq!(
            row.get::<RustDecimal, _>("price"),
            "123.456".parse().expect("valid decimal")
        );

        assert_eq!(
            row.get::<RustDecimal, _>(1),
            "99999999999999999999.123456789"
                .parse()
                .expect("valid decimal")
        );
        assert_eq!(
            row.get::<RustDecimal, _>("big_amount"),
            "99999999999999999999.123456789"
                .parse()
                .expect("valid decimal")
        );

        assert!(row.try_get::<RustDecimal, _>(2).is_err());
        assert!(row.try_get::<RustDecimal, _>("overflow_amount").is_err());

        assert_eq!(
            row.take::<Decimal, _>(0)?,
            Decimal::new().set_value("123.456")
        );
        assert_eq!(row.try_get::<Option<Decimal>, _>(0)?, None);

        assert_eq!(
            row.take::<RustDecimal, _>(1)?,
            "99999999999999999999.123456789".parse()?
        );
        assert_eq!(row.try_get::<Option<RustDecimal>, _>(1)?, None);

        Ok(())
    }

    #[tokio::test]
    async fn convert_record_from_row() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                {
                    "v": {
                        "f": [
                            { "v": "Alice" },
                            { "v": "25" }
                        ]
                    }
                }
            ]),
        )]);
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("user")
            .set_type("RECORD")
            .set_mode("NULLABLE")
            .set_fields([
                TableFieldSchema::new()
                    .set_name("name")
                    .set_type("STRING")
                    .set_mode("NULLABLE"),
                TableFieldSchema::new()
                    .set_name("age")
                    .set_type("INTEGER")
                    .set_mode("NULLABLE"),
            ])]);
        let schema = Arc::new(Schema::new(schema));
        let mut row = Row::try_new(raw_row, &schema)?;

        let expected: Struct = serde_json::from_value(json!({
            "name": "Alice",
            "age": 25,
        }))?;
        assert_eq!(row.get::<Struct, _>(0), expected);
        assert_eq!(row.get::<Struct, _>("user"), expected);
        assert_eq!(row.take::<Struct, _>("user")?, expected);
        assert_eq!(row.try_get::<Option<Struct>, _>("user")?, None);

        Ok(())
    }

    #[tokio::test]
    async fn convert_repeated_from_row() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                {
                    "v": [
                        { "v": "1" },
                        { "v": "2" },
                        { "v": "3" }
                    ]
                }
            ]),
        )]);
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("numbers")
            .set_type("INTEGER")
            .set_mode("REPEATED")]);
        let schema = Arc::new(Schema::new(schema));
        let mut row = Row::try_new(raw_row, &schema)?;

        assert_eq!(row.get::<Vec<i64>, _>(0), vec![1, 2, 3]);
        assert_eq!(row.get::<Vec<i64>, _>("numbers"), vec![1, 2, 3]);
        assert_eq!(row.take::<Vec<i64>, _>("numbers")?, vec![1, 2, 3]);
        assert_eq!(row.try_get::<Option<Vec<i64>>, _>("numbers")?, None);

        Ok(())
    }

    #[tokio::test]
    async fn convert_repeated_record_from_row() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                {
                    "v": [
                        {
                            "v": {
                                "f": [
                                    { "v": "Bob" },
                                    { "v": "28" }
                                ]
                            }
                        },
                        {
                            "v": {
                                "f": [
                                    { "v": "Charlie" },
                                    { "v": "31" }
                                ]
                            }
                        }
                    ]
                }
            ]),
        )]);
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("users")
            .set_type("RECORD")
            .set_mode("REPEATED")
            .set_fields([
                TableFieldSchema::new()
                    .set_name("name")
                    .set_type("STRING")
                    .set_mode("NULLABLE"),
                TableFieldSchema::new()
                    .set_name("age")
                    .set_type("INTEGER")
                    .set_mode("NULLABLE"),
            ])]);
        let schema = Arc::new(Schema::new(schema));
        let mut row = Row::try_new(raw_row, &schema)?;

        let expected: Vec<Struct> = serde_json::from_value(json!([
            {
                "name": "Bob",
                "age": 28,
            },
            {
                "name": "Charlie",
                "age": 31,
            },
        ]))?;
        assert_eq!(row.get::<Vec<Struct>, _>(0), expected);
        assert_eq!(row.get::<Vec<Struct>, _>("users"), expected);
        assert_eq!(row.take::<Vec<Struct>, _>("users")?, expected);
        assert_eq!(row.try_get::<Option<Vec<Struct>>, _>("users")?, None);

        Ok(())
    }

    #[test_case("INTEGER", "123", Value::Number(123.into()); "integer positive")]
    #[test_case("INTEGER", "-456", Value::Number((-456).into()); "integer negative")]
    #[test_case("INT64", "9223372036854775807", Value::Number(9223372036854775807_i64.into()); "int64 max")]
    #[test_case("FLOAT", "123.45", Value::Number(serde_json::Number::from_f64(123.45).unwrap()); "float success")]
    #[test_case("FLOAT64", "NaN", Value::String("NaN".to_string()); "float NaN")]
    #[test_case("FLOAT64", "+inf", Value::String("+inf".to_string()); "float positive infinity")]
    #[test_case("FLOAT64", "-inf", Value::String("-inf".to_string()); "float negative infinity")]
    #[test_case("BOOLEAN", "true", Value::Bool(true); "boolean true lowercase")]
    #[test_case("BOOLEAN", "TRUE", Value::Bool(true); "boolean true uppercase")]
    #[test_case("BOOL", "false", Value::Bool(false); "bool false")]
    fn convert_basic_type_cases_success(field_type: &str, value: &str, expected: Value) {
        let res = convert_basic_type(value.to_string(), "test_col", field_type);
        let value = res.expect("should succeed");
        assert_eq!(value, expected);
    }

    #[test_case("INTEGER", "abc"; "integer invalid")]
    #[test_case("INT64", "9223372036854775808"; "int64 overflow")]
    #[test_case("FLOAT", "abc"; "float invalid")]
    #[test_case("BOOL", "invalid"; "bool invalid")]
    fn convert_basic_type_cases_conversion_fail(field_type: &str, value: &str) {
        let res = convert_basic_type(value.to_string(), "test_col", field_type);
        let err = res.unwrap_err();
        assert!(matches!(err, RowError::TypeConversion { .. }));
    }

    #[test]
    fn convert_basic_type_invalid_row_format() {
        let res = convert_basic_type("value".to_string(), "test_col", "UNKNOWN");
        let err = res.unwrap_err();
        assert!(matches!(err, RowError::InvalidRowFormat(_)));
    }

    #[derive(FromRow, Debug, PartialEq)]
    struct TestRow {
        name: String,
        #[bigquery(rename = "custom_int")]
        some_int: i64,
        some_bool: bool,
        some_null: Option<i64>,
    }

    #[tokio::test]
    async fn derive_from_row_success() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": "James" },
                { "v": "272793" },
                { "v": "TRUE" },
                { "v": null },
            ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("name")
                .set_type("STRING")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("custom_int")
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
        ]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        // TryFrom<&Row>
        let from_ref = TestRow::try_from(&row)?;
        assert_eq!(
            from_ref,
            TestRow {
                name: "James".to_string(),
                some_int: 272793,
                some_bool: true,
                some_null: None,
            }
        );

        // TryFrom<Row> / FromRow::from_row
        let from_owned = TestRow::try_from(row)?;
        assert_eq!(
            from_owned,
            TestRow {
                name: "James".to_string(),
                some_int: 272793,
                some_bool: true,
                some_null: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn derive_from_row_missing_column() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": "James" },
                { "v": "123" },
                { "v": "TRUE" },
                { "v": null },
            ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("name")
                .set_type("STRING")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("wrong_col")
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
        ]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let err = TestRow::try_from(row).unwrap_err();
        assert!(matches!(err, RowError::ColumnNotFound(col) if col == "\"custom_int\""));
        Ok(())
    }
}
