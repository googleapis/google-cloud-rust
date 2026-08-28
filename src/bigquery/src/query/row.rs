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
use crate::query::from_sql::ArrowCell;
use crate::query::{FromSql, Schema};
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery_v2::model::TableFieldSchema;
use std::sync::Arc;
use wkt::{ListValue, Struct, Value};

pub type Result<T> = std::result::Result<T, RowError>;

/// A container for a single row within a query result set.
///
/// [`RowIterator::next()`](crate::query::RowIterator::next) yields a `Row`.
///
/// Each `Row` contains parsed cell values and a reference to the table schema.
///
/// # Zero-Copy Struct Mapping via Derive Macros
///
/// Define typed structs with `#[derive(FromRow)]` to convert rows directly into
/// your domain types using `TryFrom<Row>` without unnecessary allocations:
///
/// ```
/// # use google_cloud_bigquery::query::{Row, FromRow};
/// #[derive(FromRow, Debug)]
/// struct UserStats {
///     name: String,
///     count: i64,
/// }
///
/// # fn sample(row: Row) -> anyhow::Result<()> {
/// let user: UserStats = row.try_into()?;
/// println!("{}: {}", user.name, user.count);
/// # Ok(())
/// # }
/// ```
///
/// # Field Extraction by Name or Index
///
/// Retrieve individual cell values by column name (`&str`) or index (`usize`)
/// using [`get()`](Row::get), [`try_get()`](Row::try_get), or
/// [`take()`](Row::take):
///
/// ```
/// # use google_cloud_bigquery::query::Row;
/// # fn sample(row: Row) {
/// let name: String = row.get("name");
/// let age: i64 = row.get(1);
/// println!("{name} is {age} years old");
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Row {
    pub(crate) inner: RowInner,
    pub(crate) schema: Arc<Schema>,
}

#[derive(Clone, Debug)]
pub(crate) enum RowInner {
    Json(ListValue),
    Arrow {
        batch: Arc<RecordBatch>,
        row_idx: usize,
    },
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
pub trait ColumnIndex: sealed::ColumnIndex + std::fmt::Display {
    /// Returns the index of the column in the given row, if it exists.
    fn index(&self, row: &Row) -> Option<usize>;

    /// Returns the index of the column in the given arrow struct array, if it exists.
    fn arrow_index(&self, struct_arr: &arrow::array::StructArray) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn index(&self, row: &Row) -> Option<usize> {
        row.schema.get_field_by_index(*self).map(|_| *self)
    }

    fn arrow_index(&self, struct_arr: &arrow::array::StructArray) -> Option<usize> {
        if *self < struct_arr.num_columns() {
            Some(*self)
        } else {
            None
        }
    }
}

impl ColumnIndex for &str {
    fn index(&self, row: &Row) -> Option<usize> {
        row.schema.get_field_index_by_name(self)
    }

    fn arrow_index(&self, struct_arr: &arrow::array::StructArray) -> Option<usize> {
        struct_arr.fields().iter().position(|f| f.name() == self)
    }
}

impl ColumnIndex for String {
    fn index(&self, row: &Row) -> Option<usize> {
        self.as_str().index(row)
    }

    fn arrow_index(&self, struct_arr: &arrow::array::StructArray) -> Option<usize> {
        self.as_str().arrow_index(struct_arr)
    }
}

impl Row {
    pub(crate) fn try_new(row: Struct, schema: &Arc<Schema>) -> Result<Self> {
        let values = convert_row(row, schema.fields())?;

        Ok(Self {
            inner: RowInner::Json(values),
            schema: schema.clone(),
        })
    }

    pub(crate) fn try_new_from_arrow(
        batch: &Arc<RecordBatch>,
        row_idx: usize,
        schema: &Arc<Schema>,
    ) -> Result<Self> {
        if batch.num_columns() != schema.len() {
            return Err(RowError::InvalidRowFormat(format!(
                "schema and row cell mismatch (expected {}, got {})",
                schema.len(),
                batch.num_columns()
            )));
        }

        Ok(Self {
            inner: RowInner::Arrow {
                batch: Arc::clone(batch),
                row_idx,
            },
            schema: schema.clone(),
        })
    }

    fn resolve_index<I: ColumnIndex>(&self, col: &I) -> Result<usize> {
        col.index(self)
            .ok_or_else(|| RowError::ColumnNotFound(format!("{col}")))
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

    /// Attempts to retrieve a value from the row by column name or zero-based
    /// index.
    ///
    /// The return type must implement [`FromSql`](crate::query::FromSql).
    ///
    /// # Errors
    ///
    /// Returns [`RowError::ColumnNotFound`](crate::error::RowError::ColumnNotFound)
    /// if the column does not exist,
    /// [`RowError::IndexOutOfRange`](crate::error::RowError::IndexOutOfRange) if
    /// the index exceeds schema bounds, or
    /// [`RowError::TypeConversion`](crate::error::RowError::TypeConversion) if
    /// the value cannot be converted to `T`.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::query::Row;
    /// # fn sample(row: Row) -> anyhow::Result<()> {
    /// let msg: String = row.try_get("msg")?;
    /// println!("Value: {msg}");
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_get<T: FromSql, I: ColumnIndex>(&self, index: I) -> Result<T> {
        let idx = self.resolve_index(&index)?;
        match &self.inner {
            RowInner::Json(values) => {
                let val = values.get(idx).ok_or_else(|| RowError::IndexOutOfRange {
                    index: idx,
                    len: self.schema.len(),
                })?;
                self.convert_value_at(idx, val.clone())
            }
            RowInner::Arrow { batch, row_idx } => {
                let col = batch
                    .columns()
                    .get(idx)
                    .ok_or_else(|| RowError::IndexOutOfRange {
                        index: idx,
                        len: self.schema.len(),
                    })?;
                T::from_arrow(ArrowCell::new(col.as_ref(), *row_idx)).map_err(|e| {
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
        }
    }

    /// Takes ownership of a value from the row by column name or zero-based
    /// index.
    ///
    /// This replaces the cell value in the row with `Value::Null` in-place to
    /// avoid cloning. Attempting to read the column again after calling `take()`
    /// yields `Value::Null`.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`try_get()`](Row::try_get).
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::query::Row;
    /// # fn sample(mut row: Row) -> anyhow::Result<()> {
    /// let text: String = row.take("big_text")?;
    /// println!("Length: {}", text.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn take<T: FromSql, I: ColumnIndex>(&mut self, index: I) -> Result<T> {
        let idx = self.resolve_index(&index)?;
        match &mut self.inner {
            RowInner::Json(values) => {
                let val = values
                    .get_mut(idx)
                    .ok_or_else(|| RowError::IndexOutOfRange {
                        index: idx,
                        len: self.schema.len(),
                    })?;

                // swap out the value in-place to avoid clones
                let owned_val = std::mem::replace(val, Value::Null);
                self.convert_value_at(idx, owned_val)
            }
            RowInner::Arrow { batch, row_idx } => {
                let col = batch
                    .columns()
                    .get(idx)
                    .ok_or_else(|| RowError::IndexOutOfRange {
                        index: idx,
                        len: self.schema.len(),
                    })?;
                T::from_arrow(ArrowCell::new(col.as_ref(), *row_idx)).map_err(|e| {
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
        }
    }

    /// Retrieves a value from the row by column name or zero-based index.
    ///
    /// # Panics
    ///
    /// Panics if the column does not exist or if the value cannot be converted
    /// to type `T`.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::query::Row;
    /// # fn sample(row: Row) {
    /// let count: i64 = row.get("count");
    /// println!("Count: {count}");
    /// # }
    /// ```
    pub fn get<T: FromSql, I: ColumnIndex>(&self, index: I) -> T {
        self.try_get(index).unwrap()
    }
}

fn convert_row(row: Struct, fields: &[TableFieldSchema]) -> Result<ListValue> {
    let mut field_list = get_field_list(row)?;

    if field_list.len() != fields.len() {
        return Err(RowError::InvalidRowFormat(format!(
            "schema and row cell mismatch (expected {}, got {})",
            fields.len(),
            field_list.len()
        )));
    }

    for (cell, field) in field_list.iter_mut().zip(fields) {
        *cell = convert_value(get_field_value(cell.take())?, field)?;
    }
    Ok(field_list)
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

fn convert_value(value: Value, field: &TableFieldSchema) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::String(v) => convert_basic_type(v, &field.name, &field.r#type),
        Value::Object(v) => convert_nested(v, &field.fields),
        Value::Array(v) => convert_repeated(v, field),
        _ => Err(RowError::InvalidRowFormat(format!(
            "cell value is not an object: value={:?}, field_type={:?}",
            value, field.r#type
        ))),
    }
}

fn convert_repeated(mut value: ListValue, field: &TableFieldSchema) -> Result<Value> {
    for cell in &mut value {
        // each cell contains a single entry, keyed by "v"
        let val = get_field_value(cell.take())?;
        *cell = convert_value(val, field)?;
    }
    Ok(Value::Array(value))
}

fn convert_nested(value: Struct, fields: &[TableFieldSchema]) -> Result<Value> {
    let values = convert_row(value, fields)?;
    let obj: Struct = fields
        .iter()
        .zip(values)
        .map(|(field, value)| (field.name.clone(), value))
        .collect();
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
            let b = if value.eq_ignore_ascii_case("true") {
                true
            } else if value.eq_ignore_ascii_case("false") {
                false
            } else {
                return Err(RowError::TypeConversion {
                    column: field_name.to_string(),
                    source: ConvertError::Convert(
                        "provided string was not `true` or `false`".into(),
                    ),
                });
            };
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
    use crate::query::FromRow;
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
    async fn convert_bytes_from_row() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": "AQIDBA==" },
                { "v": "SGVsbG8=" },
                { "v": null },
            ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("payload_vec")
                .set_type("BYTES")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("payload_bytes")
                .set_type("BYTES")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("null_bytes")
                .set_type("BYTES")
                .set_mode("NULLABLE"),
        ]);
        let schema = Arc::new(Schema::new(schema));
        let mut row = Row::try_new(raw_row, &schema)?;

        assert_eq!(row.get::<Vec<u8>, _>(0), vec![1, 2, 3, 4]);
        assert_eq!(row.get::<Vec<u8>, _>("payload_vec"), vec![1, 2, 3, 4]);

        assert_eq!(
            row.get::<bytes::Bytes, _>(1),
            bytes::Bytes::from_static(b"Hello")
        );
        assert_eq!(
            row.get::<bytes::Bytes, _>("payload_bytes"),
            bytes::Bytes::from_static(b"Hello")
        );

        assert_eq!(row.get::<Option<Vec<u8>>, _>(2), None);
        assert_eq!(row.get::<Option<bytes::Bytes>, _>("null_bytes"), None);

        assert_eq!(row.take::<Vec<u8>, _>(0)?, vec![1, 2, 3, 4]);
        assert_eq!(row.try_get::<Option<Vec<u8>>, _>(0)?, None);

        assert_eq!(
            row.take::<bytes::Bytes, _>(1)?,
            bytes::Bytes::from_static(b"Hello")
        );
        assert_eq!(row.try_get::<Option<bytes::Bytes>, _>(1)?, None);

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

    #[test]
    fn convert_value_unsupported_value() {
        let field = TableFieldSchema::new()
            .set_name("test_col")
            .set_type("BOOLEAN")
            .set_mode("NULLABLE");
        let res = convert_value(Value::Bool(true), &field);
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

        let converted_row = TestRow::try_from(row)?;
        assert_eq!(
            converted_row,
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
        assert!(matches!(err, RowError::ColumnNotFound(col) if col == "custom_int"));
        Ok(())
    }

    #[test]
    fn try_new_from_arrow_batch() -> TestResult {
        use arrow::array::{
            BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
        };
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true),
            Field::new("active", DataType::Boolean, false),
            Field::new("score", DataType::Float64, false),
            Field::new(
                "created_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "created_dt",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));

        let name = StringArray::from(vec!["Alice", "Bob"]);
        let age = Int64Array::from(vec![Some(30), None]);
        let active = BooleanArray::from(vec![true, false]);
        let score = Float64Array::from(vec![98.5, 87.25]);
        let created_ts =
            TimestampMicrosecondArray::from(vec![1_600_000_000_000_000, 1_700_000_000_000_000])
                .with_timezone("UTC");
        let created_dt =
            TimestampMicrosecondArray::from(vec![1_600_000_000_000_000, 1_700_000_000_000_000]);

        let batch = Arc::new(RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(name),
                Arc::new(age),
                Arc::new(active),
                Arc::new(score),
                Arc::new(created_ts),
                Arc::new(created_dt),
            ],
        )?);

        let table_schema = TableSchema::new().set_fields([
            TableFieldSchema::new().set_name("name").set_type("STRING"),
            TableFieldSchema::new().set_name("age").set_type("INTEGER"),
            TableFieldSchema::new()
                .set_name("active")
                .set_type("BOOLEAN"),
            TableFieldSchema::new().set_name("score").set_type("FLOAT"),
            TableFieldSchema::new()
                .set_name("created_ts")
                .set_type("TIMESTAMP"),
            TableFieldSchema::new()
                .set_name("created_dt")
                .set_type("DATETIME"),
        ]);
        let schema = Arc::new(Schema::new(table_schema));

        let row0 = Row::try_new_from_arrow(&batch, 0, &schema)?;
        assert_eq!(row0.get::<String, _>("name"), "Alice");
        assert_eq!(row0.get::<Option<i64>, _>("age"), Some(30));
        assert!(row0.get::<bool, _>("active"));
        assert_eq!(row0.get::<f64, _>("score"), 98.5);
        assert_eq!(
            row0.get::<wkt::Timestamp, _>("created_ts"),
            wkt::Timestamp::new(1_600_000_000, 0).unwrap()
        );

        let row1 = Row::try_new_from_arrow(&batch, 1, &schema)?;
        assert_eq!(row1.get::<String, _>("name"), "Bob");
        assert_eq!(row1.get::<Option<i64>, _>("age"), None);
        assert!(!row1.get::<bool, _>("active"));
        assert_eq!(row1.get::<f64, _>("score"), 87.25);
        assert_eq!(
            row1.get::<wkt::Timestamp, _>("created_ts"),
            wkt::Timestamp::new(1_700_000_000, 0).unwrap()
        );

        Ok(())
    }

    #[test]
    fn try_new_from_arrow_interval() -> TestResult {
        use crate::datatypes::Interval;
        use arrow::array::IntervalMonthDayNanoArray;
        use arrow::datatypes::{DataType, Field, IntervalUnit, Schema as ArrowSchema};

        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "duration",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        )]));

        let intervals = IntervalMonthDayNanoArray::from(vec![
            arrow::datatypes::IntervalMonthDayNanoType::make_value(
                14,
                3,
                (4 * 3600 + 5 * 60 + 6) * 1_000_000_000 + 789_123_456,
            ),
            arrow::datatypes::IntervalMonthDayNanoType::make_value(
                -14,
                -3,
                -((4 * 3600 + 5 * 60 + 6) * 1_000_000_000 + 123_000_000),
            ),
            arrow::datatypes::IntervalMonthDayNanoType::make_value(i32::MIN, 0, i64::MIN),
        ]);

        let batch = Arc::new(RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(intervals)],
        )?);

        let table_schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("duration")
            .set_type("INTERVAL")]);
        let schema = Arc::new(Schema::new(table_schema));

        let row0 = Row::try_new_from_arrow(&batch, 0, &schema)?;
        let int0: Interval = row0.get("duration");
        assert_eq!(
            int0,
            Interval {
                years: 1,
                months: 2,
                days: 3,
                hours: 4,
                minutes: 5,
                seconds: 6,
                nanos: 789_123_456,
            }
        );

        let row1 = Row::try_new_from_arrow(&batch, 1, &schema)?;
        let int1: Interval = row1.get("duration");
        assert_eq!(
            int1,
            Interval {
                years: -1,
                months: -2,
                days: -3,
                hours: -4,
                minutes: -5,
                seconds: -6,
                nanos: -123_000_000,
            }
        );

        // Verifies no overflow on i32::MIN and i64::MIN
        let row2 = Row::try_new_from_arrow(&batch, 2, &schema)?;
        let int2: Interval = row2.get("duration");
        assert_eq!(int2.years, -178956970);
        assert_eq!(int2.months, -8);
        assert_eq!(int2.days, 0);
        assert_eq!(int2.hours, -2562047);
        assert_eq!(int2.minutes, -47);
        assert_eq!(int2.seconds, -16);
        assert_eq!(int2.nanos, -854_775_808);

        Ok(())
    }
}
