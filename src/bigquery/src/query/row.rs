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
use crate::query::from_sql::SqlValueInner;
use crate::query::{FromSql, Schema};
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
/// using [`get()`](Row::get) or [`take()`](Row::take):
///
/// ```
/// # use google_cloud_bigquery::query::Row;
/// # fn sample(row: Row) -> anyhow::Result<()> {
/// let name: String = row.get("name")?;
/// let age: i64 = row.get(1)?;
/// println!("{name} is {age} years old");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Row {
    pub(crate) values: Vec<SqlValueInner>,
    pub(crate) schema: Arc<Schema>,
}

mod sealed {
    use super::{Row, SqlValueInner};
    use crate::error::ConvertError;
    use crate::query::SqlValue;

    /// A sealed trait to prevent external implementation of `ColumnIndex`.
    pub trait ColumnIndex {
        /// Returns the index of the column in the given row, if it exists.
        fn index(&self, row: &Row) -> Option<usize>;

        /// Takes a value by column index or field name from `SqlValue`.
        fn take_sql_value(
            &self,
            value: &mut SqlValue,
        ) -> std::result::Result<SqlValue, ConvertError>;
    }

    impl ColumnIndex for usize {
        fn index(&self, row: &Row) -> Option<usize> {
            row.schema.get_field_by_index(*self).map(|_| *self)
        }

        fn take_sql_value(
            &self,
            value: &mut SqlValue,
        ) -> std::result::Result<SqlValue, ConvertError> {
            match &mut value.inner {
                SqlValueInner::Struct(entries) => {
                    let (_, slot) = entries
                        .get_mut(*self)
                        .ok_or_else(|| ConvertError::MissingField(self.to_string()))?;
                    Ok(SqlValue::from_inner(std::mem::replace(
                        slot,
                        SqlValueInner::Null,
                    )))
                }
                SqlValueInner::Array(arr) => {
                    let slot = arr
                        .get_mut(*self)
                        .ok_or_else(|| ConvertError::MissingField(self.to_string()))?;
                    Ok(SqlValue::from_inner(std::mem::replace(
                        slot,
                        SqlValueInner::Null,
                    )))
                }
                SqlValueInner::String(s) => {
                    let arr: Vec<wkt::Value> =
                        serde_json::from_str(s).map_err(|e| ConvertError::Convert(Box::new(e)))?;
                    value.inner = SqlValueInner::from_wkt(wkt::Value::Array(arr));
                    self.take_sql_value(value)
                }
                SqlValueInner::Null => Err(ConvertError::NotNull),
                other => Err(ConvertError::type_mismatch(
                    "struct, array, or string",
                    other,
                )),
            }
        }
    }

    impl ColumnIndex for &str {
        fn index(&self, row: &Row) -> Option<usize> {
            row.schema.get_field_index_by_name(self)
        }

        fn take_sql_value(
            &self,
            value: &mut SqlValue,
        ) -> std::result::Result<SqlValue, ConvertError> {
            match &mut value.inner {
                SqlValueInner::Struct(entries) => {
                    let (_, slot) = entries
                        .iter_mut()
                        .find(|(name, _)| name == *self)
                        .ok_or_else(|| ConvertError::MissingField((*self).to_string()))?;
                    Ok(SqlValue::from_inner(std::mem::replace(
                        slot,
                        SqlValueInner::Null,
                    )))
                }
                SqlValueInner::String(s) => {
                    let obj: wkt::Struct =
                        serde_json::from_str(s).map_err(|e| ConvertError::Convert(Box::new(e)))?;
                    value.inner = SqlValueInner::from_wkt(wkt::Value::Object(obj));
                    self.take_sql_value(value)
                }
                SqlValueInner::Null => Err(ConvertError::NotNull),
                other => Err(ConvertError::type_mismatch("object or string", other)),
            }
        }
    }

    impl ColumnIndex for String {
        fn index(&self, row: &Row) -> Option<usize> {
            <&str as ColumnIndex>::index(&self.as_str(), row)
        }

        fn take_sql_value(
            &self,
            value: &mut SqlValue,
        ) -> std::result::Result<SqlValue, ConvertError> {
            self.as_str().take_sql_value(value)
        }
    }
}

/// A trait for types that can be used to index into a [`Row`] or [`SqlValue`](crate::query::SqlValue).
///
/// This trait is sealed and cannot be implemented for types outside of this crate.
pub trait ColumnIndex: sealed::ColumnIndex + std::fmt::Display {}

impl ColumnIndex for usize {}
impl ColumnIndex for &str {}
impl ColumnIndex for String {}

impl Row {
    pub(crate) fn try_new(row: Struct, schema: &Arc<Schema>) -> Result<Self> {
        let values = convert_row(row, schema.fields())?;

        Ok(Self {
            values,
            schema: schema.clone(),
        })
    }

    fn resolve_index<I: ColumnIndex>(&self, col: &I) -> Result<usize> {
        sealed::ColumnIndex::index(col, self)
            .ok_or_else(|| RowError::ColumnNotFound(format!("{col}")))
    }

    fn convert_value_at<T: FromSql>(&self, idx: usize, val: SqlValueInner) -> Result<T> {
        T::from_value(crate::query::SqlValue::from_inner(val)).map_err(|e| {
            let (column, sql_type) = self
                .schema
                .get_field_by_index(idx)
                .map(|f| (f.name.clone(), f.r#type.clone()))
                .unwrap_or_else(|| (idx.to_string(), "UNKNOWN".to_string()));
            RowError::TypeConversion {
                column,
                sql_type,
                source: e,
            }
        })
    }

    /// Retrieves a value from the row by column name or zero-based index.
    ///
    /// The return type must implement [`FromSql`](crate::query::FromSql).
    ///
    /// The cell value is cloned from the row data without modifying `self`. If
    /// you want to take ownership and avoid cloning large values, see
    /// [`take()`](Row::take).
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::query::Row;
    /// # fn sample(row: Row) -> anyhow::Result<()> {
    /// let msg: String = row.get("msg")?;
    /// println!("Value: {msg}");
    /// # Ok(())
    /// # }
    /// ```
    pub fn get<T: FromSql, I: ColumnIndex>(&self, index: I) -> Result<T> {
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

    /// Takes ownership of a value from the row by column name or zero-based
    /// index.
    ///
    /// This method mutates `self` by extracting the cell value in-place to
    /// avoid cloning. The extracted cell in the row is replaced with `NULL`.
    /// Subsequent attempts to read or take the column will treat it as `NULL`
    /// (returning `Ok(None)` when reading into `Option<T>`, or returning a
    /// type conversion error for non-nullable types).
    ///
    /// <div class="warning">
    ///
    /// `take()` removes the value from `self` before converting it to `T`. If
    /// type conversion fails and returns an error, the original value has
    /// already been consumed and cannot be recovered from the row.
    ///
    /// </div>
    ///
    /// If you are not sure of the column's type or need to read the value
    /// multiple times, use [`get()`](Row::get) instead of `take()`. Use `take()`
    /// when you are confident of the type and want to avoid cloning large values
    /// (such as strings, byte buffers, or nested records).
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::query::Row;
    /// # fn sample(mut row: Row) -> anyhow::Result<()> {
    /// let text: String = row.take("big_text")?;
    /// println!("Length: {}", text.len());
    ///
    /// // Subsequent reads treat the column as NULL:
    /// assert_eq!(row.get::<Option<String>, _>("big_text")?, None);
    ///
    /// // Attempting to read or take again as a non-nullable type fails:
    /// assert!(row.take::<String, _>("big_text").is_err());
    /// # Ok(())
    /// # }
    /// ```
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
        let owned_val = std::mem::replace(val, SqlValueInner::Null);
        self.convert_value_at(idx, owned_val)
    }
}

fn convert_row(row: Struct, fields: &[TableFieldSchema]) -> Result<Vec<SqlValueInner>> {
    let field_list = get_field_list(row)?;

    if field_list.len() != fields.len() {
        return Err(RowError::InvalidRowFormat(format!(
            "schema and row cell mismatch (expected {}, got {})",
            fields.len(),
            field_list.len()
        )));
    }

    field_list
        .into_iter()
        .zip(fields)
        .map(|(cell, field)| convert_value(get_field_value(cell)?, field))
        .collect()
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

fn convert_value(value: Value, field: &TableFieldSchema) -> Result<SqlValueInner> {
    match value {
        Value::Null => Ok(SqlValueInner::Null),
        Value::String(v) => convert_basic_type(v, &field.name, &field.r#type),
        Value::Object(v) => convert_nested(v, &field.fields),
        Value::Array(v) => convert_repeated(v, field),
        _ => Err(RowError::InvalidRowFormat(format!(
            "cell value is not an object: value={:?}, field_type={:?}",
            value, field.r#type
        ))),
    }
}

fn convert_repeated(value: ListValue, field: &TableFieldSchema) -> Result<SqlValueInner> {
    let arr = value
        .into_iter()
        .map(|cell| {
            // each cell contains a single entry, keyed by "v"
            let val = get_field_value(cell)?;
            convert_value(val, field)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SqlValueInner::Array(arr))
}

fn convert_nested(value: Struct, fields: &[TableFieldSchema]) -> Result<SqlValueInner> {
    let values = convert_row(value, fields)?;
    let entries = fields
        .iter()
        .zip(values)
        .map(|(field, value)| (field.name.clone(), value))
        .collect();
    Ok(SqlValueInner::Struct(entries))
}

fn convert_basic_type(value: String, field_name: &str, field_type: &str) -> Result<SqlValueInner> {
    match field_type {
        "STRING" | "BYTES" | "TIMESTAMP" | "DATE" | "TIME" | "DATETIME" | "NUMERIC"
        | "BIGNUMERIC" | "BIGINT" | "GEOGRAPHY" | "JSON" | "INTERVAL" | "RANGE" => {
            Ok(SqlValueInner::String(value))
        }
        "INTEGER" | "INT64" => {
            let num = value.parse::<i64>().map_err(|e| RowError::TypeConversion {
                column: field_name.to_string(),
                sql_type: field_type.to_string(),
                source: ConvertError::Convert(Box::new(e)),
            })?;
            Ok(SqlValueInner::Number(serde_json::Number::from(num)))
        }
        "FLOAT" | "FLOAT64" => {
            let num = value.parse::<f64>().map_err(|e| RowError::TypeConversion {
                column: field_name.to_string(),
                sql_type: field_type.to_string(),
                source: ConvertError::Convert(Box::new(e)),
            })?;
            match serde_json::Number::from_f64(num) {
                Some(n) => Ok(SqlValueInner::Number(n)),
                None => Ok(SqlValueInner::String(value)),
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
                    sql_type: field_type.to_string(),
                    source: ConvertError::Convert(
                        "provided string was not `true` or `false`".into(),
                    ),
                });
            };
            Ok(SqlValueInner::Bool(b))
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

        assert_eq!(row.get::<String, _>(0)?, "James");
        assert_eq!(row.get::<String, _>("name")?, "James");

        assert_eq!(row.get::<i32, _>(1)?, 272793);
        assert_eq!(row.get::<i32, _>("some_int")?, 272793);
        assert_eq!(row.get::<i64, _>(1)?, 272793);
        assert_eq!(row.get::<i64, _>("some_int")?, 272793);

        assert!(row.get::<bool, _>(2)?);
        assert!(row.get::<bool, _>("some_bool")?);

        assert_eq!(row.get::<Option<i64>, _>(3)?, None);
        assert_eq!(row.get::<Option<i64>, _>("some_null")?, None);

        assert_eq!(row.get::<f32, _>(4)?, 64.0);
        assert_eq!(row.get::<f32, _>("some_float")?, 64.0);
        assert_eq!(row.get::<f64, _>(4)?, 64.0);
        assert_eq!(row.get::<f64, _>("some_float")?, 64.0);

        assert_eq!(row.take::<String, _>(0)?, "James");
        assert_eq!(row.get::<Option<String>, _>(0)?, None);

        assert_eq!(row.take::<i32, _>(1)?, 272793);
        assert_eq!(row.get::<Option<i32>, _>(1)?, None);

        assert!(row.take::<bool, _>(2)?);
        assert_eq!(row.get::<Option<bool>, _>(2)?, None);

        assert_eq!(row.take::<Option<i64>, _>(3)?, None);
        assert_eq!(row.get::<Option<i64>, _>(3)?, None);

        assert_eq!(row.take::<f32, _>(4)?, 64.0);
        assert_eq!(row.get::<Option<f32>, _>(4)?, None);

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
            row.get::<Decimal, _>(0)?,
            Decimal::new().set_value("123.456")
        );
        assert_eq!(
            row.get::<Decimal, _>("price")?,
            Decimal::new().set_value("123.456")
        );

        assert_eq!(
            row.get::<Decimal, _>(1)?,
            Decimal::new().set_value("99999999999999999999.123456789")
        );
        assert_eq!(
            row.get::<Decimal, _>("big_amount")?,
            Decimal::new().set_value("99999999999999999999.123456789")
        );

        assert_eq!(
            row.get::<RustDecimal, _>(0)?,
            "123.456".parse().expect("valid decimal")
        );
        assert_eq!(
            row.get::<RustDecimal, _>("price")?,
            "123.456".parse().expect("valid decimal")
        );

        assert_eq!(
            row.get::<RustDecimal, _>(1)?,
            "99999999999999999999.123456789"
                .parse()
                .expect("valid decimal")
        );
        assert_eq!(
            row.get::<RustDecimal, _>("big_amount")?,
            "99999999999999999999.123456789"
                .parse()
                .expect("valid decimal")
        );

        assert!(row.get::<RustDecimal, _>(2).is_err());
        assert!(row.get::<RustDecimal, _>("overflow_amount").is_err());

        assert_eq!(
            row.take::<Decimal, _>(0)?,
            Decimal::new().set_value("123.456")
        );
        assert_eq!(row.get::<Option<Decimal>, _>(0)?, None);

        assert_eq!(
            row.take::<RustDecimal, _>(1)?,
            "99999999999999999999.123456789".parse()?
        );
        assert_eq!(row.get::<Option<RustDecimal>, _>(1)?, None);

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

        assert_eq!(row.get::<Vec<u8>, _>(0)?, vec![1, 2, 3, 4]);
        assert_eq!(row.get::<Vec<u8>, _>("payload_vec")?, vec![1, 2, 3, 4]);

        assert_eq!(
            row.get::<bytes::Bytes, _>(1)?,
            bytes::Bytes::from_static(b"Hello")
        );
        assert_eq!(
            row.get::<bytes::Bytes, _>("payload_bytes")?,
            bytes::Bytes::from_static(b"Hello")
        );

        assert_eq!(row.get::<Option<Vec<u8>>, _>(2)?, None);
        assert_eq!(row.get::<Option<bytes::Bytes>, _>("null_bytes")?, None);

        assert_eq!(row.take::<Vec<u8>, _>(0)?, vec![1, 2, 3, 4]);
        assert_eq!(row.get::<Option<Vec<u8>>, _>(0)?, None);

        assert_eq!(
            row.take::<bytes::Bytes, _>(1)?,
            bytes::Bytes::from_static(b"Hello")
        );
        assert_eq!(row.get::<Option<bytes::Bytes>, _>(1)?, None);

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
        assert_eq!(row.get::<Struct, _>(0)?, expected);
        assert_eq!(row.get::<Struct, _>("user")?, expected);
        assert_eq!(row.get::<Struct, _>("user".to_string())?, expected);
        assert_eq!(row.take::<Struct, _>("user")?, expected);
        assert_eq!(row.get::<Option<Struct>, _>("user")?, None);

        Ok(())
    }

    #[derive(crate::query::FromSql, Debug, PartialEq)]
    struct JsonPayload {
        name: String,
        age: i64,
    }

    #[tokio::test]
    async fn convert_json_from_row() -> TestResult {
        let json_str = json!({"name": "Alice", "age": 30}).to_string();
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": json_str },
                { "v": null },
            ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("json_obj")
                .set_type("JSON")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("json_null")
                .set_type("JSON")
                .set_mode("NULLABLE"),
        ]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let expected_struct: Struct = serde_json::from_value(json!({
            "name": "Alice",
            "age": 30,
        }))?;
        assert_eq!(row.get::<String, _>("json_obj")?, json_str);
        assert_eq!(row.get::<Struct, _>("json_obj")?, expected_struct);
        assert_eq!(
            row.get::<JsonPayload, _>("json_obj")?,
            JsonPayload {
                name: "Alice".to_string(),
                age: 30,
            }
        );
        assert_eq!(row.get::<Option<Struct>, _>("json_null")?, None);
        assert_eq!(row.get::<Option<JsonPayload>, _>("json_null")?, None);

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

        assert_eq!(row.get::<Vec<i64>, _>(0)?, vec![1, 2, 3]);
        assert_eq!(row.get::<Vec<i64>, _>("numbers")?, vec![1, 2, 3]);
        assert_eq!(row.take::<Vec<i64>, _>("numbers")?, vec![1, 2, 3]);
        assert_eq!(row.get::<Option<Vec<i64>>, _>("numbers")?, None);

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
        assert_eq!(row.get::<Vec<Struct>, _>(0)?, expected);
        assert_eq!(row.get::<Vec<Struct>, _>("users")?, expected);
        assert_eq!(row.take::<Vec<Struct>, _>("users")?, expected);
        assert_eq!(row.get::<Option<Vec<Struct>>, _>("users")?, None);

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
    #[test_case("JSON", r#"{"a":1}"#, Value::String(r#"{"a":1}"#.to_string()); "json string")]
    fn convert_basic_type_cases_success(field_type: &str, value: &str, expected: Value) {
        let res = convert_basic_type(value.to_string(), "test_col", field_type);
        let value = res.expect("should succeed");
        assert_eq!(
            value,
            crate::query::from_sql::SqlValueInner::from_wkt(expected)
        );
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

    #[derive(FromRow, Debug, PartialEq)]
    struct RawIdentRow {
        r#type: String,
        r#match: i64,
    }

    #[tokio::test]
    async fn derive_from_row_raw_identifier() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": "click" },
                { "v": "7" },
            ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("type")
                .set_type("STRING")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("match")
                .set_type("INTEGER")
                .set_mode("NULLABLE"),
        ]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let converted = RawIdentRow::try_from(row)?;
        assert_eq!(
            converted,
            RawIdentRow {
                r#type: "click".to_string(),
                r#match: 7,
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

    #[derive(FromRow, Debug, PartialEq)]
    struct ShadowedFieldNamesRow {
        row: i64,
        name: String,
    }

    #[tokio::test]
    async fn derive_from_row_shadowing_field_names() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": "42" },
                { "v": "Alice" },
            ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("row")
                .set_type("INTEGER")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("name")
                .set_type("STRING")
                .set_mode("NULLABLE"),
        ]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let converted = ShadowedFieldNamesRow::try_from(row)?;
        assert_eq!(
            converted,
            ShadowedFieldNamesRow {
                row: 42,
                name: "Alice".to_string(),
            }
        );
        Ok(())
    }

    #[derive(FromRow, Debug, PartialEq)]
    struct TupleRow(i64, String);

    #[derive(crate::query::FromSql, Debug, PartialEq)]
    struct AnonTriple(i64, String, bool);

    #[derive(crate::query::FromSql, Debug, PartialEq)]
    struct NamedZThenA {
        z: i64,
        a: i64,
    }

    #[derive(crate::query::FromSql, Debug, PartialEq)]
    struct PositionalPair(i64, i64);

    #[derive(crate::query::FromSql, Debug, PartialEq)]
    struct DupIdNamed {
        id: i64,
    }

    #[tokio::test]
    async fn anonymous_struct_preserves_all_fields() -> TestResult {
        // Simulates `SELECT STRUCT(10, 'hello', true) AS anon`, where BigQuery
        // sets `field.name = ""` for all three subfields.
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                {
                    "v": {
                        "f": [
                            { "v": "10" },
                            { "v": "hello" },
                            { "v": "true" }
                        ]
                    }
                }
            ]),
        )]);
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("anon")
            .set_type("RECORD")
            .set_mode("NULLABLE")
            .set_fields([
                TableFieldSchema::new()
                    .set_name("")
                    .set_type("INT64")
                    .set_mode("NULLABLE"),
                TableFieldSchema::new()
                    .set_name("")
                    .set_type("STRING")
                    .set_mode("NULLABLE"),
                TableFieldSchema::new()
                    .set_name("")
                    .set_type("BOOL")
                    .set_mode("NULLABLE"),
            ])]);
        let schema = Arc::new(Schema::new(schema));
        let mut row = Row::try_new(raw_row, &schema)?;

        let anon: AnonTriple = row.take("anon")?;
        assert_eq!(anon, AnonTriple(10, "hello".to_string(), true));
        Ok(())
    }

    #[tokio::test]
    async fn struct_preserves_sql_declaration_order_over_alphabetical_order() -> TestResult {
        // Simulates `SELECT STRUCT(1 AS z, 2 AS a) AS pair`.
        // A BTreeMap would sort `"a"` before `"z"`, scrambling positional order.
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                {
                    "v": {
                        "f": [
                            { "v": "1" },
                            { "v": "2" }
                        ]
                    }
                }
            ]),
        )]);
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("pair")
            .set_type("RECORD")
            .set_mode("NULLABLE")
            .set_fields([
                TableFieldSchema::new()
                    .set_name("z")
                    .set_type("INT64")
                    .set_mode("NULLABLE"),
                TableFieldSchema::new()
                    .set_name("a")
                    .set_type("INT64")
                    .set_mode("NULLABLE"),
            ])]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        // Named extraction gets z=1, a=2
        let by_name: NamedZThenA = row.get("pair")?;
        assert_eq!(by_name, NamedZThenA { z: 1, a: 2 });

        // Positional extraction on the same struct gets (1, 2) in SQL order (z then a)
        let by_pos: PositionalPair = row.get("pair")?;
        assert_eq!(by_pos, PositionalPair(1, 2));
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_struct_field_names_match_row_behavior() -> TestResult {
        // Simulates `SELECT STRUCT(100 AS id, 200 AS id) AS dup`.
        // Name lookup must return the first `"id"` (100), while positional lookup
        // can access both `(100, 200)`.
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                {
                    "v": {
                        "f": [
                            { "v": "100" },
                            { "v": "200" }
                        ]
                    }
                }
            ]),
        )]);
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("dup")
            .set_type("RECORD")
            .set_mode("NULLABLE")
            .set_fields([
                TableFieldSchema::new()
                    .set_name("id")
                    .set_type("INT64")
                    .set_mode("NULLABLE"),
                TableFieldSchema::new()
                    .set_name("id")
                    .set_type("INT64")
                    .set_mode("NULLABLE"),
            ])]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let first_id: DupIdNamed = row.get("dup")?;
        assert_eq!(first_id, DupIdNamed { id: 100 });

        let both_ids: PositionalPair = row.get("dup")?;
        assert_eq!(both_ids, PositionalPair(100, 200));
        Ok(())
    }

    #[tokio::test]
    async fn derive_from_row_tuple_struct() -> TestResult {
        // Simulates `SELECT 42, 'world'` where top-level columns have generated names.
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
            { "v": "42" },
            { "v": "world" },
              ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("_f0")
                .set_type("INT64")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("_f1")
                .set_type("STRING")
                .set_mode("NULLABLE"),
        ]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let converted = TupleRow::try_from(row)?;
        assert_eq!(converted, TupleRow(42, "world".to_string()));

        // here
        Ok(())
    }

    #[derive(FromSql, Debug, PartialEq)]
    struct NestedGeneric<U> {
        inner_val: U,
    }

    #[derive(FromRow, Debug, PartialEq)]
    struct GenericRow<T: Clone + Default, U: std::fmt::Debug> {
        #[bigquery(rename = "custom_val")]
        single: T,
        optional: Option<T>,
        list: Vec<T>,
        nested: NestedGeneric<U>,
        common: i64,
    }

    #[tokio::test]
    async fn derive_from_row_generic() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": "100" },
                { "v": null },
                { "v": [{ "v": "1" }, { "v": "2" }, { "v": "3" }] },
                {
                    "v": {
                        "f": [
                            { "v": "nested_value" }
                        ]
                    }
                },
                { "v": "1" },
            ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("custom_val")
                .set_type("INTEGER")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("optional")
                .set_type("INTEGER")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("list")
                .set_type("INTEGER")
                .set_mode("REPEATED"),
            TableFieldSchema::new()
                .set_name("nested")
                .set_type("RECORD")
                .set_mode("NULLABLE")
                .set_fields([TableFieldSchema::new()
                    .set_name("inner_val")
                    .set_type("STRING")
                    .set_mode("NULLABLE")]),
            TableFieldSchema::new()
                .set_name("common")
                .set_type("INTEGER")
                .set_mode("NULLABLE"),
        ]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let converted = GenericRow::<i64, String>::try_from(row)?;
        assert_eq!(
            converted,
            GenericRow {
                single: 100,
                optional: None,
                list: vec![1, 2, 3],
                nested: NestedGeneric {
                    inner_val: "nested_value".to_string(),
                },
                common: 1,
            }
        );
        Ok(())
    }

    #[derive(FromRow, Debug, PartialEq)]
    struct GenericRowWhere<T>
    where
        T: std::fmt::Debug + Clone,
    {
        val: T,
    }

    #[tokio::test]
    async fn derive_from_row_generic_where_clause() -> TestResult {
        let raw_row = Map::from_iter([("f".to_string(), json!([{ "v": "hello" }]))]);
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("val")
            .set_type("STRING")
            .set_mode("NULLABLE")]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let converted = GenericRowWhere::<String>::try_from(row)?;
        assert_eq!(
            converted,
            GenericRowWhere {
                val: "hello".to_string(),
            }
        );
        Ok(())
    }

    #[derive(FromRow, Debug, PartialEq)]
    struct GenericRowDefault<T = i64> {
        val: T,
    }

    #[tokio::test]
    async fn derive_from_row_generic_default_param() -> TestResult {
        let raw_row = Map::from_iter([("f".to_string(), json!([{ "v": "42" }]))]);
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("val")
            .set_type("INTEGER")
            .set_mode("NULLABLE")]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let converted = GenericRowDefault::try_from(row)?;
        assert_eq!(converted, GenericRowDefault { val: 42 });
        Ok(())
    }

    #[derive(FromRow, Debug, PartialEq)]
    struct GenericTupleRow<T, U>(T, Option<T>, U);

    #[tokio::test]
    async fn derive_from_row_generic_tuple_struct() -> TestResult {
        let raw_row = Map::from_iter([(
            "f".to_string(),
            json!([
                { "v": "42" },
                { "v": null },
                { "v": "hello" },
            ]),
        )]);
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("_f0")
                .set_type("INT64")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("_f1")
                .set_type("INT64")
                .set_mode("NULLABLE"),
            TableFieldSchema::new()
                .set_name("_f2")
                .set_type("STRING")
                .set_mode("NULLABLE"),
        ]);
        let schema = Arc::new(Schema::new(schema));
        let row = Row::try_new(raw_row, &schema)?;

        let converted = GenericTupleRow::<i64, String>::try_from(row)?;
        assert_eq!(converted, GenericTupleRow(42, None, "hello".to_string()));
        Ok(())
    }
}
