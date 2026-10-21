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
#[cfg(any(
    google_cloud_unstable_bigquery_arrow,
    google_cloud_unstable_bigquery_storage_read
))]
use crate::query::from_sql::ArrowCell;
use crate::query::from_sql::SqlValueInner;
use crate::query::{FromSql, Schema};
#[cfg(any(
    google_cloud_unstable_bigquery_arrow,
    google_cloud_unstable_bigquery_storage_read
))]
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
    pub(crate) inner: RowInner,
    pub(crate) schema: Arc<Schema>,
}

#[derive(Clone, Debug)]
pub(crate) enum RowInner {
    Json(Vec<SqlValueInner>),
    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    #[cfg_attr(not(test), allow(dead_code))]
    Arrow {
        batch: Arc<RecordBatch>,
        row_idx: usize,
    },
}

mod sealed {
    use super::{Row, SqlValueInner};
    use crate::error::ConvertError;
    use crate::query::SqlValue;

    /// A sealed trait to prevent external implementation of `ColumnIndex`.
    pub trait ColumnIndex {
        /// Returns the index of the column in the given row, if it exists.
        fn index(&self, row: &Row) -> Option<usize>;

        /// Returns the index of the field in the given Arrow `StructArray`, if it exists.
        #[cfg(any(
            google_cloud_unstable_bigquery_arrow,
            google_cloud_unstable_bigquery_storage_read
        ))]
        fn arrow_index(&self, struct_arr: &arrow::array::StructArray) -> Option<usize>;

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

        #[cfg(any(
            google_cloud_unstable_bigquery_arrow,
            google_cloud_unstable_bigquery_storage_read
        ))]
        fn arrow_index(&self, struct_arr: &arrow::array::StructArray) -> Option<usize> {
            (*self < struct_arr.num_columns()).then_some(*self)
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
                #[cfg(any(
                    google_cloud_unstable_bigquery_arrow,
                    google_cloud_unstable_bigquery_storage_read
                ))]
                SqlValueInner::Arrow(cell) => match cell.data_type() {
                    arrow::datatypes::DataType::Struct(_) => cell
                        .struct_field_cell(self)
                        .map(|val| SqlValue::from_inner(SqlValueInner::Arrow(val))),
                    arrow::datatypes::DataType::List(_)
                    | arrow::datatypes::DataType::LargeList(_) => cell
                        .list_element_cell(*self)
                        .map(|val| SqlValue::from_inner(SqlValueInner::Arrow(val))),
                    arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                        let s = cell.as_str()?.to_string();
                        value.inner = SqlValueInner::String(s);
                        self.take_sql_value(value)
                    }
                    _ => Err(ConvertError::TypeMismatch {
                        expected: "struct, array, or string".to_string(),
                        got: cell.data_type_str(),
                    }),
                },
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

        #[cfg(any(
            google_cloud_unstable_bigquery_arrow,
            google_cloud_unstable_bigquery_storage_read
        ))]
        fn arrow_index(&self, struct_arr: &arrow::array::StructArray) -> Option<usize> {
            struct_arr.fields().iter().position(|f| f.name() == *self)
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
                #[cfg(any(
                    google_cloud_unstable_bigquery_arrow,
                    google_cloud_unstable_bigquery_storage_read
                ))]
                SqlValueInner::Arrow(cell) => match cell.data_type() {
                    arrow::datatypes::DataType::Struct(_) => cell
                        .struct_field_cell(self)
                        .map(|val| SqlValue::from_inner(SqlValueInner::Arrow(val))),
                    arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                        let s = cell.as_str()?.to_string();
                        value.inner = SqlValueInner::String(s);
                        self.take_sql_value(value)
                    }
                    _ => Err(ConvertError::TypeMismatch {
                        expected: "object or string".to_string(),
                        got: cell.data_type_str(),
                    }),
                },
                other => Err(ConvertError::type_mismatch("object or string", other)),
            }
        }
    }

    impl ColumnIndex for String {
        fn index(&self, row: &Row) -> Option<usize> {
            <&str as ColumnIndex>::index(&self.as_str(), row)
        }

        #[cfg(any(
            google_cloud_unstable_bigquery_arrow,
            google_cloud_unstable_bigquery_storage_read
        ))]
        fn arrow_index(&self, struct_arr: &arrow::array::StructArray) -> Option<usize> {
            self.as_str().arrow_index(struct_arr)
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
            inner: RowInner::Json(values),
            schema: schema.clone(),
        })
    }

    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    #[cfg_attr(not(test), allow(dead_code))]
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
        if row_idx >= batch.num_rows() {
            return Err(RowError::InvalidRowFormat(format!(
                "row index out of bounds (row_idx {}, num_rows {})",
                row_idx,
                batch.num_rows()
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
    pub fn get<T: FromSql>(&self, index: impl ColumnIndex) -> Result<T> {
        let idx = self.resolve_index(&index)?;
        match &self.inner {
            RowInner::Json(values) => {
                let val = values.get(idx).ok_or_else(|| RowError::IndexOutOfRange {
                    index: idx,
                    len: self.schema.len(),
                })?;
                self.convert_value_at(idx, val.clone())
            }
            #[cfg(any(
                google_cloud_unstable_bigquery_arrow,
                google_cloud_unstable_bigquery_storage_read
            ))]
            RowInner::Arrow { batch, row_idx } => {
                let col = batch
                    .columns()
                    .get(idx)
                    .ok_or_else(|| RowError::IndexOutOfRange {
                        index: idx,
                        len: self.schema.len(),
                    })?;
                let val = SqlValueInner::Arrow(ArrowCell::new(col.clone(), *row_idx));
                self.convert_value_at(idx, val)
            }
        }
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
    /// assert_eq!(row.get::<Option<String>>("big_text")?, None);
    ///
    /// // Attempting to read or take again as a non-nullable type fails:
    /// assert!(row.take::<String>("big_text").is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub fn take<T: FromSql>(&mut self, index: impl ColumnIndex) -> Result<T> {
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
                let owned_val = std::mem::replace(val, SqlValueInner::Null);
                self.convert_value_at(idx, owned_val)
            }
            #[cfg(any(
                google_cloud_unstable_bigquery_arrow,
                google_cloud_unstable_bigquery_storage_read
            ))]
            RowInner::Arrow { batch, row_idx } => {
                let col = batch
                    .columns()
                    .get(idx)
                    .ok_or_else(|| RowError::IndexOutOfRange {
                        index: idx,
                        len: self.schema.len(),
                    })?;
                let val = SqlValueInner::Arrow(ArrowCell::new(col.clone(), *row_idx));
                self.convert_value_at(idx, val)
            }
        }
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

        assert_eq!(row.get::<String>(0)?, "James");
        assert_eq!(row.get::<String>("name")?, "James");

        assert_eq!(row.get::<i32>(1)?, 272793);
        assert_eq!(row.get::<i32>("some_int")?, 272793);
        assert_eq!(row.get::<i64>(1)?, 272793);
        assert_eq!(row.get::<i64>("some_int")?, 272793);

        assert!(row.get::<bool>(2)?);
        assert!(row.get::<bool>("some_bool")?);

        assert_eq!(row.get::<Option<i64>>(3)?, None);
        assert_eq!(row.get::<Option<i64>>("some_null")?, None);

        assert_eq!(row.get::<f32>(4)?, 64.0);
        assert_eq!(row.get::<f32>("some_float")?, 64.0);
        assert_eq!(row.get::<f64>(4)?, 64.0);
        assert_eq!(row.get::<f64>("some_float")?, 64.0);

        assert_eq!(row.take::<String>(0)?, "James");
        assert_eq!(row.get::<Option<String>>(0)?, None);

        assert_eq!(row.take::<i32>(1)?, 272793);
        assert_eq!(row.get::<Option<i32>>(1)?, None);

        assert!(row.take::<bool>(2)?);
        assert_eq!(row.get::<Option<bool>>(2)?, None);

        assert_eq!(row.take::<Option<i64>>(3)?, None);
        assert_eq!(row.get::<Option<i64>>(3)?, None);

        assert_eq!(row.take::<f32>(4)?, 64.0);
        assert_eq!(row.get::<Option<f32>>(4)?, None);

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

        assert_eq!(row.get::<Decimal>(0)?, Decimal::new().set_value("123.456"));
        assert_eq!(
            row.get::<Decimal>("price")?,
            Decimal::new().set_value("123.456")
        );

        assert_eq!(
            row.get::<Decimal>(1)?,
            Decimal::new().set_value("99999999999999999999.123456789")
        );
        assert_eq!(
            row.get::<Decimal>("big_amount")?,
            Decimal::new().set_value("99999999999999999999.123456789")
        );

        assert_eq!(
            row.get::<RustDecimal>(0)?,
            "123.456".parse().expect("valid decimal")
        );
        assert_eq!(
            row.get::<RustDecimal>("price")?,
            "123.456".parse().expect("valid decimal")
        );

        assert_eq!(
            row.get::<RustDecimal>(1)?,
            "99999999999999999999.123456789"
                .parse()
                .expect("valid decimal")
        );
        assert_eq!(
            row.get::<RustDecimal>("big_amount")?,
            "99999999999999999999.123456789"
                .parse()
                .expect("valid decimal")
        );

        assert!(row.get::<RustDecimal>(2).is_err());
        assert!(row.get::<RustDecimal>("overflow_amount").is_err());

        assert_eq!(row.take::<Decimal>(0)?, Decimal::new().set_value("123.456"));
        assert_eq!(row.get::<Option<Decimal>>(0)?, None);

        assert_eq!(
            row.take::<RustDecimal>(1)?,
            "99999999999999999999.123456789".parse()?
        );
        assert_eq!(row.get::<Option<RustDecimal>>(1)?, None);

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

        assert_eq!(row.get::<Vec<u8>>(0)?, vec![1, 2, 3, 4]);
        assert_eq!(row.get::<Vec<u8>>("payload_vec")?, vec![1, 2, 3, 4]);

        assert_eq!(
            row.get::<bytes::Bytes>(1)?,
            bytes::Bytes::from_static(b"Hello")
        );
        assert_eq!(
            row.get::<bytes::Bytes>("payload_bytes")?,
            bytes::Bytes::from_static(b"Hello")
        );

        assert_eq!(row.get::<Option<Vec<u8>>>(2)?, None);
        assert_eq!(row.get::<Option<bytes::Bytes>>("null_bytes")?, None);

        assert_eq!(row.take::<Vec<u8>>(0)?, vec![1, 2, 3, 4]);
        assert_eq!(row.get::<Option<Vec<u8>>>(0)?, None);

        assert_eq!(
            row.take::<bytes::Bytes>(1)?,
            bytes::Bytes::from_static(b"Hello")
        );
        assert_eq!(row.get::<Option<bytes::Bytes>>(1)?, None);

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
        assert_eq!(row.get::<Struct>(0)?, expected);
        assert_eq!(row.get::<Struct>("user")?, expected);
        assert_eq!(row.get::<Struct>("user".to_string())?, expected);
        assert_eq!(row.take::<Struct>("user")?, expected);
        assert_eq!(row.get::<Option<Struct>>("user")?, None);

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
        assert_eq!(row.get::<String>("json_obj")?, json_str);
        assert_eq!(row.get::<Struct>("json_obj")?, expected_struct);
        assert_eq!(
            row.get::<JsonPayload>("json_obj")?,
            JsonPayload {
                name: "Alice".to_string(),
                age: 30,
            }
        );
        assert_eq!(row.get::<Option<Struct>>("json_null")?, None);
        assert_eq!(row.get::<Option<JsonPayload>>("json_null")?, None);

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

        assert_eq!(row.get::<Vec<i64>>(0)?, vec![1, 2, 3]);
        assert_eq!(row.get::<Vec<i64>>("numbers")?, vec![1, 2, 3]);
        assert_eq!(row.take::<Vec<i64>>("numbers")?, vec![1, 2, 3]);
        assert_eq!(row.get::<Option<Vec<i64>>>("numbers")?, None);

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
        assert_eq!(row.get::<Vec<Struct>>(0)?, expected);
        assert_eq!(row.get::<Vec<Struct>>("users")?, expected);
        assert_eq!(row.take::<Vec<Struct>>("users")?, expected);
        assert_eq!(row.get::<Option<Vec<Struct>>>("users")?, None);

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

    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    mod arrow_tests {
        use super::*;
        use crate::query::SqlValue;
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        #[derive(Debug, PartialEq)]
        struct ArrowI64(i64);

        impl FromSql for ArrowI64 {
            fn from_value(value: SqlValue) -> std::result::Result<Self, ConvertError> {
                match value.inner {
                    SqlValueInner::Arrow(cell) => cell.as_i64().map(Self),
                    other => Err(ConvertError::type_mismatch("Int64Array", &other)),
                }
            }
        }

        #[derive(Debug, PartialEq)]
        struct ArrowString(String);

        impl FromSql for ArrowString {
            fn from_value(value: SqlValue) -> std::result::Result<Self, ConvertError> {
                match value.inner {
                    SqlValueInner::Arrow(cell) => cell.as_str().map(|s| Self(s.to_owned())),
                    other => Err(ConvertError::type_mismatch("StringArray", &other)),
                }
            }
        }

        #[derive(FromRow, Debug, PartialEq)]
        struct ArrowUserRow {
            name: ArrowString,
            age: ArrowI64,
        }

        fn sample_batch_and_schema() -> anyhow::Result<(Arc<RecordBatch>, Arc<Schema>)> {
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int64, true),
            ]));
            let batch = Arc::new(RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                    Arc::new(Int64Array::from(vec![Some(30), None])),
                ],
            )?);
            let schema = Arc::new(Schema::from_arrow_schema(&arrow_schema));
            Ok((batch, schema))
        }

        #[test]
        fn try_new_from_arrow_validation() -> TestResult {
            let (batch, schema) = sample_batch_and_schema()?;

            let mismatched_schema = Arc::new(Schema::new(
                TableSchema::new().set_fields([TableFieldSchema::new()
                    .set_name("only_one")
                    .set_type("STRING")]),
            ));
            let err = Row::try_new_from_arrow(&batch, 0, &mismatched_schema).unwrap_err();
            assert!(
                matches!(&err, RowError::InvalidRowFormat(msg) if msg.contains("expected 1, got 2")),
                "unexpected error: {err:?}"
            );

            let err = Row::try_new_from_arrow(&batch, 2, &schema).unwrap_err();
            assert!(
                matches!(&err, RowError::InvalidRowFormat(msg) if msg.contains("row_idx 2, num_rows 2")),
                "unexpected error: {err:?}"
            );

            Ok(())
        }

        #[test]
        fn arrow_row_get_take_and_from_row() -> TestResult {
            let (batch, schema) = sample_batch_and_schema()?;

            let mut row0 = Row::try_new_from_arrow(&batch, 0, &schema)?;
            assert_eq!(
                row0.get::<ArrowString>("name")?,
                ArrowString("Alice".to_string())
            );
            assert_eq!(row0.get::<ArrowI64>(1)?, ArrowI64(30));
            assert_eq!(
                row0.take::<ArrowString>(0)?,
                ArrowString("Alice".to_string())
            );
            assert_eq!(row0.take::<ArrowI64>("age")?, ArrowI64(30));

            let derived = ArrowUserRow::try_from(Row::try_new_from_arrow(&batch, 0, &schema)?)?;
            assert_eq!(
                derived,
                ArrowUserRow {
                    name: ArrowString("Alice".to_string()),
                    age: ArrowI64(30),
                }
            );

            let mut row1 = Row::try_new_from_arrow(&batch, 1, &schema)?;
            assert_eq!(
                row1.get::<ArrowString>("name")?,
                ArrowString("Bob".to_string())
            );

            let err = row1.get::<ArrowI64>("age").unwrap_err();
            assert!(
                matches!(
                    &err,
                    RowError::TypeConversion {
                        column,
                        sql_type,
                        source: ConvertError::NotNull,
                    } if column == "age" && sql_type == "INTEGER"
                ),
                "unexpected error: {err:?}"
            );

            let err = row1.take::<ArrowI64>("missing").unwrap_err();
            assert!(
                matches!(&err, RowError::ColumnNotFound(col) if col == "missing"),
                "unexpected error: {err:?}"
            );

            let err = row1.get::<ArrowI64>(99).unwrap_err();
            assert!(
                matches!(&err, RowError::ColumnNotFound(col) if col == "99"),
                "unexpected error: {err:?}"
            );

            Ok(())
        }

        #[test]
        fn try_new_from_arrow_anonymous_and_named_structs() -> TestResult {
            use arrow::array::{ArrayRef, BooleanArray, StructArray};
            use arrow::datatypes::Fields;

            // 1. Anonymous struct: STRUCT(10, 'hello', true)
            let anon_fields = Fields::from(vec![
                Field::new("", DataType::Int64, false),
                Field::new("", DataType::Utf8, false),
                Field::new("", DataType::Boolean, false),
            ]);
            let anon_arr = Arc::new(StructArray::new(
                anon_fields.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![10])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
                    Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                ],
                None,
            ));

            // 2. Named pair struct with SQL declaration order: STRUCT(1 AS z, 2 AS a)
            let pair_fields = Fields::from(vec![
                Field::new("z", DataType::Int64, false),
                Field::new("a", DataType::Int64, false),
            ]);
            let pair_arr = Arc::new(StructArray::new(
                pair_fields.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                ],
                None,
            ));

            // 3. Duplicate field names: STRUCT(100 AS id, 200 AS id)
            let dup_fields = Fields::from(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("id", DataType::Int64, false),
            ]);
            let dup_arr = Arc::new(StructArray::new(
                dup_fields.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![100])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![200])) as ArrayRef,
                ],
                None,
            ));

            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("anon", DataType::Struct(anon_fields), false),
                Field::new("pair", DataType::Struct(pair_fields), false),
                Field::new("dup", DataType::Struct(dup_fields), false),
            ]));

            let batch = Arc::new(RecordBatch::try_new(
                arrow_schema,
                vec![anon_arr, pair_arr, dup_arr],
            )?);

            let table_schema = TableSchema::new().set_fields([
                TableFieldSchema::new().set_name("anon").set_type("RECORD"),
                TableFieldSchema::new().set_name("pair").set_type("RECORD"),
                TableFieldSchema::new().set_name("dup").set_type("RECORD"),
            ]);
            let schema = Arc::new(Schema::new(table_schema));

            let mut row = Row::try_new_from_arrow(&batch, 0, &schema)?;

            // Anonymous struct extraction
            let anon: AnonTriple = row.get("anon")?;
            assert_eq!(anon, AnonTriple(10, "hello".to_string(), true));

            // Named struct extraction by name and by positional order
            let by_name: NamedZThenA = row.get("pair")?;
            assert_eq!(by_name, NamedZThenA { z: 1, a: 2 });
            let by_pos: PositionalPair = row.get("pair")?;
            assert_eq!(by_pos, PositionalPair(1, 2));

            // Duplicate fields: name gets first, positional gets both
            let first_id: DupIdNamed = row.get("dup")?;
            assert_eq!(first_id, DupIdNamed { id: 100 });
            let both_ids: PositionalPair = row.get("dup")?;
            assert_eq!(both_ids, PositionalPair(100, 200));

            // Test taking values mutably
            let anon_taken: AnonTriple = row.take("anon")?;
            assert_eq!(anon_taken, AnonTriple(10, "hello".to_string(), true));

            Ok(())
        }

        #[derive(FromRow, Debug, PartialEq)]
        struct ArrowNestedRow {
            numbers: Vec<i64>,
            strings: Vec<String>,
            pairs: Vec<PositionalPair>,
        }

        #[test]
        fn try_new_from_arrow_list_arrays() -> TestResult {
            use arrow::array::{ArrayRef, LargeListArray, ListArray, StructArray};
            use arrow::buffer::OffsetBuffer;
            use arrow::datatypes::Fields;

            // List of integers: [[1, 2, 3], [42]]
            let int_values = Arc::new(Int64Array::from(vec![1, 2, 3, 42])) as ArrayRef;
            let int_offsets = OffsetBuffer::from_lengths(vec![3, 1]);
            let int_list = Arc::new(ListArray::new(
                Arc::new(Field::new("element", DataType::Int64, false)),
                int_offsets,
                int_values,
                None,
            ));

            // LargeList of strings: [["a", "b"], ["c"]]
            let str_values = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
            let str_offsets = OffsetBuffer::from_lengths(vec![2, 1]);
            let str_large_list = Arc::new(LargeListArray::new(
                Arc::new(Field::new("element", DataType::Utf8, false)),
                str_offsets,
                str_values,
                None,
            ));

            // List of structs: [[{"z": 1, "a": 2}], [{"z": 10, "a": 20}]]
            let pair_fields = Fields::from(vec![
                Field::new("z", DataType::Int64, false),
                Field::new("a", DataType::Int64, false),
            ]);
            let pair_structs = Arc::new(StructArray::new(
                pair_fields.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 10])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![2, 20])) as ArrayRef,
                ],
                None,
            )) as ArrayRef;
            let struct_offsets = OffsetBuffer::from_lengths(vec![1, 1]);
            let struct_list = Arc::new(ListArray::new(
                Arc::new(Field::new("element", DataType::Struct(pair_fields), false)),
                struct_offsets,
                pair_structs,
                None,
            ));

            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new(
                    "numbers",
                    DataType::List(Arc::new(Field::new("element", DataType::Int64, false))),
                    false,
                ),
                Field::new(
                    "strings",
                    DataType::LargeList(Arc::new(Field::new("element", DataType::Utf8, false))),
                    false,
                ),
                Field::new(
                    "pairs",
                    DataType::List(Arc::new(Field::new(
                        "element",
                        DataType::Struct(Fields::from(vec![
                            Field::new("z", DataType::Int64, false),
                            Field::new("a", DataType::Int64, false),
                        ])),
                        false,
                    ))),
                    false,
                ),
            ]));

            let batch = Arc::new(RecordBatch::try_new(
                arrow_schema,
                vec![int_list, str_large_list, struct_list],
            )?);

            let table_schema = TableSchema::new().set_fields([
                TableFieldSchema::new()
                    .set_name("numbers")
                    .set_type("INTEGER")
                    .set_mode("REPEATED"),
                TableFieldSchema::new()
                    .set_name("strings")
                    .set_type("STRING")
                    .set_mode("REPEATED"),
                TableFieldSchema::new()
                    .set_name("pairs")
                    .set_type("RECORD")
                    .set_mode("REPEATED"),
            ]);
            let schema = Arc::new(Schema::new(table_schema));

            // Row 0
            let row0 = Row::try_new_from_arrow(&batch, 0, &schema)?;
            let numbers0: Vec<i64> = row0.get("numbers")?;
            assert_eq!(numbers0, vec![1, 2, 3]);

            let strings0: Vec<String> = row0.get("strings")?;
            assert_eq!(strings0, vec!["a".to_string(), "b".to_string()]);

            let pairs0_named: Vec<NamedZThenA> = row0.get("pairs")?;
            assert_eq!(pairs0_named, vec![NamedZThenA { z: 1, a: 2 }]);

            let pairs0_pos: Vec<PositionalPair> = row0.get("pairs")?;
            assert_eq!(pairs0_pos, vec![PositionalPair(1, 2)]);

            let row0_derived = ArrowNestedRow::try_from(row0)?;
            assert_eq!(
                row0_derived,
                ArrowNestedRow {
                    numbers: vec![1, 2, 3],
                    strings: vec!["a".to_string(), "b".to_string()],
                    pairs: vec![PositionalPair(1, 2)],
                }
            );

            // Row 1
            let row1 = Row::try_new_from_arrow(&batch, 1, &schema)?;
            let numbers1: Vec<i64> = row1.get("numbers")?;
            assert_eq!(numbers1, vec![42]);

            let strings1: Vec<String> = row1.get("strings")?;
            assert_eq!(strings1, vec!["c".to_string()]);

            let pairs1_pos: Vec<PositionalPair> = row1.get("pairs")?;
            assert_eq!(pairs1_pos, vec![PositionalPair(10, 20)]);

            let row1_derived = ArrowNestedRow::try_from(row1)?;
            assert_eq!(
                row1_derived,
                ArrowNestedRow {
                    numbers: vec![42],
                    strings: vec!["c".to_string()],
                    pairs: vec![PositionalPair(10, 20)],
                }
            );

            Ok(())
        }
    }
}
