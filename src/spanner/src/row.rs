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

use std::error::Error as _;
use std::fmt::{Debug, Display};
use std::mem::replace;

use crate::Error;
use crate::Result;
use crate::from_value::ConvertError;
use crate::from_value::FromValue;
use crate::result_set_metadata::ResultSetMetadata;
use crate::types::Type;
use crate::types::TypeCode;
use crate::value::Value;

/// A row in a query result.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub(crate) values: Vec<Value>,
    pub(crate) metadata: ResultSetMetadata,
}

pub(crate) mod private {
    /// A sealed trait to prevent external implementation of `ColumnIndex`.
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for &str {}
    impl Sealed for String {}
    impl<T: ?Sized + Sealed> Sealed for &T {}
}

/// A trait for types that can be used to index into a [`Row`].
///
/// # Example
/// ```
/// # use google_cloud_spanner::result::{ColumnIndex, Row};
/// fn print_column_value<I: ColumnIndex>(row: &Row, index: I) -> Result<(), google_cloud_spanner::Error> {
///     let value: String = row.try_get(index)?;
///     println!("Value: {value}");
///     Ok(())
/// }
/// ```
///
/// This trait is sealed and cannot be implemented for types outside of this crate.
pub trait ColumnIndex: private::Sealed + Debug + Display {
    /// Returns the index of the column in the given row, if it exists.
    fn index(&self, row: &Row) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn index(&self, _row: &Row) -> Option<usize> {
        Some(*self)
    }
}

impl ColumnIndex for &str {
    fn index(&self, row: &Row) -> Option<usize> {
        row.metadata
            .column_names
            .iter()
            .position(|name| name == *self)
    }
}

impl ColumnIndex for String {
    fn index(&self, row: &Row) -> Option<usize> {
        self.as_str().index(row)
    }
}

impl<T: ?Sized + ColumnIndex> ColumnIndex for &T {
    fn index(&self, row: &Row) -> Option<usize> {
        (**self).index(row)
    }
}

/// Errors that can occur when getting a value from a [`Row`].
#[derive(thiserror::Error, Clone, Debug)]
#[non_exhaustive]
pub enum RowError {
    /// The requested column name was not found in the row.
    #[error("Could not find column: '{0}'")]
    ColumnNotFound(String),
    /// The requested column index was out of range.
    #[error("Column index out of range: {index} (expected < {len})")]
    #[non_exhaustive]
    IndexOutOfRange {
        /// The index that was requested.
        index: usize,
        /// The total number of columns in the row.
        len: usize,
    },
    /// Failed to convert the column value to the requested type.
    #[error("Type conversion error for column '{column}' (type {type_code:?}): {source}")]
    #[non_exhaustive]
    TypeConversion {
        /// The column identifier (name or index).
        column: String,
        /// The Spanner type code of the column.
        type_code: TypeCode,
        /// The underlying conversion error.
        #[source]
        source: ConvertError,
    },
}

impl RowError {
    /// Creates a [`RowError::ColumnNotFound`] with the requested column name.
    ///
    /// # Example
    /// ```
    /// use google_cloud_spanner::error::RowError;
    ///
    /// let error = RowError::column_not_found("user_id");
    /// assert_eq!(error.to_string(), "Could not find column: 'user_id'");
    /// ```
    pub fn column_not_found(column: impl Into<String>) -> Self {
        Self::ColumnNotFound(column.into())
    }

    /// Creates a [`RowError::IndexOutOfRange`] with the requested index and total column count.
    ///
    /// # Example
    /// ```
    /// use google_cloud_spanner::error::RowError;
    ///
    /// let error = RowError::index_out_of_range(5, 3);
    /// assert_eq!(error.to_string(), "Column index out of range: 5 (expected < 3)");
    /// ```
    pub fn index_out_of_range(index: usize, len: usize) -> Self {
        Self::IndexOutOfRange { index, len }
    }

    /// Creates a [`RowError::TypeConversion`] with the column identifier, type code, and source error.
    ///
    /// # Example
    /// ```
    /// use google_cloud_spanner::error::{ConvertError, RowError};
    /// use google_cloud_spanner::types::TypeCode;
    ///
    /// let source = ConvertError::type_mismatch(TypeCode::Int64, TypeCode::String);
    /// let error = RowError::type_conversion("age", TypeCode::String, source);
    /// assert_eq!(
    ///     error.to_string(),
    ///     "Type conversion error for column 'age' (type String): type mismatch, expected Int64, got String"
    /// );
    /// ```
    pub fn type_conversion(
        column: impl Into<String>,
        type_code: TypeCode,
        source: ConvertError,
    ) -> Self {
        Self::TypeConversion {
            column: column.into(),
            type_code,
            source,
        }
    }

    /// Extracts a `RowError` from a [`google_cloud_spanner::Error`][Error], if present.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::error::RowError;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn example(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.single_use().build();
    /// let mut result_set = transaction
    ///     .execute_query(Statement::builder("SELECT 42 AS id").build())
    ///     .await?;
    ///
    /// if let Some(row) = result_set.next().await {
    ///     let result: Result<i64, _> = row?.try_get("nonexistent_column");
    ///     if let Err(error) = result {
    ///         if let Some(row_error) = RowError::extract(&error) {
    ///             match row_error {
    ///                 RowError::ColumnNotFound(column) => println!("Column not found: {column}"),
    ///                 RowError::IndexOutOfRange { index, len, .. } => {
    ///                     println!("Index {index} out of range (length {len})");
    ///                 }
    ///                 RowError::TypeConversion { column, type_code, source, .. } => {
    ///                     println!("Conversion error for {column} ({type_code:?}): {source}");
    ///                 }
    ///                 _ => {}
    ///             }
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Row operations return [`Error`] with a source of [`RowError`] when a column lookup
    /// fails, an index is out of bounds, or type conversion fails. This method traverses
    /// the error chain to extract the underlying [`RowError`].
    pub fn extract(err: &Error) -> Option<&Self> {
        let mut current = err.source();
        while let Some(source) = current {
            if let Some(row_error) = source.downcast_ref::<Self>() {
                return Some(row_error);
            }
            current = source.source();
        }
        None
    }
}

impl Row {
    /// Returns the raw values of the row.
    pub fn raw_values(&self) -> &[Value] {
        &self.values
    }

    /// Returns true if the value at the specified column name or index is null.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn test_doc() -> anyhow::Result<()> {
    /// let client = Spanner::builder().build().await?;
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.single_use().build();
    /// let mut result_set = transaction.execute_query(Statement::builder("SELECT NULL AS Age").build()).await?;
    ///
    /// if let Some(row) = result_set.next().await {
    ///     let is_null = row?.try_is_null("Age")?;
    ///     println!("Is null: {}", is_null);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `index` - The column name (string) or index (zero-based integer).
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` if the value is null or not.
    /// * `Err(Error)` if the column name or index is invalid.
    pub fn try_is_null<I: ColumnIndex>(&self, index: I) -> Result<bool> {
        let column_index = self.validate_column_index(&index)?;
        Ok(self.values[column_index].is_null())
    }

    /// Returns true if the value at the specified column name or index is null, panicking on error.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn test_doc() -> anyhow::Result<()> {
    /// let client = Spanner::builder().build().await?;
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.single_use().build();
    /// let mut result_set = transaction.execute_query(Statement::builder("SELECT NULL AS Age").build()).await?;
    ///
    /// if let Some(row) = result_set.next().await {
    ///     let is_null = row?.is_null("Age");
    ///     println!("Is null: {}", is_null);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This is a convenience wrapper around [`try_is_null`](Row::try_is_null).
    ///
    /// # Panics
    ///
    /// Panics if the column name or index is invalid.
    pub fn is_null<I: ColumnIndex>(&self, index: I) -> bool {
        match self.try_is_null(&index) {
            Ok(is_null) => is_null,
            Err(error) => panic!("failed to check if column {index:?} is null: {error}"),
        }
    }

    /// Retrieves a value from the row by column name or zero-based index.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn test_doc() -> anyhow::Result<()> {
    /// let client = Spanner::builder().build().await?;
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.single_use().build();
    /// let mut result_set = transaction.execute_query(Statement::builder("SELECT 42 AS Age").build()).await?;
    ///
    /// if let Some(row) = result_set.next().await {
    ///     let age: i64 = row?.try_get("Age")?;
    ///     println!("Age: {}", age);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `index` - The column name (string) or index (zero-based integer).
    ///
    /// # Returns
    ///
    /// * `Ok(T)` if the value was successfully retrieved and converted to type `T`.
    /// * `Err(Error)` if:
    ///     * The column name or index is invalid. The underlying [`RowError`] can be extracted using [`RowError::extract`].
    ///     * The column value is incompatible with type `T` or conversion fails. The underlying [`RowError::TypeConversion`]
    ///       can be extracted using [`RowError::extract`], and the inner [`ConvertError`][crate::error::ConvertError]
    ///       can also be extracted using [`ConvertError::extract`][crate::error::ConvertError::extract].
    pub fn try_get<T: FromValue, I: ColumnIndex>(&self, index: I) -> Result<T> {
        let (column_index, value) = self.get_value(index)?;
        let column_type = Self::column_type(&self.metadata.column_types, column_index)?;
        T::from_value(value, column_type).map_err(|error| {
            let column = self.column_identifier(column_index);
            Error::deser(RowError::type_conversion(column, column_type.code(), error))
        })
    }

    /// Retrieves a value from the row by column name or zero-based index, panicking on error.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn test_doc() -> anyhow::Result<()> {
    /// let client = Spanner::builder().build().await?;
    /// let database_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = database_client.single_use().build();
    /// let mut result_set = transaction.execute_query(Statement::builder("SELECT 42 AS Age").build()).await?;
    ///
    /// if let Some(row) = result_set.next().await {
    ///     let age: i64 = row?.get("Age");
    ///     println!("Age: {}", age);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This is a convenience wrapper around [`try_get`](Row::try_get).
    ///
    /// # Panics
    ///
    /// Panics if:
    /// * The column name or index is invalid.
    /// * The column value is incompatible with type `T` (or is null when `T` is not an [`Option`]).
    pub fn get<T: FromValue, I: ColumnIndex>(&self, index: I) -> T {
        match self.try_get(&index) {
            Ok(value) => value,
            Err(error) => panic!("failed to retrieve column {index:?}: {error}"),
        }
    }

    /// Takes ownership of a value from the row by column name or zero-based index,
    /// replacing the column in the row with a null value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn example(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let database_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = database_client.single_use().build();
    /// let mut result_set = transaction
    ///     .execute_query(Statement::builder("SELECT 'hello' AS greeting").build())
    ///     .await?;
    ///
    /// if let Some(row) = result_set.next().await {
    ///     let mut row = row?;
    ///     let greeting: String = row.try_take("greeting")?;
    ///     assert_eq!(greeting, "hello");
    ///
    ///     // Subsequent reads treat the column as null:
    ///     assert!(row.is_null("greeting"));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This method avoids cloning heap-allocated types such as [`String`], [`Vec<T>`],
    /// [`serde_json::Value`], and [`Value`].
    ///
    /// # Note
    ///
    /// If the SQL query contains duplicate column names, indexing by column name
    /// resolves to the first matching column. Once taken, that column becomes null,
    /// and subsequent calls with the same column name will still resolve to that first
    /// (now null) column. To take subsequent duplicate columns, index by column position.
    ///
    /// If type conversion fails, the column value in the row has already been
    /// replaced with `NULL` and cannot be recovered.
    ///
    /// # Arguments
    ///
    /// * `index` - The column name (string) or index (zero-based integer).
    ///
    /// # Returns
    ///
    /// * `Ok(T)` if the value was successfully taken and converted to type `T`.
    /// * `Err(Error)` if:
    ///     * The column name or index is invalid. The underlying [`RowError`] can be extracted using [`RowError::extract`].
    ///     * The column value is incompatible with type `T` or conversion fails. The underlying [`RowError::TypeConversion`]
    ///       can be extracted using [`RowError::extract`], and the inner [`ConvertError`][crate::error::ConvertError]
    ///       can also be extracted using [`ConvertError::extract`][crate::error::ConvertError::extract].
    pub fn try_take<T: FromValue, I: ColumnIndex>(&mut self, index: I) -> Result<T> {
        let column_index = self.validate_column_index(&index)?;
        let column_type = Self::column_type(&self.metadata.column_types, column_index)?;
        let type_code = column_type.code();
        let value = replace(&mut self.values[column_index], Value::null());
        T::from_owned_value(value, column_type).map_err(|error| {
            let column = self.column_identifier(column_index);
            Error::deser(RowError::type_conversion(column, type_code, error))
        })
    }

    /// Takes ownership of a value from the row by column name or zero-based index,
    /// replacing the column in the row with a null value, panicking on error.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn example(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let database_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = database_client.single_use().build();
    /// let mut result_set = transaction
    ///     .execute_query(Statement::builder("SELECT 'hello' AS greeting").build())
    ///     .await?;
    ///
    /// if let Some(row) = result_set.next().await {
    ///     let mut row = row?;
    ///     let greeting: String = row.take("greeting");
    ///     println!("Greeting: {greeting}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This is a convenience wrapper around [`try_take`](Row::try_take).
    ///
    /// # Note
    ///
    /// If the SQL query contains duplicate column names, indexing by column name
    /// resolves to the first matching column. Once taken, that column becomes null,
    /// and subsequent calls with the same column name will still resolve to that first
    /// (now null) column. To take subsequent duplicate columns, index by column position.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// * The column name or index is invalid.
    /// * The column value is incompatible with type `T` (or is null when `T` is not an [`Option`]).
    pub fn take<T: FromValue, I: ColumnIndex>(&mut self, index: I) -> T {
        match self.try_take(&index) {
            Ok(value) => value,
            Err(error) => panic!("failed to take column {index:?}: {error}"),
        }
    }

    /// Consumes the row and returns its raw values.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn example(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let database_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = database_client.single_use().build();
    /// let mut result_set = transaction
    ///     .execute_query(Statement::builder("SELECT 1 AS a, 'b' AS b").build())
    ///     .await?;
    ///
    /// if let Some(row) = result_set.next().await {
    ///     let values = row?.into_values();
    ///     assert_eq!(values.len(), 2);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Returns
    ///
    /// Returns a [`Vec<Value>`] containing the row's values in ordinal column order.
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    fn get_value<I: ColumnIndex>(&self, index: I) -> Result<(usize, &Value)> {
        let column_index = self.validate_column_index(&index)?;
        let value = &self.values[column_index];
        Ok((column_index, value))
    }

    fn validate_column_index<I: ColumnIndex>(&self, index: &I) -> Result<usize> {
        let column_index = index
            .index(self)
            .ok_or_else(|| Error::deser(RowError::column_not_found(index.to_string())))?;
        let total_columns = self.values.len();
        if column_index >= total_columns {
            return Err(Error::deser(RowError::index_out_of_range(
                column_index,
                total_columns,
            )));
        }
        Ok(column_index)
    }

    fn column_type(column_types: &[Type], column_index: usize) -> Result<&Type> {
        column_types.get(column_index).ok_or_else(|| {
            Error::deser(RowError::index_out_of_range(
                column_index,
                column_types.len(),
            ))
        })
    }

    fn column_identifier(&self, column_index: usize) -> String {
        self.metadata
            .column_names
            .get(column_index)
            .filter(|name| !name.is_empty())
            .cloned()
            .unwrap_or_else(|| column_index.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::from_value::ConvertError;
    use crate::to_value::ToValue;
    use crate::types;
    use crate::value::Date;
    use rust_decimal::Decimal;
    use serde_json::{Value as JsonValue, json};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use wkt::Timestamp;

    fn empty_row() -> Row {
        Row {
            values: Vec::new(),
            metadata: ResultSetMetadata::new(None),
        }
    }

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(Row: Clone, Debug, PartialEq, Send, Sync);
        static_assertions::assert_impl_all!(RowError: Clone, Debug, Send, Sync);
        static_assertions::assert_impl_all!(usize: ColumnIndex, Display);
        static_assertions::assert_impl_all!(&str: ColumnIndex, Display);
        static_assertions::assert_impl_all!(String: ColumnIndex, Display);
        static_assertions::assert_impl_all!(&usize: ColumnIndex, Display);
        static_assertions::assert_impl_all!(&&str: ColumnIndex, Display);
        static_assertions::assert_impl_all!(&String: ColumnIndex, Display);
    }

    #[test]
    #[allow(clippy::needless_borrows_for_generic_args)]
    fn row_get() {
        let names = vec![
            "col_string".to_string(),
            "col_int64".to_string(),
            "col_float64".to_string(),
            "col_bool".to_string(),
            "col_bytes".to_string(),
            "col_numeric".to_string(),
            "col_date".to_string(),
            "col_timestamp".to_string(),
            "col_float32".to_string(),
            "col_json".to_string(),
            "col_uuid".to_string(),
            "col_interval".to_string(),
        ];

        let types = vec![
            types::string(),
            types::int64(),
            types::float64(),
            types::bool(),
            types::bytes(),
            types::numeric(),
            types::date(),
            types::timestamp(),
            types::float32(),
            types::json(),
            types::uuid(),
            types::interval(),
        ];

        let decimal = Decimal::from_str_exact("123.456").expect("valid decimal");
        let date = Date::new().set_year(2023).set_month(10).set_day(27);
        let timestamp = Timestamp::clamp(1_698_400_800, 0);

        let values = vec![
            "hello".to_string().to_value(),
            42_i64.to_value(),
            42.5_f64.to_value(),
            true.to_value(),
            vec![1_u8, 2, 3].to_value(),
            decimal.to_value(),
            date.clone().to_value(),
            timestamp.to_value(),
            1.23_f32.to_value(),
            "{\"key\":\"value\"}".to_string().to_value(),
            "123e4567-e89b-12d3-a456-426614174000"
                .to_string()
                .to_value(),
            "P1Y2M3D".to_string().to_value(),
        ];

        let row = Row {
            values,
            metadata: ResultSetMetadata {
                column_names: Arc::new(names),
                column_types: Arc::new(types),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        // Test getting by valid index
        assert_eq!(
            row.get::<String, _>(0),
            "hello",
            "expected string at index 0"
        );
        assert_eq!(row.get::<i64, _>(1), 42, "expected int64 at index 1");
        assert_eq!(row.get::<f64, _>(2), 42.5, "expected float64 at index 2");
        assert!(
            row.get::<bool, _>(3),
            "expected bool value at index 3 to be true"
        );
        assert_eq!(
            row.get::<Vec<u8>, _>(4),
            vec![1_u8, 2, 3],
            "expected bytes at index 4"
        );
        assert_eq!(
            row.get::<Decimal, _>(5),
            decimal,
            "expected numeric at index 5"
        );
        assert_eq!(
            row.get::<String, _>(5),
            "123.456",
            "expected numeric as string at index 5"
        );
        assert_eq!(row.get::<Date, _>(6), date, "expected date at index 6");
        assert_eq!(
            row.get::<String, _>(6),
            "2023-10-27",
            "expected date as string at index 6"
        );
        assert_eq!(
            row.get::<Timestamp, _>(7),
            timestamp,
            "expected timestamp at index 7"
        );
        assert_eq!(
            row.get::<String, _>(7),
            "2023-10-27T10:00:00.000000000Z",
            "expected timestamp as string at index 7"
        );
        assert_eq!(
            row.get::<f32, _>(8),
            1.23_f32,
            "expected float32 at index 8"
        );
        assert_eq!(
            row.get::<String, _>(9),
            "{\"key\":\"value\"}",
            "expected json at index 9"
        );
        assert_eq!(
            row.get::<String, _>(10),
            "123e4567-e89b-12d3-a456-426614174000",
            "expected uuid at index 10"
        );
        assert_eq!(
            row.get::<String, _>(11),
            "P1Y2M3D",
            "expected interval at index 11"
        );

        // Test getting by valid name
        assert_eq!(
            row.get::<String, _>("col_string"),
            "hello",
            "expected col_string by name"
        );
        assert_eq!(
            row.get::<i64, _>("col_int64"),
            42,
            "expected col_int64 by name"
        );
        assert_eq!(
            row.get::<f64, _>("col_float64"),
            42.5,
            "expected col_float64 by name"
        );
        assert!(
            row.get::<bool, _>("col_bool"),
            "expected col_bool to be true"
        );
        assert_eq!(
            row.get::<Vec<u8>, _>("col_bytes"),
            vec![1_u8, 2, 3],
            "expected col_bytes by name"
        );
        assert_eq!(
            row.get::<Decimal, _>("col_numeric"),
            decimal,
            "expected col_numeric by name"
        );
        assert_eq!(
            row.get::<String, _>("col_numeric"),
            "123.456",
            "expected col_numeric as string by name"
        );
        assert_eq!(
            row.get::<Date, _>("col_date"),
            date,
            "expected col_date by name"
        );
        assert_eq!(
            row.get::<String, _>("col_date"),
            "2023-10-27",
            "expected col_date as string by name"
        );
        assert_eq!(
            row.get::<Timestamp, _>("col_timestamp"),
            timestamp,
            "expected col_timestamp by name"
        );
        assert_eq!(
            row.get::<String, _>("col_timestamp"),
            "2023-10-27T10:00:00.000000000Z",
            "expected col_timestamp as string by name"
        );
        assert_eq!(
            row.get::<f32, _>("col_float32"),
            1.23_f32,
            "expected col_float32 by name"
        );
        assert_eq!(
            row.get::<String, _>("col_json"),
            "{\"key\":\"value\"}",
            "expected col_json by name"
        );
        assert_eq!(
            row.get::<String, _>("col_uuid"),
            "123e4567-e89b-12d3-a456-426614174000",
            "expected col_uuid by name"
        );
        assert_eq!(
            row.get::<String, _>("col_interval"),
            "P1Y2M3D",
            "expected col_interval by name"
        );

        // Test getting by invalid index
        assert!(
            row.try_get::<String, _>(12).is_err(),
            "expected out of range index 12 to return an error"
        );

        // Test getting by invalid name
        assert!(
            row.try_get::<String, _>("col_invalid").is_err(),
            "expected non-existent column name to return an error"
        );

        // Test getting mismatched type
        assert!(
            row.try_get::<i64, _>(0).is_err(),
            "expected string-to-int conversion error at index 0"
        );
        assert!(
            row.try_get::<bool, _>(1).is_err(),
            "expected int-to-bool conversion error at index 1"
        );

        let err_int_as_string = row
            .try_get::<String, _>(1)
            .expect_err("reading INT64 column as String must fail with TypeConversion error");
        let convert_err = ConvertError::extract(&err_int_as_string)
            .expect("should extract ConvertError::TypeMismatch");
        assert_eq!(
            convert_err.to_string(),
            "type mismatch, expected String, got Int64",
            "expected TypeMismatch when reading INT64 column as String"
        );

        // Test getting with reference index types (&usize, &&str, &String) and owned String
        assert_eq!(row.get::<String, _>(&0), "hello");
        assert_eq!(row.get::<String, _>(&"col_string"), "hello");
        let column_name = "col_string".to_string();
        assert_eq!(row.get::<String, _>(&column_name), "hello");
        assert_eq!(row.get::<String, _>(column_name), "hello");
    }

    #[test]
    fn row_get_enum() {
        let column_names = vec!["col_enum".to_string()];
        let column_types = vec![types::enum_type("customer.Priority")];
        let values = vec![3_i64.to_value()];
        let row = Row {
            values,
            metadata: ResultSetMetadata {
                column_names: Arc::new(column_names),
                column_types: Arc::new(column_types),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        // Getting by index as i64 and i32
        assert_eq!(
            row.get::<i64, _>(0),
            3,
            "expected i64 value 3 from enum column by index"
        );
        assert_eq!(
            row.get::<i32, _>(0),
            3,
            "expected i32 value 3 from enum column by index"
        );

        // Getting by name as i64 and i32
        assert_eq!(
            row.get::<i64, _>("col_enum"),
            3,
            "expected i64 value 3 from enum column by name"
        );
        assert_eq!(
            row.get::<i32, _>("col_enum"),
            3,
            "expected i32 value 3 from enum column by name"
        );

        // Getting as String should fail with TypeConversion
        let string_error = row
            .try_get::<String, _>("col_enum")
            .expect_err("reading ENUM column as String must fail");
        let convert_error = ConvertError::extract(&string_error)
            .expect("should extract ConvertError from string_error");
        assert_eq!(
            convert_error.to_string(),
            "type mismatch, expected String, got Enum",
            "expected TypeMismatch when reading ENUM column as String"
        );

        // Null ENUM column decoding as Option<i64> and Option<i32>
        let null_row = Row {
            values: vec![Value::null()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_enum".to_string()]),
                column_types: Arc::new(vec![types::enum_type("customer.Priority")]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        assert_eq!(
            null_row.get::<Option<i64>, _>("col_enum"),
            None,
            "expected None for null enum column as Option<i64>"
        );
        assert_eq!(
            null_row.get::<Option<i32>, _>("col_enum"),
            None,
            "expected None for null enum column as Option<i32>"
        );
    }

    #[test]
    #[allow(clippy::needless_borrows_for_generic_args)]
    fn row_is_null() {
        let names = vec!["non_null_col".to_string(), "null_col".to_string()];
        let types = vec![types::string(), types::string()];
        let values = vec!["hello".to_string().to_value(), Value::null()];
        let row = Row {
            values,
            metadata: ResultSetMetadata {
                column_names: Arc::new(names),
                column_types: Arc::new(types),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        assert!(
            !row.is_null("non_null_col"),
            "expected non_null_col to not be null"
        );
        assert!(row.is_null("null_col"), "expected null_col to be null");
        assert!(!row.is_null(0), "expected index 0 to not be null");
        assert!(row.is_null(1), "expected index 1 to be null");

        // Test checking null with reference index types (&usize, &&str, &String) and owned String
        assert!(!row.is_null(&0), "expected &0 to not be null");
        assert!(
            !row.is_null(&"non_null_col"),
            "expected &\"non_null_col\" to not be null"
        );
        let non_null_name = "non_null_col".to_string();
        assert!(
            !row.is_null(&non_null_name),
            "expected &String to not be null"
        );
        assert!(
            !row.is_null(non_null_name),
            "expected owned String to not be null"
        );

        assert!(
            !row.try_is_null("non_null_col").expect("valid column name"),
            "expected non_null_col to return false"
        );
        assert!(
            row.try_is_null("null_col").expect("valid column name"),
            "expected null_col to return true"
        );
        assert!(
            !row.try_is_null(0).expect("valid column index"),
            "expected index 0 to return false"
        );
        assert!(
            row.try_is_null(1).expect("valid column index"),
            "expected index 1 to return true"
        );
    }

    #[test]
    fn row_error_extract_and_derives() {
        let metadata = ResultSetMetadata {
            column_names: Arc::new(vec!["col0".to_string()]),
            column_types: Arc::new(vec![types::string()]),
            undeclared_parameters: Arc::new(BTreeMap::new()),
        };
        let row = Row {
            values: vec!["val0".to_value()],
            metadata,
        };

        let err_missing = row
            .try_get::<String, _>("nonexistent")
            .expect_err("column not found");
        let extracted_missing = RowError::extract(&err_missing).expect("should extract RowError");
        let cloned_missing = extracted_missing.clone();
        assert_eq!(
            cloned_missing.to_string(),
            extracted_missing.to_string(),
            "expected cloned RowError display to match original"
        );
        assert_eq!(
            extracted_missing.to_string(),
            "Could not find column: 'nonexistent'",
            "expected 'Could not find column: \\'nonexistent\\'' display string"
        );
        assert_eq!(
            extracted_missing.to_string(),
            RowError::column_not_found("nonexistent").to_string(),
            "expected ColumnNotFound display to match constructor"
        );

        let error_out_of_range = row.try_get::<String, _>(5).expect_err("index out of range");
        let extracted_out_of_range =
            RowError::extract(&error_out_of_range).expect("should extract RowError");
        assert_eq!(
            extracted_out_of_range.to_string(),
            "Column index out of range: 5 (expected < 1)",
            "expected 'Column index out of range: 5 (expected < 1)' display string"
        );
        assert_eq!(
            extracted_out_of_range.to_string(),
            RowError::index_out_of_range(5, 1).to_string(),
            "expected IndexOutOfRange display to match constructor"
        );

        let unrelated_error = Error::deser(ConvertError::NotNull);
        assert!(
            RowError::extract(&unrelated_error).is_none(),
            "expected None when extracting RowError from an unrelated error source"
        );

        use google_cloud_rpc::model::Status;
        let no_source_error = Error::service(Status::default().into());
        assert!(
            RowError::extract(&no_source_error).is_none(),
            "expected None when error has no source"
        );
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column \"col_invalid\": cannot deserialize the response Could not find column: 'col_invalid'"
    )]
    fn row_get_panics_on_invalid_column_name() {
        let row = empty_row();
        let _: String = row.get("col_invalid");
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column \"col_invalid\": cannot deserialize the response Could not find column: 'col_invalid'"
    )]
    fn row_get_panics_on_invalid_column_name_owned_string() {
        let row = empty_row();
        let _: String = row.get("col_invalid".to_string());
    }

    #[test]
    #[allow(clippy::needless_borrows_for_generic_args)]
    #[should_panic(
        expected = "failed to retrieve column \"col_invalid\": cannot deserialize the response Could not find column: 'col_invalid'"
    )]
    fn row_get_panics_on_invalid_column_name_reference() {
        let row = empty_row();
        let _: String = row.get(&"col_invalid");
    }

    #[test]
    #[allow(clippy::needless_borrows_for_generic_args)]
    #[should_panic(
        expected = "failed to retrieve column \"col_invalid\": cannot deserialize the response Could not find column: 'col_invalid'"
    )]
    fn row_get_panics_on_invalid_column_name_ref_string() {
        let row = empty_row();
        let invalid_column = "col_invalid".to_string();
        let _: String = row.get(&invalid_column);
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column 0: cannot deserialize the response Column index out of range: 0 (expected < 0)"
    )]
    fn row_get_panics_on_invalid_column_index_empty_row() {
        let row = empty_row();
        let _: String = row.get(0);
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column 5: cannot deserialize the response Column index out of range: 5 (expected < 2)"
    )]
    fn row_get_panics_on_invalid_column_index_non_empty_row() {
        let row = Row {
            values: vec!["a".to_string().to_value(), "b".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_a".to_string(), "col_b".to_string()]),
                column_types: Arc::new(vec![types::string(), types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: String = row.get(5);
    }

    #[test]
    #[allow(clippy::needless_borrows_for_generic_args)]
    #[should_panic(
        expected = "failed to retrieve column 5: cannot deserialize the response Column index out of range: 5 (expected < 2)"
    )]
    fn row_get_panics_on_invalid_column_index_reference() {
        let row = Row {
            values: vec!["a".to_string().to_value(), "b".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_a".to_string(), "col_b".to_string()]),
                column_types: Arc::new(vec![types::string(), types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: String = row.get(&5);
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column \"col_string\": cannot deserialize the response Type conversion error for column 'col_string' (type String): type mismatch, expected Int64, got String"
    )]
    fn row_get_panics_on_type_mismatch_by_name() {
        let row = Row {
            values: vec!["hello".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_string".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: i64 = row.get("col_string");
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column 0: cannot deserialize the response Type conversion error for column 'col_string' (type String): type mismatch, expected Int64, got String"
    )]
    fn row_get_panics_on_type_mismatch_by_index() {
        let row = Row {
            values: vec!["hello".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_string".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: i64 = row.get(0);
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column 0: cannot deserialize the response Type conversion error for column '0' (type String): type mismatch, expected Int64, got String"
    )]
    fn row_get_panics_on_type_mismatch_for_unnamed_column() {
        let row = Row {
            values: vec!["hello".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: i64 = row.get(0);
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column \"col_int\": cannot deserialize the response Type conversion error for column 'col_int' (type Int64): expected String, got Bool"
    )]
    fn row_get_panics_on_kind_mismatch() {
        let row = Row {
            values: vec![true.to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_int".to_string()]),
                column_types: Arc::new(vec![types::int64()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: i64 = row.get("col_int");
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column \"null_col\": cannot deserialize the response Type conversion error for column 'null_col' (type String): expected non-null value, got null"
    )]
    fn row_get_panics_on_null_value() {
        let row = Row {
            values: vec![Value::null()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["null_col".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: String = row.get("null_col");
    }

    #[test]
    fn row_type_conversion_error() {
        let row = Row {
            values: vec!["hello".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_string".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let error = row
            .try_get::<i64, _>("col_string")
            .expect_err("reading string column as i64 should fail");
        let extracted_row_error =
            RowError::extract(&error).expect("should extract RowError::TypeConversion");
        let cloned_row_error = extracted_row_error.clone();
        assert_eq!(
            cloned_row_error.to_string(),
            extracted_row_error.to_string(),
            "expected cloned RowError display to match original"
        );
        assert_eq!(
            extracted_row_error.to_string(),
            "Type conversion error for column 'col_string' (type String): type mismatch, expected Int64, got String",
            "expected formatted display string"
        );

        let extracted_convert_error =
            ConvertError::extract(&error).expect("should extract inner ConvertError");
        assert_eq!(
            extracted_convert_error.to_string(),
            "type mismatch, expected Int64, got String",
            "expected inner TypeMismatch error"
        );

        // Test with unnamed column fallback to index string
        let unnamed_row = Row {
            values: vec!["hello".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let unnamed_error = unnamed_row
            .try_get::<i64, _>(0)
            .expect_err("reading unnamed string column as i64 should fail");
        let extracted_unnamed =
            RowError::extract(&unnamed_error).expect("should extract RowError for unnamed column");
        assert_eq!(
            extracted_unnamed.to_string(),
            "Type conversion error for column '0' (type String): type mismatch, expected Int64, got String",
            "expected formatted display string with column index '0'"
        );

        // Test with empty column_names slice entirely
        let empty_names_row = Row {
            values: vec!["hello".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec![]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let empty_names_error = empty_names_row
            .try_get::<i64, _>(0)
            .expect_err("reading from row with empty column_names slice should fail");
        let extracted_empty_names = RowError::extract(&empty_names_error)
            .expect("should extract RowError for row with empty column_names");
        assert_eq!(
            extracted_empty_names.to_string(),
            "Type conversion error for column '0' (type String): type mismatch, expected Int64, got String",
            "expected formatted display string with column index '0' for empty column_names"
        );
    }

    #[test]
    fn row_try_get_mismatched_metadata_types_length() {
        let row = Row {
            values: vec!["a".to_string().to_value(), "b".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_a".to_string(), "col_b".to_string()]),
                column_types: Arc::new(vec![types::string()]), // length 1 < values length 2
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        assert!(
            row.try_get::<String, _>(1).is_err(),
            "expected error when metadata column_types is shorter than values"
        );
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column 1: cannot deserialize the response Column index out of range: 1 (expected < 1)"
    )]
    fn row_get_panics_on_mismatched_metadata_types_length() {
        let row = Row {
            values: vec!["a".to_string().to_value(), "b".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_a".to_string(), "col_b".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: String = row.get(1);
    }

    #[test]
    fn generic_column_index_helper() {
        fn fetch_value<I: ColumnIndex>(row: &Row, index: I) -> Result<String> {
            row.try_get(index)
        }

        let metadata = ResultSetMetadata {
            column_names: Arc::new(vec!["username".to_string()]),
            column_types: Arc::new(vec![types::string()]),
            undeclared_parameters: Arc::new(BTreeMap::new()),
        };
        let row = Row {
            values: vec!["alice".to_value()],
            metadata,
        };

        let by_name = fetch_value(&row, "username").expect("fetch by string slice");
        assert_eq!(by_name, "alice", "expected value fetched by name");

        let by_string = fetch_value(&row, "username".to_string()).expect("fetch by String");
        assert_eq!(by_string, "alice", "expected value fetched by owned String");

        let owned_username = "username".to_string();
        let by_borrowed_string = fetch_value(&row, &owned_username).expect("fetch by &String");
        assert_eq!(
            by_borrowed_string, "alice",
            "expected value fetched by &String"
        );

        let by_index = fetch_value(&row, 0_usize).expect("fetch by usize");
        assert_eq!(by_index, "alice", "expected value fetched by index");
    }

    #[test]
    #[should_panic(
        expected = "failed to check if column \"col_invalid\" is null: cannot deserialize the response Could not find column: 'col_invalid'"
    )]
    fn row_is_null_panics_on_invalid_column_name() {
        let row = empty_row();
        let _ = row.is_null("col_invalid");
    }

    #[test]
    #[should_panic(
        expected = "failed to check if column \"col_invalid\" is null: cannot deserialize the response Could not find column: 'col_invalid'"
    )]
    fn row_is_null_panics_on_invalid_column_name_owned_string() {
        let row = empty_row();
        let _ = row.is_null("col_invalid".to_string());
    }

    #[test]
    #[allow(clippy::needless_borrows_for_generic_args)]
    #[should_panic(
        expected = "failed to check if column \"col_invalid\" is null: cannot deserialize the response Could not find column: 'col_invalid'"
    )]
    fn row_is_null_panics_on_invalid_column_name_reference() {
        let row = empty_row();
        let _ = row.is_null(&"col_invalid");
    }

    #[test]
    #[allow(clippy::needless_borrows_for_generic_args)]
    #[should_panic(
        expected = "failed to check if column \"col_invalid\" is null: cannot deserialize the response Could not find column: 'col_invalid'"
    )]
    fn row_is_null_panics_on_invalid_column_name_ref_string() {
        let row = empty_row();
        let invalid_column = "col_invalid".to_string();
        let _ = row.is_null(&invalid_column);
    }

    #[test]
    #[should_panic(
        expected = "failed to check if column 0 is null: cannot deserialize the response Column index out of range: 0 (expected < 0)"
    )]
    fn row_is_null_panics_on_invalid_column_index_empty_row() {
        let row = empty_row();
        let _ = row.is_null(0);
    }

    #[test]
    #[should_panic(
        expected = "failed to check if column 5 is null: cannot deserialize the response Column index out of range: 5 (expected < 2)"
    )]
    fn row_is_null_panics_on_invalid_column_index_non_empty_row() {
        let row = Row {
            values: vec!["a".to_string().to_value(), "b".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_a".to_string(), "col_b".to_string()]),
                column_types: Arc::new(vec![types::string(), types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _ = row.is_null(5);
    }

    #[test]
    #[allow(clippy::needless_borrows_for_generic_args)]
    #[should_panic(
        expected = "failed to check if column 5 is null: cannot deserialize the response Column index out of range: 5 (expected < 2)"
    )]
    fn row_is_null_panics_on_invalid_column_index_reference() {
        let row = Row {
            values: vec!["a".to_string().to_value(), "b".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_a".to_string(), "col_b".to_string()]),
                column_types: Arc::new(vec![types::string(), types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _ = row.is_null(&5);
    }

    #[test]
    fn row_try_take() {
        let mut row = Row {
            values: vec![
                "test_string".to_string().to_value(),
                42_i64.to_value(),
                vec!["a".to_string(), "b".to_string()].to_value(),
            ],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec![
                    "col_string".to_string(),
                    "col_int".to_string(),
                    "col_vec".to_string(),
                ]),
                column_types: Arc::new(vec![
                    types::string(),
                    types::int64(),
                    types::array(types::string()),
                ]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        // Take string column
        let string_value: String = row.try_take("col_string").expect("should take col_string");
        assert_eq!(string_value, "test_string", "expected 'test_string'");

        // Take integer column by index
        let integer_value: i64 = row.try_take(1).expect("should take col_int by index");
        assert_eq!(integer_value, 42, "expected 42");

        // Take array column
        let vector_value: Vec<String> = row.try_take("col_vec").expect("should take col_vec");
        assert_eq!(
            vector_value,
            vec!["a".to_string(), "b".to_string()],
            "expected array matching ['a', 'b']"
        );
    }

    #[test]
    fn row_take_success() {
        let mut row = Row {
            values: vec![
                "test_string".to_string().to_value(),
                42_i64.to_value(),
                vec!["x".to_string()].to_value(),
                Value::null(),
            ],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec![
                    "col_string".to_string(),
                    "col_int".to_string(),
                    "col_vec".to_string(),
                    "col_null".to_string(),
                ]),
                column_types: Arc::new(vec![
                    types::string(),
                    types::int64(),
                    types::array(types::string()),
                    types::string(),
                ]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        // Take by index
        let string_value: String = row.take(0);
        assert_eq!(string_value, "test_string", "expected 'test_string'");

        // Take array column
        let vector_value: Vec<String> = row.take("col_vec");
        assert_eq!(vector_value, vec!["x".to_string()], "expected ['x']");

        // Take nullable null column
        let optional_null: Option<String> = row.take("col_null");
        assert_eq!(optional_null, None, "expected None for null column");

        // Take nullable present column
        let optional_present: Option<i64> = row.take("col_int");
        assert_eq!(optional_present, Some(42), "expected Some(42)");
    }

    #[test]
    fn row_take_subsequent_reads_null() {
        let mut row = Row {
            values: vec!["hello".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["text".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let taken: String = row.take("text");
        assert_eq!(taken, "hello", "expected 'hello'");

        // Column is now null
        assert!(row.is_null("text"), "taken column must be null");
        assert!(
            row.try_is_null("text").expect("try_is_null should succeed"),
            "taken column must be null"
        );

        // Reading or taking as Option returns None
        let optional_get: Option<String> = row.try_get("text").expect("get Option must succeed");
        assert_eq!(optional_get, None, "expected None for taken column");

        let optional_take: Option<String> = row.try_take("text").expect("take Option must succeed");
        assert_eq!(optional_take, None, "expected None for taken column");

        // Reading or taking again as non-nullable fails with NotNull
        let error_get = row
            .try_get::<String, _>("text")
            .expect_err("get non-nullable on taken column must fail");
        let convert_error_get =
            ConvertError::extract(&error_get).expect("should extract ConvertError");
        assert!(
            matches!(convert_error_get, ConvertError::NotNull),
            "expected NotNull error"
        );

        let error_take = row
            .try_take::<String, _>("text")
            .expect_err("take non-nullable on taken column must fail");
        let convert_error_take =
            ConvertError::extract(&error_take).expect("should extract ConvertError");
        assert!(
            matches!(convert_error_take, ConvertError::NotNull),
            "expected NotNull error"
        );
    }

    #[test]
    fn row_try_take_invalid_column() {
        let mut row = Row {
            values: vec!["a".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_a".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let error_not_found = row
            .try_take::<String, _>("nonexistent")
            .expect_err("taking nonexistent column must fail");
        let row_error_not_found =
            RowError::extract(&error_not_found).expect("should extract RowError");
        assert!(
            matches!(row_error_not_found, RowError::ColumnNotFound(column) if column == "nonexistent"),
            "expected ColumnNotFound error"
        );

        let error_out_of_range = row
            .try_take::<String, _>(5)
            .expect_err("taking out of range index must fail");
        let row_error_out_of_range =
            RowError::extract(&error_out_of_range).expect("should extract RowError");
        assert_eq!(
            row_error_out_of_range.to_string(),
            RowError::index_out_of_range(5, 1).to_string(),
            "expected IndexOutOfRange error"
        );
    }

    #[test]
    fn row_try_take_missing_column_type_metadata() {
        let mut row = Row {
            values: vec!["a".to_string().to_value(), "b".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_a".to_string(), "col_b".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let error = row
            .try_take::<String, _>(1)
            .expect_err("taking column with missing metadata type must fail");
        let row_error = RowError::extract(&error).expect("should extract RowError");
        assert_eq!(
            row_error.to_string(),
            RowError::index_out_of_range(1, 1).to_string(),
            "expected IndexOutOfRange for column_types"
        );
        // Column value must not be replaced with null if validation fails beforehand:
        assert_eq!(
            row.values[1],
            "b".to_string().to_value(),
            "column value must not be replaced with null if metadata validation fails"
        );
    }

    #[test]
    fn row_into_values() {
        let row = Row {
            values: vec!["val1".to_string().to_value(), 100_i64.to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["c1".to_string(), "c2".to_string()]),
                column_types: Arc::new(vec![types::string(), types::int64()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let values = row.into_values();
        assert_eq!(values.len(), 2, "expected 2 values");
        assert_eq!(
            values[0],
            "val1".to_string().to_value(),
            "expected matching first value"
        );
        assert_eq!(
            values[1],
            100_i64.to_value(),
            "expected matching second value"
        );
    }

    #[test]
    #[should_panic(
        expected = "failed to take column \"invalid\": cannot deserialize the response Could not find column: 'invalid'"
    )]
    fn row_take_panics_on_invalid_column() {
        let mut row = empty_row();
        let _: String = row.take("invalid");
    }

    #[test]
    #[should_panic(
        expected = "failed to take column 0: cannot deserialize the response Column index out of range: 0 (expected < 0)"
    )]
    fn row_take_panics_on_index_out_of_range() {
        let mut row = empty_row();
        let _: String = row.take(0);
    }

    #[test]
    #[should_panic(
        expected = "failed to take column \"text\": cannot deserialize the response Type conversion error for column 'text' (type String): type mismatch, expected Int64, got String"
    )]
    fn row_take_panics_on_type_mismatch() {
        let mut row = Row {
            values: vec!["not_an_int".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["text".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: i64 = row.take("text");
    }

    #[test]
    fn row_try_take_type_mismatch_replaces_with_null() {
        let mut row = Row {
            values: vec!["not_an_int".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["text".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let error = row
            .try_take::<i64, _>("text")
            .expect_err("taking string as i64 must fail");
        assert!(
            error.is_deserialization(),
            "expected deserialization error on type mismatch"
        );
        assert!(
            row.values[0].is_null(),
            "column value must be replaced with null when try_take consumes it even if conversion fails"
        );
    }

    #[test]
    fn row_try_take_owned_string_index() {
        let mut row = Row {
            values: vec!["hello".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["text".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let result: String = row
            .try_take(String::from("text"))
            .expect("should take column using owned String");
        assert_eq!(result, "hello", "expected column value");
    }

    #[test]
    fn row_into_values_empty() {
        let row = empty_row();
        let values = row.into_values();
        assert!(values.is_empty(), "expected empty values vector");
    }

    #[test]
    #[should_panic(
        expected = "failed to take column \"null_col\": cannot deserialize the response Type conversion error for column 'null_col' (type String): expected non-null value, got null"
    )]
    fn row_take_panics_on_null_for_non_nullable() {
        let mut row = Row {
            values: vec![Value::null()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["null_col".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: String = row.take("null_col");
    }

    #[test]
    fn row_try_take_bytes() {
        let mut row = Row {
            values: vec![b"hello bytes".to_vec().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["bytes_col".to_string()]),
                column_types: Arc::new(vec![types::bytes()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let bytes: Vec<u8> = row
            .try_take("bytes_col")
            .expect("taking bytes column should succeed");
        assert_eq!(bytes, b"hello bytes", "expected matching bytes");
        assert!(row.is_null("bytes_col"), "column must be null after take");
    }

    #[test]
    fn row_try_take_json() {
        let mut row = Row {
            values: vec![r#"{"key":"value"}"#.to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["json_col".to_string()]),
                column_types: Arc::new(vec![types::json()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let json: JsonValue = row
            .try_take("json_col")
            .expect("taking json column should succeed");
        assert_eq!(json, json!({"key": "value"}), "expected matching json");
        assert!(row.is_null("json_col"), "column must be null after take");
    }

    #[test]
    fn row_take_raw_value() {
        let mut row = Row {
            values: vec!["raw string".to_string().to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["raw_col".to_string()]),
                column_types: Arc::new(vec![types::string()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };

        let raw_value: Value = row.take("raw_col");
        assert_eq!(
            raw_value,
            "raw string".to_string().to_value(),
            "expected matching raw Value"
        );
        assert!(row.is_null("raw_col"), "column must be null after take");
    }
}
