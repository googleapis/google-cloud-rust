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

use crate::Error;
use crate::result_set_metadata::ResultSetMetadata;
use crate::value::Value;

/// A row in a query result.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub(crate) values: Vec<Value>,
    pub(crate) metadata: ResultSetMetadata,
}

pub(crate) mod sealed {
    use super::Row;

    /// A sealed trait to prevent external implementation of `ColumnIndex`.
    pub trait ColumnIndex {
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

    impl ColumnIndex for &String {
        fn index(&self, row: &Row) -> Option<usize> {
            self.as_str().index(row)
        }
    }
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
/// Supported index types are `usize`, `&str`, `String`, and `&String`.
pub trait ColumnIndex: sealed::ColumnIndex + Display + Debug {}

impl ColumnIndex for usize {}
impl ColumnIndex for &str {}
impl ColumnIndex for String {}
impl ColumnIndex for &String {}

/// Errors that can occur when getting a value from a [`Row`].
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RowError {
    /// The requested column name was not found in the row.
    #[error("could not find column: {0}")]
    ColumnNotFound(String),
    /// The requested column index was out of range.
    #[error("column index out of range: {index} (expected < {len})")]
    IndexOutOfRange {
        /// The index that was requested.
        index: usize,
        /// The total number of columns in the row.
        len: usize,
    },
}

impl RowError {
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
    ///                 RowError::IndexOutOfRange { index, len } => {
    ///                     println!("Index {index} out of range (length {len})");
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
    /// fails or an index is out of bounds. This method downcasts the immediate error source.
    pub fn extract(err: &Error) -> Option<&Self> {
        err.source()
            .and_then(|source| source.downcast_ref::<Self>())
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
    pub fn try_is_null<I: ColumnIndex>(&self, index: I) -> crate::Result<bool> {
        let (_, value) = self.get_value(index)?;
        Ok(value.kind() == crate::value::Kind::Null)
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
        self.try_is_null(index).expect("invalid column index")
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
    ///     * The column value is incompatible with type `T`. The underlying [`ConvertError`][crate::error::ConvertError] can be extracted using [`ConvertError::extract`][crate::error::ConvertError::extract].
    pub fn try_get<T: crate::from_value::FromValue, I: ColumnIndex>(
        &self,
        index: I,
    ) -> crate::Result<T> {
        let (idx, value) = self.get_value(index)?;
        let r#type = self.metadata.column_types.get(idx).ok_or_else(|| {
            crate::Error::deser(RowError::IndexOutOfRange {
                index: idx,
                len: self.metadata.column_types.len(),
            })
        })?;
        T::from_value(value, r#type).map_err(crate::Error::deser)
    }

    /// Retrieves a value from the row by column name or zero-based index, panicking on error.
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
    /// * The column value is incompatible with type `T`.
    pub fn get<T: crate::from_value::FromValue, I: ColumnIndex>(&self, index: I) -> T {
        self.try_get(index)
            .expect("column not found or type mismatch")
    }

    fn get_value<I: ColumnIndex>(&self, index: I) -> crate::Result<(usize, &Value)> {
        let idx = index
            .index(self)
            .ok_or_else(|| crate::Error::deser(RowError::ColumnNotFound(format!("{index}"))))?;
        let value = self.values.get(idx).ok_or_else(|| {
            crate::Error::deser(RowError::IndexOutOfRange {
                index: idx,
                len: self.values.len(),
            })
        })?;
        Ok((idx, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::to_value::ToValue;
    use crate::types;
    use rust_decimal::Decimal;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use time::{Date, Month, OffsetDateTime};

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(Row: Clone, Debug, PartialEq, Send, Sync);
        static_assertions::assert_impl_all!(RowError: Clone, Debug, PartialEq, Eq, Send, Sync);
    }

    #[test]
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

        let d = Decimal::from_str_exact("123.456").expect("valid decimal");
        let dt = Date::from_calendar_date(2023, Month::October, 27).expect("valid date");
        let ts = OffsetDateTime::parse(
            "2023-10-27T10:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("valid timestamp");

        let values = vec![
            "hello".to_string().to_value(),
            42_i64.to_value(),
            42.5_f64.to_value(),
            true.to_value(),
            vec![1_u8, 2, 3].to_value(),
            d.to_value(),
            dt.to_value(),
            ts.to_value(),
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
                undeclared_parameters: Arc::new(std::collections::BTreeMap::new()),
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
        assert!(row.get::<bool, _>(3), "expected bool at index 3 to be true");
        assert_eq!(
            row.get::<Vec<u8>, _>(4),
            vec![1_u8, 2, 3],
            "expected bytes at index 4"
        );
        assert_eq!(row.get::<Decimal, _>(5), d, "expected numeric at index 5");
        assert_eq!(row.get::<Date, _>(6), dt, "expected date at index 6");
        assert_eq!(
            row.get::<OffsetDateTime, _>(7),
            ts,
            "expected timestamp at index 7"
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
            "expected col_bool by name to be true"
        );
        assert_eq!(
            row.get::<Vec<u8>, _>("col_bytes"),
            vec![1_u8, 2, 3],
            "expected col_bytes by name"
        );
        assert_eq!(
            row.get::<Decimal, _>("col_numeric"),
            d,
            "expected col_numeric by name"
        );
        assert_eq!(
            row.get::<Date, _>("col_date"),
            dt,
            "expected col_date by name"
        );
        assert_eq!(
            row.get::<OffsetDateTime, _>("col_timestamp"),
            ts,
            "expected col_timestamp by name"
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
            "expected error for index out of range"
        );

        // Test getting by invalid name
        assert!(
            row.try_get::<String, _>("col_invalid").is_err(),
            "expected error for invalid column name"
        );

        // Test getting mismatched type
        assert!(
            row.try_get::<i64, _>(0).is_err(),
            "expected error for mismatched type i64"
        );
        assert!(
            row.try_get::<bool, _>(1).is_err(),
            "expected error for mismatched type bool"
        );

        // int64 is encoded as a string, so getting it as a string is also possible.
        assert_eq!(
            row.get::<String, _>(1),
            "42",
            "expected int64 converted to string"
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
        assert_eq!(
            *extracted_missing,
            RowError::ColumnNotFound("nonexistent".to_string()),
            "expected ColumnNotFound variant"
        );
        assert_eq!(
            extracted_missing.clone(),
            *extracted_missing,
            "expected cloned RowError to equal original"
        );
        assert_eq!(
            extracted_missing.to_string(),
            "could not find column: nonexistent",
            "expected 'could not find column: nonexistent' display string"
        );

        let err_out_of_range = row.try_get::<String, _>(5).expect_err("index out of range");
        let extracted_out_of_range =
            RowError::extract(&err_out_of_range).expect("should extract RowError");
        assert_eq!(
            *extracted_out_of_range,
            RowError::IndexOutOfRange { index: 5, len: 1 },
            "expected IndexOutOfRange variant"
        );
    }

    #[test]
    fn generic_column_index_helper() {
        fn fetch_value<I: ColumnIndex>(row: &Row, index: I) -> crate::Result<String> {
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
}
