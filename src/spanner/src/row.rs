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

use crate::Error;
use crate::Result;
use crate::from_value::FromValue;
use crate::result_set_metadata::ResultSetMetadata;
use crate::value::Value;
use std::fmt::{Debug, Display};

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
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RowError {
    /// The requested column name was not found in the row.
    #[error("Could not find column: '{0}'")]
    ColumnNotFound(String),
    /// The requested column index was out of range.
    #[error("Column index out of range: {index} (expected < {len})")]
    IndexOutOfRange { index: usize, len: usize },
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
        let (_, value) = self.get_value(index)?;
        Ok(value.is_null())
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
    ///     * The column name or index is invalid.
    ///     * The column value is incompatible with type `T`.
    pub fn try_get<T: FromValue, I: ColumnIndex>(&self, index: I) -> Result<T> {
        let (column_index, value) = self.get_value(index)?;
        let r#type = self
            .metadata
            .column_types
            .get(column_index)
            .ok_or_else(|| {
                Error::deser(RowError::IndexOutOfRange {
                    index: column_index,
                    len: self.metadata.column_types.len(),
                })
            })?;
        T::from_value(value, r#type).map_err(Error::deser)
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
    /// * The column value is incompatible with type `T` (or is null when `T` is not an [`Option`]).
    pub fn get<T: FromValue, I: ColumnIndex>(&self, index: I) -> T {
        match self.try_get(&index) {
            Ok(value) => value,
            Err(error) => panic!("failed to retrieve column {index:?}: {error}"),
        }
    }

    fn get_value<I: ColumnIndex>(&self, index: I) -> Result<(usize, &Value)> {
        let column_index = index
            .index(self)
            .ok_or_else(|| Error::deser(RowError::ColumnNotFound(index.to_string())))?;
        let value = self.values.get(column_index).ok_or_else(|| {
            Error::deser(RowError::IndexOutOfRange {
                index: column_index,
                len: self.values.len(),
            })
        })?;
        Ok((column_index, value))
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

    fn empty_row() -> Row {
        Row {
            values: Vec::new(),
            metadata: ResultSetMetadata::new(None),
        }
    }

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(Row: Clone, Debug, PartialEq, Send, Sync);
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
        let date = Date::from_calendar_date(2023, Month::October, 27).expect("valid date");
        let timestamp = OffsetDateTime::parse(
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
            decimal.to_value(),
            date.to_value(),
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
        assert_eq!(row.get::<String, _>(0), "hello");
        assert_eq!(row.get::<i64, _>(1), 42);
        assert_eq!(row.get::<f64, _>(2), 42.5);
        assert!(
            row.get::<bool, _>(3),
            "expected bool value at index 3 to be true"
        );
        assert_eq!(row.get::<Vec<u8>, _>(4), vec![1_u8, 2, 3]);
        assert_eq!(row.get::<Decimal, _>(5), decimal);
        assert_eq!(row.get::<Date, _>(6), date);
        assert_eq!(row.get::<OffsetDateTime, _>(7), timestamp);
        assert_eq!(row.get::<f32, _>(8), 1.23_f32);
        assert_eq!(row.get::<String, _>(9), "{\"key\":\"value\"}");
        assert_eq!(
            row.get::<String, _>(10),
            "123e4567-e89b-12d3-a456-426614174000"
        );
        assert_eq!(row.get::<String, _>(11), "P1Y2M3D");

        // Test getting by valid name
        assert_eq!(row.get::<String, _>("col_string"), "hello");
        assert_eq!(row.get::<i64, _>("col_int64"), 42);
        assert_eq!(row.get::<f64, _>("col_float64"), 42.5);
        assert!(
            row.get::<bool, _>("col_bool"),
            "expected col_bool to be true"
        );
        assert_eq!(row.get::<Vec<u8>, _>("col_bytes"), vec![1_u8, 2, 3]);
        assert_eq!(row.get::<Decimal, _>("col_numeric"), decimal);
        assert_eq!(row.get::<Date, _>("col_date"), date);
        assert_eq!(row.get::<OffsetDateTime, _>("col_timestamp"), timestamp);
        assert_eq!(row.get::<f32, _>("col_float32"), 1.23_f32);
        assert_eq!(row.get::<String, _>("col_json"), "{\"key\":\"value\"}");
        assert_eq!(
            row.get::<String, _>("col_uuid"),
            "123e4567-e89b-12d3-a456-426614174000"
        );
        assert_eq!(row.get::<String, _>("col_interval"), "P1Y2M3D");

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

        // int64 is encoded as a string, so getting it as a string is also possible.
        assert_eq!(row.get::<String, _>(1), "42");

        // Test getting with reference index types (&usize, &&str, &String) and owned String
        assert_eq!(row.get::<String, _>(&0), "hello");
        assert_eq!(row.get::<String, _>(&"col_string"), "hello");
        let column_name = "col_string".to_string();
        assert_eq!(row.get::<String, _>(&column_name), "hello");
        assert_eq!(row.get::<String, _>(column_name), "hello");
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
        expected = "failed to retrieve column \"col_string\": cannot deserialize the response cannot convert value, source=invalid digit found in string"
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
        expected = "failed to retrieve column 0: cannot deserialize the response cannot convert value, source=invalid digit found in string"
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
        expected = "failed to retrieve column \"col_bool\": cannot deserialize the response expected String, got Bool"
    )]
    fn row_get_panics_on_kind_mismatch() {
        let row = Row {
            values: vec![true.to_value()],
            metadata: ResultSetMetadata {
                column_names: Arc::new(vec!["col_bool".to_string()]),
                column_types: Arc::new(vec![types::bool()]),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        };
        let _: i64 = row.get("col_bool");
    }

    #[test]
    #[should_panic(
        expected = "failed to retrieve column \"null_col\": cannot deserialize the response expected non-null value, got null"
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
}
