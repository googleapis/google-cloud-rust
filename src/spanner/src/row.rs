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

use crate::result_set_metadata::ResultSetMetadata;
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
}

/// A trait for types that can be used to index into a [`Row`].
///
/// This trait is sealed and cannot be implemented for types outside of this crate.
pub trait ColumnIndex: private::Sealed + std::fmt::Debug {
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
        row.metadata
            .column_names
            .iter()
            .position(|name| name == self)
    }
}

/// Errors that can occur when getting a value from a [`Row`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RowError {
    /// The requested column name or index was not found in the row.
    #[error("Could not find column with index: {0}")]
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

    /// Retrieves a value from the row by column name or zero-based index.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # async fn test_doc() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Spanner::builder().build().await?;
    /// let db = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db.single_use().build();
    /// let mut rs = tx.execute_query(Statement::builder("SELECT 42 AS Age").build()).await?;
    ///
    /// if let Some(row) = rs.next().await.transpose()? {
    ///     let age: i64 = row.try_get("Age")?;
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
    pub fn try_get<T: crate::from_value::FromValue, I: ColumnIndex>(
        &self,
        index: I,
    ) -> crate::Result<T> {
        let idx = index
            .index(self)
            .ok_or_else(|| crate::Error::deser(RowError::ColumnNotFound(format!("{:?}", index))))?;
        let value = self.values.get(idx).ok_or_else(|| {
            crate::Error::deser(RowError::IndexOutOfRange {
                index: idx,
                len: self.values.len(),
            })
        })?;
        let type_ = self.metadata.column_types.get(idx).ok_or_else(|| {
            crate::Error::deser(RowError::IndexOutOfRange {
                index: idx,
                len: self.metadata.column_types.len(),
            })
        })?;
        T::from_value(value, type_).map_err(crate::Error::deser)
    }

    /// Retrieves a value from the row by column name or zero-based index, panicking on error.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # async fn test_doc() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Spanner::builder().build().await?;
    /// let db = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db.single_use().build();
    /// let mut rs = tx.execute_query(Statement::builder("SELECT 42 AS Age").build()).await?;
    ///
    /// if let Some(row) = rs.next().await.transpose()? {
    ///     let age: i64 = row.get("Age");
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
        self.try_get(index).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::to_value::ToValue;
    use crate::types;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use time::{Date, Month, OffsetDateTime};

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(Row: Clone, std::fmt::Debug, PartialEq, Send, Sync);
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

        let d = Decimal::from_str_exact("123.456").unwrap();
        let dt = Date::from_calendar_date(2023, Month::October, 27).unwrap();
        let ts = OffsetDateTime::parse(
            "2023-10-27T10:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();

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
            },
        };

        // Test getting by valid index
        assert_eq!(row.get::<String, _>(0), "hello");
        assert_eq!(row.get::<i64, _>(1), 42);
        assert_eq!(row.get::<f64, _>(2), 42.5);
        assert!(row.get::<bool, _>(3));
        assert_eq!(row.get::<Vec<u8>, _>(4), vec![1_u8, 2, 3]);
        assert_eq!(row.get::<Decimal, _>(5), d);
        assert_eq!(row.get::<Date, _>(6), dt);
        assert_eq!(row.get::<OffsetDateTime, _>(7), ts);
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
        assert!(row.get::<bool, _>("col_bool"));
        assert_eq!(row.get::<Vec<u8>, _>("col_bytes"), vec![1_u8, 2, 3]);
        assert_eq!(row.get::<Decimal, _>("col_numeric"), d);
        assert_eq!(row.get::<Date, _>("col_date"), dt);
        assert_eq!(row.get::<OffsetDateTime, _>("col_timestamp"), ts);
        assert_eq!(row.get::<f32, _>("col_float32"), 1.23_f32);
        assert_eq!(row.get::<String, _>("col_json"), "{\"key\":\"value\"}");
        assert_eq!(
            row.get::<String, _>("col_uuid"),
            "123e4567-e89b-12d3-a456-426614174000"
        );
        assert_eq!(row.get::<String, _>("col_interval"), "P1Y2M3D");

        // Test getting by invalid index
        assert!(row.try_get::<String, _>(12).is_err());

        // Test getting by invalid name
        assert!(row.try_get::<String, _>("col_invalid").is_err());

        // Test getting mismatched type
        assert!(row.try_get::<i64, _>(0).is_err());
        assert!(row.try_get::<bool, _>(1).is_err());

        // int64 is encoded as a string, so getting it as a string is also possible.
        assert_eq!(row.get::<String, _>(1), "42");
    }
}
