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
use crate::row::Row;

/// Converts a query [`Row`] into a Rust data structure.
///
/// # Example
///
/// ```
/// use google_cloud_spanner::Error;
/// use google_cloud_spanner::result::{FromRow, Row};
///
/// struct UserRecord {
///     id: String,
///     age: i64,
///     nickname: Option<String>,
/// }
///
/// impl FromRow for UserRecord {
///     fn from_row(mut row: Row) -> Result<Self, Error> {
///         Ok(Self {
///             id: row.try_take("id")?,
///             age: row.try_take("age")?,
///             nickname: row.try_take("nickname")?,
///         })
///     }
/// }
///
/// // Implementing `TryFrom<Row>` is optional, and enables standard `row.try_into()` conversion:
/// impl TryFrom<Row> for UserRecord {
///     type Error = Error;
///
///     fn try_from(row: Row) -> Result<Self, Self::Error> {
///         Self::from_row(row)
///     }
/// }
///
/// # fn run(row: Row) -> Result<(), Error> {
/// // A row can be converted into the record using `Row::try_into_record`:
/// let user: UserRecord = row.try_into_record()?;
/// println!("User: {}, age: {}", user.id, user.age);
/// # Ok(())
/// # }
/// ```
///
/// This trait is designed for zero-copy row consumption. The implementing method
/// takes ownership of [`Row`], allowing callers to move heap-allocated types
/// (such as [`String`], `Vec<T>`, and `Vec<u8>`) using [`Row::try_take`]
/// without cloning.
///
/// `FromRow` is also implemented for [`Row`] (identity) and standard tuples up to
/// 12 elements (positional indexing). Single-column scalar queries (such as
/// `SELECT COUNT(*)`) can be read using a 1-tuple `(T,)` (e.g. `(i64,)`).
///
/// # Example: Tuples and Scalar Queries
///
/// Standard tuples up to 12 elements implement [`FromRow`] using positional indexing:
///
/// ```
/// # use google_cloud_spanner::Error;
/// # use google_cloud_spanner::result::Row;
/// # fn run(row: Row) -> Result<(), Error> {
/// // Multi-column positional query:
/// let (name, age): (String, i64) = row.try_into_record()?;
/// println!("Name: {name}, Age: {age}");
/// # Ok(())
/// # }
/// # fn scalar(row: Row) -> Result<(), Error> {
/// // Single-column scalar query (e.g. `SELECT COUNT(*)`):
/// let (count,): (i64,) = row.try_into_record()?;
/// println!("Count: {count}");
/// # Ok(())
/// # }
/// ```
pub trait FromRow: Sized {
    /// Converts a Spanner [`Row`] into the target type.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - A required column name or positional index does not exist in the row.
    /// - A column value cannot be converted into the target type.
    /// - A column is `NULL` but the target field is not an [`Option`].
    fn from_row(row: Row) -> Result<Self>;
}

impl FromRow for Row {
    fn from_row(row: Row) -> Result<Self> {
        Ok(row)
    }
}

impl FromRow for () {
    fn from_row(_row: Row) -> Result<Self> {
        Ok(())
    }
}

impl TryFrom<Row> for () {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self> {
        Self::from_row(row)
    }
}

// Implements `FromRow` and `TryFrom<Row>` for standard tuples from 1 to 12 elements.
// Each tuple element is extracted positionally from the row using `row.try_take(index)`.
macro_rules! impl_from_row_for_tuple {
    ( $( $idx:tt => $T:ident ),+ ) => {
        impl<$($T),+> FromRow for ($($T,)+)
        where
            $($T: FromValue,)+
        {
            fn from_row(mut row: Row) -> Result<Self> {
                Ok((
                    $(
                        row.try_take($idx as usize)?,
                    )+
                ))
            }
        }

        impl<$($T),+> TryFrom<Row> for ($($T,)+)
        where
            $($T: FromValue,)+
        {
            type Error = Error;

            fn try_from(row: Row) -> Result<Self> {
                Self::from_row(row)
            }
        }
    };
}

impl_from_row_for_tuple!(0 => T0);
impl_from_row_for_tuple!(0 => T0, 1 => T1);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7, 8 => T8);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7, 8 => T8, 9 => T9);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7, 8 => T8, 9 => T9, 10 => T10);
impl_from_row_for_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7, 8 => T8, 9 => T9, 10 => T10, 11 => T11);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::error::RowError;
    use crate::result_set_metadata::ResultSetMetadata;
    use crate::to_value::ToValue;
    use crate::types::{self, Type};
    use crate::value::Value;
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::sync::Arc;
    use wkt::Timestamp;

    #[derive(Debug, PartialEq)]
    struct UserRecord {
        id: String,
        age: i64,
        active: bool,
        nickname: Option<String>,
        created_at: Timestamp,
    }

    impl FromRow for UserRecord {
        fn from_row(mut row: Row) -> Result<Self> {
            Ok(Self {
                id: row.try_take("id")?,
                age: row.try_take("age")?,
                active: row.try_take("active")?,
                nickname: row.try_take("nickname")?,
                created_at: row.try_take("created_at")?,
            })
        }
    }

    impl TryFrom<Row> for UserRecord {
        type Error = Error;

        fn try_from(row: Row) -> Result<Self> {
            Self::from_row(row)
        }
    }

    #[derive(Debug, PartialEq)]
    struct Coordinates(f64, f64);

    impl FromRow for Coordinates {
        fn from_row(mut row: Row) -> Result<Self> {
            Ok(Self(row.try_take(0usize)?, row.try_take(1usize)?))
        }
    }

    impl TryFrom<Row> for Coordinates {
        type Error = Error;

        fn try_from(row: Row) -> Result<Self> {
            Self::from_row(row)
        }
    }

    fn create_row(columns: Vec<(&str, Type, Value)>) -> Row {
        let mut names = Vec::with_capacity(columns.len());
        let mut column_types = Vec::with_capacity(columns.len());
        let mut values = Vec::with_capacity(columns.len());

        for (name, column_type, value) in columns {
            names.push(name.to_string());
            column_types.push(column_type);
            values.push(value);
        }

        Row {
            values,
            metadata: ResultSetMetadata {
                column_names: Arc::new(names),
                column_types: Arc::new(column_types),
                undeclared_parameters: Arc::new(BTreeMap::new()),
            },
        }
    }

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(UserRecord: FromRow, TryFrom<Row>, Debug, PartialEq, Send, Sync);
        static_assertions::assert_impl_all!(Coordinates: FromRow, TryFrom<Row>, Debug, PartialEq, Send, Sync);
        static_assertions::assert_impl_all!(Row: FromRow, TryFrom<Row>);
        static_assertions::assert_impl_all!((): FromRow, TryFrom<Row>);
        static_assertions::assert_impl_all!((String,): FromRow, TryFrom<Row>);
        static_assertions::assert_impl_all!((String, i64): FromRow, TryFrom<Row>);
        static_assertions::assert_impl_all!((String, i64, bool, f64): FromRow, TryFrom<Row>);
    }

    #[test]
    fn row_identity() -> Result<()> {
        let row = create_row(vec![(
            "name",
            types::string(),
            "alice".to_string().to_value(),
        )]);
        let extracted_row = Row::from_row(row.clone())?;
        assert_eq!(
            extracted_row, row,
            "Row::from_row must return the row unmodified"
        );
        Ok(())
    }

    #[test]
    fn unit_tuple() -> Result<()> {
        let row = create_row(vec![("col", types::int64(), 100_i64.to_value())]);
        assert_eq!(
            <()>::from_row(row.clone())?,
            (),
            "()::from_row must return ()"
        );

        let unit_try_from: Result<()> = row.try_into();
        assert!(unit_try_from.is_ok(), "row.try_into() for () must succeed");
        Ok(())
    }

    #[test]
    fn tuple_1_from_row() -> Result<()> {
        let row = create_row(vec![(
            "name",
            types::string(),
            "test-name".to_string().to_value(),
        )]);
        let (name,) = <(String,)>::from_row(row)?;
        assert_eq!(name, "test-name", "1-tuple must extract first column");
        Ok(())
    }

    #[test]
    fn tuple_2_from_row() -> Result<()> {
        let row = create_row(vec![
            ("first", types::string(), "val1".to_string().to_value()),
            ("second", types::int64(), 42_i64.to_value()),
        ]);
        let (first, second) = <(String, i64)>::from_row(row)?;
        assert_eq!(first, "val1", "first element must match index 0");
        assert_eq!(second, 42, "second element must match index 1");
        Ok(())
    }

    #[test]
    fn tuple_3_with_option() -> Result<()> {
        let row = create_row(vec![
            ("str", types::string(), "hello".to_string().to_value()),
            ("opt", types::int64(), Value::null()),
            ("flag", types::bool(), true.to_value()),
        ]);
        let (text, maybe_number, flag) = <(String, Option<i64>, bool)>::from_row(row)?;
        assert_eq!(text, "hello", "text column must match");
        assert_eq!(
            maybe_number, None,
            "null column must deserialize to None in tuple"
        );
        assert!(flag, "boolean flag must be true");
        Ok(())
    }

    #[test]
    fn tuple_12_from_row() -> Result<()> {
        let row = create_row(vec![
            ("c0", types::int64(), 0_i64.to_value()),
            ("c1", types::int64(), 1_i64.to_value()),
            ("c2", types::int64(), 2_i64.to_value()),
            ("c3", types::int64(), 3_i64.to_value()),
            ("c4", types::int64(), 4_i64.to_value()),
            ("c5", types::int64(), 5_i64.to_value()),
            ("c6", types::int64(), 6_i64.to_value()),
            ("c7", types::int64(), 7_i64.to_value()),
            ("c8", types::int64(), 8_i64.to_value()),
            ("c9", types::int64(), 9_i64.to_value()),
            ("c10", types::int64(), 10_i64.to_value()),
            ("c11", types::int64(), 11_i64.to_value()),
        ]);
        type TwelveTuple = (i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64);
        let result = TwelveTuple::from_row(row)?;
        assert_eq!(
            result,
            (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
            "12-element tuple must extract all 12 positional columns"
        );
        Ok(())
    }

    #[test]
    fn tuple_try_from() -> Result<()> {
        let row = create_row(vec![
            ("first", types::string(), "alice".to_string().to_value()),
            ("second", types::int64(), 100_i64.to_value()),
        ]);
        let (first, second): (String, i64) = row.try_into()?;
        assert_eq!(first, "alice", "first element must match via TryInto");
        assert_eq!(second, 100, "second element must match via TryInto");
        Ok(())
    }

    #[test]
    fn tuple_index_out_of_range() {
        let row = create_row(vec![(
            "only_one",
            types::string(),
            "val".to_string().to_value(),
        )]);
        let error = <(String, i64)>::from_row(row)
            .expect_err("tuple extraction must fail if row has fewer columns than tuple elements");
        let row_error = RowError::extract(&error).expect("error chain must contain a RowError");
        match row_error {
            RowError::IndexOutOfRange { index, len } => {
                assert_eq!(*index, 1, "expected index 1 to be out of range");
                assert_eq!(*len, 1, "expected row length to be 1");
            }
            other => panic!("expected RowError::IndexOutOfRange, got: {other:?}"),
        }
    }

    #[test]
    fn tuple_type_mismatch() {
        let row = create_row(vec![
            ("str", types::string(), "abc".to_string().to_value()),
            (
                "not_int",
                types::string(),
                "not-a-number".to_string().to_value(),
            ),
        ]);
        let error = <(String, i64)>::from_row(row)
            .expect_err("tuple extraction must fail when column type mismatches");
        let row_error = RowError::extract(&error).expect("error chain must contain a RowError");
        match row_error {
            RowError::TypeConversion { column, .. } => {
                assert_eq!(
                    column, "not_int",
                    "expected conversion error on column 'not_int'"
                );
            }
            other => panic!("expected RowError::TypeConversion, got: {other:?}"),
        }
    }

    #[test]
    fn manual_struct_from_row() -> Result<()> {
        let timestamp = Timestamp::clamp(1_700_000_000, 0);
        let row = create_row(vec![
            ("id", types::string(), "user-123".to_string().to_value()),
            ("age", types::int64(), 32_i64.to_value()),
            ("active", types::bool(), true.to_value()),
            ("nickname", types::string(), "ace".to_string().to_value()),
            ("created_at", types::timestamp(), timestamp.to_value()),
        ]);

        let record = UserRecord::from_row(row)?;
        assert_eq!(
            record,
            UserRecord {
                id: "user-123".to_string(),
                age: 32,
                active: true,
                nickname: Some("ace".to_string()),
                created_at: timestamp,
            },
            "UserRecord parsed via FromRow::from_row must match expected values"
        );
        Ok(())
    }

    #[test]
    fn manual_struct_with_null_optional() -> Result<()> {
        let timestamp = Timestamp::clamp(1_700_000_000, 0);
        let row = create_row(vec![
            ("id", types::string(), "user-456".to_string().to_value()),
            ("age", types::int64(), 28_i64.to_value()),
            ("active", types::bool(), false.to_value()),
            ("nickname", types::string(), Value::null()),
            ("created_at", types::timestamp(), timestamp.to_value()),
        ]);

        let record = UserRecord::from_row(row)?;
        assert_eq!(
            record.nickname, None,
            "null column must deserialize to None for Option field"
        );
        assert_eq!(record.id, "user-456", "id must match");
        Ok(())
    }

    #[test]
    fn manual_struct_try_from() -> Result<()> {
        let timestamp = Timestamp::clamp(1_700_000_000, 0);
        let row = create_row(vec![
            ("id", types::string(), "user-789".to_string().to_value()),
            ("age", types::int64(), 45_i64.to_value()),
            ("active", types::bool(), true.to_value()),
            ("nickname", types::string(), Value::null()),
            ("created_at", types::timestamp(), timestamp.to_value()),
        ]);

        let record: UserRecord = row.try_into()?;
        assert_eq!(record.id, "user-789", "id must match via TryInto");
        assert_eq!(record.age, 45, "age must match via TryInto");
        assert!(record.active, "active must be true via TryInto");
        assert_eq!(record.nickname, None, "nickname must be None via TryInto");
        Ok(())
    }

    #[test]
    fn tuple_struct_from_row() -> Result<()> {
        let row = create_row(vec![
            ("latitude", types::float64(), 37.7749_f64.to_value()),
            ("longitude", types::float64(), (-122.4194_f64).to_value()),
        ]);

        let coordinates = Coordinates::from_row(row)?;
        assert_eq!(
            coordinates,
            Coordinates(37.7749, -122.4194),
            "Coordinates parsed positionally via FromRow must match expected values"
        );
        Ok(())
    }

    #[test]
    fn tuple_struct_try_from() -> Result<()> {
        let row = create_row(vec![
            ("latitude", types::float64(), 37.7749_f64.to_value()),
            ("longitude", types::float64(), (-122.4194_f64).to_value()),
        ]);

        let coordinates: Coordinates = row.try_into()?;
        assert_eq!(
            coordinates,
            Coordinates(37.7749, -122.4194),
            "Coordinates parsed positionally via TryInto must match expected values"
        );
        Ok(())
    }

    #[test]
    fn manual_struct_out_of_order_columns() -> Result<()> {
        let timestamp = Timestamp::clamp(1_700_000_000, 0);
        let row = create_row(vec![
            ("nickname", types::string(), "ace".to_string().to_value()),
            ("age", types::int64(), 32_i64.to_value()),
            ("active", types::bool(), true.to_value()),
            ("id", types::string(), "user-123".to_string().to_value()),
            ("created_at", types::timestamp(), timestamp.to_value()),
        ]);

        let record = UserRecord::from_row(row)?;
        assert_eq!(
            record,
            UserRecord {
                id: "user-123".to_string(),
                age: 32,
                active: true,
                nickname: Some("ace".to_string()),
                created_at: timestamp,
            },
            "UserRecord parsed via FromRow must resolve columns by name regardless of order"
        );
        Ok(())
    }

    #[test]
    fn missing_column_error() {
        let timestamp = Timestamp::clamp(1_700_000_000, 0);
        let row = create_row(vec![
            ("id", types::string(), "user-123".to_string().to_value()),
            ("age", types::int64(), 32_i64.to_value()),
            // "active" column is intentionally omitted
            ("nickname", types::string(), "ace".to_string().to_value()),
            ("created_at", types::timestamp(), timestamp.to_value()),
        ]);

        let error = UserRecord::from_row(row)
            .expect_err("from_row must fail when a required column is missing");
        let row_error = RowError::extract(&error).expect("error chain must contain a RowError");
        match row_error {
            RowError::ColumnNotFound(column) => {
                assert_eq!(
                    column, "active",
                    "expected missing column name to be 'active'"
                );
            }
            other => panic!("expected RowError::ColumnNotFound, got: {other:?}"),
        }
    }

    #[test]
    fn type_mismatch_error() {
        let timestamp = Timestamp::clamp(1_700_000_000, 0);
        let row = create_row(vec![
            ("id", types::string(), "user-123".to_string().to_value()),
            // "age" is given as STRING instead of INT64
            (
                "age",
                types::string(),
                "not-a-number".to_string().to_value(),
            ),
            ("active", types::bool(), true.to_value()),
            ("nickname", types::string(), "ace".to_string().to_value()),
            ("created_at", types::timestamp(), timestamp.to_value()),
        ]);

        let error =
            UserRecord::from_row(row).expect_err("from_row must fail when column type mismatches");
        let row_error = RowError::extract(&error).expect("error chain must contain a RowError");
        match row_error {
            RowError::TypeConversion { column, .. } => {
                assert_eq!(column, "age", "expected conversion error on column 'age'");
            }
            other => panic!("expected RowError::TypeConversion, got: {other:?}"),
        }
    }

    #[test]
    fn null_for_non_optional_field_error() {
        let timestamp = Timestamp::clamp(1_700_000_000, 0);
        let row = create_row(vec![
            // "id" is NULL but UserRecord::id is String (not Option<String>)
            ("id", types::string(), Value::null()),
            ("age", types::int64(), 32_i64.to_value()),
            ("active", types::bool(), true.to_value()),
            ("nickname", types::string(), "ace".to_string().to_value()),
            ("created_at", types::timestamp(), timestamp.to_value()),
        ]);

        let error = UserRecord::from_row(row)
            .expect_err("from_row must fail when non-optional column is null");
        let row_error = RowError::extract(&error).expect("error chain must contain a RowError");
        match row_error {
            RowError::TypeConversion { column, .. } => {
                assert_eq!(column, "id", "expected conversion error on column 'id'");
            }
            other => panic!("expected RowError::TypeConversion, got: {other:?}"),
        }
    }

    #[test]
    fn positional_index_out_of_range_error() {
        // Only 1 column provided, but Coordinates expects 2 positional columns (0 and 1)
        let row = create_row(vec![("latitude", types::float64(), 37.7749_f64.to_value())]);

        let error = Coordinates::from_row(row)
            .expect_err("from_row must fail when positional index is out of bounds");
        let row_error = RowError::extract(&error).expect("error chain must contain a RowError");
        match row_error {
            RowError::IndexOutOfRange { index, len } => {
                assert_eq!(*index, 1, "expected index 1 to be out of range");
                assert_eq!(*len, 1, "expected row length to be 1");
            }
            other => panic!("expected RowError::IndexOutOfRange, got: {other:?}"),
        }
    }

    #[test]
    fn struct_ignores_extra_columns() -> Result<()> {
        let timestamp = Timestamp::clamp(1_700_000_000, 0);
        let row = create_row(vec![
            ("id", types::string(), "user-123".to_string().to_value()),
            ("age", types::int64(), 32_i64.to_value()),
            ("active", types::bool(), true.to_value()),
            ("nickname", types::string(), "ace".to_string().to_value()),
            ("created_at", types::timestamp(), timestamp.to_value()),
            (
                "extra_column_1",
                types::string(),
                "unused".to_string().to_value(),
            ),
            ("extra_column_2", types::int64(), 999_i64.to_value()),
        ]);

        let record = UserRecord::from_row(row)?;
        assert_eq!(
            record.id, "user-123",
            "id should match despite extra columns"
        );
        assert_eq!(record.age, 32, "age should match despite extra columns");
        assert!(record.active, "active should match despite extra columns");
        Ok(())
    }
}
