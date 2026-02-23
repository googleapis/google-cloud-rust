use crate::google::spanner::v1::Type;
use crate::value::FromValue;
use prost_types::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// A single row of results from a Spanner query.
///
/// `Row` provides access to column values by name or index.
///
/// # Examples
///
/// ```rust
/// # use google_cloud_spanner::row::Row;
/// # use google_cloud_spanner::google::spanner::v1::Type;
///
/// fn process_row(row: Row) -> Result<(), Box<dyn std::error::Error>> {
///     // Get value by column name
///     let name: String = row.try_get("name")?;
///     
///     // Get value by column index (zero-based)
///     let age: i64 = row.try_get(1)?;
///
///     // Handle nullable columns
///     let email: Option<String> = row.try_get("email")?;
///
///     // Get raw protobuf value
///     let raw_value: &prost_types::Value = row.get("name");
///
///     Ok(())
/// }
/// ```
#[derive(Debug, PartialEq)]
pub struct Row {
    pub(crate) raw_values: Vec<Value>,
    pub(crate) column_names: Arc<HashMap<String, usize>>,
    pub(crate) column_types: Arc<Vec<Type>>,
}

pub trait ColumnIndex: std::fmt::Debug {
    fn index(&self, row: &Row) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn index(&self, _row: &Row) -> Option<usize> {
        Some(*self)
    }
}

impl ColumnIndex for &str {
    fn index(&self, row: &Row) -> Option<usize> {
        row.column_names.get(*self).cloned()
    }
}

impl ColumnIndex for String {
    fn index(&self, row: &Row) -> Option<usize> {
        row.column_names.get(self).cloned()
    }
}

impl Row {
    /// Retrieves a value from the row by column name or index.
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
    pub fn try_get<'a, T: FromValue<'a>, I: ColumnIndex>(&'a self, index: I) -> crate::Result<T> {
        let idx = index.index(self).ok_or_else(|| {
            crate::Error::service(
                google_cloud_gax::error::rpc::Status::default()
                    .set_code(google_cloud_gax::error::rpc::Code::InvalidArgument)
                    .set_message(format!("could not find column with index: {:?}", index)),
            )
        })?;
        let value = self.raw_values.get(idx).ok_or_else(|| {
            crate::Error::service(
                google_cloud_gax::error::rpc::Status::default()
                    .set_code(google_cloud_gax::error::rpc::Code::InvalidArgument)
                    .set_message(format!(
                        "column index out of range: {:?} (expected < {})",
                        idx,
                        self.raw_values.len()
                    )),
            )
        })?;
        let type_ = self.column_types.get(idx).ok_or_else(|| {
            crate::Error::service(
                google_cloud_gax::error::rpc::Status::default()
                    .set_code(google_cloud_gax::error::rpc::Code::InvalidArgument)
                    .set_message(format!(
                        "column type out of range: {:?} (expected < {})",
                        idx,
                        self.column_types.len()
                    )),
            )
        })?;
        T::from_value(value, type_)
    }

    /// Retrieves a value from the row by column name or index, panicking on error.
    ///
    /// This is a convenience wrapper around [`try_get`](Row::try_get).
    ///
    /// # Panics
    ///
    /// Panics if:
    /// * The column name or index is invalid.
    /// * The column value is incompatible with type `T`.
    pub fn get<'a, T: FromValue<'a>, I: ColumnIndex>(&'a self, index: I) -> T {
        self.try_get(index).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::spanner::v1::TypeCode;
    use bigdecimal::BigDecimal;
    use std::str::FromStr;
    use std::time::SystemTime;

    fn string_val(s: &str) -> Value {
        Value {
            kind: Some(prost_types::value::Kind::StringValue(s.to_string())),
        }
    }

    fn bool_val(b: bool) -> Value {
        Value {
            kind: Some(prost_types::value::Kind::BoolValue(b)),
        }
    }

    fn bytes_type() -> Type {
        Type {
            code: TypeCode::Bytes as i32,
            array_element_type: None,
            struct_type: None,
            type_annotation: 0,
            proto_type_fqn: "".to_string(),
        }
    }

    fn proto_type() -> Type {
        Type {
            code: TypeCode::Proto as i32,
            array_element_type: None,
            struct_type: None,
            type_annotation: 0,
            proto_type_fqn: "some.proto.Message".to_string(),
        }
    }



    fn numeric_type() -> Type {
        Type {
            code: TypeCode::Numeric as i32,
            array_element_type: None,
            struct_type: None,
            type_annotation: 0,
            proto_type_fqn: "".to_string(),
        }
    }


    fn timestamp_type() -> Type {
        Type {
            code: TypeCode::Timestamp as i32,
            array_element_type: None,
            struct_type: None,
            type_annotation: 0,
            proto_type_fqn: "".to_string(),
        }
    }
    fn date_type() -> Type {
        Type {
            code: TypeCode::Date as i32,
            array_element_type: None,
            struct_type: None,
            type_annotation: 0,
            proto_type_fqn: "".to_string(),
        }
    }

    fn create_row(values: Vec<Value>, columns: Vec<&str>) -> Row {
        let mut names = HashMap::new();
        let mut types = Vec::new();
        for (i, c) in columns.iter().enumerate() {
            names.insert(c.to_string(), i);
            types.push(Type::default());
        }
        Row {
            raw_values: values,
            column_names: Arc::new(names),
            column_types: Arc::new(types),
        }
    }

    fn null_val() -> Value {
        Value {
            kind: Some(prost_types::value::Kind::NullValue(0)),
        }
    }

    fn f64_val(f: f64) -> Value {
        Value {
            kind: Some(prost_types::value::Kind::NumberValue(f)),
        }
    }

    #[test]
    fn test_row_get_string() {
        let row = create_row(vec![string_val("a"), string_val("b")], vec!["col0", "col1"]);

        // Test generic get for &Value
        assert_eq!(row.get::<&Value, _>(0).kind, row.raw_values[0].kind);
        assert_eq!(row.get::<&Value, _>(1).kind, row.raw_values[1].kind);
        assert_eq!(row.get::<&Value, _>("col0").kind, row.raw_values[0].kind);
        assert_eq!(row.get::<&Value, _>("col1").kind, row.raw_values[1].kind);

        // Test generic get for String and &str
        assert_eq!(row.get::<String, _>(0), "a");
        assert_eq!(row.get::<&str, _>(0), "a");
        assert_eq!(row.get::<String, _>("col0"), "a");
        assert_eq!(row.get::<&str, _>("col0"), "a");
    }

    #[test]
    fn test_row_get_int32() {
        let row = create_row(vec![string_val("123"), string_val("-456")], vec!["col0", "col1"]);

        assert_eq!(row.get::<i32, _>(0), 123);
        assert_eq!(row.get::<i32, _>(1), -456);
        assert_eq!(row.get::<i32, _>("col0"), 123);
        assert_eq!(row.get::<i32, _>("col1"), -456);
    }

    #[test]
    fn test_row_get_bigdecimal() {
        let row = {
            let col_types = std::sync::Arc::new(vec![
                numeric_type(),
                numeric_type(),
                Type { code: TypeCode::String as i32, ..numeric_type() }, // Wrong type code
            ]);
            let values = vec![
                string_val("123.456"),
                string_val("invalid"),
                string_val("123.456"),
            ];
            let mut names = HashMap::new();
            names.insert("numeric".to_string(), 0);
            names.insert("bad_fmt".to_string(), 1);
            names.insert("wrong_type".to_string(), 2);
            
             Row {
                raw_values: values,
                column_types: col_types,
                column_names: std::sync::Arc::new(names),
            }
        };

        assert_eq!(row.get::<BigDecimal, _>("numeric"), BigDecimal::from_str("123.456").unwrap());
        
        // Error cases
        assert!(row.try_get::<BigDecimal, _>("bad_fmt").is_err());
        assert!(row.try_get::<BigDecimal, _>("wrong_type").is_err());
        assert!(row.try_get::<BigDecimal, _>("wrong_type").is_err());
    }

    #[test]
    fn test_row_get_system_time() {
        let row = {
            let col_types = std::sync::Arc::new(vec![
                timestamp_type(),
                timestamp_type(),
                Type { code: TypeCode::String as i32, ..timestamp_type() }, // Wrong type code
            ]);
            let values = vec![
                string_val("2023-10-26T12:00:00Z"),
                string_val("invalid"),
                string_val("2023-10-26T12:00:00Z"),
            ];
            let mut names = HashMap::new();
            names.insert("timestamp".to_string(), 0);
            names.insert("bad_fmt".to_string(), 1);
            names.insert("wrong_type".to_string(), 2);
            
             Row {
                raw_values: values,
                column_types: col_types,
                column_names: std::sync::Arc::new(names),
            }
        };

        let ts = row.get::<SystemTime, _>("timestamp");
        let dt: chrono::DateTime<chrono::Utc> = ts.into();
        assert_eq!(dt.to_rfc3339(), "2023-10-26T12:00:00+00:00");
        
        // Error cases
        assert!(row.try_get::<SystemTime, _>("bad_fmt").is_err());
        assert!(row.try_get::<SystemTime, _>("wrong_type").is_err());
    }

    #[test]
    fn test_row_get_datetime_utc() {
        let row = {
            let col_types = std::sync::Arc::new(vec![
                timestamp_type(),
                // Wrong type code
                Type { code: TypeCode::String as i32, ..timestamp_type() },
            ]);
            let values = vec![
                string_val("2023-10-26T12:00:00Z"),
                string_val("2023-10-26T12:00:00Z"),
            ];
            let mut names = HashMap::new();
            names.insert("timestamp".to_string(), 0);
            names.insert("wrong_type".to_string(), 1);
            
             Row {
                raw_values: values,
                column_types: col_types,
                column_names: std::sync::Arc::new(names),
            }
        };

        let dt = row.get::<chrono::DateTime<chrono::Utc>, _>("timestamp");
        assert_eq!(dt.to_rfc3339(), "2023-10-26T12:00:00+00:00");
        
        // Error cases
        assert!(row.try_get::<chrono::DateTime<chrono::Utc>, _>("wrong_type").is_err());
        assert!(row.try_get::<chrono::DateTime<chrono::Utc>, _>("wrong_type").is_err());
    }

    #[test]
    fn test_row_get_naive_date() {
        let row = {
            let col_types = std::sync::Arc::new(vec![
                date_type(),
                // Wrong type code
                Type { code: TypeCode::String as i32, ..date_type() },
            ]);
            let values = vec![
                string_val("2023-10-26"),
                string_val("2023-10-26"),
            ];
            let mut names = HashMap::new();
            names.insert("date".to_string(), 0);
            names.insert("wrong_type".to_string(), 1);
            
             Row {
                raw_values: values,
                column_types: col_types,
                column_names: std::sync::Arc::new(names),
            }
        };

        let date = row.get::<chrono::NaiveDate, _>("date");
        assert_eq!(date.to_string(), "2023-10-26");
        
        // Error cases
        assert!(row.try_get::<chrono::NaiveDate, _>("wrong_type").is_err());
    }

    #[test]
    fn test_row_get_int64() {
        let row = create_row(vec![string_val("123"), string_val("-456")], vec!["col0", "col1"]);

        assert_eq!(row.get::<i64, _>(0), 123);
        assert_eq!(row.get::<i64, _>(1), -456);
        assert_eq!(row.get::<i64, _>("col0"), 123);
        assert_eq!(row.get::<i64, _>("col1"), -456);
    }

    #[test]
    fn test_row_get_bool() {
        let row = create_row(vec![bool_val(true), bool_val(false)], vec!["col0", "col1"]);

        assert_eq!(row.get::<bool, _>(0), true);
        assert_eq!(row.get::<bool, _>(1), false);
        assert_eq!(row.get::<bool, _>("col0"), true);
        assert_eq!(row.get::<bool, _>("col1"), false);
    }

    #[test]
    fn test_row_get_invalid_index() {
        let row = create_row(vec![string_val("a")], vec!["col0"]);
        assert!(row.try_get::<String, _>(1).is_err());
        assert!(row.try_get::<String, _>("col1").is_err());
    }

    #[test]
    fn test_row_get_null() {
        let row = create_row(vec![null_val(), null_val()], vec!["col0", "col1"]);

        // Option<T> should be Ok(None)
        assert_eq!(row.get::<Option<String>, _>(0), None);
        assert_eq!(row.get::<Option<i64>, _>(1), None);

        // T should be Err (unexpected null)
        assert!(row.try_get::<String, _>(0).is_err());
        assert!(row.try_get::<i64, _>(1).is_err());
    }

    #[test]
    fn test_row_get_int32_invalid() {
        let row = create_row(vec![string_val("abc"), string_val("1.23")], vec!["abc", "float"]);
        assert!(row.try_get::<i32, _>("abc").is_err());
        assert!(row.try_get::<i32, _>("float").is_err());
    }

    #[test]
    fn test_row_get_int64_invalid() {
        let row = create_row(vec![string_val("not_a_number")], vec!["col0"]);
        assert!(row.try_get::<i64, _>(0).is_err());
    }

    #[test]
    fn test_row_get_missing_kind() {
        let row = create_row(
            vec![Value { kind: None }],
            vec!["col0"],
        );
        // Should be error even for Option<T>
        assert!(row.try_get::<Option<String>, _>(0).is_err());
        assert!(row.try_get::<String, _>(0).is_err());
    }

    #[test]
    fn test_row_get_f64() {
        let row = create_row(
            vec![
                f64_val(1.23),
                string_val("Infinity"),
                string_val("-Infinity"),
                string_val("NaN"),
                string_val("not_a_number"),
                null_val(),
            ],
            vec!["v", "inf", "neg_inf", "nan", "err", "null"],
        );

        assert_eq!(row.get::<f64, _>("v"), 1.23);
        assert_eq!(row.get::<f64, _>("inf"), f64::INFINITY);
        assert_eq!(row.get::<f64, _>("neg_inf"), f64::NEG_INFINITY);
        assert!(row.get::<f64, _>("nan").is_nan());

        // Error cases
        assert!(row.try_get::<f64, _>("err").is_err());
        assert!(row.try_get::<f64, _>("null").is_err());
        assert_eq!(row.get::<Option<f64>, _>("null"), None);
    }

    #[test]
    fn test_row_get_f32() {
        let row = create_row(
            vec![
                f64_val(1.23),
                string_val("Infinity"),
                string_val("-Infinity"),
                string_val("NaN"),
                string_val("not_a_number"),
                null_val(),
            ],
            vec!["v", "inf", "neg_inf", "nan", "err", "null"],
        );

        assert_eq!(row.get::<f32, _>("v"), 1.23f32);
        assert_eq!(row.get::<f32, _>("inf"), f32::INFINITY);
        assert_eq!(row.get::<f32, _>("neg_inf"), f32::NEG_INFINITY);
        assert!(row.get::<f32, _>("nan").is_nan());

        // Error cases
        assert!(row.try_get::<f32, _>("err").is_err());
        assert!(row.try_get::<f32, _>("null").is_err());
        assert_eq!(row.get::<Option<f32>, _>("null"), None);
    }

    #[test]
    fn test_row_get_bytes() {
        let valid_b64 = "SGVsbG8gV29ybGQ="; // "Hello World"
        let invalid_b64 = "NotBase64!";

        let col_types = std::sync::Arc::new(vec![
            bytes_type(),
            proto_type(),
            bytes_type(),
            bytes_type(),
            Type { code: TypeCode::String as i32, ..bytes_type() }, // Wrong type code
        ]);

        let row = Row {
            raw_values: vec![
                string_val(valid_b64),
                string_val(valid_b64),
                string_val(invalid_b64),
                null_val(),
                string_val(valid_b64),
            ],
            column_types: col_types,
            column_names: std::sync::Arc::new(
                vec!["bytes", "proto", "bad_b64", "null", "wrong_type"]
                    .into_iter()
                    .enumerate()
                    .map(|(i, s)| (s.to_string(), i))
                    .collect()
            ),
        };

        assert_eq!(row.get::<Vec<u8>, _>("bytes"), b"Hello World");
        assert_eq!(row.get::<Vec<u8>, _>("proto"), b"Hello World");
        
        // Error cases
        assert!(row.try_get::<Vec<u8>, _>("bad_b64").is_err());
        assert!(row.try_get::<Vec<u8>, _>("wrong_type").is_err());
        assert!(row.try_get::<Vec<u8>, _>("null").is_err());
        assert_eq!(row.get::<Option<Vec<u8>>, _>("null"), None);
    }
}
