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
pub use crate::types::{Type, TypeCode};
use crate::value::Kind;
use crate::value::SPANNER_DATE_FORMAT;
use crate::value::Value;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use google_cloud_type::model::Date;
use prost_types::ListValue as ProtoListValue;
use prost_types::value::Kind as ProtoKind;
use rust_decimal::Decimal;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::error::Error as StdError;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use time::Date as TimeDate;
#[cfg(feature = "unstable-time")]
use time::OffsetDateTime;
#[cfg(feature = "unstable-time")]
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;
use wkt::Timestamp;

/// Represent failures in converting a Spanner Value to a Rust type.
///
/// # Example
/// ```
/// # use google_cloud_spanner::error::ConvertError;
/// # use google_cloud_spanner::types;
/// # use google_cloud_spanner::value::{FromValue, Value};
/// let value = Value::from("not-a-bool");
/// let string_type = types::string();
/// let error = bool::from_value(&value, &string_type).expect_err("string is not a bool");
/// match error {
///     ConvertError::TypeMismatch { want, got, .. } => {
///         println!("Type mismatch: expected {want:?}, got {got:?}");
///     }
///     _ => {}
/// }
/// ```
#[derive(thiserror::Error, Clone, Debug)]
#[non_exhaustive]
pub enum ConvertError {
    /// The value kind is not as expected.
    #[error("expected {want:?}, got {got:?}")]
    #[non_exhaustive]
    KindMismatch {
        /// The expected Spanner value kind.
        want: Kind,
        /// The actual Spanner value kind.
        got: Kind,
    },

    /// The column type does not match the requested type.
    #[error("type mismatch, expected {want:?}, got {got:?}")]
    #[non_exhaustive]
    TypeMismatch {
        /// The expected Spanner type code.
        want: TypeCode,
        /// The actual Spanner type code.
        got: TypeCode,
    },

    /// The value is null, but the target type does not support nulls.
    #[error("expected non-null value, got null")]
    NotNull,

    /// There was a problem during conversion.
    #[error("cannot convert value, source={0}")]
    Convert(#[source] SharedError),
}

impl ConvertError {
    /// Creates a [`ConvertError::Convert`] wrapping a custom error.
    ///
    /// This constructor is useful when implementing [`FromValue`] for custom domain or wrapper types.
    ///
    /// # Example: Custom ID type
    ///
    /// ```
    /// use google_cloud_spanner::types::Type;
    /// use google_cloud_spanner::value::{ConvertError, FromValue, Value};
    ///
    /// #[derive(Debug, PartialEq)]
    /// struct CustomerId(u32);
    ///
    /// impl FromValue for CustomerId {
    ///     fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
    ///         let raw_int = i64::from_value(value, spanner_type)?;
    ///         let parsed = u32::try_from(raw_int).map_err(ConvertError::custom)?;
    ///         Ok(CustomerId(parsed))
    ///     }
    /// }
    /// ```
    pub fn custom<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Convert(Arc::new(error))
    }

    /// Creates a [`ConvertError::Convert`] from a message string.
    ///
    /// This constructor is useful when implementing [`FromValue`] for custom validation,
    /// domain enums, or parsing formatted strings.
    ///
    /// # Example: Validating numeric values
    ///
    /// A custom wrapper type can deserialize a Spanner `NUMERIC` column as a [`String`]
    /// and apply custom business validation, returning [`ConvertError::message`] on failure:
    ///
    /// ```
    /// use google_cloud_spanner::types::{Type, TypeCode};
    /// use google_cloud_spanner::value::{ConvertError, FromValue, Value};
    ///
    /// /// A wrapper type that extracts a Spanner `NUMERIC` column, ensuring it is non-negative.
    /// #[derive(Debug, PartialEq)]
    /// struct PositiveNumeric(pub String);
    ///
    /// impl FromValue for PositiveNumeric {
    ///     fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
    ///         if spanner_type.code() != TypeCode::Numeric {
    ///             return Err(ConvertError::type_mismatch(
    ///                 TypeCode::Numeric,
    ///                 spanner_type.code(),
    ///             ));
    ///         }
    ///         let s = String::from_value(value, spanner_type)?;
    ///         if s.starts_with('-') {
    ///             return Err(ConvertError::message("numeric value must be non-negative"));
    ///         }
    ///         Ok(PositiveNumeric(s))
    ///     }
    /// }
    /// ```
    ///
    /// # Example: Custom enum parsing
    ///
    /// ```
    /// use google_cloud_spanner::types::Type;
    /// use google_cloud_spanner::value::{ConvertError, FromValue, Value};
    ///
    /// #[derive(Debug, PartialEq)]
    /// enum OrderStatus {
    ///     Pending,
    ///     Shipped,
    /// }
    ///
    /// impl FromValue for OrderStatus {
    ///     fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
    ///         let raw_status = String::from_value(value, spanner_type)?;
    ///         match raw_status.as_str() {
    ///             "PENDING" => Ok(OrderStatus::Pending),
    ///             "SHIPPED" => Ok(OrderStatus::Shipped),
    ///             other => Err(ConvertError::message(format!("unknown order status: '{other}'"))),
    ///         }
    ///     }
    /// }
    /// ```
    pub fn message(message: impl Into<String>) -> Self {
        Self::Convert(Arc::new(MessageError(message.into())))
    }

    /// Creates a [`ConvertError::KindMismatch`] for the expected and actual value kinds.
    ///
    /// # Example
    /// ```
    /// use google_cloud_spanner::error::ConvertError;
    /// use google_cloud_spanner::value::Kind;
    ///
    /// let error = ConvertError::kind_mismatch(Kind::String, Kind::Bool);
    /// assert_eq!(error.to_string(), "expected String, got Bool");
    /// ```
    pub fn kind_mismatch(want: Kind, got: Kind) -> Self {
        Self::KindMismatch { want, got }
    }

    /// Creates a [`ConvertError::TypeMismatch`] for the expected and actual type codes.
    ///
    /// This constructor is preferred when validating column schema types in custom
    /// [`FromValue`] implementations.
    ///
    /// # Example
    /// ```
    /// use google_cloud_spanner::error::ConvertError;
    /// use google_cloud_spanner::types::TypeCode;
    ///
    /// let error = ConvertError::type_mismatch(TypeCode::Int64, TypeCode::String);
    /// assert_eq!(error.to_string(), "type mismatch, expected Int64, got String");
    /// ```
    pub fn type_mismatch(want: TypeCode, got: TypeCode) -> Self {
        Self::TypeMismatch { want, got }
    }

    /// Extracts a `ConvertError` from a [`google_cloud_spanner::Error`][Error], if present.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::error::ConvertError;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn example(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.single_use().build();
    /// let mut result_set = transaction
    ///     .execute_query(Statement::builder("SELECT 'not-a-number' AS text").build())
    ///     .await?;
    ///
    /// if let Some(row) = result_set.next().await {
    ///     let result: Result<i64, _> = row?.get("text");
    ///     if let Err(error) = result {
    ///         if let Some(convert_error) = ConvertError::extract(&error) {
    ///             println!("Conversion failed: {convert_error}");
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Row deserialization returns [`Error`] with a source of [`RowError::TypeConversion`][crate::error::RowError::TypeConversion]
    /// (wrapping [`ConvertError`]) or a direct [`ConvertError`] when value conversion fails.
    /// This method traverses the error chain to extract the underlying [`ConvertError`].
    pub fn extract(err: &Error) -> Option<&Self> {
        let mut current = err.source();
        while let Some(source) = current {
            if let Some(convert_error) = source.downcast_ref::<Self>() {
                return Some(convert_error);
            }
            current = source.source();
        }
        None
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct MessageError(String);

/// An error type representing a thread-safe error wrapped in an [`Arc`].
pub type SharedError = Arc<dyn StdError + Send + Sync>;

/// Converts a Spanner [Value] into a Rust type.
///
/// Implementations are provided for all standard types like `String`, primitive integer
/// and float types, decimals, timestamps, dates, vectors, and options for nullable fields.
///
/// # Null Handling Contract
/// Custom implementations of [`FromValue`] must return [`ConvertError::NotNull`] when given a null
/// value ([`Value::is_null`]). This allows [`Option<T>`] to intercept the null value and return
/// `Ok(None)` while still validating the column's schema [`Type`].
///
/// # Example: Custom converter for domain wrapper
///
/// An application can implement [`FromValue`] for custom domain or newtype wrappers:
///
/// ```
/// use google_cloud_spanner::types::Type;
/// use google_cloud_spanner::value::{ConvertError, FromValue, Value};
///
/// #[derive(Debug, PartialEq)]
/// struct AccountNumber(pub String);
///
/// impl FromValue for AccountNumber {
///     fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
///         let s = String::from_value(value, spanner_type)?;
///         Ok(AccountNumber(s))
///     }
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let spanner_type = google_cloud_spanner::types::string();
/// let val = Value::from("ACC-98765");
/// let account = AccountNumber::from_value(&val, &spanner_type)?;
/// assert_eq!(account.0, "ACC-98765");
/// # Ok(())
/// # }
/// ```
pub trait FromValue: Sized {
    /// Converts a Spanner value into the target Rust type, using the provided
    /// Spanner `Type` metadata for compatibility checks.
    ///
    /// # Errors
    ///
    /// Returns a [`ConvertError`] if the kind of the value does not match the expected kind,
    /// if the value is null but the target type is not optional (e.g., `Option<T>`), or if
    /// parsing or decoding the inner value format fails.
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError>;

    /// Converts an owned Spanner value into the target Rust type, using the provided
    /// Spanner `Type` metadata for compatibility checks.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::types;
    /// # use google_cloud_spanner::value::{FromValue, Value};
    /// let value = Value::from("example");
    /// let string = String::from_owned_value(value, &types::string())?;
    /// assert_eq!(string, "example");
    /// # Ok::<(), google_cloud_spanner::error::ConvertError>(())
    /// ```
    ///
    /// The default implementation delegates to [`from_value`](FromValue::from_value)
    /// using a reference to `value`. Types that contain heap allocations (such as
    /// [`String`], [`Vec<T>`], [`serde_json::Value`], and [`Value`]) override this method
    /// to move the underlying data directly without cloning.
    ///
    /// # Errors
    ///
    /// Returns a [`ConvertError`] if the kind of the value does not match the expected kind,
    /// if the value is null but the target type is not optional (e.g., `Option<T>`), or if
    /// parsing or decoding the inner value format fails.
    fn from_owned_value(value: Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        Self::from_value(&value, spanner_type)
    }
}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if value.is_null() {
            return match T::from_value(value, spanner_type) {
                Ok(_) | Err(ConvertError::NotNull) => Ok(None),
                Err(error) => Err(error),
            };
        }
        T::from_value(value, spanner_type).map(Some)
    }

    fn from_owned_value(value: Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if value.is_null() {
            return match T::from_owned_value(value, spanner_type) {
                Ok(_) | Err(ConvertError::NotNull) => Ok(None),
                Err(error) => Err(error),
            };
        }
        T::from_owned_value(value, spanner_type).map(Some)
    }
}

impl FromValue for Value {
    fn from_value(value: &Value, _spanner_type: &Type) -> Result<Self, ConvertError> {
        Ok(value.clone())
    }

    fn from_owned_value(value: Value, _spanner_type: &Type) -> Result<Self, ConvertError> {
        Ok(value)
    }
}

/// Converts any Spanner [`Value`] into a [`serde_json::Value`] using runtime [`Type`] metadata.
///
/// This enables dynamic deserialization of STRUCT, ARRAY, and nested
/// `ARRAY<STRUCT<ARRAY<...>>>` columns without requiring predefined Rust types.
///
/// # Type mapping
///
/// | Spanner wire format | JSON output |
/// |---------------------|-------------|
/// | `NullValue` | `null` |
/// | `BoolValue` | `true` / `false` |
/// | `NumberValue` (finite) | JSON number |
/// | `NumberValue` (NaN/±Infinity) | `null` |
/// | `StringValue` ("NaN"/`±Infinity` on FLOAT columns) | `null` |
/// | `StringValue` (standard) | JSON string (includes stringified `INT64`, `NUMERIC`, Base64 `BYTES`, `TIMESTAMP`, `DATE`) |
/// | `StringValue` (+ `TypeCode::Json`) | Parsed JSON object/array/value |
/// | `ListValue` + `TypeCode::Struct` | JSON object (positional → named via metadata) |
/// | `ListValue` + `TypeCode::Array` | JSON array |
///
/// # Known limitations
///
/// - **Precision Safety**: Types such as `INT64`, `NUMERIC`, and temporal types (which
///   Spanner transmits as `StringValue` on the wire) are preserved as JSON strings.
///   This prevents precision loss (e.g. for integers exceeding $2^{53}-1$ or
///   high-precision decimals) during deserialization.
/// - **NaN/Infinity → null**: Non-finite floats become `null` when deserialized as
///   `serde_json::Value` since standard JSON has no representation for NaN/Infinity.
///   Callers cannot distinguish these from genuine SQL NULLs.
/// - **Duplicate field names**: Spanner allows structs with duplicate field names
///   (e.g., unnamed columns). Since JSON objects require unique keys, last-write-wins
///   applies via `serde_json::Map::insert`.
/// - **Missing struct metadata**: When `TypeCode::Struct` is indicated but no
///   `struct_type` metadata is available, positional values are returned as a plain
///   JSON array to avoid silent data loss.
impl FromValue for JsonValue {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        from_value_recursive(value, Some(spanner_type), 0)
    }

    fn from_owned_value(value: Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        value_to_json(value, Some(spanner_type), 0)
    }
}

trait SpannerFloat: FromStr {
    const NAN: Self;
    const INFINITY: Self;
    const NEG_INFINITY: Self;
}

impl SpannerFloat for f64 {
    const NAN: Self = f64::NAN;
    const INFINITY: Self = f64::INFINITY;
    const NEG_INFINITY: Self = f64::NEG_INFINITY;
}

impl SpannerFloat for f32 {
    const NAN: Self = f32::NAN;
    const INFINITY: Self = f32::INFINITY;
    const NEG_INFINITY: Self = f32::NEG_INFINITY;
}

fn parse_spanner_float<F>(string_value: &str) -> Result<F, ConvertError>
where
    F: SpannerFloat,
    F::Err: StdError + Send + Sync + 'static,
{
    match string_value {
        "NaN" => Ok(F::NAN),
        "Infinity" => Ok(F::INFINITY),
        "-Infinity" => Ok(F::NEG_INFINITY),
        _ => string_value.parse::<F>().map_err(ConvertError::custom),
    }
}

fn decode_borrowed_string_to_json(
    string_value: &str,
    target_type: Option<&Type>,
) -> Result<JsonValue, ConvertError> {
    let Some(target_type) = target_type else {
        return Ok(JsonValue::String(string_value.to_string()));
    };

    match target_type.code() {
        TypeCode::Json => serde_json::from_str(string_value).map_err(ConvertError::custom),
        TypeCode::Float64 | TypeCode::Float32 => {
            let float_value = parse_spanner_float::<f64>(string_value)?;
            Ok(JsonNumber::from_f64(float_value)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null))
        }
        _ => Ok(JsonValue::String(string_value.to_string())),
    }
}

fn from_value_recursive(
    value: &Value,
    spanner_type: Option<&Type>,
    depth: usize,
) -> Result<JsonValue, ConvertError> {
    const MAX_RECURSION_DEPTH: usize = 64;
    if depth > MAX_RECURSION_DEPTH {
        return Err(ConvertError::message("maximum nesting depth exceeded"));
    }

    match &value.0.kind {
        Some(ProtoKind::NullValue(_)) | None => Ok(JsonValue::Null),

        Some(ProtoKind::NumberValue(number)) => Ok(JsonNumber::from_f64(*number)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null)),

        Some(ProtoKind::StringValue(string_value)) => {
            decode_borrowed_string_to_json(string_value, spanner_type)
        }

        Some(ProtoKind::BoolValue(boolean_value)) => Ok(JsonValue::Bool(*boolean_value)),

        Some(ProtoKind::StructValue(_)) => Err(ConvertError::message(
            "unexpected protobuf StructValue on wire; Spanner SQL STRUCT values are encoded as ListValue",
        )),

        Some(ProtoKind::ListValue(list_value)) => {
            borrowed_list_value_to_json(list_value, spanner_type, depth + 1)
        }
    }
}

fn borrowed_list_value_to_json(
    list_value: &ProtoListValue,
    spanner_type: Option<&Type>,
    depth: usize,
) -> Result<JsonValue, ConvertError> {
    let code = spanner_type.map_or(TypeCode::Unspecified, |target_type| target_type.code());
    match code {
        TypeCode::Struct => {
            let Some(struct_type) = spanner_type.and_then(|target_type| target_type.struct_type())
            else {
                let mut array = Vec::with_capacity(list_value.values.len());
                for proto_value in &list_value.values {
                    let val = Value::from_ref(proto_value);
                    array.push(from_value_recursive(val, None, depth)?);
                }
                return Ok(JsonValue::Array(array));
            };

            let mut map = JsonMap::new();
            for (index, field) in struct_type.fields.iter().enumerate() {
                let field_type = field.r#type.as_deref().map(Type::from_ref);
                let value = if let Some(proto_value) = list_value.values.get(index) {
                    let val = Value::from_ref(proto_value);
                    from_value_recursive(val, field_type, depth)?
                } else {
                    JsonValue::Null
                };
                map.insert(field.name.clone(), value);
            }
            Ok(JsonValue::Object(map))
        }

        _ => {
            let element_type = spanner_type.and_then(Type::array_element_type);
            let mut array = Vec::with_capacity(list_value.values.len());
            for proto_value in &list_value.values {
                let val = Value::from_ref(proto_value);
                array.push(from_value_recursive(val, element_type, depth)?);
            }
            Ok(JsonValue::Array(array))
        }
    }
}

fn decode_owned_string_to_json(
    string_value: String,
    target_type: Option<&Type>,
) -> Result<JsonValue, ConvertError> {
    let Some(target_type) = target_type else {
        return Ok(JsonValue::String(string_value));
    };

    match target_type.code() {
        TypeCode::Json => serde_json::from_str(&string_value).map_err(ConvertError::custom),
        TypeCode::Float64 | TypeCode::Float32 => {
            let float_value = parse_spanner_float::<f64>(&string_value)?;
            Ok(JsonNumber::from_f64(float_value)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null))
        }
        _ => Ok(JsonValue::String(string_value)),
    }
}

fn value_to_json(
    value: Value,
    spanner_type: Option<&Type>,
    depth: usize,
) -> Result<JsonValue, ConvertError> {
    const MAX_RECURSION_DEPTH: usize = 64;
    if depth > MAX_RECURSION_DEPTH {
        return Err(ConvertError::message("maximum nesting depth exceeded"));
    }

    match value.0.kind {
        Some(ProtoKind::NullValue(_)) | None => Ok(JsonValue::Null),

        Some(ProtoKind::NumberValue(number)) => Ok(JsonNumber::from_f64(number)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null)),

        Some(ProtoKind::StringValue(string_value)) => {
            decode_owned_string_to_json(string_value, spanner_type)
        }

        Some(ProtoKind::BoolValue(boolean_value)) => Ok(JsonValue::Bool(boolean_value)),

        Some(ProtoKind::StructValue(_)) => Err(ConvertError::message(
            "unexpected protobuf StructValue on wire; Spanner SQL STRUCT values are encoded as ListValue",
        )),

        Some(ProtoKind::ListValue(list_value)) => {
            list_value_to_json(list_value, spanner_type, depth + 1)
        }
    }
}

fn list_value_to_json(
    list_value: ProtoListValue,
    spanner_type: Option<&Type>,
    depth: usize,
) -> Result<JsonValue, ConvertError> {
    let code = spanner_type.map_or(TypeCode::Unspecified, |target_type| target_type.code());
    match code {
        TypeCode::Struct => {
            let Some(struct_type) = spanner_type.and_then(|target_type| target_type.struct_type())
            else {
                let mut array = Vec::with_capacity(list_value.values.len());
                for proto_value in list_value.values {
                    array.push(value_to_json(Value(proto_value), None, depth)?);
                }
                return Ok(JsonValue::Array(array));
            };

            let mut map = JsonMap::new();
            let mut iterator = list_value.values.into_iter();
            for field in &struct_type.fields {
                let field_type = field.r#type.as_deref().map(Type::from_ref);
                let value = if let Some(field_value) = iterator.next() {
                    value_to_json(Value(field_value), field_type, depth)?
                } else {
                    JsonValue::Null
                };
                map.insert(field.name.clone(), value);
            }
            Ok(JsonValue::Object(map))
        }

        _ => {
            let element_type = spanner_type.and_then(Type::array_element_type);
            let mut array = Vec::with_capacity(list_value.values.len());
            for proto_value in list_value.values {
                array.push(value_to_json(Value(proto_value), element_type, depth)?);
            }
            Ok(JsonValue::Array(array))
        }
    }
}

/// Deserializes a string value from a Spanner [`Value`].
///
/// Supported Spanner types are:
/// - Textual types: [`TypeCode::String`], [`TypeCode::Json`], [`TypeCode::Uuid`], and [`TypeCode::Interval`].
/// - Temporal types: [`TypeCode::Date`] (formatted as `YYYY-MM-DD`) and [`TypeCode::Timestamp`] (formatted as RFC 3339).
/// - Exact decimal types: [`TypeCode::Numeric`]. Retrieving `NUMERIC` as [`String`] preserves full 38-digit precision and handles special PostgreSQL values like `NaN` without loss or parsing errors.
///
/// Non-string types (such as `INT64`, `BYTES`, `FLOAT64`, `FLOAT32`, `BOOL`, `ARRAY`, and `STRUCT`)
/// are rejected with [`ConvertError::TypeMismatch`].
impl FromValue for String {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        match spanner_type.code() {
            TypeCode::String
            | TypeCode::Json
            | TypeCode::Uuid
            | TypeCode::Interval
            | TypeCode::Date
            | TypeCode::Timestamp
            | TypeCode::Numeric => {}
            got => {
                return Err(ConvertError::TypeMismatch {
                    want: TypeCode::String,
                    got,
                });
            }
        }
        match &value.0.kind {
            Some(ProtoKind::StringValue(s)) => Ok(s.clone()),
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: value.kind(),
            }),
        }
    }

    fn from_owned_value(value: Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        match spanner_type.code() {
            TypeCode::String
            | TypeCode::Json
            | TypeCode::Uuid
            | TypeCode::Interval
            | TypeCode::Date
            | TypeCode::Timestamp
            | TypeCode::Numeric => {}
            got => {
                return Err(ConvertError::TypeMismatch {
                    want: TypeCode::String,
                    got,
                });
            }
        }
        match value.0.kind {
            Some(ProtoKind::StringValue(string_value)) => Ok(string_value),
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            other => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: Kind::from(other),
            }),
        }
    }
}

/// Deserializes a 64-bit integer from a Spanner [`Value`].
///
/// Accepts Spanner [`TypeCode::Int64`] and [`TypeCode::Enum`].
impl FromValue for i64 {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        match spanner_type.code() {
            TypeCode::Int64 | TypeCode::Enum => {}
            got => {
                return Err(ConvertError::TypeMismatch {
                    want: TypeCode::Int64,
                    got,
                });
            }
        }
        match &value.0.kind {
            Some(ProtoKind::StringValue(s)) => s.parse().map_err(ConvertError::custom),
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: value.kind(),
            }),
        }
    }
}

/// Deserializes a 32-bit integer from a Spanner [`Value`].
///
/// Accepts Spanner [`TypeCode::Int64`] and [`TypeCode::Enum`].
impl FromValue for i32 {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        match spanner_type.code() {
            TypeCode::Int64 | TypeCode::Enum => {}
            got => {
                return Err(ConvertError::TypeMismatch {
                    want: TypeCode::Int64,
                    got,
                });
            }
        }
        match &value.0.kind {
            Some(ProtoKind::StringValue(s)) => s.parse().map_err(ConvertError::custom),
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: value.kind(),
            }),
        }
    }
}

/// Converts a Spanner `TypeCode::Numeric` column into a `Decimal`.
///
/// Supports both standard fixed-point decimal notation (e.g., `"123.456"`) and
/// scientific notation format (e.g., `"1e5"`, `"1.5e-3"`).
///
/// # Precision Limitations
///
/// Cloud Spanner `NUMERIC` supports up to 38 decimal digits of precision (up to 29
/// digits before the decimal point and 9 digits after). However, [`Decimal`] uses a
/// 96-bit unsigned integer mantissa, which can represent at most ~28–29 total decimal
/// digits (and at most 19–20 digits before the decimal point when scale is 9).
///
/// If a Spanner numeric value exceeds the 96-bit limit of [`Decimal`], or if a PostgreSQL
/// dialect database returns `"NaN"`, converting to [`Decimal`] will return a
/// [`ConvertError::Convert`]. For values that exceed 96 bits or for PostgreSQL `"NaN"`,
/// callers should read the column as a [`String`] instead (e.g., `let value: String = row.get(...)?;`).
impl FromValue for Decimal {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Numeric {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Numeric,
                got: spanner_type.code(),
            });
        }
        let string_value = match &value.0.kind {
            Some(ProtoKind::StringValue(string_value)) => string_value,
            Some(ProtoKind::NullValue(_)) | None => return Err(ConvertError::NotNull),
            _ => {
                return Err(ConvertError::KindMismatch {
                    want: Kind::String,
                    got: value.kind(),
                });
            }
        };

        match Decimal::from_str_exact(string_value) {
            Ok(decimal) => Ok(decimal),
            Err(_) if string_value.contains(['e', 'E']) => {
                Decimal::from_scientific(string_value).map_err(ConvertError::custom)
            }
            Err(error) => Err(ConvertError::custom(error)),
        }
    }
}

impl FromValue for SystemTime {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        let timestamp = Timestamp::from_value(value, spanner_type)?;
        Self::try_from(timestamp).map_err(ConvertError::custom)
    }
}

#[cfg(feature = "unstable-time")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-time")))]
impl FromValue for OffsetDateTime {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Timestamp {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Timestamp,
                got: spanner_type.code(),
            });
        }
        match &value.0.kind {
            Some(ProtoKind::StringValue(s)) => {
                let date_time = Self::parse(s, &Rfc3339).map_err(ConvertError::custom)?;
                Ok(date_time)
            }
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for Timestamp {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Timestamp {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Timestamp,
                got: spanner_type.code(),
            });
        }
        match &value.0.kind {
            Some(ProtoKind::StringValue(s)) => {
                Self::try_from(s.as_str()).map_err(ConvertError::custom)
            }
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: value.kind(),
            }),
        }
    }
}

#[cfg(feature = "unstable-time")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-time")))]
impl FromValue for TimeDate {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Date {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Date,
                got: spanner_type.code(),
            });
        }
        match &value.0.kind {
            Some(ProtoKind::StringValue(s)) => {
                let date = Self::parse(s, SPANNER_DATE_FORMAT).map_err(ConvertError::custom)?;
                Ok(date)
            }
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for Date {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Date {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Date,
                got: spanner_type.code(),
            });
        }
        match &value.0.kind {
            Some(ProtoKind::StringValue(s)) => {
                let date = TimeDate::parse(s, SPANNER_DATE_FORMAT).map_err(ConvertError::custom)?;
                Ok(Self::new()
                    .set_year(date.year())
                    .set_month(u8::from(date.month()) as i32)
                    .set_day(date.day() as i32))
            }
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Bool {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Bool,
                got: spanner_type.code(),
            });
        }
        match &value.0.kind {
            Some(ProtoKind::BoolValue(b)) => Ok(*b),
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::Bool,
                got: value.kind(),
            }),
        }
    }
}

/// Deserializes a 64-bit floating-point value from a Spanner [`Value`].
///
/// Accepts Spanner `TypeCode::Float64` and `TypeCode::Float32`.
/// Values from `TypeCode::Float32` columns are widened to `f64` losslessly.
impl FromValue for f64 {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Float64 && spanner_type.code() != TypeCode::Float32 {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Float64,
                got: spanner_type.code(),
            });
        }
        match &value.0.kind {
            Some(ProtoKind::NumberValue(n)) => Ok(*n),
            Some(ProtoKind::StringValue(s)) => s.parse().map_err(ConvertError::custom),
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::Number,
                got: value.kind(),
            }),
        }
    }
}

fn is_infinity_literal(value: &str) -> bool {
    let trimmed = value.trim();
    let body = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    body.eq_ignore_ascii_case("inf") || body.eq_ignore_ascii_case("infinity")
}

/// Deserializes a 32-bit floating-point value from a Spanner [`Value`].
///
/// Accepts Spanner `TypeCode::Float32` and `TypeCode::Float64`.
///
/// # Precision & Range Limitations
///
/// When deserializing from a Spanner `TypeCode::Float64` column, 64-bit IEEE 754
/// floating-point numbers are narrowed to 32-bit `f32`.
///
/// If a finite float value exceeds the representable range of `f32`
/// (greater than [`f32::MAX`] or less than [`f32::MIN`] / `-f32::MAX`), this conversion
/// returns a `ConvertError::Convert`. Non-finite values (`NaN`, `Infinity`,
/// `+Infinity`, and `-Infinity`) are preserved.
impl FromValue for f32 {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Float32 && spanner_type.code() != TypeCode::Float64 {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Float32,
                got: spanner_type.code(),
            });
        }
        match &value.0.kind {
            Some(ProtoKind::NumberValue(number_value)) => {
                let converted_value = *number_value as f32;
                if number_value.is_finite() && converted_value.is_infinite() {
                    return Err(ConvertError::message(format!(
                        "float value '{number_value}' exceeds f32 range"
                    )));
                }
                Ok(converted_value)
            }
            Some(ProtoKind::StringValue(string_value)) => {
                let parsed_value: f32 = string_value.parse().map_err(ConvertError::custom)?;
                if parsed_value.is_infinite() && !is_infinity_literal(string_value) {
                    return Err(ConvertError::message(format!(
                        "float value '{string_value}' exceeds f32 range"
                    )));
                }
                Ok(parsed_value)
            }
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::Number,
                got: value.kind(),
            }),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Bytes && spanner_type.code() != TypeCode::Proto {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Bytes,
                got: spanner_type.code(),
            });
        }
        match &value.0.kind {
            Some(ProtoKind::StringValue(string_value)) => BASE64_STANDARD
                .decode(string_value)
                .map_err(ConvertError::custom),
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: value.kind(),
            }),
        }
    }

    fn from_owned_value(value: Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Bytes && spanner_type.code() != TypeCode::Proto {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Bytes,
                got: spanner_type.code(),
            });
        }
        match value.0.kind {
            Some(ProtoKind::StringValue(string_value)) => BASE64_STANDARD
                .decode(string_value)
                .map_err(ConvertError::custom),
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            other => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: Kind::from(other),
            }),
        }
    }
}

/// Deserializes a byte buffer from a Spanner [`Value`].
///
/// Accepts Spanner [`TypeCode::Bytes`] and [`TypeCode::Proto`].
/// Values are decoded from the wire base64-encoded string representation.
impl FromValue for Bytes {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        Vec::<u8>::from_value(value, spanner_type).map(Bytes::from)
    }

    fn from_owned_value(value: Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        Vec::<u8>::from_owned_value(value, spanner_type).map(Bytes::from)
    }
}

/// Deserializes a UUID from a Spanner [`Value`].
///
/// Accepts Spanner [`TypeCode::Uuid`] and [`TypeCode::String`].
/// Values are parsed from the wire 8-4-4-4-12 hex string representation.
impl FromValue for Uuid {
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        match spanner_type.code() {
            TypeCode::Uuid | TypeCode::String => {}
            got => {
                return Err(ConvertError::TypeMismatch {
                    want: TypeCode::Uuid,
                    got,
                });
            }
        }
        match &value.0.kind {
            Some(ProtoKind::StringValue(string_value)) => {
                Uuid::parse_str(string_value).map_err(ConvertError::custom)
            }
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::String,
                got: value.kind(),
            }),
        }
    }
}

impl<T> FromValue for Vec<T>
where
    T: FromValue,
{
    fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Array {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Array,
                got: spanner_type.code(),
            });
        }
        let element_type = spanner_type
            .array_element_type()
            .ok_or_else(|| ConvertError::message("Array type missing element type"))?;

        match &value.0.kind {
            Some(ProtoKind::ListValue(list)) => {
                let mut vec = Vec::with_capacity(list.values.len());
                for v in &list.values {
                    // `Value` is a `#[repr(transparent)]` wrapper around `ProtoValue`.
                    // We use `from_ref` to safely cast the pointer and avoid cloning elements.
                    let val = Value::from_ref(v);
                    vec.push(T::from_value(val, element_type)?);
                }
                Ok(vec)
            }
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            _ => Err(ConvertError::KindMismatch {
                want: Kind::List,
                got: value.kind(),
            }),
        }
    }

    fn from_owned_value(value: Value, spanner_type: &Type) -> Result<Self, ConvertError> {
        if spanner_type.code() != TypeCode::Array {
            return Err(ConvertError::TypeMismatch {
                want: TypeCode::Array,
                got: spanner_type.code(),
            });
        }
        let element_type = spanner_type
            .array_element_type()
            .ok_or_else(|| ConvertError::message("Array type missing element type"))?;

        match value.0.kind {
            Some(ProtoKind::ListValue(list_value)) => {
                let mut vector = Vec::with_capacity(list_value.values.len());
                for proto_value in list_value.values {
                    vector.push(T::from_owned_value(Value(proto_value), element_type)?);
                }
                Ok(vector)
            }
            Some(ProtoKind::NullValue(_)) | None => Err(ConvertError::NotNull),
            other => Err(ConvertError::KindMismatch {
                want: Kind::List,
                got: Kind::from(other),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::gapic_dataplane::model;
    use crate::row::RowError;
    use crate::to_value::ToValue;
    use crate::types;
    use prost_types::Value as ProtoValue;
    use serde_json::{Value as JsonValue, json};
    #[cfg(feature = "unstable-time")]
    use time::Month;

    #[test]
    fn test_from_value_string() {
        let v = "hello".to_value();
        let s = String::from_value(&v, &types::string()).expect("valid string value");
        assert_eq!(s, "hello", "expected string content");

        // String from JSON
        let v_json = "{\"key\":\"value\"}".to_string().to_value();
        let s_json = String::from_value(&v_json, &types::json()).expect("valid json value");
        assert_eq!(s_json, "{\"key\":\"value\"}", "expected json text");

        // String from UUID
        let value_uuid = "123e4567-e89b-12d3-a456-426614174000"
            .to_string()
            .to_value();
        let string_uuid =
            String::from_value(&value_uuid, &types::uuid()).expect("valid uuid value");
        assert_eq!(
            string_uuid, "123e4567-e89b-12d3-a456-426614174000",
            "expected uuid text"
        );

        // String from INTERVAL
        let value_interval = "P1Y2M3D".to_string().to_value();
        let string_interval =
            String::from_value(&value_interval, &types::interval()).expect("valid interval value");
        assert_eq!(string_interval, "P1Y2M3D", "expected interval text");

        // String from DATE
        let date = Date::new().set_year(2023).set_month(10).set_day(27);
        let string_date =
            String::from_value(&date.to_value(), &types::date()).expect("valid date value");
        assert_eq!(string_date, "2023-10-27", "expected date text");

        // String from TIMESTAMP
        let timestamp = Timestamp::clamp(1_698_400_800, 0);
        let string_timestamp = String::from_value(&timestamp.to_value(), &types::timestamp())
            .expect("valid timestamp value");
        assert_eq!(
            string_timestamp, "2023-10-27T10:00:00.000000000Z",
            "expected timestamp text"
        );

        // String from NUMERIC
        let decimal = Decimal::from_str_exact("123.456").expect("valid decimal");
        let string_numeric = String::from_value(&decimal.to_value(), &types::numeric())
            .expect("valid numeric value");
        assert_eq!(string_numeric, "123.456", "expected numeric text");
    }

    #[test]
    fn test_from_value_int() {
        let value_int = 42i64.to_value();
        let int64_value = i64::from_value(&value_int, &types::int64()).expect("valid i64");
        assert_eq!(int64_value, 42);

        let int32_value = i32::from_value(&value_int, &types::int64()).expect("valid i32");
        assert_eq!(int32_value, 42);

        // Enum support
        let enum_type = types::proto_enum("customer.Priority");
        let enum_value = 7i64.to_value();
        let int64_enum = i64::from_value(&enum_value, &enum_type).expect("valid i64 from ENUM");
        assert_eq!(int64_enum, 7);

        let int32_enum = i32::from_value(&enum_value, &enum_type).expect("valid i32 from ENUM");
        assert_eq!(int32_enum, 7);

        // Negative tests
        let invalid_int = "not an int".to_value();
        let error_i64 = i64::from_value(&invalid_int, &types::int64()).expect_err("invalid i64");
        assert!(format!("{error_i64}").contains("cannot convert value"));

        let error_i32 = i32::from_value(&invalid_int, &types::int64()).expect_err("invalid i32");
        assert!(format!("{error_i32}").contains("cannot convert value"));

        let bool_value = true.to_value();
        let error_kind_i64 = i64::from_value(&bool_value, &types::int64())
            .expect_err("reading bool value as i64 must fail with KindMismatch");
        assert_eq!(
            error_kind_i64.to_string(),
            "expected String, got Bool",
            "expected KindMismatch error message for i64"
        );

        let error_kind_i32 = i32::from_value(&bool_value, &types::int64())
            .expect_err("reading bool value as i32 must fail with KindMismatch");
        assert_eq!(
            error_kind_i32.to_string(),
            "expected String, got Bool",
            "expected KindMismatch error message for i32"
        );
    }

    #[test]
    fn test_from_value_float() {
        let v = 42.5f64.to_value();
        let f = f64::from_value(&v, &types::float64()).expect("valid float value");
        assert_eq!(f, 42.5, "expected 42.5");

        let v = "Infinity".to_string().to_value();
        let f = f64::from_value(&v, &types::float64()).expect("valid infinity value");
        assert_eq!(f, f64::INFINITY, "expected Infinity");

        let v = "invalid float".to_string().to_value();
        let err = f64::from_value(&v, &types::float64()).expect_err("invalid float must fail");
        assert!(
            format!("{}", err).contains("invalid float literal"),
            "expected invalid float literal error"
        );
    }

    #[test]
    fn from_value_float_non_finite_and_type_checks() {
        // f64 non-finite strings
        let nan_val = "NaN".to_string().to_value();
        let f64_nan = f64::from_value(&nan_val, &types::float64()).expect("valid NaN f64");
        assert!(f64_nan.is_nan(), "expected NaN for f64");

        let inf_val = "Infinity".to_string().to_value();
        let f64_inf = f64::from_value(&inf_val, &types::float64()).expect("valid Infinity f64");
        assert_eq!(f64_inf, f64::INFINITY, "expected Infinity for f64");

        let neg_inf_val = "-Infinity".to_string().to_value();
        let f64_neginf =
            f64::from_value(&neg_inf_val, &types::float64()).expect("valid -Infinity f64");
        assert_eq!(f64_neginf, f64::NEG_INFINITY, "expected -Infinity for f64");

        // f64 from FLOAT32 column (lossless widening)
        let f32_widened =
            f64::from_value(&42.5f32.to_value(), &types::float32()).expect("FLOAT32 widens to f64");
        assert_eq!(f32_widened, 42.5, "expected 42.5 from FLOAT32 to f64");

        // f64 rejects STRING column even if it contains "NaN"
        let string_col_err =
            f64::from_value(&nan_val, &types::string()).expect_err("f64 must reject STRING column");
        assert_eq!(
            string_col_err.to_string(),
            "type mismatch, expected Float64, got String",
            "expected TypeMismatch when reading STRING column as f64"
        );

        // f64 rejects non-numeric string in FLOAT64 column
        let invalid_str_err =
            f64::from_value(&"not-a-number".to_string().to_value(), &types::float64())
                .expect_err("f64 must reject arbitrary string representation");
        assert!(
            format!("{}", invalid_str_err).contains("invalid float literal"),
            "expected invalid float literal error"
        );

        // f32 non-finite strings and numbers
        let f32_num = f32::from_value(&12.5f32.to_value(), &types::float32()).expect("valid f32");
        assert_eq!(f32_num, 12.5, "expected 12.5 for f32");

        let f32_nan = f32::from_value(&nan_val, &types::float32()).expect("valid NaN f32");
        assert!(f32_nan.is_nan(), "expected NaN for f32");

        let f32_inf = f32::from_value(&inf_val, &types::float32()).expect("valid Infinity f32");
        assert_eq!(f32_inf, f32::INFINITY, "expected Infinity for f32");

        let f32_neginf =
            f32::from_value(&neg_inf_val, &types::float32()).expect("valid -Infinity f32");
        assert_eq!(f32_neginf, f32::NEG_INFINITY, "expected -Infinity for f32");

        // f32 accepts NumberValue from FLOAT64 column
        let f32_from_f64 = f32::from_value(&42.5f64.to_value(), &types::float64())
            .expect("f32 accepts NumberValue from FLOAT64 column");
        assert_eq!(f32_from_f64, 42.5, "expected 42.5 from NumberValue for f32");

        // f32 rejects STRING column
        let f32_str_err =
            f32::from_value(&nan_val, &types::string()).expect_err("f32 must reject STRING column");
        assert_eq!(
            f32_str_err.to_string(),
            "type mismatch, expected Float32, got String",
            "expected TypeMismatch when reading STRING column as f32"
        );

        // f32 parses numeric string in FLOAT32 column
        let f32_from_str = f32::from_value(&"12.5".to_string().to_value(), &types::float32())
            .expect("valid f32 from numeric string");
        assert_eq!(f32_from_str, 12.5, "expected 12.5 from string for f32");

        // f32 rejects non-numeric string in FLOAT32 column
        let invalid_f32_str_error =
            f32::from_value(&"not-a-number".to_string().to_value(), &types::float32())
                .expect_err("f32 must reject arbitrary string representation");
        assert!(
            format!("{invalid_f32_str_error}").contains("invalid float literal"),
            "expected invalid float literal error for f32"
        );

        // String::from_value rejects FLOAT64 and FLOAT32 columns (even with non-finite string representation)
        let string_f64_nan_err = String::from_value(&nan_val, &types::float64())
            .expect_err("String must reject FLOAT64 column with NaN");
        assert_eq!(
            string_f64_nan_err.to_string(),
            "type mismatch, expected String, got Float64",
            "expected TypeMismatch when reading FLOAT64 column as String"
        );

        let string_f32_inf_err = String::from_value(&inf_val, &types::float32())
            .expect_err("String must reject FLOAT32 column with Infinity");
        assert_eq!(
            string_f32_inf_err.to_string(),
            "type mismatch, expected String, got Float32",
            "expected TypeMismatch when reading FLOAT32 column as String"
        );

        let string_f64_num_err = String::from_value(&42.5f64.to_value(), &types::float64())
            .expect_err("String must reject FLOAT64 column with NumberValue");
        assert_eq!(
            string_f64_num_err.to_string(),
            "type mismatch, expected String, got Float64",
            "expected TypeMismatch when reading FLOAT64 NumberValue as String"
        );
    }

    #[test]
    fn from_value_f32_overflow_rejected() {
        let test_cases = [
            (
                Value::from(1e300_f64),
                types::float64(),
                "positive f64 overflow in NumberValue",
            ),
            (
                Value::from(-1e300_f64),
                types::float64(),
                "negative f64 overflow in NumberValue",
            ),
            (
                Value::from(1e39_f64),
                types::float64(),
                "moderate positive overflow in NumberValue",
            ),
            (
                Value::from(-1e39_f64),
                types::float64(),
                "moderate negative overflow in NumberValue",
            ),
            (
                Value::from(f64::MAX),
                types::float64(),
                "extreme positive boundary f64::MAX in NumberValue",
            ),
            (
                Value::from(f64::MIN),
                types::float64(),
                "extreme negative boundary f64::MIN in NumberValue",
            ),
            (
                Value::from(3.4028236e38_f64),
                types::float64(),
                "positive float just above f32::MAX",
            ),
            (
                Value::from(-3.4028236e38_f64),
                types::float64(),
                "negative float just below f32::MIN",
            ),
            (
                "1e300".to_string().to_value(),
                types::float32(),
                "positive overflow in StringValue for FLOAT32",
            ),
            (
                "-1e300".to_string().to_value(),
                types::float32(),
                "negative overflow in StringValue for FLOAT32",
            ),
            (
                "1e300".to_string().to_value(),
                types::float64(),
                "positive overflow in StringValue for FLOAT64",
            ),
            (
                "-1e300".to_string().to_value(),
                types::float64(),
                "negative overflow in StringValue for FLOAT64",
            ),
            (
                "3.4028236e38".to_string().to_value(),
                types::float32(),
                "StringValue just above f32::MAX for FLOAT32",
            ),
            (
                "-3.4028236e38".to_string().to_value(),
                types::float32(),
                "StringValue just below f32::MIN for FLOAT32",
            ),
            (
                "3.4028236e38".to_string().to_value(),
                types::float64(),
                "StringValue just above f32::MAX for FLOAT64",
            ),
            (
                "-3.4028236e38".to_string().to_value(),
                types::float64(),
                "StringValue just below f32::MIN for FLOAT64",
            ),
            (
                "1e500".to_string().to_value(),
                types::float32(),
                "extreme positive overflow in StringValue for FLOAT32",
            ),
            (
                "-1e500".to_string().to_value(),
                types::float32(),
                "extreme negative overflow in StringValue for FLOAT32",
            ),
            (
                "1e500".to_string().to_value(),
                types::float64(),
                "extreme positive overflow in StringValue for FLOAT64",
            ),
            (
                "-1e500".to_string().to_value(),
                types::float64(),
                "extreme negative overflow in StringValue for FLOAT64",
            ),
        ];

        for (value, spanner_type, description) in test_cases {
            let error = f32::from_value(&value, &spanner_type)
                .expect_err(&format!("{description} must return ConvertError"));
            let error_message = error.to_string();
            assert!(
                error_message.contains("exceeds f32 range"),
                "expected 'exceeds f32 range' in error message for {description}, got: {error_message}"
            );
        }

        // Verify boundary values within f32 range succeed
        let max_f32_as_f64 = Value::from(f32::MAX as f64);
        let parsed_max_f32 = f32::from_value(&max_f32_as_f64, &types::float64())
            .expect("f32::MAX as f64 must convert successfully");
        assert_eq!(parsed_max_f32, f32::MAX, "expected f32::MAX");

        let min_f32_as_f64 = Value::from(f32::MIN as f64);
        let parsed_min_f32 = f32::from_value(&min_f32_as_f64, &types::float64())
            .expect("f32::MIN as f64 must convert successfully");
        assert_eq!(parsed_min_f32, f32::MIN, "expected f32::MIN");

        let zero_f64 = Value::from(0.0_f64);
        let parsed_zero_f32 = f32::from_value(&zero_f64, &types::float64())
            .expect("0.0 as f64 must convert successfully");
        assert_eq!(parsed_zero_f32, 0.0_f32, "expected 0.0");

        let negative_zero_f64 = Value::from(-0.0_f64);
        let parsed_negative_zero_f32 = f32::from_value(&negative_zero_f64, &types::float64())
            .expect("-0.0 as f64 must convert successfully");
        assert_eq!(parsed_negative_zero_f32, -0.0_f32, "expected -0.0");
        assert!(
            parsed_negative_zero_f32.is_sign_negative(),
            "expected negative zero to retain sign"
        );

        let string_max_value = "3.4028234e38".to_string().to_value();
        let parsed_string_max = f32::from_value(&string_max_value, &types::float32())
            .expect("valid string at f32 upper boundary must succeed");
        assert_eq!(
            parsed_string_max, 3.4028234e38_f32,
            "expected valid f32 boundary representation for string max"
        );

        let string_min_value = "-3.4028234e38".to_string().to_value();
        let parsed_string_min = f32::from_value(&string_min_value, &types::float32())
            .expect("valid string at f32 lower boundary must succeed");
        assert_eq!(
            parsed_string_min, -3.4028234e38_f32,
            "expected valid f32 boundary representation for string min"
        );

        let string_max_f64_value = "3.4028234e38".to_string().to_value();
        let parsed_string_max_f64 = f32::from_value(&string_max_f64_value, &types::float64())
            .expect("valid string at f32 upper boundary on FLOAT64 column must succeed");
        assert_eq!(
            parsed_string_max_f64, 3.4028234e38_f32,
            "expected valid f32 upper boundary from FLOAT64 string"
        );

        let string_min_f64_value = "-3.4028234e38".to_string().to_value();
        let parsed_string_min_f64 = f32::from_value(&string_min_f64_value, &types::float64())
            .expect("valid string at f32 lower boundary on FLOAT64 column must succeed");
        assert_eq!(
            parsed_string_min_f64, -3.4028234e38_f32,
            "expected valid f32 lower boundary from FLOAT64 string"
        );

        // Verify gradual underflow to zero
        let underflow_f64 = Value::from(1e-50_f64);
        let parsed_underflow_f32 = f32::from_value(&underflow_f64, &types::float64())
            .expect("underflow f64 must convert to 0.0 without error");
        assert_eq!(parsed_underflow_f32, 0.0_f32, "expected 0.0 on underflow");

        let string_underflow_value = "1e-50".to_string().to_value();
        let parsed_string_underflow = f32::from_value(&string_underflow_value, &types::float32())
            .expect("underflow string must convert to 0.0 without error");
        assert_eq!(
            parsed_string_underflow, 0.0_f32,
            "expected 0.0 on string underflow"
        );

        let string_underflow_f64_value = "1e-50".to_string().to_value();
        let parsed_string_underflow_f64 =
            f32::from_value(&string_underflow_f64_value, &types::float64())
                .expect("underflow string on FLOAT64 column must convert to 0.0 without error");
        assert_eq!(
            parsed_string_underflow_f64, 0.0_f32,
            "expected 0.0 on string underflow from FLOAT64"
        );
    }

    #[test]
    fn from_value_f32_non_finite_and_edge_cases() {
        // "+Infinity" explicitly accepted across FLOAT32 and FLOAT64
        let plus_infinity_value = "+Infinity".to_string().to_value();
        let f32_plus_infinity = f32::from_value(&plus_infinity_value, &types::float32())
            .expect("+Infinity must succeed for FLOAT32");
        assert_eq!(
            f32_plus_infinity,
            f32::INFINITY,
            "expected Infinity for +Infinity"
        );

        let f32_plus_infinity_f64 = f32::from_value(&plus_infinity_value, &types::float64())
            .expect("+Infinity must succeed for FLOAT64");
        assert_eq!(
            f32_plus_infinity_f64,
            f32::INFINITY,
            "expected Infinity for +Infinity on FLOAT64"
        );

        let f64_plus_infinity = f64::from_value(&plus_infinity_value, &types::float64())
            .expect("+Infinity must succeed for f64 on FLOAT64");
        assert_eq!(
            f64_plus_infinity,
            f64::INFINITY,
            "expected Infinity for +Infinity on f64 FLOAT64"
        );

        let f64_plus_infinity_from_f32 = f64::from_value(&plus_infinity_value, &types::float32())
            .expect("+Infinity must succeed for f64 on FLOAT32");
        assert_eq!(
            f64_plus_infinity_from_f32,
            f64::INFINITY,
            "expected Infinity for +Infinity on f64 FLOAT32"
        );

        // FLOAT64 column non-finite strings into f32
        let infinity_value = "Infinity".to_string().to_value();
        let f32_infinity_f64 = f32::from_value(&infinity_value, &types::float64())
            .expect("Infinity must succeed for FLOAT64");
        assert_eq!(
            f32_infinity_f64,
            f32::INFINITY,
            "expected Infinity for f32 on FLOAT64 column"
        );

        let negative_infinity_value = "-Infinity".to_string().to_value();
        let f32_negative_infinity_f64 =
            f32::from_value(&negative_infinity_value, &types::float64())
                .expect("-Infinity must succeed for FLOAT64");
        assert_eq!(
            f32_negative_infinity_f64,
            f32::NEG_INFINITY,
            "expected -Infinity for f32 on FLOAT64 column"
        );

        let nan_value = "NaN".to_string().to_value();
        let f32_nan_f64 =
            f32::from_value(&nan_value, &types::float64()).expect("NaN must succeed for FLOAT64");
        assert!(
            f32_nan_f64.is_nan(),
            "expected NaN for f32 on FLOAT64 column"
        );

        // Non-finite ProtoKind::NumberValue passes through
        let number_infinity = Value(ProtoValue {
            kind: Some(ProtoKind::NumberValue(f64::INFINITY)),
        });
        let parsed_number_infinity = f32::from_value(&number_infinity, &types::float64())
            .expect("NumberValue(INFINITY) must pass through");
        assert_eq!(
            parsed_number_infinity,
            f32::INFINITY,
            "expected Infinity for NumberValue(f64::INFINITY)"
        );

        let number_negative_infinity = Value(ProtoValue {
            kind: Some(ProtoKind::NumberValue(f64::NEG_INFINITY)),
        });
        let parsed_number_negative_infinity =
            f32::from_value(&number_negative_infinity, &types::float64())
                .expect("NumberValue(-INFINITY) must pass through");
        assert_eq!(
            parsed_number_negative_infinity,
            f32::NEG_INFINITY,
            "expected -Infinity for NumberValue(f64::NEG_INFINITY)"
        );

        let number_nan = Value(ProtoValue {
            kind: Some(ProtoKind::NumberValue(f64::NAN)),
        });
        let parsed_number_nan = f32::from_value(&number_nan, &types::float64())
            .expect("NumberValue(NAN) must pass through");
        assert!(
            parsed_number_nan.is_nan(),
            "expected NaN for NumberValue(f64::NAN)"
        );

        // Type mismatches
        let type_mismatch_int64 = f32::from_value(&42.5f32.to_value(), &types::int64())
            .expect_err("f32 must reject INT64 column");
        assert_eq!(
            type_mismatch_int64.to_string(),
            "type mismatch, expected Float32, got Int64",
            "expected type mismatch for Int64"
        );

        let type_mismatch_numeric = f32::from_value(&42.5f32.to_value(), &types::numeric())
            .expect_err("f32 must reject NUMERIC column");
        assert_eq!(
            type_mismatch_numeric.to_string(),
            "type mismatch, expected Float32, got Numeric",
            "expected type mismatch for Numeric"
        );

        let type_mismatch_bool = f32::from_value(&42.5f32.to_value(), &types::bool())
            .expect_err("f32 must reject BOOL column");
        assert_eq!(
            type_mismatch_bool.to_string(),
            "type mismatch, expected Float32, got Bool",
            "expected type mismatch for Bool"
        );

        // Kind mismatch on FLOAT64 column
        let kind_mismatch_error = f32::from_value(&Value::from(true), &types::float64())
            .expect_err("f32 must reject boolean Value on FLOAT64 column");
        assert!(
            matches!(
                kind_mismatch_error,
                ConvertError::KindMismatch {
                    want: Kind::Number,
                    got: Kind::Bool,
                }
            ),
            "expected KindMismatch {{ want: Kind::Number, got: Kind::Bool }} for bool Value on FLOAT64, got: {kind_mismatch_error}"
        );

        // Null checks: Value::null() (NullValue) and Value::default() (None kind)
        let not_null_error = f32::from_value(&Value::null(), &types::float64())
            .expect_err("f32 must reject null on FLOAT64 column");
        assert_eq!(
            not_null_error.to_string(),
            "expected non-null value, got null",
            "expected NotNull error for null on FLOAT64"
        );

        let default_none_error = f32::from_value(&Value::default(), &types::float64())
            .expect_err("f32 must reject Value::default() with None kind on FLOAT64 column");
        assert_eq!(
            default_none_error.to_string(),
            "expected non-null value, got null",
            "expected NotNull error for default Value (None kind) on FLOAT64"
        );

        // Alternative infinity strings across FLOAT32 and FLOAT64
        let alternative_infinities = vec![
            ("inf", f32::INFINITY, f64::INFINITY),
            ("+inf", f32::INFINITY, f64::INFINITY),
            ("-inf", f32::NEG_INFINITY, f64::NEG_INFINITY),
            ("infinity", f32::INFINITY, f64::INFINITY),
            ("+infinity", f32::INFINITY, f64::INFINITY),
            ("-infinity", f32::NEG_INFINITY, f64::NEG_INFINITY),
            ("INF", f32::INFINITY, f64::INFINITY),
            ("+INF", f32::INFINITY, f64::INFINITY),
            ("-INF", f32::NEG_INFINITY, f64::NEG_INFINITY),
            ("INFINITY", f32::INFINITY, f64::INFINITY),
            ("-INFINITY", f32::NEG_INFINITY, f64::NEG_INFINITY),
        ];

        for (infinity_str, expected_f32, expected_f64) in alternative_infinities {
            let value = infinity_str.to_string().to_value();
            let parsed_f32_from_f32 = f32::from_value(&value, &types::float32())
                .expect("infinity string must parse to f32 on FLOAT32");
            assert_eq!(
                parsed_f32_from_f32, expected_f32,
                "expected {expected_f32} for '{infinity_str}' on FLOAT32"
            );

            let parsed_f32_from_f64 = f32::from_value(&value, &types::float64())
                .expect("infinity string must parse to f32 on FLOAT64");
            assert_eq!(
                parsed_f32_from_f64, expected_f32,
                "expected {expected_f32} for '{infinity_str}' on FLOAT64"
            );

            let parsed_f64_from_f64 = f64::from_value(&value, &types::float64())
                .expect("infinity string must parse to f64 on FLOAT64");
            assert_eq!(
                parsed_f64_from_f64, expected_f64,
                "expected {expected_f64} for '{infinity_str}' on FLOAT64"
            );
        }

        // Non-numeric string on FLOAT64 column
        let invalid_string_error =
            f32::from_value(&"not-a-number".to_string().to_value(), &types::float64())
                .expect_err("f32 must reject arbitrary string representation");
        assert!(
            format!("{invalid_string_error}").contains("invalid float literal"),
            "expected invalid float literal error for f32 on FLOAT64"
        );

        // Malformed infinity strings
        let invalid_inf_error =
            f32::from_value(&"invalid inf".to_string().to_value(), &types::float64())
                .expect_err("f32 must reject 'invalid inf'");
        assert!(
            format!("{invalid_inf_error}").contains("invalid float literal"),
            "expected invalid float literal error for 'invalid inf', got: {invalid_inf_error}"
        );

        let inff_error = f32::from_value(&"inff".to_string().to_value(), &types::float64())
            .expect_err("f32 must reject 'inff'");
        assert!(
            format!("{inff_error}").contains("invalid float literal"),
            "expected invalid float literal error for 'inff', got: {inff_error}"
        );
    }

    #[test]
    fn is_infinity_literal_cases() {
        let test_cases = [
            ("inf", true),
            ("INF", true),
            ("Inf", true),
            ("+inf", true),
            ("-inf", true),
            ("infinity", true),
            ("Infinity", true),
            ("INFINITY", true),
            ("+Infinity", true),
            ("-Infinity", true),
            ("  inf  ", true),
            ("  +infinity  ", true),
            ("1e300", false),
            ("-1e300", false),
            ("1e500", false),
            ("NaN", false),
            ("inff", false),
            ("invalid inf", false),
            ("0.0", false),
        ];

        for (input_literal, expected_result) in test_cases {
            assert_eq!(
                is_infinity_literal(input_literal),
                expected_result,
                "mismatch for '{input_literal}'"
            );
        }
    }

    #[test]
    fn from_value_non_finite_float_containers() {
        // Option<f64>
        let nan_val = "NaN".to_string().to_value();
        let opt_nan = Option::<f64>::from_value(&nan_val, &types::float64())
            .expect("parse Option<f64> containing NaN");
        assert!(
            opt_nan.is_some_and(|f| f.is_nan()),
            "expected Some(NaN) for Option<f64>"
        );

        let opt_null = Option::<f64>::from_value(&Value::null(), &types::float64())
            .expect("parse Option<f64> containing null");
        assert_eq!(opt_null, None, "expected None for null Option<f64>");

        // Vec<f64> with non-finite values
        let f64_vec = vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 123.5];
        let v = f64_vec.to_value();
        let parsed_vec = Vec::<f64>::from_value(&v, &types::array(types::float64()))
            .expect("parse Vec<f64> with non-finite elements");
        assert_eq!(parsed_vec.len(), 4);
        assert!(parsed_vec[0].is_nan(), "expected NaN in Vec<f64>[0]");
        assert_eq!(
            parsed_vec[1],
            f64::INFINITY,
            "expected Infinity in Vec<f64>[1]"
        );
        assert_eq!(
            parsed_vec[2],
            f64::NEG_INFINITY,
            "expected -Infinity in Vec<f64>[2]"
        );
        assert_eq!(parsed_vec[3], 123.5, "expected 123.5 in Vec<f64>[3]");

        // Vec<Option<f64>> with non-finite values and None
        let opt_f64_vec = vec![
            Some(f64::NAN),
            None,
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ];
        let v_opt = opt_f64_vec.to_value();
        let parsed_opt_vec =
            Vec::<Option<f64>>::from_value(&v_opt, &types::array(types::float64()))
                .expect("parse Vec<Option<f64>> with non-finite elements and null");
        assert_eq!(parsed_opt_vec.len(), 4);
        assert!(
            parsed_opt_vec[0].is_some_and(|f| f.is_nan()),
            "expected Some(NaN) in Vec<Option<f64>>[0]"
        );
        assert_eq!(
            parsed_opt_vec[1], None,
            "expected None in Vec<Option<f64>>[1]"
        );
        assert_eq!(
            parsed_opt_vec[2],
            Some(f64::INFINITY),
            "expected Some(Infinity) in Vec<Option<f64>>[2]"
        );
        assert_eq!(
            parsed_opt_vec[3],
            Some(f64::NEG_INFINITY),
            "expected Some(-Infinity) in Vec<Option<f64>>[3]"
        );

        // Vec<f32> with non-finite values
        let f32_vec = vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY, 12.5f32];
        let v_f32 = f32_vec.to_value();
        let parsed_f32_vec = Vec::<f32>::from_value(&v_f32, &types::array(types::float32()))
            .expect("parse Vec<f32> with non-finite elements");
        assert_eq!(parsed_f32_vec.len(), 4);
        assert!(parsed_f32_vec[0].is_nan(), "expected NaN in Vec<f32>[0]");
        assert_eq!(
            parsed_f32_vec[1],
            f32::INFINITY,
            "expected Infinity in Vec<f32>[1]"
        );
        assert_eq!(
            parsed_f32_vec[2],
            f32::NEG_INFINITY,
            "expected -Infinity in Vec<f32>[2]"
        );
        assert_eq!(parsed_f32_vec[3], 12.5, "expected 12.5 in Vec<f32>[3]");
    }

    #[test]
    fn from_value_f32_containers_from_float64() {
        // Vec<f32> from ARRAY<FLOAT64>
        let float64_for_f32_vec = vec![1.5_f64, 2.5_f64];
        let value_float64_for_f32 = float64_for_f32_vec.to_value();
        let parsed_float32_from_float64 =
            Vec::<f32>::from_value(&value_float64_for_f32, &types::array(types::float64()))
                .expect("parse Vec<f32> from ARRAY<FLOAT64>");
        assert_eq!(
            parsed_float32_from_float64,
            vec![1.5_f32, 2.5_f32],
            "expected parsed Vec<f32> elements from FLOAT64"
        );

        // Vec<f32> from ARRAY<FLOAT64> with non-finite values
        let float64_non_finite_vec = vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
        let value_non_finite = float64_non_finite_vec.to_value();
        let parsed_non_finite_f32 =
            Vec::<f32>::from_value(&value_non_finite, &types::array(types::float64()))
                .expect("parse Vec<f32> with non-finite values from ARRAY<FLOAT64>");
        assert_eq!(
            parsed_non_finite_f32.len(),
            3,
            "expected 3 non-finite elements"
        );
        assert!(
            parsed_non_finite_f32[0].is_nan(),
            "expected NaN in parsed non-finite Vec<f32>[0]"
        );
        assert_eq!(
            parsed_non_finite_f32[1],
            f32::INFINITY,
            "expected Infinity in parsed non-finite Vec<f32>[1]"
        );
        assert_eq!(
            parsed_non_finite_f32[2],
            f32::NEG_INFINITY,
            "expected -Infinity in parsed non-finite Vec<f32>[2]"
        );

        // Vec<Option<f32>> from ARRAY<FLOAT64> containing null, finite, and non-finite floats
        let optional_float64_for_f32_vec = vec![
            Some(1.5_f64),
            None,
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ];
        let value_optional_float64_for_f32 = optional_float64_for_f32_vec.to_value();
        let parsed_optional_f32_from_f64 = Vec::<Option<f32>>::from_value(
            &value_optional_float64_for_f32,
            &types::array(types::float64()),
        )
        .expect("parse Vec<Option<f32>> from ARRAY<FLOAT64>");
        assert_eq!(
            parsed_optional_f32_from_f64.len(),
            5,
            "expected 5 elements in Vec<Option<f32>>"
        );
        assert_eq!(
            parsed_optional_f32_from_f64[0],
            Some(1.5_f32),
            "expected Some(1.5) in Vec<Option<f32>>[0]"
        );
        assert_eq!(
            parsed_optional_f32_from_f64[1], None,
            "expected None in Vec<Option<f32>>[1]"
        );
        assert!(
            parsed_optional_f32_from_f64[2].is_some_and(|value| value.is_nan()),
            "expected Some(NaN) in Vec<Option<f32>>[2]"
        );
        assert_eq!(
            parsed_optional_f32_from_f64[3],
            Some(f32::INFINITY),
            "expected Some(Infinity) in Vec<Option<f32>>[3]"
        );
        assert_eq!(
            parsed_optional_f32_from_f64[4],
            Some(f32::NEG_INFINITY),
            "expected Some(-Infinity) in Vec<Option<f32>>[4]"
        );

        // Vec<Option<f32>> from ARRAY<FLOAT64> with overflowing value
        let optional_float64_overflow_vec = vec![Some(1.0_f64), Some(1e300_f64)];
        let value_optional_overflow = optional_float64_overflow_vec.to_value();
        let optional_overflow_error = Vec::<Option<f32>>::from_value(
            &value_optional_overflow,
            &types::array(types::float64()),
        )
        .expect_err("Vec<Option<f32>> must reject overflowing FLOAT64 element");
        assert!(
            optional_overflow_error
                .to_string()
                .contains("exceeds f32 range"),
            "expected 'exceeds f32 range' in Vec<Option<f32>> error, got: {optional_overflow_error}"
        );

        // Option<Vec<f32>> from ARRAY<FLOAT64>
        let optional_vector_null =
            Option::<Vec<f32>>::from_value(&Value::null(), &types::array(types::float64()))
                .expect("Option<Vec<f32>> from null ARRAY<FLOAT64> must be Ok(None)");
        assert_eq!(
            optional_vector_null, None,
            "expected None for null Option<Vec<f32>>"
        );

        let optional_vector_value =
            Option::<Vec<f32>>::from_value(&value_float64_for_f32, &types::array(types::float64()))
                .expect("Option<Vec<f32>> from populated ARRAY<FLOAT64> must succeed");
        assert_eq!(
            optional_vector_value,
            Some(vec![1.5_f32, 2.5_f32]),
            "expected Some([1.5, 2.5]) for Option<Vec<f32>>"
        );

        // Vec<f32> from ARRAY<FLOAT64> with overflowing value
        let float64_overflow_vec = vec![1.0_f64, 1e300_f64];
        let value_overflow = float64_overflow_vec.to_value();
        let overflow_error =
            Vec::<f32>::from_value(&value_overflow, &types::array(types::float64()))
                .expect_err("Vec<f32> must reject overflowing FLOAT64 element");
        assert!(
            overflow_error.to_string().contains("exceeds f32 range"),
            "expected 'exceeds f32 range' in Vec<f32> error, got: {overflow_error}"
        );

        // Option<f32> from FLOAT64
        let optional_f32_null = Option::<f32>::from_value(&Value::null(), &types::float64())
            .expect("Option<f32> from null FLOAT64 must be Ok(None)");
        assert_eq!(
            optional_f32_null, None,
            "expected None for null Option<f32> from FLOAT64"
        );

        let optional_f32_value =
            Option::<f32>::from_value(&Value::from(42.5_f64), &types::float64())
                .expect("Option<f32> from 42.5 FLOAT64 must succeed");
        assert_eq!(
            optional_f32_value,
            Some(42.5_f32),
            "expected Some(42.5) for Option<f32> from FLOAT64"
        );

        let optional_f32_overflow_error =
            Option::<f32>::from_value(&Value::from(1e300_f64), &types::float64())
                .expect_err("Option<f32> must propagate overflow error");
        assert!(
            optional_f32_overflow_error
                .to_string()
                .contains("exceeds f32 range"),
            "expected 'exceeds f32 range' in Option<f32> error, got: {optional_f32_overflow_error}"
        );
    }

    #[test]
    fn test_from_value_bool() {
        let v = true.to_value();
        let b = bool::from_value(&v, &types::bool()).unwrap();
        assert!(b);
    }

    #[test]
    fn test_from_value_array() {
        // String array
        let str_array = vec!["one".to_string(), "two".to_string()];
        let v = str_array.to_value();
        let res = Vec::<String>::from_value(&v, &types::array(types::string()))
            .expect("parsed string array");
        assert_eq!(res, str_array);

        // Int array
        let int_array = vec![42i64, 100i64];
        let v = int_array.to_value();
        let res =
            Vec::<i64>::from_value(&v, &types::array(types::int64())).expect("parsed int array");
        assert_eq!(res, int_array);

        // Bool array
        let bool_array = vec![true, false];
        let v = bool_array.to_value();
        let res =
            Vec::<bool>::from_value(&v, &types::array(types::bool())).expect("parsed bool array");
        assert_eq!(res, bool_array);

        // Float array
        let float_array = vec![9.9f64, -2.5f64];
        let v = float_array.to_value();
        let res = Vec::<f64>::from_value(&v, &types::array(types::float64()))
            .expect("parsed float array");
        assert_eq!(res, float_array);

        // Empty array
        let empty_array: Vec<f64> = vec![];
        let v = empty_array.to_value();
        let res = Vec::<f64>::from_value(&v, &types::array(types::float64()))
            .expect("parsed empty array");
        assert_eq!(res, empty_array);

        // Array with nulls
        let opt_array: Vec<Option<i64>> = vec![Some(42), None, Some(100)];
        let v = opt_array.to_value();
        let res = Vec::<Option<i64>>::from_value(&v, &types::array(types::int64()))
            .expect("parsed optional array");
        assert_eq!(res, opt_array);

        // Null array entirely
        let null_array: Option<Vec<i64>> = None;
        let v = null_array.to_value();
        let res = Option::<Vec<i64>>::from_value(&v, &types::array(types::int64()))
            .expect("parsed null array");
        assert_eq!(res, null_array);

        // Wrong TypeCode test
        let err = Vec::<i64>::from_value(&int_array.to_value(), &types::int64())
            .expect_err("wrong type code should fail");
        assert_eq!(
            err.to_string(),
            "type mismatch, expected Array, got Int64",
            "expected exact TypeMismatch error message"
        );

        // Array element TypeMismatch test (ARRAY<STRING> column decoded as Vec<i64>)
        let err = Vec::<i64>::from_value(&str_array.to_value(), &types::array(types::string()))
            .expect_err("element type mismatch should fail");
        assert_eq!(
            err.to_string(),
            "type mismatch, expected Int64, got String",
            "expected TypeMismatch for array element type"
        );

        // Invalid array element values
        let err = Vec::<i64>::from_value(&str_array.to_value(), &types::array(types::int64()))
            .expect_err("invalid array element values should fail");
        assert!(format!("{}", err).contains("cannot convert value, source="));
    }

    #[test]
    fn test_from_value_bytes() {
        let bytes: Vec<u8> = vec![1, 2, 3];
        let v = bytes.to_value();
        let b = Vec::<u8>::from_value(&v, &types::bytes()).unwrap();
        assert_eq!(b, bytes);

        let v = "invalid base64".to_string().to_value();
        let err = Vec::<u8>::from_value(&v, &types::bytes()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));
    }

    #[test]
    fn from_value_decimal() -> Result<(), ConvertError> {
        let numeric_type = types::numeric();

        let decimal = Decimal::from_str_exact("123.456").expect("valid decimal");
        let value = decimal.to_value();
        let result = Decimal::from_value(&value, &numeric_type)?;
        assert_eq!(result, decimal);

        // Fixed-point sign and zero variations
        let valid_cases = [
            ("+123.456", "123.456"),
            ("-123.456", "-123.456"),
            ("+1", "1"),
            ("-1", "-1"),
            ("0", "0"),
            ("0.0", "0"),
            ("+0", "0"),
            ("-0", "0"),
            (".1", "0.1"),
            ("+.1", "0.1"),
            ("-.1", "-0.1"),
            ("1.", "1"),
            ("+1.", "1"),
            ("-1.", "-1"),
            ("100000000000000000000.1", "100000000000000000000.1"),
        ];

        for (input_str, expected_str) in valid_cases {
            let value = input_str.to_string().to_value();
            let parsed = Decimal::from_value(&value, &numeric_type)
                .expect("valid decimal conversion from supported format");
            assert_eq!(
                parsed,
                Decimal::from_str_exact(expected_str).expect("valid decimal"),
                "mismatch for {input_str}"
            );
        }

        let value = "invalid decimal".to_string().to_value();
        let error =
            Decimal::from_value(&value, &numeric_type).expect_err("invalid decimal should fail");
        assert!(format!("{error}").contains("cannot convert value"));

        Ok(())
    }

    #[test]
    fn from_value_decimal_scientific_notation() -> Result<(), ConvertError> {
        let numeric_type = types::numeric();

        let valid_scientific_cases = [
            // Integer scientific notation
            ("1e5", "100000"),
            ("+1e5", "100000"),
            ("-1e5", "-100000"),
            // Positive exponent with explicit plus sign
            ("1E+1", "10"),
            ("1e+1", "10"),
            ("1E1", "10"),
            ("01e1", "10"),
            ("1e01", "10"),
            // Negative exponent
            ("1E-1", "0.1"),
            ("1e-1", "0.1"),
            ("01e-1", "0.1"),
            ("1e-01", "0.1"),
            // Fractional base with exponent
            ("1.e2", "100"),
            (".5e2", "50"),
            ("+.5e-2", "0.005"),
            ("-.5e-2", "-0.005"),
            // Fractional base with positive exponent plus sign
            (".5e+2", "50"),
            ("1.e+2", "100"),
            ("-1.e+2", "-100"),
            // Zero mantissa in scientific notation
            ("0e0", "0"),
            ("0e5", "0"),
            ("0e-5", "0"),
            ("-0e0", "0"),
            ("+0e0", "0"),
            // Zero exponent in scientific notation
            ("1e0", "1"),
            ("1E0", "1"),
            ("1e+0", "1"),
            ("1e-0", "1"),
        ];

        for (input_str, expected_str) in valid_scientific_cases {
            let value = input_str.to_string().to_value();
            assert_eq!(
                Decimal::from_value(&value, &numeric_type)?,
                Decimal::from_str_exact(expected_str).expect("valid decimal"),
                "mismatch for {input_str}"
            );
        }

        // Owned value conversion
        let value = "2.5e3".to_string().to_value();
        assert_eq!(
            Decimal::from_owned_value(value, &numeric_type)?,
            Decimal::from_str_exact("2500").expect("valid decimal")
        );

        // Optional decimal with scientific notation and null
        let value = "1e5".to_string().to_value();
        assert_eq!(
            Option::<Decimal>::from_value(&value, &numeric_type)?,
            Some(Decimal::from_str_exact("100000").expect("valid decimal"))
        );

        let value = None::<Decimal>.to_value();
        assert_eq!(Option::<Decimal>::from_value(&value, &numeric_type)?, None);

        // Array with mixed scientific notation and null elements
        let array_type = types::array(types::numeric());
        let list_value = vec![Some("1e2".to_string()), None, Some("1e-1".to_string())].to_value();
        assert_eq!(
            Vec::<Option<Decimal>>::from_value(&list_value, &array_type)?,
            vec![
                Some(Decimal::from_str_exact("100").expect("valid decimal")),
                None,
                Some(Decimal::from_str_exact("0.1").expect("valid decimal")),
            ]
        );

        Ok(())
    }

    #[test]
    fn from_value_decimal_scientific_notation_errors() {
        let numeric_type = types::numeric();

        let invalid_cases = [
            "1e",
            "1E",
            "1e+",
            "1e-",
            "e1",
            "1e5e5",
            "1e50",  // Overflow scale exceeds maximum precision
            "1e-50", // Underflow scale exceeds maximum precision
            // Values exceeding rust_decimal 96-bit mantissa limit (even if valid in Spanner NUMERIC)
            // (2^96 - 1 ≈ 7.9228e28, up to 28-29 digits total)
            "99999999999999999999999999999.999999999", // 38 digits (max Spanner NUMERIC)
            "100000000000000000000.123456789",         // 30 digits
            "100000000000000000000000000000",          // 30 integer digits
            "8e28",
            // PostgreSQL dialect PG_NUMERIC NaN
            "NaN",
        ];

        for invalid_string in invalid_cases {
            let value = invalid_string.to_string().to_value();
            let error = Decimal::from_value(&value, &numeric_type)
                .expect_err("malformed or out-of-range scientific notation should fail");
            assert!(
                matches!(error, ConvertError::Convert(_)),
                "expected ConvertError::Convert for {invalid_string}, got {error:?}"
            );
        }
    }

    #[test]
    fn test_from_value_date() {
        let date = Date::new().set_year(2023).set_month(10).set_day(27);
        let value = date.clone().to_value();
        let result = Date::from_value(&value, &types::date()).expect("valid date conversion");
        assert_eq!(result, date);

        let value = "invalid date".to_string().to_value();
        let error = Date::from_value(&value, &types::date()).expect_err("invalid date should fail");
        assert!(format!("{}", error).contains("cannot convert value"));
    }

    #[cfg(feature = "unstable-time")]
    #[test]
    fn test_from_value_time_date() {
        let date =
            TimeDate::from_calendar_date(2023, Month::October, 27).expect("valid calendar date");
        let value = date.to_value();
        let result =
            TimeDate::from_value(&value, &types::date()).expect("valid time date conversion");
        assert_eq!(result, date);

        let value = "invalid date".to_string().to_value();
        let error =
            TimeDate::from_value(&value, &types::date()).expect_err("invalid date should fail");
        assert!(format!("{}", error).contains("cannot convert value"));
    }

    #[cfg(feature = "unstable-time")]
    #[test]
    fn test_from_value_timestamp() {
        let date_time = OffsetDateTime::parse("2023-10-27T10:00:00Z", &Rfc3339)
            .expect("valid timestamp format");
        let value = date_time.to_value();
        let result = OffsetDateTime::from_value(&value, &types::timestamp())
            .expect("valid timestamp conversion");
        assert_eq!(result, date_time);

        let value = "invalid timestamp".to_string().to_value();
        let error = OffsetDateTime::from_value(&value, &types::timestamp())
            .expect_err("invalid timestamp should fail");
        assert!(format!("{}", error).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_null() {
        let v = Option::<i32>::None.to_value();
        let res = Option::<i32>::from_value(&v, &types::int64()).expect("valid none option");
        assert_eq!(res, None);

        let v = Option::<i32>::None.to_value();
        let err = i32::from_value(&v, &types::int64()).expect_err("expected non-null error");
        assert!(format!("{}", err).contains("expected non-null value, got null"));
    }

    #[test]
    fn test_from_value_system_time() {
        let timestamp = Timestamp::clamp(1_698_400_800, 0);
        let system_time = SystemTime::try_from(timestamp).expect("valid system time conversion");
        let value = system_time.to_value();
        let result = SystemTime::from_value(&value, &types::timestamp())
            .expect("valid system time conversion");
        assert_eq!(result, system_time);

        let value = "invalid timestamp".to_string().to_value();
        let error = SystemTime::from_value(&value, &types::timestamp())
            .expect_err("invalid timestamp should fail");
        assert!(format!("{}", error).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_wkt_timestamp() {
        let timestamp = Timestamp::clamp(1_698_400_800, 0);
        let value = timestamp.to_value();
        let result = Timestamp::from_value(&value, &types::timestamp())
            .expect("valid wkt timestamp decoding");
        assert_eq!(result, timestamp);

        let value = "invalid timestamp".to_string().to_value();
        let error = Timestamp::from_value(&value, &types::timestamp())
            .expect_err("invalid timestamp should fail");
        assert!(format!("{}", error).contains("cannot convert value"));
    }

    #[test]
    fn from_value_type_mismatch() {
        let decimal_value = Decimal::from(42).to_value();
        let decimal_error =
            Decimal::from_value(&decimal_value, &types::int64()).expect_err("type mismatch");
        assert_eq!(
            decimal_error.to_string(),
            "type mismatch, expected Numeric, got Int64",
            "expected exact error message for Decimal"
        );

        let system_time_value = SystemTime::now().to_value();
        let system_time_error = SystemTime::from_value(&system_time_value, &types::string())
            .expect_err("type mismatch");
        assert_eq!(
            system_time_error.to_string(),
            "type mismatch, expected Timestamp, got String",
            "expected exact error message for SystemTime"
        );

        #[cfg(feature = "unstable-time")]
        {
            let value = OffsetDateTime::now_utc().to_value();
            let error =
                OffsetDateTime::from_value(&value, &types::string()).expect_err("type mismatch");
            assert_eq!(
                error.to_string(),
                "type mismatch, expected Timestamp, got String",
                "expected exact error message for OffsetDateTime"
            );

            let value = TimeDate::from_calendar_date(2023, Month::October, 27)
                .expect("valid calendar date")
                .to_value();
            let error = TimeDate::from_value(&value, &types::string()).expect_err("type mismatch");
            assert_eq!(
                error.to_string(),
                "type mismatch, expected Date, got String",
                "expected exact error message for TimeDate"
            );
        }

        let date = Date::new()
            .set_year(2023)
            .set_month(10)
            .set_day(27)
            .to_value();
        let error = Date::from_value(&date, &types::string()).expect_err("type mismatch");
        assert_eq!(
            error.to_string(),
            "type mismatch, expected Date, got String",
            "expected exact error message for Date"
        );

        let bytes_value = vec![1u8].to_value();
        let bytes_error =
            Vec::<u8>::from_value(&bytes_value, &types::string()).expect_err("type mismatch");
        assert_eq!(
            bytes_error.to_string(),
            "type mismatch, expected Bytes, got String",
            "expected exact error message for Vec<u8>"
        );

        let bool_value = true.to_value();
        let bool_error =
            bool::from_value(&bool_value, &types::string()).expect_err("type mismatch");
        assert_eq!(
            bool_error.to_string(),
            "type mismatch, expected Bool, got String",
            "expected exact error message for bool"
        );

        let int64_value = 42i64.to_value();
        let int64_error =
            i64::from_value(&int64_value, &types::string()).expect_err("type mismatch");
        assert_eq!(
            int64_error.to_string(),
            "type mismatch, expected Int64, got String",
            "expected exact error message for i64"
        );

        let int32_error =
            i32::from_value(&int64_value, &types::string()).expect_err("type mismatch");
        assert_eq!(
            int32_error.to_string(),
            "type mismatch, expected Int64, got String",
            "expected exact error message for i32"
        );

        // Option<T> must validate schema type even when value is NULL
        let null_value = Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::NullValue(0)),
        });
        let error_optional_null = Option::<i64>::from_value(&null_value, &types::string())
            .expect_err("Option<i64> must reject STRING column even on NULL");
        assert_eq!(
            error_optional_null.to_string(),
            "type mismatch, expected Int64, got String",
            "expected TypeMismatch for Option<i64> on null STRING column"
        );
        let optional_int64_null = Option::<i64>::from_value(&null_value, &types::int64())
            .expect("Option<i64> must accept NULL on INT64 column");
        assert_eq!(optional_int64_null, None);

        // Option<String> rejects non-string types even on NULL
        let error_null_string_on_int = Option::<String>::from_value(&null_value, &types::int64())
            .expect_err("Option<String> must reject INT64 column even on NULL");
        assert_eq!(
            error_null_string_on_int.to_string(),
            "type mismatch, expected String, got Int64",
            "expected TypeMismatch for Option<String> on null INT64 column"
        );

        let result_null_string = Option::<String>::from_value(&null_value, &types::string())
            .expect("Option<String> accepts NULL on STRING column");
        assert_eq!(result_null_string, None);

        let result_null_date = Option::<String>::from_value(&null_value, &types::date())
            .expect("Option<String> accepts NULL on DATE column");
        assert_eq!(result_null_date, None);

        let result_null_timestamp = Option::<String>::from_value(&null_value, &types::timestamp())
            .expect("Option<String> accepts NULL on TIMESTAMP column");
        assert_eq!(result_null_timestamp, None);

        let result_null_numeric = Option::<String>::from_value(&null_value, &types::numeric())
            .expect("Option<String> accepts NULL on NUMERIC column");
        assert_eq!(result_null_numeric, None);

        // Option<i64> and Option<i32> accept NULL on ENUM columns
        let enum_type = types::proto_enum("customer.Priority");
        let result_null_enum_i64 = Option::<i64>::from_value(&null_value, &enum_type)
            .expect("Option<i64> accepts NULL on ENUM column");
        assert_eq!(result_null_enum_i64, None);

        let result_null_enum_i32 = Option::<i32>::from_value(&null_value, &enum_type)
            .expect("Option<i32> accepts NULL on ENUM column");
        assert_eq!(result_null_enum_i32, None);

        // Option<String> rejects ENUM even on NULL
        let error_null_string_on_enum = Option::<String>::from_value(&null_value, &enum_type)
            .expect_err("Option<String> must reject ENUM column even on NULL");
        assert_eq!(
            error_null_string_on_enum.to_string(),
            "type mismatch, expected String, got Enum",
            "expected TypeMismatch for Option<String> on null ENUM column"
        );

        // Option<String> rejects non-text wire types even on NULL
        let error_null_float = Option::<String>::from_value(&null_value, &types::float64())
            .expect_err("Option<String> must reject FLOAT64 column even on NULL");
        assert_eq!(
            error_null_float.to_string(),
            "type mismatch, expected String, got Float64",
            "expected TypeMismatch for Option<String> on null FLOAT64 column"
        );

        let error_null_bool = Option::<String>::from_value(&null_value, &types::bool())
            .expect_err("Option<String> must reject BOOL column even on NULL");
        assert_eq!(
            error_null_bool.to_string(),
            "type mismatch, expected String, got Bool",
            "expected TypeMismatch for Option<String> on null BOOL column"
        );

        // String::from_value rejects non-string types with TypeMismatch
        let error_string_int = String::from_value(&int64_value, &types::int64())
            .expect_err("String::from_value rejects INT64 column");
        assert_eq!(
            error_string_int.to_string(),
            "type mismatch, expected String, got Int64",
            "expected TypeMismatch for INT64 column"
        );

        let test_bytes_value = vec![1u8, 2, 3].to_value();
        let error_string_bytes = String::from_value(&test_bytes_value, &types::bytes())
            .expect_err("String::from_value rejects BYTES column");
        assert_eq!(
            error_string_bytes.to_string(),
            "type mismatch, expected String, got Bytes",
            "expected TypeMismatch for BYTES column"
        );

        // String::from_value rejects non-text wire types like BOOL and FLOAT64 with TypeMismatch
        let error_string_bool = String::from_value(&bool_value, &types::bool())
            .expect_err("String::from_value rejects BOOL column");
        assert_eq!(
            error_string_bool.to_string(),
            "type mismatch, expected String, got Bool",
            "expected TypeMismatch for BOOL column"
        );

        let float_value = 42.5f64.to_value();
        let error_string_float = String::from_value(&float_value, &types::float64())
            .expect_err("String::from_value rejects FLOAT64 column");
        assert_eq!(
            error_string_float.to_string(),
            "type mismatch, expected String, got Float64",
            "expected TypeMismatch for FLOAT64 column"
        );

        // String::from_value rejects ENUM column with TypeMismatch
        let enum_value = 3i64.to_value();
        let error_string_enum = String::from_value(&enum_value, &enum_type)
            .expect_err("String::from_value rejects ENUM column");
        assert_eq!(
            error_string_enum.to_string(),
            "type mismatch, expected String, got Enum",
            "expected TypeMismatch for ENUM column as String"
        );

        // i64 and i32 accept ENUM column
        let int64_from_enum =
            i64::from_value(&enum_value, &enum_type).expect("i64::from_value accepts ENUM column");
        assert_eq!(int64_from_enum, 3, "expected i64 value 3 from ENUM");

        let int32_from_enum =
            i32::from_value(&enum_value, &enum_type).expect("i32::from_value accepts ENUM column");
        assert_eq!(int32_from_enum, 3, "expected i32 value 3 from ENUM");

        // String::from_value accepts textual types (String, Json, Uuid, Interval)
        let string_from_json =
            String::from_value(&"{\"a\":1}".to_string().to_value(), &types::json())
                .expect("String::from_value accepts JSON column");
        assert_eq!(string_from_json, "{\"a\":1}", "expected JSON text");

        let string_from_uuid = String::from_value(
            &"123e4567-e89b-12d3-a456-426614174000"
                .to_string()
                .to_value(),
            &types::uuid(),
        )
        .expect("String::from_value accepts UUID column");
        assert_eq!(
            string_from_uuid, "123e4567-e89b-12d3-a456-426614174000",
            "expected UUID text"
        );

        let string_from_interval =
            String::from_value(&"P1Y2M3D".to_string().to_value(), &types::interval())
                .expect("String::from_value accepts INTERVAL column");
        assert_eq!(string_from_interval, "P1Y2M3D", "expected INTERVAL text");

        // String::from_value accepts DATE, NUMERIC, and TIMESTAMP
        let string_from_date = String::from_value(&date, &types::date())
            .expect("String::from_value accepts DATE column");
        assert_eq!(string_from_date, "2023-10-27", "expected DATE string");

        let decimal_value = Decimal::from(42).to_value();
        let string_from_decimal = String::from_value(&decimal_value, &types::numeric())
            .expect("String::from_value accepts NUMERIC column");
        assert_eq!(string_from_decimal, "42", "expected NUMERIC string");

        let timestamp_value = Timestamp::clamp(1_698_400_800, 0).to_value();
        let string_from_timestamp = String::from_value(&timestamp_value, &types::timestamp())
            .expect("String::from_value accepts TIMESTAMP column");
        assert_eq!(
            string_from_timestamp, "2023-10-27T10:00:00.000000000Z",
            "expected TIMESTAMP string"
        );
    }

    #[test]
    fn test_from_value_wrong_kind() {
        let v_bool = true.to_value();
        let err = String::from_value(&v_bool, &types::string())
            .expect_err("expected non-string kind mismatch for String");
        assert!(format!("{}", err).contains("expected String, got Bool"));

        let err = Timestamp::from_value(&v_bool, &types::timestamp())
            .expect_err("expected non-string kind mismatch for Timestamp");
        assert!(format!("{}", err).contains("expected String, got Bool"));

        let err = Date::from_value(&v_bool, &types::date())
            .expect_err("expected non-string kind mismatch for Date");
        assert!(format!("{}", err).contains("expected String, got Bool"));

        #[cfg(feature = "unstable-time")]
        {
            let err = OffsetDateTime::from_value(&v_bool, &types::timestamp())
                .expect_err("expected non-string kind mismatch for OffsetDateTime");
            assert!(format!("{}", err).contains("expected String, got Bool"));

            let err = TimeDate::from_value(&v_bool, &types::date())
                .expect_err("expected non-string kind mismatch for TimeDate");
            assert!(format!("{}", err).contains("expected String, got Bool"));
        }

        let v_string = "hello".to_value();
        let err = i64::from_value(&v_string, &types::int64())
            .expect_err("expected cannot convert value for i64");
        assert!(format!("{}", err).contains("cannot convert value"));

        let err = i64::from_value(&v_bool, &types::int64())
            .expect_err("expected non-string kind mismatch for i64");
        assert!(format!("{}", err).contains("expected String, got Bool"));

        let err = f64::from_value(&v_bool, &types::float64())
            .expect_err("expected non-number kind mismatch for f64");
        assert!(format!("{}", err).contains("expected Number, got Bool"));

        let err = bool::from_value(&v_string, &types::bool())
            .expect_err("expected non-bool kind mismatch for bool");
        assert!(format!("{}", err).contains("expected Bool, got String"));
    }

    #[test]
    fn test_from_value_null_errors() {
        let v_null = Option::<i32>::None.to_value();

        let err = String::from_value(&v_null, &types::string())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = i64::from_value(&v_null, &types::int64())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = f64::from_value(&v_null, &types::float64())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = f32::from_value(&v_null, &types::float32())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = bool::from_value(&v_null, &types::bool())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = Decimal::from_value(&v_null, &types::numeric())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = SystemTime::from_value(&v_null, &types::timestamp())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = wkt::Timestamp::from_value(&v_null, &types::timestamp())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        #[cfg(feature = "unstable-time")]
        {
            let err = OffsetDateTime::from_value(&v_null, &types::timestamp())
                .expect_err("expected non-null value, got null");
            assert!(format!("{}", err).contains("expected non-null value, got null"));

            let err = TimeDate::from_value(&v_null, &types::date())
                .expect_err("expected non-null value, got null");
            assert!(format!("{}", err).contains("expected non-null value, got null"));
        }

        let err = Date::from_value(&v_null, &types::date())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));

        let err = Vec::<u8>::from_value(&v_null, &types::bytes())
            .expect_err("expected non-null value, got null");
        assert!(format!("{}", err).contains("expected non-null value, got null"));
    }

    #[test]
    fn from_value_option_missing_kind() {
        let value = Value(prost_types::Value { kind: None });
        let optional_int32 = Option::<i32>::from_value(&value, &types::int64())
            .expect("Option::<i32> on missing kind should produce None");
        assert_eq!(optional_int32, None, "expected None for missing kind");

        let error = i32::from_value(&value, &types::int64())
            .expect_err("non-option i32 on missing kind should fail");
        assert!(
            matches!(error, ConvertError::NotNull),
            "expected NotNull error for missing kind on non-option type"
        );
    }

    // ── JSON value conversion tests ──────────────────────────────────────

    #[test]
    fn test_from_value_json_primitives() {
        // String → JSON string
        let v = "hello".to_value();
        let j = JsonValue::from_value(&v, &types::string()).unwrap();
        assert_eq!(j, JsonValue::String("hello".to_string()));

        // INT64 is string-encoded on the wire → stays as JSON string
        // (preserves full i64 range without precision loss)
        let v = 42i64.to_value();
        let j = JsonValue::from_value(&v, &types::int64()).unwrap();
        assert_eq!(j, JsonValue::String("42".to_string()));

        // Bool → JSON bool
        let v = true.to_value();
        let j = JsonValue::from_value(&v, &types::bool()).unwrap();
        assert_eq!(j, JsonValue::Bool(true));

        // Float64 → JSON number
        let v = 1.5f64.to_value();
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, serde_json::json!(1.5));

        // Null → JSON null
        let v: Option<i64> = None;
        let j = JsonValue::from_value(&v.to_value(), &types::int64()).unwrap();
        assert_eq!(j, JsonValue::Null);

        // Missing kind → JSON null
        let v = crate::value::Value(prost_types::Value { kind: None });
        let j = JsonValue::from_value(&v, &types::string()).unwrap();
        assert_eq!(j, JsonValue::Null);
    }

    #[test]
    fn test_from_value_json_string_array() {
        let str_array = vec!["a".to_string(), "b".to_string()];
        let v = str_array.to_value();
        let j = JsonValue::from_value(&v, &types::array(types::string())).unwrap();
        assert_eq!(j, serde_json::json!(["a", "b"]));
    }

    #[test]
    fn test_from_value_json_int_array() {
        // INT64 array — values are string-encoded
        let int_array = vec![10i64, 20i64];
        let v = int_array.to_value();
        let j = JsonValue::from_value(&v, &types::array(types::int64())).unwrap();
        assert_eq!(j, serde_json::json!(["10", "20"]));
    }

    #[test]
    fn test_from_value_json_positional_struct() {
        // Type: STRUCT<name STRING, age INT64>
        let struct_type = types::create_type(TypeCode::Struct);
        let mut inner: model::Type = struct_type.0;
        inner.struct_type = Some(Box::new(model::StructType {
            fields: vec![
                model::struct_type::Field::new()
                    .set_name("name")
                    .set_type(model::Type {
                        code: model::TypeCode::String,
                        ..Default::default()
                    }),
                model::struct_type::Field::new()
                    .set_name("age")
                    .set_type(model::Type {
                        code: model::TypeCode::Int64,
                        ..Default::default()
                    }),
            ],
            _unknown_fields: Default::default(),
        }));
        let spanner_type = Type(inner);

        // Wire value: positional ListValue ["Alice", "30"]
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("Alice".to_string())),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("30".to_string())),
                        },
                    ],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"name": "Alice", "age": "30"}));
    }

    #[test]
    fn test_from_value_json_array_of_structs() {
        // Type: ARRAY<STRUCT<a STRING, b INT64>>
        let elem_struct = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new()
                        .set_name("a")
                        .set_type(model::Type {
                            code: model::TypeCode::String,
                            ..Default::default()
                        }),
                    model::struct_type::Field::new()
                        .set_name("b")
                        .set_type(model::Type {
                            code: model::TypeCode::Int64,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let array_type = model::Type {
            code: model::TypeCode::Array,
            array_element_type: Some(Box::new(elem_struct)),
            ..Default::default()
        };
        let spanner_type = Type(array_type);

        // Wire: [[x, 1], [y, 2]] — positional structs inside an array
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::ListValue(
                                prost_types::ListValue {
                                    values: vec![
                                        prost_types::Value {
                                            kind: Some(prost_types::value::Kind::StringValue(
                                                "x".to_string(),
                                            )),
                                        },
                                        prost_types::Value {
                                            kind: Some(prost_types::value::Kind::StringValue(
                                                "1".to_string(),
                                            )),
                                        },
                                    ],
                                },
                            )),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::ListValue(
                                prost_types::ListValue {
                                    values: vec![
                                        prost_types::Value {
                                            kind: Some(prost_types::value::Kind::StringValue(
                                                "y".to_string(),
                                            )),
                                        },
                                        prost_types::Value {
                                            kind: Some(prost_types::value::Kind::StringValue(
                                                "2".to_string(),
                                            )),
                                        },
                                    ],
                                },
                            )),
                        },
                    ],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(
            j,
            serde_json::json!([
                {"a": "x", "b": "1"},
                {"a": "y", "b": "2"},
            ])
        );
    }

    #[test]
    fn test_from_value_json_unexpected_struct_value_errors() {
        let spanner_value = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(
                prost_types::Struct::default(),
            )),
        });
        let expected_error = JsonValue::from_value(&spanner_value, &Type::default())
            .expect_err("StructValue must produce error in JsonValue::from_value");
        assert!(
            format!("{expected_error}").contains("unexpected protobuf StructValue"),
            "error message should mention unexpected protobuf StructValue, got: {expected_error}"
        );
    }

    #[test]
    fn test_from_owned_value_json_unexpected_struct_value_errors() {
        let spanner_value = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(
                prost_types::Struct::default(),
            )),
        });
        let expected_error = JsonValue::from_owned_value(spanner_value, &Type::default())
            .expect_err("StructValue must produce error in JsonValue::from_owned_value");
        assert!(
            format!("{expected_error}").contains("unexpected protobuf StructValue"),
            "error message should mention unexpected protobuf StructValue, got: {expected_error}"
        );
    }

    #[test]
    fn test_from_value_json_spanner_json_column() {
        // Spanner JSON column: value arrives as a StringValue containing JSON text
        let json_str = r#"{"key": "value", "nested": [1, 2, 3]}"#;
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue(json_str.to_string())),
        });

        let j = JsonValue::from_value(&v, &types::json())
            .expect("parsing JSON string to JsonValue should succeed");
        assert_eq!(
            j,
            serde_json::json!({"key": "value", "nested": [1, 2, 3]}),
            "expected JSON object matching input string"
        );
    }

    #[test]
    fn test_from_value_json_nested_struct_in_struct() {
        // Type: STRUCT<outer_field STRING, inner STRUCT<x INT64, y BOOL>>
        let inner_struct_type = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new()
                        .set_name("x")
                        .set_type(model::Type {
                            code: model::TypeCode::Int64,
                            ..Default::default()
                        }),
                    model::struct_type::Field::new()
                        .set_name("y")
                        .set_type(model::Type {
                            code: model::TypeCode::Bool,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };

        let outer_type = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new()
                        .set_name("outer_field")
                        .set_type(model::Type {
                            code: model::TypeCode::String,
                            ..Default::default()
                        }),
                    model::struct_type::Field::new()
                        .set_name("inner")
                        .set_type(inner_struct_type),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(outer_type);

        // Wire: positional list ["hello", ["42", true]]
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("hello".to_string())),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::ListValue(
                                prost_types::ListValue {
                                    values: vec![
                                        prost_types::Value {
                                            kind: Some(prost_types::value::Kind::StringValue(
                                                "42".to_string(),
                                            )),
                                        },
                                        prost_types::Value {
                                            kind: Some(prost_types::value::Kind::BoolValue(true)),
                                        },
                                    ],
                                },
                            )),
                        },
                    ],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(
            j,
            serde_json::json!({"outer_field": "hello", "inner": {"x": "42", "y": true}})
        );
    }

    #[test]
    fn test_from_value_json_null_in_struct() {
        // Type: STRUCT<name STRING, value INT64>
        let struct_type_model = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new()
                        .set_name("name")
                        .set_type(model::Type {
                            code: model::TypeCode::String,
                            ..Default::default()
                        }),
                    model::struct_type::Field::new()
                        .set_name("value")
                        .set_type(model::Type {
                            code: model::TypeCode::Int64,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_model);

        // Wire: ["test", null]
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("test".to_string())),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::NullValue(0)),
                        },
                    ],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"name": "test", "value": null}));
    }

    #[test]
    fn test_from_value_json_nan_becomes_null() {
        // NumberValue representations
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::NumberValue(f64::NAN)),
        });
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, JsonValue::Null);

        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::NumberValue(f64::INFINITY)),
        });
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, JsonValue::Null);

        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::NumberValue(f64::NEG_INFINITY)),
        });
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, JsonValue::Null);

        // StringValue representations (as sent by Spanner wire format)
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue("NaN".to_string())),
        });
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, JsonValue::Null);

        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue(
                "Infinity".to_string(),
            )),
        });
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, JsonValue::Null);

        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue(
                "-Infinity".to_string(),
            )),
        });
        let j = JsonValue::from_value(&v, &types::float64()).unwrap();
        assert_eq!(j, JsonValue::Null);
    }

    #[test]
    fn from_value_json_float_strings() {
        // Valid numeric string representations (as sent by Spanner wire format for floats)
        let numeric_string_value = "42.5".to_string().to_value();
        let json_from_float64 = JsonValue::from_value(&numeric_string_value, &types::float64())
            .expect("valid float64 string to JsonValue");
        assert_eq!(
            json_from_float64,
            json!(42.5),
            "expected 42.5 JsonValue number from FLOAT64 string"
        );

        let json_from_float32 = JsonValue::from_value(&numeric_string_value, &types::float32())
            .expect("valid float32 string to JsonValue");
        assert_eq!(
            json_from_float32,
            json!(42.5),
            "expected 42.5 JsonValue number from FLOAT32 string"
        );

        // Invalid numeric string in float column
        let invalid_string_value = "not_a_float".to_string().to_value();
        let error_invalid_string = JsonValue::from_value(&invalid_string_value, &types::float64())
            .expect_err("invalid float string to JsonValue must fail");
        assert!(
            error_invalid_string
                .to_string()
                .contains("cannot convert value"),
            "expected conversion failure for invalid float string"
        );
    }

    #[test]
    fn test_from_value_json_option_wrapping() {
        // Option<JsonValue> for a non-null value
        let v = "hello".to_value();
        let j = Option::<JsonValue>::from_value(&v, &types::string()).unwrap();
        assert_eq!(j, Some(JsonValue::String("hello".to_string())));

        // Option<JsonValue> for a null value
        let v: Option<String> = None;
        let j = Option::<JsonValue>::from_value(&v.to_value(), &types::string()).unwrap();
        assert_eq!(j, None);
    }

    #[test]
    fn test_from_value_json_empty_struct() {
        // Type: STRUCT<> (zero fields)
        let struct_type_model = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_model);

        // Wire: empty positional list
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue { values: vec![] },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({}));
    }

    #[test]
    fn test_from_value_json_unnamed_fields() {
        // Type: STRUCT with unnamed fields (empty string names)
        // This is valid in Spanner for SELECT expressions without aliases.
        let struct_type_model = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new()
                        .set_name("")
                        .set_type(model::Type {
                            code: model::TypeCode::String,
                            ..Default::default()
                        }),
                    model::struct_type::Field::new()
                        .set_name("")
                        .set_type(model::Type {
                            code: model::TypeCode::Int64,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_model);

        // Wire: positional values
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("val".to_string())),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("99".to_string())),
                        },
                    ],
                },
            )),
        });

        // Unnamed fields map to empty-string keys; last write wins for duplicates.
        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert!(j.is_object());
        // With duplicate empty keys, the map retains the last inserted value
        assert_eq!(
            j.as_object().unwrap().get("").unwrap(),
            &serde_json::json!("99")
        );
    }

    #[test]
    fn test_from_value_json_vec_composition() {
        // Vec<JsonValue> uses the existing Vec<T>: FromValue impl
        let str_array = vec!["one".to_string(), "two".to_string()];
        let v = str_array.to_value();
        let res = Vec::<JsonValue>::from_value(&v, &types::array(types::string())).unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], JsonValue::String("one".to_string()));
        assert_eq!(res[1], JsonValue::String("two".to_string()));
    }

    #[test]
    fn test_from_value_json_list_without_type_info() {
        // ListValue with no type context — falls back to plain array
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("a".to_string())),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::BoolValue(false)),
                        },
                    ],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &Type::default()).unwrap();
        assert_eq!(j, serde_json::json!(["a", false]));
    }

    #[test]
    fn test_from_value_json_invalid_json_column() {
        // Spanner JSON column with invalid JSON text → error
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue(
                "not valid json{".to_string(),
            )),
        });

        let err = JsonValue::from_value(&v, &types::json()).unwrap_err();
        assert!(format!("{}", err).contains("cannot convert value"));
    }

    #[test]
    fn test_from_value_json_missing_positional_fields_become_null() {
        // Type declares 3 fields but wire only has 1 value
        let struct_type_model = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new()
                        .set_name("a")
                        .set_type(model::Type {
                            code: model::TypeCode::String,
                            ..Default::default()
                        }),
                    model::struct_type::Field::new()
                        .set_name("b")
                        .set_type(model::Type {
                            code: model::TypeCode::Int64,
                            ..Default::default()
                        }),
                    model::struct_type::Field::new()
                        .set_name("c")
                        .set_type(model::Type {
                            code: model::TypeCode::Bool,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_model);

        // Wire: only one value present
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("only_a".to_string())),
                    }],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"a": "only_a", "b": null, "c": null}));
    }

    #[test]
    fn test_from_value_json_array_of_json_columns() {
        // Type: ARRAY<JSON>
        let json_type = model::Type {
            code: model::TypeCode::Json,
            ..Default::default()
        };
        let array_type = model::Type {
            code: model::TypeCode::Array,
            array_element_type: Some(Box::new(json_type)),
            ..Default::default()
        };
        let spanner_type = Type(array_type);

        // Wire: array of JSON strings
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue(
                                r#"{"x":1}"#.to_string(),
                            )),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue(
                                r#"{"y":2}"#.to_string(),
                            )),
                        },
                    ],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!([{"x": 1}, {"y": 2}]));
    }

    #[test]
    fn test_from_value_json_nested_array_in_struct() {
        // Type: STRUCT<tags ARRAY<STRING>, id INT64>
        let struct_type_model = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new()
                        .set_name("tags")
                        .set_type(model::Type {
                            code: model::TypeCode::Array,
                            array_element_type: Some(Box::new(model::Type {
                                code: model::TypeCode::String,
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                    model::struct_type::Field::new()
                        .set_name("id")
                        .set_type(model::Type {
                            code: model::TypeCode::Int64,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_model);

        // Wire: [["foo", "bar"], "42"]
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::ListValue(
                                prost_types::ListValue {
                                    values: vec![
                                        prost_types::Value {
                                            kind: Some(prost_types::value::Kind::StringValue(
                                                "foo".to_string(),
                                            )),
                                        },
                                        prost_types::Value {
                                            kind: Some(prost_types::value::Kind::StringValue(
                                                "bar".to_string(),
                                            )),
                                        },
                                    ],
                                },
                            )),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("42".to_string())),
                        },
                    ],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"tags": ["foo", "bar"], "id": "42"}));
    }

    #[test]
    fn test_from_value_json_recursion_depth_limit() {
        // Build a deeply nested ListValue (65 levels deep) to exceed MAX_RECURSION_DEPTH
        let mut inner = prost_types::Value {
            kind: Some(prost_types::value::Kind::BoolValue(true)),
        };
        for _ in 0..65 {
            inner = prost_types::Value {
                kind: Some(prost_types::value::Kind::ListValue(
                    prost_types::ListValue {
                        values: vec![inner],
                    },
                )),
            };
        }
        let v = crate::value::Value(inner);

        let err = JsonValue::from_value(&v, &Type::default()).unwrap_err();
        assert!(format!("{}", err).contains("nesting depth exceeded"));
    }

    #[test]
    fn test_from_value_json_array_without_element_type() {
        // Type: ARRAY but array_element_type is None
        let array_type = model::Type {
            code: model::TypeCode::Array,
            array_element_type: None,
            ..Default::default()
        };
        let spanner_type = Type(array_type);

        // Wire: array of strings
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("hello".to_string())),
                    }],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!(["hello"]));
    }

    #[test]
    fn test_from_value_json_struct_with_missing_field_type() {
        // Type: STRUCT where field "a" has no type metadata
        let struct_type_model = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new().set_name("a"), // type is None
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        };
        let spanner_type = Type(struct_type_model);

        // Wire: positional list ["hello"]
        let v = crate::value::Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("hello".to_string())),
                    }],
                },
            )),
        });

        let j = JsonValue::from_value(&v, &spanner_type).unwrap();
        assert_eq!(j, serde_json::json!({"a": "hello"}));
    }

    #[test]
    fn convert_error_auto_traits() {
        use std::fmt::Debug;
        static_assertions::assert_impl_all!(ConvertError: Clone, Debug, Send, Sync);
    }

    #[test]
    fn convert_error_extract() {
        let error = Error::deser(ConvertError::NotNull);
        let extracted = ConvertError::extract(&error).expect("should extract ConvertError");
        assert!(
            matches!(extracted, ConvertError::NotNull),
            "expected NotNull variant"
        );

        let type_conversion_error = Error::deser(RowError::type_conversion(
            "col_a",
            TypeCode::String,
            ConvertError::type_mismatch(TypeCode::Int64, TypeCode::String),
        ));
        let extracted_from_type_conversion = ConvertError::extract(&type_conversion_error)
            .expect("should extract ConvertError from RowError::TypeConversion");
        assert_eq!(
            extracted_from_type_conversion.to_string(),
            "type mismatch, expected Int64, got String",
            "expected TypeMismatch extracted from TypeConversion"
        );

        let other_error = Error::deser(RowError::column_not_found("col"));
        assert!(
            ConvertError::extract(&other_error).is_none(),
            "should return None for non-ConvertError"
        );

        use google_cloud_rpc::model::Status;
        let no_source_error = Error::service(Status::default().into());
        assert!(
            ConvertError::extract(&no_source_error).is_none(),
            "should return None when error has no source"
        );
    }

    #[test]
    fn from_value_for_value() {
        let original_value = 42i64.to_value();
        let converted = Value::from_value(&original_value, &types::int64())
            .expect("Value::from_value should succeed");
        assert_eq!(
            converted, original_value,
            "expected Value::from_value to clone value"
        );
    }

    #[test]
    fn convert_error_traits_and_display() {
        let kind_error = ConvertError::KindMismatch {
            want: Kind::String,
            got: Kind::Bool,
        };
        assert_eq!(
            kind_error.to_string(),
            "expected String, got Bool",
            "expected kind mismatch display format"
        );
        let cloned_kind_error = kind_error.clone();
        assert_eq!(
            cloned_kind_error.to_string(),
            kind_error.to_string(),
            "expected cloned KindMismatch display to match original"
        );

        let type_error = ConvertError::TypeMismatch {
            want: TypeCode::Int64,
            got: TypeCode::String,
        };
        assert_eq!(
            type_error.to_string(),
            "type mismatch, expected Int64, got String",
            "expected type mismatch display format"
        );
        let cloned_type_error = type_error.clone();
        assert_eq!(
            cloned_type_error.to_string(),
            type_error.to_string(),
            "expected cloned TypeMismatch display to match original"
        );

        let not_null_error = ConvertError::NotNull;
        assert_eq!(
            not_null_error.to_string(),
            "expected non-null value, got null",
            "expected not null display format"
        );
        let cloned_not_null = not_null_error.clone();
        assert_eq!(
            cloned_not_null.to_string(),
            not_null_error.to_string(),
            "expected cloned NotNull display to match original"
        );

        let message_str_error = ConvertError::message("custom string error");
        let message_string_error = ConvertError::message("custom string error".to_string());
        assert_eq!(
            message_str_error.to_string(),
            message_string_error.to_string(),
            "expected ConvertError::message(&str) display to match ConvertError::message(String)"
        );
        let cloned_message = message_str_error.clone();
        assert_eq!(
            cloned_message.to_string(),
            message_str_error.to_string(),
            "expected cloned Convert error display to match original"
        );
        assert!(
            message_str_error
                .to_string()
                .contains("custom string error"),
            "expected display to contain custom message"
        );
    }

    #[test]
    fn convert_error_custom_downcast() {
        use std::io::Error as IoError;

        #[derive(Debug, thiserror::Error, PartialEq, Eq)]
        #[error("custom validation code {code}")]
        struct CustomValidationError {
            code: u32,
        }

        let original_error = CustomValidationError { code: 404 };
        let convert_error = ConvertError::custom(original_error);

        // Verify downcasting via ConvertError::Convert variant pattern matching
        let extract_shared = |error: &ConvertError| match error {
            ConvertError::Convert(shared) => Some(Arc::clone(shared)),
            _ => None,
        };
        let shared_error =
            extract_shared(&convert_error).expect("expected ConvertError::Convert variant");
        let downcasted_from_shared = (*shared_error).downcast_ref::<CustomValidationError>();
        assert_eq!(
            downcasted_from_shared,
            Some(&CustomValidationError { code: 404 }),
            "expected downcast_ref on SharedError to recover CustomValidationError"
        );

        let wrong_type_error = (*shared_error).downcast_ref::<IoError>();
        assert!(
            wrong_type_error.is_none(),
            "expected downcast_ref to return None for non-matching type"
        );

        let non_convert_error = ConvertError::NotNull;
        assert!(
            extract_shared(&non_convert_error).is_none(),
            "expected None for non-Convert variant"
        );
    }

    #[test]
    fn from_value_kind_mismatch_branches() {
        let bool_value = true.to_value();

        let decimal_error = Decimal::from_value(&bool_value, &types::numeric())
            .expect_err("Decimal must reject bool wire kind with KindMismatch");
        assert_eq!(
            decimal_error.to_string(),
            "expected String, got Bool",
            "expected KindMismatch for Decimal on bool kind"
        );

        let float32_error = f32::from_value(&bool_value, &types::float32())
            .expect_err("f32 must reject bool wire kind with KindMismatch");
        assert_eq!(
            float32_error.to_string(),
            "expected Number, got Bool",
            "expected KindMismatch for f32 on bool kind"
        );

        let bytes_error = Vec::<u8>::from_value(&bool_value, &types::bytes())
            .expect_err("Vec<u8> must reject bool wire kind with KindMismatch");
        assert_eq!(
            bytes_error.to_string(),
            "expected String, got Bool",
            "expected KindMismatch for Vec<u8> on bool kind"
        );

        let array_error = Vec::<String>::from_value(&bool_value, &types::array(types::string()))
            .expect_err("Vec<T> must reject bool wire kind with KindMismatch");
        assert_eq!(
            array_error.to_string(),
            "expected List, got Bool",
            "expected KindMismatch for Vec<T> on bool kind"
        );
    }

    #[test]
    fn from_value_array_missing_element_type() {
        let malformed_type = Type::from(model::Type {
            code: model::TypeCode::Array,
            array_element_type: None,
            ..Default::default()
        });
        let list_value = Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("test".to_string())),
                    }],
                },
            )),
        });
        let element_type_error = Vec::<String>::from_value(&list_value, &malformed_type)
            .expect_err("array missing element type must fail");
        assert!(
            element_type_error
                .to_string()
                .contains("Array type missing element type"),
            "expected 'Array type missing element type' in error message"
        );
    }

    #[test]
    fn string_from_value_rejects_array_and_struct() {
        let array_type = types::array(types::string());
        let string_value = "hello".to_string().to_value();
        let array_error = String::from_value(&string_value, &array_type)
            .expect_err("String::from_value must reject Array column type");
        assert_eq!(
            array_error.to_string(),
            "type mismatch, expected String, got Array",
            "expected TypeMismatch when reading Array column as String"
        );

        let struct_type = types::create_type(TypeCode::Struct);
        let struct_error = String::from_value(&string_value, &struct_type)
            .expect_err("String::from_value must reject Struct column type");
        assert_eq!(
            struct_error.to_string(),
            "type mismatch, expected String, got Struct",
            "expected TypeMismatch when reading Struct column as String"
        );
    }

    #[test]
    fn from_value_none_kind_null_contract() {
        let none_kind_value = Value::default(); // kind: None
        assert!(none_kind_value.is_null(), "Value::default() must be null");

        // Option<T> should succeed and return None for kind: None
        let optional_int64 = Option::<i64>::from_value(&none_kind_value, &types::int64())
            .expect("Option<i64> should succeed for kind: None");
        assert_eq!(optional_int64, None, "expected None for Option<i64>");

        let optional_string = Option::<String>::from_value(&none_kind_value, &types::string())
            .expect("Option<String> should succeed for kind: None");
        assert_eq!(optional_string, None, "expected None for Option<String>");

        // Non-Option types must return ConvertError::NotNull for kind: None
        let string_error = String::from_value(&none_kind_value, &types::string())
            .expect_err("String must reject kind: None with NotNull");
        assert!(
            matches!(string_error, ConvertError::NotNull),
            "expected NotNull for String"
        );

        let int64_error = i64::from_value(&none_kind_value, &types::int64())
            .expect_err("i64 must reject kind: None with NotNull");
        assert!(
            matches!(int64_error, ConvertError::NotNull),
            "expected NotNull for i64"
        );

        let int32_error = i32::from_value(&none_kind_value, &types::int64())
            .expect_err("i32 must reject kind: None with NotNull");
        assert!(
            matches!(int32_error, ConvertError::NotNull),
            "expected NotNull for i32"
        );

        let bool_error = bool::from_value(&none_kind_value, &types::bool())
            .expect_err("bool must reject kind: None with NotNull");
        assert!(
            matches!(bool_error, ConvertError::NotNull),
            "expected NotNull for bool"
        );

        let float64_error = f64::from_value(&none_kind_value, &types::float64())
            .expect_err("f64 must reject kind: None with NotNull");
        assert!(
            matches!(float64_error, ConvertError::NotNull),
            "expected NotNull for f64"
        );

        let float32_error = f32::from_value(&none_kind_value, &types::float32())
            .expect_err("f32 must reject kind: None with NotNull");
        assert!(
            matches!(float32_error, ConvertError::NotNull),
            "expected NotNull for f32"
        );

        let decimal_error = Decimal::from_value(&none_kind_value, &types::numeric())
            .expect_err("Decimal must reject kind: None with NotNull");
        assert!(
            matches!(decimal_error, ConvertError::NotNull),
            "expected NotNull for Decimal"
        );

        let bytes_error = Vec::<u8>::from_value(&none_kind_value, &types::bytes())
            .expect_err("Vec<u8> must reject kind: None with NotNull");
        assert!(
            matches!(bytes_error, ConvertError::NotNull),
            "expected NotNull for Vec<u8>"
        );

        let vector_error =
            Vec::<String>::from_value(&none_kind_value, &types::array(types::string()))
                .expect_err("Vec<T> must reject kind: None with NotNull");
        assert!(
            matches!(vector_error, ConvertError::NotNull),
            "expected NotNull for Vec<T>"
        );

        let timestamp_error = Timestamp::from_value(&none_kind_value, &types::timestamp())
            .expect_err("Timestamp must reject kind: None with NotNull");
        assert!(
            matches!(timestamp_error, ConvertError::NotNull),
            "expected NotNull for Timestamp"
        );

        let date_error = Date::from_value(&none_kind_value, &types::date())
            .expect_err("Date must reject kind: None with NotNull");
        assert!(
            matches!(date_error, ConvertError::NotNull),
            "expected NotNull for Date"
        );
    }

    #[test]
    fn from_owned_value_string() {
        let value = "hello".to_value();
        let string_type = types::string();
        let string_value =
            String::from_owned_value(value, &string_type).expect("valid string value");
        assert_eq!(string_value, "hello", "expected string 'hello'");

        // Null value check
        let null_value = Value::null();
        let null_error = String::from_owned_value(null_value, &string_type)
            .expect_err("null value must fail for String");
        assert!(
            matches!(null_error, ConvertError::NotNull),
            "expected NotNull error"
        );

        // Kind mismatch check
        let bool_value = true.to_value();
        let mismatch_error = String::from_owned_value(bool_value, &string_type)
            .expect_err("bool value must fail for String");
        assert!(
            matches!(mismatch_error, ConvertError::KindMismatch { .. }),
            "expected KindMismatch error"
        );

        // Float type check
        let float_string_value = "1.5".to_value();
        let float_type = types::float64();
        let float_error = String::from_owned_value(float_string_value, &float_type)
            .expect_err("float column must not decode as String");
        assert!(
            matches!(
                float_error,
                ConvertError::TypeMismatch {
                    want: TypeCode::String,
                    got: TypeCode::Float64,
                }
            ),
            "expected TypeMismatch error for float column"
        );
    }

    #[test]
    fn from_owned_value_option() {
        let null_value = Value::null();
        let string_type = types::string();
        let optional_null: Option<String> =
            Option::from_owned_value(null_value, &string_type).expect("valid optional null");
        assert_eq!(optional_null, None, "expected None for null value");

        let present_value = "present".to_value();
        let optional_present: Option<String> =
            Option::from_owned_value(present_value, &string_type).expect("valid optional string");
        assert_eq!(
            optional_present,
            Some("present".to_string()),
            "expected Some('present')"
        );
    }

    #[test]
    fn from_owned_value_value() {
        let value = "raw".to_value();
        let string_type = types::string();
        let returned_value =
            Value::from_owned_value(value.clone(), &string_type).expect("valid Value conversion");
        assert_eq!(returned_value, value, "expected identical Value");
    }

    #[test]
    fn from_owned_value_vector() {
        let array_type = types::array(types::string());
        let list_value = vec!["one".to_string(), "two".to_string()].to_value();
        let string_vector: Vec<String> =
            Vec::from_owned_value(list_value, &array_type).expect("valid Vec<String>");
        assert_eq!(
            string_vector,
            vec!["one".to_string(), "two".to_string()],
            "expected matching vector of strings"
        );

        // Null value check
        let null_value = Value::null();
        let null_error = Vec::<String>::from_owned_value(null_value, &array_type)
            .expect_err("null value must fail for Vec<String>");
        assert!(
            matches!(null_error, ConvertError::NotNull),
            "expected NotNull error"
        );

        // Not an array type
        let string_type = types::string();
        let non_array_value = vec!["one".to_string()].to_value();
        let type_error = Vec::<String>::from_owned_value(non_array_value, &string_type)
            .expect_err("non-array type must fail");
        assert!(
            matches!(
                type_error,
                ConvertError::TypeMismatch {
                    want: TypeCode::Array,
                    got: TypeCode::String,
                }
            ),
            "expected TypeMismatch for non-array type"
        );
    }

    #[test]
    fn from_owned_value_json() {
        let json_type = types::json();
        let json_string_value = "{\"key\":\"value\"}".to_value();
        let parsed_json =
            JsonValue::from_owned_value(json_string_value, &json_type).expect("valid JSON value");
        assert_eq!(
            parsed_json,
            serde_json::json!({"key": "value"}),
            "expected parsed JSON object"
        );
    }

    #[test]
    fn from_owned_value_default_delegation() {
        let int_value = 123_i64.to_value();
        let int_type = types::int64();
        let integer = i64::from_owned_value(int_value, &int_type).expect("valid i64");
        assert_eq!(integer, 123, "expected i64 value 123");

        let bool_value = true.to_value();
        let bool_type = types::bool();
        let boolean = bool::from_owned_value(bool_value, &bool_type).expect("valid bool");
        assert!(boolean, "expected boolean value true");
    }

    #[test]
    fn from_value_value() {
        let value = "raw".to_value();
        let string_type = types::string();
        let returned_value =
            Value::from_value(&value, &string_type).expect("valid Value conversion");
        assert_eq!(returned_value, value, "expected identical Value");
    }

    #[test]
    fn from_owned_value_vector_kind_mismatch() {
        let array_type = types::array(types::string());
        let non_list_value = 42_i64.to_value();
        let type_error = Vec::<String>::from_owned_value(non_list_value, &array_type)
            .expect_err("non-list value with array type must fail with KindMismatch");
        assert!(
            matches!(type_error, ConvertError::KindMismatch { .. }),
            "expected KindMismatch for non-list value with array type"
        );
    }

    #[test]
    fn from_owned_value_json_struct_without_metadata() {
        let struct_type_without_metadata = Type(model::Type {
            code: model::TypeCode::Struct,
            struct_type: None,
            ..Default::default()
        });
        let list_value = Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            "anonymous".to_string(),
                        )),
                    }],
                },
            )),
        });
        let json_value = JsonValue::from_owned_value(list_value, &struct_type_without_metadata)
            .expect("valid conversion to JSON array fallback");
        assert_eq!(
            json_value,
            serde_json::json!(["anonymous"]),
            "expected positional array fallback when struct metadata is missing"
        );
    }

    #[test]
    fn from_owned_value_option_null() {
        let null_value = Value(prost_types::Value {
            kind: Some(ProtoKind::NullValue(0)),
        });
        let string_type = types::string();
        let optional_value: Option<String> = Option::from_owned_value(null_value, &string_type)
            .expect("sql null value should convert to None");
        assert_eq!(optional_value, None, "expected None for sql null");
    }

    #[test]
    fn from_owned_value_option_missing_kind() {
        let missing_kind_value = Value(prost_types::Value { kind: None });
        let string_type = types::string();
        let optional_value: Option<String> =
            Option::from_owned_value(missing_kind_value, &string_type)
                .expect("Option::<String> on missing kind should produce None");
        assert_eq!(optional_value, None, "expected None for missing kind");

        let missing_kind_value = Value(prost_types::Value { kind: None });
        let error = String::from_owned_value(missing_kind_value, &string_type)
            .expect_err("non-option String on missing kind should fail");
        assert!(
            matches!(error, ConvertError::NotNull),
            "expected NotNull error for missing kind on non-option type"
        );
    }

    #[test]
    fn from_owned_value_option_error() {
        let bool_value = true.to_value();
        let string_type = types::string();
        let error = Option::<String>::from_owned_value(bool_value, &string_type)
            .expect_err("type mismatch inside Option must return error");
        assert!(
            matches!(error, ConvertError::KindMismatch { .. }),
            "expected KindMismatch error"
        );
    }

    #[test]
    fn from_value_option_unexpected_struct_value_fails() {
        let struct_value = Value(prost_types::Value {
            kind: Some(ProtoKind::StructValue(prost_types::Struct::default())),
        });
        let string_type = types::string();
        let error = Option::<String>::from_value(&struct_value, &string_type)
            .expect_err("unexpected StructValue must fail for Option<String>");
        assert!(
            matches!(
                error,
                ConvertError::KindMismatch {
                    want: Kind::String,
                    got: Kind::Unknown,
                }
            ),
            "expected KindMismatch with got: Kind::Unknown, got: {error}"
        );

        let json_err = Option::<JsonValue>::from_value(&struct_value, &Type::default())
            .expect_err("unexpected StructValue must fail for Option<JsonValue>");
        assert!(
            format!("{json_err}").contains("unexpected protobuf StructValue"),
            "expected error to mention unexpected protobuf StructValue"
        );
    }

    #[test]
    fn from_owned_value_vector_element_error() {
        let array_type = types::array(types::int64());
        let list_value = Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("42".to_string())),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue(
                                "not_a_number".to_string(),
                            )),
                        },
                    ],
                },
            )),
        });
        let error = Vec::<i64>::from_owned_value(list_value, &array_type)
            .expect_err("invalid inner element must fail vector conversion");
        assert!(
            matches!(error, ConvertError::Convert(_)),
            "expected Convert error for invalid integer element"
        );
    }

    #[test]
    fn from_owned_value_json_list_struct() {
        let struct_type_model = model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new()
                        .set_name("id")
                        .set_type(types::int64().0),
                    model::struct_type::Field::new()
                        .set_name("name")
                        .set_type(types::string().0),
                ],
                ..Default::default()
            })),
            ..Default::default()
        };
        let struct_type = Type(struct_type_model);
        // Spanner wire format sends structs as ListValue
        let wire_struct = Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("101".to_string())),
                        },
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("Alice".to_string())),
                        },
                    ],
                },
            )),
        });
        let json_value = JsonValue::from_owned_value(wire_struct, &struct_type)
            .expect("valid conversion of wire-format struct to JSON object");
        assert_eq!(
            json_value,
            serde_json::json!({
                "id": "101",
                "name": "Alice",
            }),
            "expected JSON object matching wire struct fields"
        );
    }

    #[test]
    fn from_value_vector_null() {
        let array_type = types::array(types::string());
        let null_value = Value::null();
        let error = Vec::<String>::from_value(&null_value, &array_type)
            .expect_err("null value for Vec must return NotNull");
        assert!(
            matches!(error, ConvertError::NotNull),
            "expected NotNull error"
        );
    }

    #[test]
    fn from_value_vector_kind_mismatch() {
        let array_type = types::array(types::string());
        let non_list_value = "string".to_value();
        let error = Vec::<String>::from_value(&non_list_value, &array_type)
            .expect_err("non-list value for Vec must return KindMismatch");
        assert!(
            format!("{error}").contains("expected List, got String"),
            "expected KindMismatch error message"
        );
    }

    #[test]
    fn from_value_bytes_kind_mismatch() {
        let bytes_type = types::bytes();
        let bool_value = true.to_value();
        let error = Vec::<u8>::from_value(&bool_value, &bytes_type)
            .expect_err("bool value for bytes must return KindMismatch");
        assert!(
            format!("{error}").contains("expected String, got Bool"),
            "expected KindMismatch error message"
        );
    }

    #[test]
    fn from_owned_value_bytes() {
        let bytes_type = types::bytes();
        let bytes_value = b"test bytes".to_vec().to_value();
        let decoded = Vec::<u8>::from_owned_value(bytes_value, &bytes_type)
            .expect("should decode bytes from owned value");
        assert_eq!(decoded, b"test bytes", "expected matching bytes");

        let null_value = Value::null();
        let null_error = Vec::<u8>::from_owned_value(null_value, &bytes_type)
            .expect_err("null bytes must return NotNull");
        assert!(
            format!("{null_error}").contains("expected non-null value, got null"),
            "expected NotNull error message"
        );

        let bool_value = true.to_value();
        let mismatch_error = Vec::<u8>::from_owned_value(bool_value, &bytes_type)
            .expect_err("bool for bytes must return KindMismatch");
        assert!(
            format!("{mismatch_error}").contains("expected String, got Bool"),
            "expected KindMismatch error message"
        );
    }

    #[test]
    fn from_value_json_borrowed_primitive_and_struct() {
        let json_type = types::json();
        let json_string_value = Value::from(r#"{"field":123}"#);
        let parsed = JsonValue::from_value(&json_string_value, &json_type)
            .expect("parsing JSON from borrowed Value");
        assert_eq!(
            parsed,
            json!({"field": 123}),
            "expected matching JSON object from borrowed Value"
        );
    }

    #[test]
    fn from_owned_value_json_list_struct_fewer_values_than_fields() {
        let struct_type = Type(model::Type {
            code: model::TypeCode::Struct,
            struct_type: Some(Box::new(model::StructType {
                fields: vec![
                    model::struct_type::Field::new()
                        .set_name("first")
                        .set_type(model::Type {
                            code: model::TypeCode::String,
                            ..Default::default()
                        }),
                    model::struct_type::Field::new()
                        .set_name("second")
                        .set_type(model::Type {
                            code: model::TypeCode::String,
                            ..Default::default()
                        }),
                ],
                _unknown_fields: Default::default(),
            })),
            ..Default::default()
        });

        let list_val = Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            "only_first".to_string(),
                        )),
                    }],
                },
            )),
        });
        let result = JsonValue::from_owned_value(list_val, &struct_type)
            .expect("should convert truncated list to JSON struct with null for missing fields");
        assert_eq!(
            result,
            json!({"first": "only_first", "second": null}),
            "expected missing field to be populated with null"
        );
    }

    #[test]
    fn from_owned_value_json_float_string() {
        let float64_type = types::float64();
        let val64 = Value::from("12.5");
        let json64 = JsonValue::from_owned_value(val64, &float64_type)
            .expect("should decode float64 string to json number");
        assert_eq!(json64, json!(12.5), "expected JSON number 12.5 for float64");

        let float32_type = types::float32();
        let val32 = Value::from("3.25");
        let json32 = JsonValue::from_owned_value(val32, &float32_type)
            .expect("should decode float32 string to json number");
        assert_eq!(json32, json!(3.25), "expected JSON number 3.25 for float32");
    }

    #[test]
    fn from_owned_value_json_max_recursion_depth() {
        let mut inner = prost_types::Value {
            kind: Some(prost_types::value::Kind::BoolValue(true)),
        };
        for _ in 0..65 {
            inner = prost_types::Value {
                kind: Some(prost_types::value::Kind::ListValue(
                    prost_types::ListValue {
                        values: vec![inner],
                    },
                )),
            };
        }
        let v = Value(inner);

        let err = JsonValue::from_owned_value(v, &Type::default())
            .expect_err("nested depth exceeding 64 must fail");
        assert!(
            format!("{}", err).contains("nesting depth exceeded"),
            "expected error to mention nesting depth exceeded"
        );
    }

    #[test]
    fn from_value_json_borrowed_struct_without_metadata() {
        let struct_type_without_metadata = Type(model::Type {
            code: model::TypeCode::Struct,
            struct_type: None,
            ..Default::default()
        });
        let list_value = Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            "anonymous".to_string(),
                        )),
                    }],
                },
            )),
        });
        let json_value = JsonValue::from_value(&list_value, &struct_type_without_metadata)
            .expect("valid conversion to JSON array fallback for borrowed Value");
        assert_eq!(
            json_value,
            json!(["anonymous"]),
            "expected positional array fallback when struct metadata is missing"
        );
    }

    #[test]
    fn from_owned_value_json_primitives_and_arrays() {
        // Null
        let null_json = JsonValue::from_owned_value(Value::null(), &Type::default())
            .expect("null to JSON should succeed");
        assert_eq!(null_json, JsonValue::Null, "expected JsonValue::Null");

        // Missing kind (None)
        let missing_kind = Value(prost_types::Value { kind: None });
        let none_json = JsonValue::from_owned_value(missing_kind, &Type::default())
            .expect("missing kind to JSON should succeed");
        assert_eq!(
            none_json,
            JsonValue::Null,
            "expected JsonValue::Null for None"
        );

        // Number
        let num_val = 42.5_f64.to_value();
        let num_json = JsonValue::from_owned_value(num_val, &types::float64())
            .expect("number to JSON should succeed");
        assert_eq!(num_json, json!(42.5), "expected JSON number 42.5");

        // Bool
        let bool_val = true.to_value();
        let bool_json = JsonValue::from_owned_value(bool_val, &types::bool())
            .expect("bool to JSON should succeed");
        assert_eq!(bool_json, json!(true), "expected JSON bool true");

        // Non-struct array
        let array_type = types::array(types::string());
        let list_val = Value::from(vec!["hello".to_string(), "world".to_string()]);
        let array_json = JsonValue::from_owned_value(list_val, &array_type)
            .expect("array to JSON should succeed");
        assert_eq!(
            array_json,
            json!(["hello", "world"]),
            "expected JSON array matching input list"
        );
    }

    #[test]
    fn from_value_decimal_kind_mismatch() {
        let numeric_type = types::numeric();
        let bool_value = true.to_value();
        let error = Decimal::from_value(&bool_value, &numeric_type)
            .expect_err("non-string value for numeric must return KindMismatch");
        assert!(
            format!("{error}").contains("expected String, got Bool"),
            "expected KindMismatch error message"
        );
    }

    #[test]
    fn from_value_string_from_numeric() -> Result<(), ConvertError> {
        let max_spanner_numeric = "99999999999999999999999999999.999999999";
        let min_spanner_numeric = "-99999999999999999999999999999.999999999";
        let pg_extreme_numeric =
            "12345678901234567890123456789012345678901234567890.1234567890123456789";
        let pg_negative_extreme_numeric =
            "-12345678901234567890123456789012345678901234567890.1234567890123456789";

        let test_cases = [
            (types::numeric(), "123.456", "standard GoogleSQL NUMERIC"),
            (types::numeric(), "-123.456", "negative GoogleSQL NUMERIC"),
            (
                types::numeric(),
                max_spanner_numeric,
                "38-digit max GoogleSQL NUMERIC exceeding 96-bit Decimal mantissa",
            ),
            (
                types::numeric(),
                min_spanner_numeric,
                "38-digit negative max GoogleSQL NUMERIC",
            ),
            (
                types::numeric(),
                "100000000000000000000000000000",
                "30-digit integer NUMERIC exceeding Decimal limit",
            ),
            (types::numeric(), "0", "zero NUMERIC"),
            (types::numeric(), "0.000000000", "zero NUMERIC with scale"),
            (types::numeric(), "-0", "negative zero NUMERIC"),
            (types::numeric(), "1.5e3", "scientific notation NUMERIC"),
            (
                types::numeric(),
                "-1.5e-3",
                "negative scientific notation NUMERIC",
            ),
            (
                types::pg_numeric(),
                "NaN",
                "PostgreSQL dialect PG_NUMERIC NaN",
            ),
            (
                types::pg_numeric(),
                pg_extreme_numeric,
                "extreme arbitrary precision PG_NUMERIC",
            ),
            (
                types::pg_numeric(),
                pg_negative_extreme_numeric,
                "negative extreme arbitrary precision PG_NUMERIC",
            ),
            (types::pg_numeric(), "42.5", "standard PostgreSQL numeric"),
            (types::pg_numeric(), "-42.5", "negative PostgreSQL numeric"),
            (
                types::pg_numeric(),
                ".123",
                "PostgreSQL numeric without leading zero",
            ),
            (
                types::pg_numeric(),
                "42.",
                "PostgreSQL numeric with trailing decimal",
            ),
            (
                types::pg_numeric(),
                "+123.456",
                "PostgreSQL numeric with explicit positive sign",
            ),
            (
                types::pg_numeric(),
                "1.5E-3",
                "PostgreSQL numeric scientific notation with capital E",
            ),
            (
                types::pg_numeric(),
                "0.00000000000000000000000000000000000000000000000001",
                "extreme scale PG_NUMERIC",
            ),
        ];

        for (column_type, text_content, description) in test_cases {
            // Borrowed conversion: from_value (using borrowed string slice directly)
            let value = text_content.to_value();
            let parsed_string = String::from_value(&value, &column_type)?;
            assert_eq!(
                parsed_string, text_content,
                "mismatch in borrowed String::from_value for {description}"
            );

            // Owned zero-copy conversion: from_owned_value
            let owned_value = text_content.to_string().to_value();
            let owned_parsed_string = String::from_owned_value(owned_value, &column_type)?;
            assert_eq!(
                owned_parsed_string, text_content,
                "mismatch in owned String::from_owned_value for {description}"
            );

            // Option<String> conversion
            let optional_parsed_string = Option::<String>::from_value(&value, &column_type)?;
            assert_eq!(
                optional_parsed_string,
                Some(text_content.to_string()),
                "mismatch in Option::<String>::from_value for {description}"
            );
        }

        // Null Option<String> handling across NUMERIC and PG_NUMERIC
        let null_cases = [
            (types::numeric(), "null NUMERIC"),
            (types::pg_numeric(), "null PG_NUMERIC"),
        ];

        for (column_type, description) in null_cases {
            let null_value = Value::default();
            let null_result = Option::<String>::from_value(&null_value, &column_type)?;
            assert_eq!(
                null_result, None,
                "expected None for Option::<String> on {description}"
            );

            let not_null_error = String::from_value(&null_value, &column_type)
                .expect_err("non-optional String must reject null value");
            assert!(
                matches!(not_null_error, ConvertError::NotNull),
                "expected ConvertError::NotNull for {description}"
            );
        }

        // Decimal conversion behavior on PG_NUMERIC
        let valid_pg_decimal = "42.5".to_value();
        let decimal = Decimal::from_value(&valid_pg_decimal, &types::pg_numeric())
            .expect("standard PG_NUMERIC must convert to Decimal");
        assert_eq!(
            decimal,
            Decimal::from_str_exact("42.5").expect("valid decimal"),
            "expected parsed Decimal from PG_NUMERIC"
        );

        let nan_pg = "NaN".to_value();
        let nan_error = Decimal::from_value(&nan_pg, &types::pg_numeric())
            .expect_err("Decimal must reject NaN PG_NUMERIC");
        assert!(
            matches!(nan_error, ConvertError::Convert(_)),
            "expected ConvertError::Convert for NaN PG_NUMERIC"
        );

        let extreme_pg = pg_extreme_numeric.to_value();
        let extreme_error = Decimal::from_value(&extreme_pg, &types::pg_numeric())
            .expect_err("Decimal must reject extreme PG_NUMERIC exceeding 96 bits");
        assert!(
            matches!(extreme_error, ConvertError::Convert(_)),
            "expected ConvertError::Convert for extreme PG_NUMERIC"
        );

        // GoogleSQL ARRAY<NUMERIC> into Vec<String>
        let numeric_array_type = types::array(types::numeric());
        let numeric_list = vec!["123.456", max_spanner_numeric].to_value();
        let parsed_numeric_vec = Vec::<String>::from_value(&numeric_list, &numeric_array_type)?;
        assert_eq!(
            parsed_numeric_vec,
            vec!["123.456".to_string(), max_spanner_numeric.to_string()],
            "mismatch in decoded ARRAY<NUMERIC>"
        );

        // PostgreSQL ARRAY<PG_NUMERIC> into Vec<Option<String>>
        let pg_numeric_array_type = types::array(types::pg_numeric());
        let pg_numeric_list = vec![
            Some("NaN".to_string()),
            None,
            Some(pg_extreme_numeric.to_string()),
        ]
        .to_value();

        let parsed_pg_numeric_vec =
            Vec::<Option<String>>::from_value(&pg_numeric_list, &pg_numeric_array_type)?;
        assert_eq!(
            parsed_pg_numeric_vec,
            vec![
                Some("NaN".to_string()),
                None,
                Some(pg_extreme_numeric.to_string()),
            ],
            "mismatch in decoded ARRAY<PG_NUMERIC>"
        );

        // Non-optional Vec<String> rejects null elements in numeric arrays
        let null_element_error =
            Vec::<String>::from_value(&pg_numeric_list, &pg_numeric_array_type)
                .expect_err("Vec::<String> must reject array containing null element");
        assert!(
            matches!(null_element_error, ConvertError::NotNull),
            "expected ConvertError::NotNull for null array element"
        );
        Ok(())
    }

    #[test]
    fn from_value_string_from_interval() -> Result<(), ConvertError> {
        let test_cases = [
            ("P1Y2M3D", "standard INTERVAL duration"),
            (
                "P1Y2M10DT2H30M15.5S",
                "detailed date-time INTERVAL duration",
            ),
            ("P0Y0M0DT0H0M0S", "full zero INTERVAL"),
            (
                "P-1Y2M-3DT-4H5M6.78912345S",
                "mixed sign INTERVAL components",
            ),
            ("P-1Y-2M-3DT-12H-12M-6S", "all negative INTERVAL components"),
            ("-P1Y2M3D", "negative prefixed ISO duration"),
            ("PT12H30M15S", "time-only INTERVAL"),
            ("PT0.000000001S", "subsecond nanosecond precision INTERVAL"),
            (
                "P1Y2M3DT12H12M6.789000123S",
                "full 9-digit fractional second INTERVAL",
            ),
            ("P2W", "weeks only INTERVAL"),
        ];

        let interval_type = types::interval();

        for (text_content, description) in test_cases {
            // Borrowed conversion: from_value (using borrowed string slice directly)
            let value = text_content.to_value();
            let parsed_string = String::from_value(&value, &interval_type)?;
            assert_eq!(
                parsed_string, text_content,
                "mismatch in borrowed String::from_value for {description}"
            );

            // Owned zero-copy conversion: from_owned_value
            let owned_value = text_content.to_string().to_value();
            let owned_parsed_string = String::from_owned_value(owned_value, &interval_type)?;
            assert_eq!(
                owned_parsed_string, text_content,
                "mismatch in owned String::from_owned_value for {description}"
            );

            // Option<String> conversion
            let optional_parsed_string = Option::<String>::from_value(&value, &interval_type)?;
            assert_eq!(
                optional_parsed_string,
                Some(text_content.to_string()),
                "mismatch in Option::<String>::from_value for {description}"
            );
        }

        // Null Option<String> handling for INTERVAL
        let null_value = Value::default();
        let null_result = Option::<String>::from_value(&null_value, &interval_type)?;
        assert_eq!(
            null_result, None,
            "expected None for Option::<String> on null INTERVAL"
        );

        let not_null_error = String::from_value(&null_value, &interval_type)
            .expect_err("non-optional String must reject null value");
        assert!(
            matches!(not_null_error, ConvertError::NotNull),
            "expected ConvertError::NotNull for null INTERVAL"
        );

        // Non-string types reject INTERVAL columns with TypeMismatch
        let interval_value = "P1Y2M3D".to_value();
        let int64_error =
            i64::from_value(&interval_value, &interval_type).expect_err("i64 must reject INTERVAL");
        assert_eq!(
            int64_error.to_string(),
            "type mismatch, expected Int64, got Interval",
            "expected TypeMismatch for i64 on INTERVAL"
        );

        let decimal_error = Decimal::from_value(&interval_value, &interval_type)
            .expect_err("Decimal must reject INTERVAL");
        assert_eq!(
            decimal_error.to_string(),
            "type mismatch, expected Numeric, got Interval",
            "expected TypeMismatch for Decimal on INTERVAL"
        );

        let float64_error =
            f64::from_value(&interval_value, &interval_type).expect_err("f64 must reject INTERVAL");
        assert_eq!(
            float64_error.to_string(),
            "type mismatch, expected Float64, got Interval",
            "expected TypeMismatch for f64 on INTERVAL"
        );

        let bool_error = bool::from_value(&interval_value, &interval_type)
            .expect_err("bool must reject INTERVAL");
        assert_eq!(
            bool_error.to_string(),
            "type mismatch, expected Bool, got Interval",
            "expected TypeMismatch for bool on INTERVAL"
        );

        // ARRAY<INTERVAL> into Vec<String>
        let interval_array_type = types::array(types::interval());
        let interval_list = vec!["P1Y2M3D", "P2Y"].to_value();
        let parsed_interval_vec = Vec::<String>::from_value(&interval_list, &interval_array_type)?;
        assert_eq!(
            parsed_interval_vec,
            vec!["P1Y2M3D".to_string(), "P2Y".to_string()],
            "mismatch in decoded ARRAY<INTERVAL>"
        );

        // ARRAY<INTERVAL> into Vec<Option<String>> with nulls
        let interval_nullable_list = vec![Some("P1Y2M3D"), None, Some("PT5M")].to_value();
        let parsed_nullable_vec =
            Vec::<Option<String>>::from_value(&interval_nullable_list, &interval_array_type)?;
        assert_eq!(
            parsed_nullable_vec,
            vec![Some("P1Y2M3D".to_string()), None, Some("PT5M".to_string())],
            "mismatch in decoded nullable ARRAY<INTERVAL>"
        );

        // Vec<String> rejects null elements in ARRAY<INTERVAL>
        let null_element_error =
            Vec::<String>::from_value(&interval_nullable_list, &interval_array_type)
                .expect_err("Vec::<String> must reject ARRAY<INTERVAL> with null element");
        assert!(
            matches!(null_element_error, ConvertError::NotNull),
            "expected ConvertError::NotNull for null INTERVAL array element"
        );
        Ok(())
    }

    use prost::{Enumeration, Message};

    #[derive(Clone, PartialEq, Message)]
    struct TestAddress {
        #[prost(string, tag = "1")]
        street: String,
        #[prost(string, tag = "2")]
        city: String,
    }

    impl FromValue for TestAddress {
        fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
            let bytes = Vec::<u8>::from_value(value, spanner_type)?;
            Self::decode(bytes.as_slice()).map_err(ConvertError::custom)
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
    #[repr(i32)]
    enum TestOrderStatus {
        Unspecified = 0,
        Pending = 1,
        Shipped = 2,
        Delivered = 3,
    }

    impl FromValue for TestOrderStatus {
        fn from_value(value: &Value, spanner_type: &Type) -> Result<Self, ConvertError> {
            let raw_value = i32::from_value(value, spanner_type)?;
            Self::try_from(raw_value).map_err(ConvertError::custom)
        }
    }

    #[test]
    fn from_value_proto() {
        let sample_address = TestAddress {
            street: "1600 Amphitheatre Pkwy".to_string(),
            city: "Mountain View".to_string(),
        };
        let raw_payload = sample_address.encode_to_vec();
        let proto_type = types::proto("examples.shipping.Address");
        let encoded_string = BASE64_STANDARD.encode(&raw_payload);
        let proto_value = Value::from(encoded_string);

        // Borrowed deserialization
        let decoded_bytes = Vec::<u8>::from_value(&proto_value, &proto_type)
            .expect("borrowed Vec<u8> from_value should succeed for proto type");
        let decoded_address = TestAddress::decode(decoded_bytes.as_slice())
            .expect("decode TestAddress from borrowed bytes");
        assert_eq!(
            decoded_address, sample_address,
            "decoded proto message must match original struct"
        );

        // Custom type FromValue direct deserialization
        let direct_address = TestAddress::from_value(&proto_value, &proto_type)
            .expect("direct TestAddress from_value should succeed");
        assert_eq!(
            direct_address, sample_address,
            "direct TestAddress must match original struct"
        );

        // Owned deserialization
        let decoded_owned = Vec::<u8>::from_owned_value(proto_value.clone(), &proto_type)
            .expect("owned Vec<u8> from_owned_value should succeed for proto type");
        let decoded_address_owned = TestAddress::decode(decoded_owned.as_slice())
            .expect("decode TestAddress from owned bytes");
        assert_eq!(
            decoded_address_owned, sample_address,
            "decoded proto message from owned bytes must match original struct"
        );

        // Option<Vec<u8>> with value
        let some_proto = Option::<Vec<u8>>::from_value(&proto_value, &proto_type)
            .expect("present Option<Vec<u8>> should succeed for proto type");
        assert_eq!(
            some_proto,
            Some(raw_payload.to_vec()),
            "expected Some(payload) for proto type"
        );

        // Option<Vec<u8>> with null
        let null_proto = Option::<Vec<u8>>::from_value(&Value::null(), &proto_type)
            .expect("null Option<Vec<u8>> should succeed for proto type");
        assert_eq!(
            null_proto, None,
            "expected None for null Option<Vec<u8>> on proto type"
        );

        // Array of proto
        let array_proto_type = types::array(proto_type.clone());
        let address_1 = TestAddress {
            street: "1 Market St".to_string(),
            city: "San Francisco".to_string(),
        };
        let address_2 = TestAddress {
            street: "111 8th Ave".to_string(),
            city: "New York".to_string(),
        };
        let list_value = Value::from(vec![
            Value::from(BASE64_STANDARD.encode(address_1.encode_to_vec())),
            Value::from(BASE64_STANDARD.encode(address_2.encode_to_vec())),
        ]);
        let decoded_list = Vec::<Vec<u8>>::from_value(&list_value, &array_proto_type)
            .expect("Vec<Vec<u8>> should succeed for ARRAY<PROTO>");
        let decoded_addresses: Vec<TestAddress> = decoded_list
            .iter()
            .map(|bytes| {
                TestAddress::decode(bytes.as_slice()).expect("decode TestAddress from list element")
            })
            .collect();
        assert_eq!(
            decoded_addresses,
            vec![address_1.clone(), address_2],
            "expected decoded list of proto addresses"
        );

        // Array of proto containing NULL elements
        let nullable_list_value = Value::from(vec![
            Value::from(BASE64_STANDARD.encode(address_1.encode_to_vec())),
            Value::null(),
        ]);
        let decoded_nullable_list =
            Vec::<Option<Vec<u8>>>::from_value(&nullable_list_value, &array_proto_type)
                .expect("Vec<Option<Vec<u8>>> should succeed for ARRAY<PROTO> with nulls");
        assert_eq!(
            decoded_nullable_list.len(),
            2,
            "decoded_nullable_list length must be 2"
        );
        assert_eq!(
            TestAddress::decode(
                decoded_nullable_list[0]
                    .as_ref()
                    .expect("element 0 must exist")
                    .as_slice()
            )
            .expect("decode element 0"),
            address_1,
            "element 0 must match address_1"
        );
        assert!(
            decoded_nullable_list[1].is_none(),
            "expected element 1 in nullable list to be None"
        );

        // Error: non-nullable null
        let null_error = Vec::<u8>::from_value(&Value::null(), &proto_type)
            .expect_err("null value for non-optional Vec<u8> must return NotNull");
        assert!(
            matches!(null_error, ConvertError::NotNull),
            "expected NotNull error"
        );

        // Error: invalid base64
        let invalid_base64_value = Value::from("not-valid-base64!!!");
        let base64_error = Vec::<u8>::from_value(&invalid_base64_value, &proto_type)
            .expect_err("invalid base64 must return Convert error");
        assert!(
            matches!(base64_error, ConvertError::Convert(_)),
            "expected Convert error for invalid base64"
        );

        // Error: type mismatch (e.g. String type instead of Proto)
        let type_mismatch_error = Vec::<u8>::from_value(&proto_value, &types::string())
            .expect_err("String type for Vec<u8> must return TypeMismatch");
        assert!(
            matches!(
                type_mismatch_error,
                ConvertError::TypeMismatch {
                    want: TypeCode::Bytes,
                    got: TypeCode::String,
                }
            ),
            "expected TypeMismatch error"
        );

        // Error: kind mismatch (e.g. boolean value)
        let kind_mismatch_error = Vec::<u8>::from_value(&true.to_value(), &proto_type)
            .expect_err("boolean value for Vec<u8> must return KindMismatch");
        assert!(
            matches!(
                kind_mismatch_error,
                ConvertError::KindMismatch {
                    want: Kind::String,
                    got: Kind::Bool,
                }
            ),
            "expected KindMismatch error"
        );

        // Error: invalid proto decode for custom FromValue struct
        let malformed_proto_value = Value::from(BASE64_STANDARD.encode([0xFF, 0xFF, 0xFF]));
        let decode_error = TestAddress::from_value(&malformed_proto_value, &proto_type)
            .expect_err("malformed proto bytes must return Convert error for TestAddress");
        assert!(
            matches!(decode_error, ConvertError::Convert(_)),
            "expected Convert error for malformed proto bytes"
        );
    }

    #[test]
    fn from_value_proto_enum() {
        let enum_type = types::proto_enum("examples.shipping.OrderStatus");
        let enum_value = Value::from("2");

        // i64 borrowed & owned
        let int64_borrowed = i64::from_value(&enum_value, &enum_type)
            .expect("borrowed i64 from_value should succeed for enum type");
        assert_eq!(int64_borrowed, 2_i64, "expected enum int64 value 2");
        let order_status = TestOrderStatus::try_from(int64_borrowed as i32)
            .expect("convert i64 to TestOrderStatus");
        assert_eq!(
            order_status,
            TestOrderStatus::Shipped,
            "expected Shipped enum variant"
        );

        // Custom type FromValue direct deserialization
        let direct_status = TestOrderStatus::from_value(&enum_value, &enum_type)
            .expect("direct TestOrderStatus from_value should succeed");
        assert_eq!(
            direct_status,
            TestOrderStatus::Shipped,
            "expected direct Shipped enum variant"
        );

        let int64_owned = i64::from_owned_value(enum_value.clone(), &enum_type)
            .expect("owned i64 from_owned_value should succeed for enum type");
        assert_eq!(int64_owned, 2_i64, "expected owned enum int64 value 2");

        // i32 borrowed & owned
        let int32_borrowed = i32::from_value(&enum_value, &enum_type)
            .expect("borrowed i32 from_value should succeed for enum type");
        assert_eq!(int32_borrowed, 2_i32, "expected enum int32 value 2");
        let order_status_32 =
            TestOrderStatus::try_from(int32_borrowed).expect("convert i32 to TestOrderStatus");
        assert_eq!(
            order_status_32,
            TestOrderStatus::Shipped,
            "expected Shipped enum variant"
        );

        let int32_owned = i32::from_owned_value(enum_value.clone(), &enum_type)
            .expect("owned i32 from_owned_value should succeed for enum type");
        assert_eq!(int32_owned, 2_i32, "expected owned enum int32 value 2");

        // Option<i64> with value & null
        let some_enum = Option::<i64>::from_value(&enum_value, &enum_type)
            .expect("present Option<i64> should succeed for enum type");
        assert_eq!(some_enum, Some(2_i64), "expected Some(2) for enum type");

        let null_enum = Option::<i64>::from_value(&Value::null(), &enum_type)
            .expect("null Option<i64> should succeed for enum type");
        assert_eq!(
            null_enum, None,
            "expected None for null Option<i64> on enum type"
        );

        // Array of enums
        let array_enum_type = types::array(enum_type.clone());
        let list_value = Value::from(vec![Value::from("1"), Value::from("3")]);
        let decoded_list = Vec::<i64>::from_value(&list_value, &array_enum_type)
            .expect("Vec<i64> should succeed for ARRAY<ENUM>");
        assert_eq!(
            decoded_list,
            vec![1_i64, 3_i64],
            "expected decoded list of enum integers"
        );
        let decoded_statuses: Vec<TestOrderStatus> = decoded_list
            .iter()
            .map(|&val| {
                TestOrderStatus::try_from(val as i32)
                    .expect("convert array element to TestOrderStatus")
            })
            .collect();
        assert_eq!(
            decoded_statuses,
            vec![TestOrderStatus::Pending, TestOrderStatus::Delivered],
            "expected decoded enum statuses"
        );

        // Array of enums containing NULL elements
        let nullable_list_value = Value::from(vec![Value::from("1"), Value::null()]);
        let decoded_nullable_list =
            Vec::<Option<i64>>::from_value(&nullable_list_value, &array_enum_type)
                .expect("Vec<Option<i64>> should succeed for ARRAY<ENUM> with nulls");
        assert_eq!(
            decoded_nullable_list,
            vec![Some(1_i64), None],
            "expected decoded nullable enum list with Some(1) and None"
        );

        // Error: non-nullable null
        let null_error = i64::from_value(&Value::null(), &enum_type)
            .expect_err("null value for non-optional i64 must return NotNull");
        assert!(
            matches!(null_error, ConvertError::NotNull),
            "expected NotNull error"
        );

        // Error: non-numeric string
        let invalid_enum_value = Value::from("PENDING");
        let parse_error = i64::from_value(&invalid_enum_value, &enum_type)
            .expect_err("non-numeric string for enum must return Convert error");
        assert!(
            matches!(parse_error, ConvertError::Convert(_)),
            "expected parse error for non-numeric enum string"
        );

        // Error: type mismatch (e.g. String type instead of Enum)
        let type_mismatch_error = i64::from_value(&enum_value, &types::string())
            .expect_err("String type for i64 must return TypeMismatch");
        assert!(
            matches!(
                type_mismatch_error,
                ConvertError::TypeMismatch {
                    want: TypeCode::Int64,
                    got: TypeCode::String,
                }
            ),
            "expected TypeMismatch error"
        );

        // Error: kind mismatch (e.g. boolean value)
        let kind_mismatch_error = i64::from_value(&true.to_value(), &enum_type)
            .expect_err("boolean value for i64 must return KindMismatch");
        assert!(
            matches!(
                kind_mismatch_error,
                ConvertError::KindMismatch {
                    want: Kind::String,
                    got: Kind::Bool,
                }
            ),
            "expected KindMismatch error"
        );

        // Error: invalid enum variant integer
        let invalid_variant_value = Value::from("99");
        let invalid_variant_error = TestOrderStatus::from_value(&invalid_variant_value, &enum_type)
            .expect_err("unknown enum integer must return Convert error for TestOrderStatus");
        assert!(
            matches!(invalid_variant_error, ConvertError::Convert(_)),
            "expected Convert error for invalid enum variant"
        );
    }

    #[test]
    fn from_value_bytes_crate() {
        let bytes_type = types::bytes();
        let proto_type = types::proto("com.example.CustomerProto");
        let raw_bytes = b"hello binary world";
        let encoded_value = raw_bytes.to_vec().to_value();

        let decoded_borrowed = Bytes::from_value(&encoded_value, &bytes_type)
            .expect("borrowed Bytes from_value should succeed");
        assert_eq!(
            decoded_borrowed,
            Bytes::from_static(raw_bytes),
            "borrowed Bytes content must match"
        );

        let decoded_proto = Bytes::from_value(&encoded_value, &proto_type)
            .expect("borrowed Bytes from proto type should succeed");
        assert_eq!(
            decoded_proto,
            Bytes::from_static(raw_bytes),
            "proto Bytes content must match"
        );

        let decoded_owned = Bytes::from_owned_value(encoded_value, &bytes_type)
            .expect("owned Bytes from_owned_value should succeed");
        assert_eq!(
            decoded_owned,
            Bytes::from_static(raw_bytes),
            "owned Bytes content must match"
        );

        let null_error = Bytes::from_value(&Value::null(), &bytes_type)
            .expect_err("null Bytes must return NotNull");
        assert!(
            matches!(null_error, ConvertError::NotNull),
            "expected NotNull error"
        );

        let type_mismatch_error =
            Bytes::from_value(&raw_bytes.to_vec().to_value(), &types::string())
                .expect_err("string type for Bytes must return TypeMismatch");
        assert!(
            matches!(
                type_mismatch_error,
                ConvertError::TypeMismatch {
                    want: TypeCode::Bytes,
                    got: TypeCode::String
                }
            ),
            "expected TypeMismatch error"
        );

        let kind_mismatch_error = Bytes::from_value(&true.to_value(), &bytes_type)
            .expect_err("bool value for Bytes must return KindMismatch");
        assert!(
            matches!(
                kind_mismatch_error,
                ConvertError::KindMismatch {
                    want: Kind::String,
                    got: Kind::Bool
                }
            ),
            "expected KindMismatch error"
        );

        let invalid_base64_value = Value::from("not-valid-base64!@#$");
        let decode_error = Bytes::from_value(&invalid_base64_value, &bytes_type)
            .expect_err("invalid base64 must return Convert error");
        assert!(
            matches!(decode_error, ConvertError::Convert(_)),
            "expected Convert error for invalid base64"
        );

        let empty_bytes = Bytes::from_static(b"");
        let empty_value = empty_bytes.clone().to_value();
        let decoded_empty = Bytes::from_value(&empty_value, &bytes_type)
            .expect("empty Bytes from_value should succeed");
        assert_eq!(decoded_empty, empty_bytes, "empty Bytes must match");

        let owned_type_mismatch =
            Bytes::from_owned_value(raw_bytes.to_vec().to_value(), &types::string())
                .expect_err("owned string type for Bytes must return TypeMismatch");
        assert!(
            matches!(
                owned_type_mismatch,
                ConvertError::TypeMismatch {
                    want: TypeCode::Bytes,
                    got: TypeCode::String
                }
            ),
            "expected TypeMismatch error for owned Bytes"
        );

        let owned_null_error = Bytes::from_owned_value(Value::null(), &bytes_type)
            .expect_err("owned null Bytes must return NotNull");
        assert!(
            matches!(owned_null_error, ConvertError::NotNull),
            "expected NotNull error for owned Bytes"
        );

        let owned_kind_mismatch = Bytes::from_owned_value(true.to_value(), &bytes_type)
            .expect_err("owned bool value for Bytes must return KindMismatch");
        assert!(
            matches!(
                owned_kind_mismatch,
                ConvertError::KindMismatch {
                    want: Kind::String,
                    got: Kind::Bool
                }
            ),
            "expected KindMismatch error for owned Bytes"
        );

        let owned_decode_error =
            Bytes::from_owned_value(Value::from("not-valid-base64!@#$"), &bytes_type)
                .expect_err("owned invalid base64 must return Convert error");
        assert!(
            matches!(owned_decode_error, ConvertError::Convert(_)),
            "expected Convert error for owned invalid base64"
        );
    }

    #[test]
    fn from_value_uuid() {
        let uuid_type = types::uuid();
        let string_type = types::string();
        let uuid_string = "123e4567-e89b-12d3-a456-426614174000";
        let expected_uuid = Uuid::parse_str(uuid_string).expect("valid uuid string");
        let uuid_value = Value::from(uuid_string);

        let decoded_borrowed = Uuid::from_value(&uuid_value, &uuid_type)
            .expect("borrowed Uuid from_value should succeed with uuid type");
        assert_eq!(decoded_borrowed, expected_uuid, "borrowed Uuid must match");

        let decoded_from_string_type = Uuid::from_value(&uuid_value, &string_type)
            .expect("borrowed Uuid from_value should succeed with string type");
        assert_eq!(
            decoded_from_string_type, expected_uuid,
            "Uuid from string type must match"
        );

        let decoded_owned = Uuid::from_owned_value(uuid_value, &uuid_type)
            .expect("owned Uuid from_owned_value should succeed");
        assert_eq!(decoded_owned, expected_uuid, "owned Uuid must match");

        let null_error = Uuid::from_value(&Value::null(), &uuid_type)
            .expect_err("null Uuid must return NotNull");
        assert!(
            matches!(null_error, ConvertError::NotNull),
            "expected NotNull error"
        );

        let kind_none_error = Uuid::from_value(&Value(ProtoValue { kind: None }), &uuid_type)
            .expect_err("kind None for Uuid must return NotNull");
        assert!(
            matches!(kind_none_error, ConvertError::NotNull),
            "expected NotNull error for kind None"
        );

        let type_mismatch_error = Uuid::from_value(&Value::from(uuid_string), &types::int64())
            .expect_err("int64 type for Uuid must return TypeMismatch");
        assert!(
            matches!(
                type_mismatch_error,
                ConvertError::TypeMismatch {
                    want: TypeCode::Uuid,
                    got: TypeCode::Int64
                }
            ),
            "expected TypeMismatch error"
        );

        let kind_mismatch_error = Uuid::from_value(&true.to_value(), &uuid_type)
            .expect_err("bool kind for Uuid must return KindMismatch");
        assert!(
            matches!(
                kind_mismatch_error,
                ConvertError::KindMismatch {
                    want: Kind::String,
                    got: Kind::Bool
                }
            ),
            "expected KindMismatch error"
        );

        let invalid_uuid_value = Value::from("not-a-valid-uuid");
        let parse_error = Uuid::from_value(&invalid_uuid_value, &uuid_type)
            .expect_err("malformed uuid string must return Convert error");
        assert!(
            matches!(parse_error, ConvertError::Convert(_)),
            "expected Convert error for invalid uuid format"
        );
    }

    #[test]
    fn from_value_bytes_and_uuid_in_collections() {
        let uuid_type = types::uuid();
        let bytes_type = types::bytes();
        let uuid_string = "123e4567-e89b-12d3-a456-426614174000";
        let expected_uuid = Uuid::parse_str(uuid_string).expect("valid uuid string");

        // Option<Uuid>
        let present_uuid = Option::<Uuid>::from_value(&Value::from(uuid_string), &uuid_type)
            .expect("present Option<Uuid> should succeed");
        assert_eq!(present_uuid, Some(expected_uuid), "expected Some(uuid)");

        let null_uuid = Option::<Uuid>::from_value(&Value::null(), &uuid_type)
            .expect("null Option<Uuid> should succeed");
        assert_eq!(null_uuid, None, "expected None for null Option<Uuid>");

        // Option<Bytes>
        let raw_bytes = b"sample bytes";
        let encoded_bytes = raw_bytes.to_vec().to_value();
        let present_bytes = Option::<Bytes>::from_value(&encoded_bytes, &bytes_type)
            .expect("present Option<Bytes> should succeed");
        assert_eq!(
            present_bytes,
            Some(Bytes::from_static(raw_bytes)),
            "expected Some(Bytes)"
        );

        let null_bytes = Option::<Bytes>::from_value(&Value::null(), &bytes_type)
            .expect("null Option<Bytes> should succeed");
        assert_eq!(null_bytes, None, "expected None for null Option<Bytes>");

        // Vec<Uuid>
        let array_uuid_type = types::array(types::uuid());
        let uuid_list_value = Value::from(vec![uuid_string.to_string(), uuid_string.to_string()]);
        let uuid_vector = Vec::<Uuid>::from_value(&uuid_list_value, &array_uuid_type)
            .expect("Vec<Uuid> from_value should succeed");
        assert_eq!(
            uuid_vector,
            vec![expected_uuid, expected_uuid],
            "expected matching uuid vector"
        );

        let invalid_uuid_list_value = Value::from(vec!["not-a-uuid".to_string()]);
        let uuid_collection_error =
            Vec::<Uuid>::from_value(&invalid_uuid_list_value, &array_uuid_type)
                .expect_err("invalid uuid element in collection must error");
        assert!(
            matches!(uuid_collection_error, ConvertError::Convert(_)),
            "expected Convert error in Vec<Uuid>"
        );

        // Vec<Bytes>
        let array_bytes_type = types::array(types::bytes());
        let bytes_list_value = Value::from(vec![
            raw_bytes.to_vec().to_value(),
            raw_bytes.to_vec().to_value(),
        ]);
        let bytes_vector = Vec::<Bytes>::from_value(&bytes_list_value, &array_bytes_type)
            .expect("Vec<Bytes> from_value should succeed");
        assert_eq!(
            bytes_vector,
            vec![Bytes::from_static(raw_bytes), Bytes::from_static(raw_bytes)],
            "expected matching bytes vector"
        );

        let invalid_bytes_list_value = Value::from(vec![Value::from("invalid-base64!#%")]);
        let bytes_collection_error =
            Vec::<Bytes>::from_value(&invalid_bytes_list_value, &array_bytes_type)
                .expect_err("invalid base64 element in collection must error");
        assert!(
            matches!(bytes_collection_error, ConvertError::Convert(_)),
            "expected Convert error in Vec<Bytes>"
        );
    }
}
