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

//! Key recipe evaluation engine for Spanner location-aware routing.
//!
//! Evaluates a [`KeyRecipe`] against SQL query parameter values or primary key tuples and encodes
//! them into a lexicographical binary storage specification key (`Vec<u8>`) using [`ssformat`](crate::routing::ssformat).

// TODO(#6236): Remove dead_code allowance once KeyRecipe and KeyRecipeCache are integrated into DatabaseClient.
#![allow(dead_code)]

use crate::Result;
use crate::model::key_recipe::Part;
use crate::model::key_recipe::part::{NullOrder, Order};
use crate::model::{KeyRecipe, TypeCode};
use crate::routing::ssformat;
use crate::value::{Kind, Value};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;

/// Encodes a Spanner routing key (`Vec<u8>`) from a [`KeyRecipe`] and a slice of column [`Value`]s.
///
/// Allocates a new vector with sufficient capacity for the encoded parts. For zero-allocation
/// key encoding into a reused scratch buffer on hot query paths, use [`encode_key_from_recipe_into`].
///
/// Each part of the recipe is evaluated in order:
/// - If `part.tag != 0`, the tag number is appended to the binary key.
/// - If `part.tag == 0`, the corresponding column value from `values` is encoded using `ssformat`
///   according to the part's sort order (`ASCENDING` / `DESCENDING`) and null order (`NULLS_FIRST` / `NULLS_LAST`).
///
/// # Caller Fallback Contract
/// If encoding returns an error (for example, due to an unsupported key column type like `NUMERIC`
/// or `FLOAT32`), callers (`LocationRouter` / `DatabaseClient`) MUST catch the error and silently
/// fall back to default routing (without tablet affinity) rather than failing the user's RPC.
pub(crate) fn encode_key_from_recipe(recipe: &KeyRecipe, values: &[Value]) -> Result<Vec<u8>> {
    let mut buffer = Vec::with_capacity(recipe.part.len().saturating_mul(16));
    encode_key_from_recipe_into(recipe, values, &mut buffer)?;
    Ok(buffer)
}

/// Encodes a Spanner routing key from a [`KeyRecipe`] and a slice of column [`Value`]s directly
/// into an existing output buffer.
///
/// This avoids new `Vec<u8>` heap allocations when callers reuse a scratch buffer across RPCs
/// on the Spanner Omni hot path.
///
/// # Caller Fallback Contract
/// If encoding returns an error (for example, due to an unsupported key column type like `NUMERIC`
/// or `FLOAT32`), callers (`LocationRouter` / `DatabaseClient`) MUST catch the error and silently
/// fall back to default routing (without tablet affinity) rather than failing the user's RPC.
pub(crate) fn encode_key_from_recipe_into(
    recipe: &KeyRecipe,
    values: &[Value],
    buffer: &mut Vec<u8>,
) -> Result<()> {
    let mut values_iter = values.iter();

    for part in &recipe.part {
        if part.tag != 0 {
            let tag = i32::try_from(part.tag).map_err(|_| {
                crate::error::internal_error(format!(
                    "KeyRecipe part tag {} exceeds i32::MAX",
                    part.tag
                ))
            })?;
            ssformat::append_composite_tag(buffer, tag)?;
            continue;
        }

        let value = values_iter.next().ok_or_else(|| {
            crate::error::internal_error(
                "Not enough column values to encode key recipe: more values required",
            )
        })?;

        encode_part(buffer, part, value)?;
    }

    Ok(())
}

/// Verifies that the recipe part's data type is supported by this key recipe encoder.
///
/// According to [Spanner documentation](https://docs.cloud.google.com/spanner/docs/reference/standard-sql/data-types#valid_key_column_types),
/// all data types are valid key column types except for `FLOAT32`, `ARRAY`, `JSON`, and `STRUCT`.
///
/// Currently, this encoder supports types whose wire-format encoding or ISO 8601 string format
/// preserves lexicographical sort order: `BOOL`, `INT64`, `FLOAT64`, `STRING`, `BYTES`, `DATE`,
/// and `TIMESTAMP`.
///
/// Note: `NUMERIC` is a valid Spanner key column type, but is not currently supported here because
/// it requires a specialized binary decimal storage encoding (`ssformat`) so that `"10.0"` sorts
/// after `"2.0"`. Unsupported types explicitly return an error so that requests gracefully fall back
/// to default routing rather than emitting invalid shard keys.
fn check_supported_key_type(part: &Part) -> Result<()> {
    if let Some(ref t) = part.r#type {
        match &t.code {
            TypeCode::Bool
            | TypeCode::Int64
            | TypeCode::Float64
            | TypeCode::String
            | TypeCode::Bytes
            | TypeCode::Date
            | TypeCode::Timestamp
            | TypeCode::Unspecified => Ok(()),
            unsupported => Err(crate::error::internal_error(format!(
                "TypeCode {unsupported:?} is not supported for key recipe encoding",
            ))),
        }
    } else {
        Ok(())
    }
}

/// Evaluates a single key column part against a [`Value`] and appends it to `buffer`.
fn encode_part(buffer: &mut Vec<u8>, part: &Part, value: &Value) -> Result<()> {
    check_supported_key_type(part)?;

    if value.kind() == Kind::Null {
        return encode_null_part(buffer, &part.null_order);
    }

    let decreasing = matches!(part.order, Order::Descending);

    match value.kind() {
        Kind::String => encode_string_part(buffer, part, value, decreasing),
        Kind::Number => encode_number_part(buffer, part, value, decreasing),
        Kind::Bool => encode_bool_part(buffer, value, decreasing),
        other => Err(crate::error::internal_error(format!(
            "Unsupported Value kind {other:?} for key recipe encoding",
        ))),
    }
}

/// Appends a NULL value marker according to [`NullOrder`].
fn encode_null_part(buffer: &mut Vec<u8>, null_order: &NullOrder) -> Result<()> {
    match null_order {
        NullOrder::NullsFirst | NullOrder::Unspecified | NullOrder::UnknownValue(_) => {
            ssformat::append_null_ordered_first(buffer);
            Ok(())
        }
        NullOrder::NullsLast => {
            ssformat::append_null_ordered_last(buffer);
            Ok(())
        }
        NullOrder::NotNull => Err(crate::error::internal_error(
            "NULL value provided for NOT NULL key recipe column",
        )),
    }
}

/// Evaluates a string-encoded column value.
///
/// In Spanner Protobuf wire format, 64-bit integers (`INT64`), arbitrary-precision decimals
/// (`NUMERIC`), dates, timestamps, base64-encoded byte arrays (`BYTES`), and special floating-point
/// values (`"NaN"`, `"Infinity"`, `"-Infinity"`) are sent as JSON strings (`StringValue`).
///
/// - `INT64` strings must be parsed to integer values because ASCII string sort order does not
///   match numerical sort order (`"100" < "20"` in ASCII, but `20 < 100` numerically).
/// - `BYTES` strings must be base64-decoded into raw byte slices because base64 ASCII character
///   ordering does not match raw lexicographical byte ordering.
/// - `FLOAT64` strings (`"NaN"`, `"Infinity"`, `"-Infinity"`, or number strings) must be
///   parsed to `f64` and encoded as floating-point numbers.
/// - `STRING`, `DATE`, and `TIMESTAMP` ISO 8601 strings naturally sort in ASCII lexicographical
///   order and can be encoded directly.
fn encode_string_part(
    buffer: &mut Vec<u8>,
    part: &Part,
    value: &Value,
    decreasing: bool,
) -> Result<()> {
    let string_val = value
        .try_as_string()
        .ok_or_else(|| crate::error::internal_error("Expected String value"))?;

    if let Some(ref t) = part.r#type {
        if t.code == TypeCode::Int64 {
            let int_val = string_val.parse::<i64>().map_err(|e| {
                crate::error::internal_error(format!(
                    "Failed to parse Int64 from string '{string_val}': {e}"
                ))
            })?;
            return append_int64_ordered(buffer, int_val, decreasing);
        }
        if t.code == TypeCode::Bytes {
            // Base64 decoding into raw bytes is required before ssformat escaping because base64
            // ASCII order does not match raw binary byte order.
            let bytes_val = BASE64_STANDARD.decode(string_val).map_err(|e| {
                crate::error::internal_error(format!(
                    "Failed to decode base64 Bytes from string '{string_val}': {e}"
                ))
            })?;
            return append_bytes_ordered(buffer, &bytes_val, decreasing);
        }
        if t.code == TypeCode::Float64 {
            let float_val = match string_val {
                "NaN" => f64::NAN,
                "Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                other => other.parse::<f64>().map_err(|e| {
                    crate::error::internal_error(format!(
                        "Failed to parse Float64 from string '{string_val}': {e}"
                    ))
                })?,
            };
            return append_double_ordered(buffer, float_val, decreasing);
        }
    }

    append_string_ordered(buffer, string_val, decreasing)
}

/// Evaluates a floating-point number column value (`Kind::Number`).
///
/// Note: 64-bit integers (`INT64`) are never received as `Kind::Number` in Spanner Protobuf format;
/// they are always sent as `Kind::String` to avoid IEEE 754 53-bit integer precision loss.
fn encode_number_part(
    buffer: &mut Vec<u8>,
    part: &Part,
    value: &Value,
    decreasing: bool,
) -> Result<()> {
    check_supported_key_type(part)?;
    let double_val = value
        .try_as_f64()
        .ok_or_else(|| crate::error::internal_error("Expected Number value"))?;
    append_double_ordered(buffer, double_val, decreasing)
}

/// Evaluates a boolean column value (`Kind::Bool`).
fn encode_bool_part(buffer: &mut Vec<u8>, value: &Value, decreasing: bool) -> Result<()> {
    let bool_val = value
        .try_as_bool()
        .ok_or_else(|| crate::error::internal_error("Expected Bool value"))?;
    append_bool_ordered(buffer, bool_val, decreasing)
}

#[inline]
fn append_int64_ordered(buffer: &mut Vec<u8>, value: i64, decreasing: bool) -> Result<()> {
    if decreasing {
        ssformat::append_int64_decreasing(buffer, value);
    } else {
        ssformat::append_int64_increasing(buffer, value);
    }
    Ok(())
}

#[inline]
fn append_bytes_ordered(buffer: &mut Vec<u8>, value: &[u8], decreasing: bool) -> Result<()> {
    if decreasing {
        ssformat::append_bytes_decreasing(buffer, value);
    } else {
        ssformat::append_bytes_increasing(buffer, value);
    }
    Ok(())
}

#[inline]
fn append_string_ordered(buffer: &mut Vec<u8>, value: &str, decreasing: bool) -> Result<()> {
    if decreasing {
        ssformat::append_string_decreasing(buffer, value);
    } else {
        ssformat::append_string_increasing(buffer, value);
    }
    Ok(())
}

#[inline]
fn append_double_ordered(buffer: &mut Vec<u8>, value: f64, decreasing: bool) -> Result<()> {
    if decreasing {
        ssformat::append_double_decreasing(buffer, value);
    } else {
        ssformat::append_double_increasing(buffer, value);
    }
    Ok(())
}

#[inline]
fn append_bool_ordered(buffer: &mut Vec<u8>, value: bool, decreasing: bool) -> Result<()> {
    if decreasing {
        ssformat::append_bool_decreasing(buffer, value);
    } else {
        ssformat::append_bool_increasing(buffer, value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Type;
    use crate::model::key_recipe::Part;
    use crate::value::ToValue;

    #[test]
    fn encode_key_from_recipe_composite_tags_only() {
        let recipe = KeyRecipe::new().set_part(vec![Part::new().set_tag(1u32)]);
        let encoded = encode_key_from_recipe(&recipe, &[])
            .expect("tag encoding should succeed without column values");
        assert!(
            !encoded.is_empty(),
            "encoded tag buffer should not be empty"
        );
    }

    #[test]
    fn encode_key_from_recipe_composite_tag_overflow_returns_err() {
        let recipe = KeyRecipe::new().set_part(vec![Part::new().set_tag(u32::MAX)]);
        let error = encode_key_from_recipe(&recipe, &[])
            .expect_err("exceeding i32::MAX should return error");
        assert!(
            error.to_string().contains("exceeds i32::MAX"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_string_ascending_and_descending() {
        let asc_recipe = KeyRecipe::new().set_part(vec![Part::new().set_order(Order::Ascending)]);
        let desc_recipe = KeyRecipe::new().set_part(vec![Part::new().set_order(Order::Descending)]);
        let values = vec!["alpha".to_value()];

        let asc_encoded = encode_key_from_recipe(&asc_recipe, &values)
            .expect("ascending string encoding should succeed");
        let desc_encoded = encode_key_from_recipe(&desc_recipe, &values)
            .expect("descending string encoding should succeed");

        assert_ne!(
            asc_encoded, desc_encoded,
            "ascending and descending encodings must differ"
        );
    }

    #[test]
    fn encode_key_from_recipe_int64_string_encoding() {
        let part = Part::new()
            .set_order(Order::Ascending)
            .set_type(Type::default().set_code(TypeCode::Int64));

        let recipe = KeyRecipe::new().set_part(vec![part]);
        let values = vec!["12345".to_value()];

        let encoded =
            encode_key_from_recipe(&recipe, &values).expect("int64 string encoding should succeed");
        assert!(!encoded.is_empty(), "encoded buffer should not be empty");
    }

    #[test]
    fn encode_key_from_recipe_float_number_encoding() {
        let asc_recipe = KeyRecipe::new().set_part(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);
        let desc_recipe = KeyRecipe::new().set_part(vec![
            Part::new()
                .set_order(Order::Descending)
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);
        let values = vec![2.5_f64.to_value()];

        let asc_encoded = encode_key_from_recipe(&asc_recipe, &values)
            .expect("ascending float encoding should succeed");
        let desc_encoded = encode_key_from_recipe(&desc_recipe, &values)
            .expect("descending float encoding should succeed");
        assert_ne!(
            asc_encoded, desc_encoded,
            "ascending and descending float encodings must differ"
        );
    }

    #[test]
    fn encode_key_from_recipe_float_string_nan_infinity_encoding() {
        let recipe = KeyRecipe::new().set_part(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);

        let nan_encoded = encode_key_from_recipe(&recipe, &["NaN".to_value()])
            .expect("NaN string float encoding should succeed");
        let inf_encoded = encode_key_from_recipe(&recipe, &["Infinity".to_value()])
            .expect("Infinity string float encoding should succeed");
        let neg_inf_encoded = encode_key_from_recipe(&recipe, &["-Infinity".to_value()])
            .expect("-Infinity string float encoding should succeed");

        assert_ne!(
            nan_encoded, inf_encoded,
            "NaN and Infinity must encode differently"
        );
        assert_ne!(
            inf_encoded, neg_inf_encoded,
            "Infinity and -Infinity must encode differently"
        );
    }

    #[test]
    fn encode_key_from_recipe_bytes_base64_encoding() {
        let part = Part::new()
            .set_order(Order::Ascending)
            .set_type(Type::default().set_code(TypeCode::Bytes));

        let recipe = KeyRecipe::new().set_part(vec![part]);
        let base64_str = BASE64_STANDARD.encode(b"hello spanner");
        let values = vec![base64_str.to_value()];

        let encoded =
            encode_key_from_recipe(&recipe, &values).expect("base64 bytes encoding should succeed");
        assert!(!encoded.is_empty(), "encoded buffer should not be empty");
    }

    #[test]
    fn encode_key_from_recipe_bool_encoding() {
        let asc_recipe = KeyRecipe::new().set_part(vec![Part::new().set_order(Order::Ascending)]);
        let desc_recipe = KeyRecipe::new().set_part(vec![Part::new().set_order(Order::Descending)]);
        let values = vec![true.to_value()];

        let asc_encoded = encode_key_from_recipe(&asc_recipe, &values)
            .expect("ascending bool encoding should succeed");
        let desc_encoded = encode_key_from_recipe(&desc_recipe, &values)
            .expect("descending bool encoding should succeed");
        assert_ne!(
            asc_encoded, desc_encoded,
            "ascending and descending bool encodings must differ"
        );
    }

    #[test]
    fn encode_key_from_recipe_null_ordering() {
        let first_recipe =
            KeyRecipe::new().set_part(vec![Part::new().set_null_order(NullOrder::NullsFirst)]);
        let last_recipe =
            KeyRecipe::new().set_part(vec![Part::new().set_null_order(NullOrder::NullsLast)]);
        let values = vec![Value::null()];

        let first_encoded = encode_key_from_recipe(&first_recipe, &values)
            .expect("nulls first encoding should succeed");
        let last_encoded = encode_key_from_recipe(&last_recipe, &values)
            .expect("nulls last encoding should succeed");
        assert_ne!(
            first_encoded, last_encoded,
            "nulls first and last encodings must differ"
        );
    }

    #[test]
    fn encode_key_from_recipe_not_enough_values_returns_err() {
        let recipe = KeyRecipe::new().set_part(vec![Part::new().set_order(Order::Ascending)]);
        let error = encode_key_from_recipe(&recipe, &[])
            .expect_err("missing column values should return error");
        assert!(
            error.to_string().contains("Not enough column values"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_not_null_violation_returns_err() {
        let recipe =
            KeyRecipe::new().set_part(vec![Part::new().set_null_order(NullOrder::NotNull)]);
        let values = vec![Value::null()];
        let error = encode_key_from_recipe(&recipe, &values)
            .expect_err("null value for NOT NULL column should return error");
        assert!(
            error
                .to_string()
                .contains("NULL value provided for NOT NULL"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unsupported_value_kind_returns_err() {
        let recipe = KeyRecipe::new().set_part(vec![Part::new().set_order(Order::Ascending)]);
        let values = vec![Value(prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue::default(),
            )),
        })];
        let error = encode_key_from_recipe(&recipe, &values)
            .expect_err("unsupported value kind should return error");
        assert!(
            error.to_string().contains("Unsupported Value kind"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unsupported_type_float32_returns_err() {
        let recipe = KeyRecipe::new().set_part(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Float32)),
        ]);
        let values = vec!["1.0".to_value()];
        let error =
            encode_key_from_recipe(&recipe, &values).expect_err("Float32 should return error");
        assert!(
            error
                .to_string()
                .contains("is not supported for key recipe encoding"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unsupported_type_numeric_returns_err() {
        let recipe = KeyRecipe::new().set_part(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Numeric)),
        ]);
        let values = vec!["100.5".to_value()];
        let error =
            encode_key_from_recipe(&recipe, &values).expect_err("Numeric should return error");
        assert!(
            error
                .to_string()
                .contains("is not supported for key recipe encoding"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_timestamp_string_encoding() {
        let asc_recipe = KeyRecipe::new().set_part(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Timestamp)),
        ]);
        let desc_recipe = KeyRecipe::new().set_part(vec![
            Part::new()
                .set_order(Order::Descending)
                .set_type(Type::default().set_code(TypeCode::Timestamp)),
        ]);
        let values = vec!["2026-08-05T00:00:00Z".to_value()];

        let asc_encoded = encode_key_from_recipe(&asc_recipe, &values)
            .expect("ascending timestamp encoding should succeed");
        let desc_encoded = encode_key_from_recipe(&desc_recipe, &values)
            .expect("descending timestamp encoding should succeed");
        assert_ne!(
            asc_encoded, desc_encoded,
            "ascending and descending timestamp encodings must differ"
        );
    }

    #[test]
    fn encode_key_from_recipe_date_string_encoding() {
        let asc_recipe = KeyRecipe::new().set_part(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Date)),
        ]);
        let desc_recipe = KeyRecipe::new().set_part(vec![
            Part::new()
                .set_order(Order::Descending)
                .set_type(Type::default().set_code(TypeCode::Date)),
        ]);
        let values = vec!["2026-08-05".to_value()];

        let asc_encoded = encode_key_from_recipe(&asc_recipe, &values)
            .expect("ascending date encoding should succeed");
        let desc_encoded = encode_key_from_recipe(&desc_recipe, &values)
            .expect("descending date encoding should succeed");
        assert_ne!(
            asc_encoded, desc_encoded,
            "ascending and descending date encodings must differ"
        );
    }

    #[test]
    fn encode_key_from_recipe_into_scratch_buffer() {
        let recipe = KeyRecipe::new().set_part(vec![
            Part::new().set_tag(1u32),
            Part::new().set_order(Order::Ascending),
        ]);
        let values = vec!["spanner".to_value()];
        let mut buffer = Vec::with_capacity(32);

        encode_key_from_recipe_into(&recipe, &values, &mut buffer)
            .expect("encoding into reusable buffer should succeed");
        assert!(
            !buffer.is_empty(),
            "encoded scratch buffer should not be empty"
        );

        let direct_encoded =
            encode_key_from_recipe(&recipe, &values).expect("direct encoding should succeed");
        assert_eq!(
            buffer, direct_encoded,
            "reusable buffer output must match direct encoding"
        );
    }
}
