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
    if recipe.part.is_empty() {
        return Err(crate::error::internal_error(
            "Invalid KeyRecipe: must have at least one part",
        ));
    }
    if recipe.part[0].tag == 0 {
        return Err(crate::error::internal_error(
            "Invalid KeyRecipe: must start with a table or index tag",
        ));
    }

    let initial_len = buffer.len();
    let mut values_iter = values.iter();

    for part in &recipe.part {
        if part.tag != 0 {
            if let Err(e) = ssformat::append_composite_tag(buffer, part.tag) {
                buffer.truncate(initial_len);
                return Err(e);
            }
            continue;
        }

        let value = match values_iter.next() {
            Some(v) => v,
            None => {
                buffer.truncate(initial_len);
                return Err(crate::error::internal_error(
                    "Not enough column values to encode key recipe: more values required",
                ));
            }
        };

        if let Err(e) = encode_part(buffer, part, value) {
            buffer.truncate(initial_len);
            return Err(e);
        }
    }

    Ok(())
}

/// Verifies that the recipe part's data type is supported by this key recipe encoder.
///
/// According to [Spanner documentation](https://docs.cloud.google.com/spanner/docs/reference/standard-sql/data-types#valid_key_column_types),
/// all data types are valid key column types except for `FLOAT32`, `ARRAY`, `JSON`, and `STRUCT`.
///
/// Currently, this encoder supports types whose wire-format encoding preserves lexicographical
/// sort order without custom binary date/timestamp packing: `BOOL`, `INT64`, `FLOAT64`, `STRING`,
/// and `BYTES`.
///
/// Note: `NUMERIC`, `DATE`, and `TIMESTAMP` are valid Spanner key column types, but are not
/// currently supported here because they require specialized binary storage encodings (`ssformat`)
/// (e.g., days since epoch for `DATE`, 12-byte binary format for `TIMESTAMP`, decimal bit-packing
/// for `NUMERIC`). Unsupported types explicitly return an error so that requests gracefully fall back
/// to default routing rather than emitting invalid shard keys.
fn check_supported_key_type(part: &Part) -> Result<&TypeCode> {
    let part_type = part.r#type.as_ref().ok_or_else(|| {
        crate::error::internal_error("Invalid KeyRecipe part: missing type definition")
    })?;
    match &part_type.code {
        TypeCode::Bool
        | TypeCode::Int64
        | TypeCode::Float64
        | TypeCode::String
        | TypeCode::Bytes => Ok(&part_type.code),
        TypeCode::Unspecified => Err(crate::error::internal_error(
            "Invalid KeyRecipe part: TypeCode::Unspecified is not permitted",
        )),
        // TODO(#6279): Support TypeCode::Date binary ssformat encoding in a separate pull request.
        TypeCode::Date => Err(crate::error::internal_error(
            "TypeCode::Date is not yet supported for key recipe encoding (TODO(#6279): will be added in a separate pull request)",
        )),
        // TODO(#6279): Support TypeCode::Timestamp binary ssformat encoding in a separate pull request.
        TypeCode::Timestamp => Err(crate::error::internal_error(
            "TypeCode::Timestamp is not yet supported for key recipe encoding (TODO(#6279): will be added in a separate pull request)",
        )),
        // TODO(#6279): Support TypeCode::Uuid binary ssformat encoding in a separate pull request.
        TypeCode::Uuid => Err(crate::error::internal_error(
            "TypeCode::Uuid is not yet supported for key recipe encoding (TODO(#6279): will be added in a separate pull request)",
        )),
        // TODO(#6279): Support TypeCode::Enum binary ssformat encoding in a separate pull request.
        TypeCode::Enum => Err(crate::error::internal_error(
            "TypeCode::Enum is not yet supported for key recipe encoding (TODO(#6279): will be added in a separate pull request)",
        )),
        unsupported => Err(crate::error::internal_error(format!(
            "TypeCode {unsupported:?} is not supported for key recipe encoding",
        ))),
    }
}

/// Evaluates a single key column part against a [`Value`] and appends it to `buffer`.
fn encode_part(buffer: &mut Vec<u8>, part: &Part, value: &Value) -> Result<()> {
    let type_code = check_supported_key_type(part)?;

    let decreasing = matches!(part.order, Order::Descending);

    if value.kind() == Kind::Null {
        return encode_null_part(buffer, &part.null_order, decreasing);
    }

    // Emit NOT NULL prefix marker byte before encoding any non-null value on nullable columns.
    encode_not_null_marker(buffer, &part.null_order, decreasing)?;

    match *type_code {
        TypeCode::Bool => encode_bool_part(buffer, value, decreasing),
        TypeCode::Int64 => encode_int64_part(buffer, value, decreasing),
        TypeCode::Float64 => encode_float64_part(buffer, value, decreasing),
        TypeCode::String => encode_string_part(buffer, value, decreasing),
        TypeCode::Bytes => encode_bytes_part(buffer, value, decreasing),
        ref other => Err(crate::error::internal_error(format!(
            "Unsupported TypeCode {other:?} for key recipe encoding",
        ))),
    }
}

/// Appends a NULL value marker according to [`NullOrder`].
fn encode_null_part(buffer: &mut Vec<u8>, null_order: &NullOrder, decreasing: bool) -> Result<()> {
    match null_order {
        NullOrder::NullsFirst | NullOrder::UnknownValue(_) => append_null_ordered(buffer, false),
        NullOrder::NullsLast => append_null_ordered(buffer, true),
        NullOrder::Unspecified => append_null_ordered(buffer, decreasing),
        NullOrder::NotNull => Err(crate::error::internal_error(
            "NULL value provided for NOT NULL key recipe column",
        )),
    }
}

/// Appends a NOT NULL prefix marker byte before non-null values on nullable key columns.
fn encode_not_null_marker(
    buffer: &mut Vec<u8>,
    null_order: &NullOrder,
    decreasing: bool,
) -> Result<()> {
    match null_order {
        NullOrder::NullsFirst | NullOrder::UnknownValue(_) => {
            append_not_null_marker_ordered(buffer, false)
        }
        NullOrder::NullsLast => append_not_null_marker_ordered(buffer, true),
        NullOrder::Unspecified => append_not_null_marker_ordered(buffer, decreasing),
        NullOrder::NotNull => {
            // NOT NULL columns do not emit nullable marker bytes.
            Ok(())
        }
    }
}

/// Evaluates a boolean column value (`BOOL`).
fn encode_bool_part(buffer: &mut Vec<u8>, value: &Value, decreasing: bool) -> Result<()> {
    let boolean_value = value.try_as_bool().ok_or_else(|| {
        crate::error::internal_error("Type mismatch: expected Bool value for BOOL column")
    })?;
    append_bool_ordered(buffer, boolean_value, decreasing)
}

/// Evaluates an integer column value (`INT64`).
fn encode_int64_part(buffer: &mut Vec<u8>, value: &Value, decreasing: bool) -> Result<()> {
    let string_value = value.try_as_string().ok_or_else(|| {
        crate::error::internal_error("Type mismatch: expected String value for INT64 column")
    })?;
    let integer_value = string_value.parse::<i64>().map_err(|e| {
        crate::error::internal_error(format!(
            "Failed to parse Int64 from string '{string_value}': {e}"
        ))
    })?;
    append_int64_ordered(buffer, integer_value, decreasing)
}

/// Evaluates a floating-point column value (`FLOAT64`).
fn encode_float64_part(buffer: &mut Vec<u8>, value: &Value, decreasing: bool) -> Result<()> {
    if let Some(string_value) = value.try_as_string() {
        let num = match string_value {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            _ => {
                return Err(crate::error::internal_error(format!(
                    "Type mismatch: invalid FLOAT64 string '{string_value}' (only NaN and Infinity strings permitted)",
                )));
            }
        };
        return append_double_ordered(buffer, num, decreasing);
    }
    let num = value.try_as_f64().ok_or_else(|| {
        crate::error::internal_error(
            "Type mismatch: expected Number or special String value for FLOAT64 column",
        )
    })?;
    append_double_ordered(buffer, num, decreasing)
}

/// Evaluates a string column value (`STRING`).
fn encode_string_part(buffer: &mut Vec<u8>, value: &Value, decreasing: bool) -> Result<()> {
    let string_value = value.try_as_string().ok_or_else(|| {
        crate::error::internal_error("Type mismatch: expected String value for STRING column")
    })?;
    append_string_ordered(buffer, string_value, decreasing)
}

/// Evaluates a byte array column value (`BYTES`).
fn encode_bytes_part(buffer: &mut Vec<u8>, value: &Value, decreasing: bool) -> Result<()> {
    let string_value = value.try_as_string().ok_or_else(|| {
        crate::error::internal_error("Type mismatch: expected base64 String value for BYTES column")
    })?;
    let mut stack_buffer = [0u8; 512];
    if let Ok(len) = BASE64_STANDARD.decode_slice(string_value, &mut stack_buffer) {
        return append_bytes_ordered(buffer, &stack_buffer[..len], decreasing);
    }
    let bytes_value = BASE64_STANDARD.decode(string_value).map_err(|e| {
        crate::error::internal_error(format!(
            "Failed to decode base64 Bytes from string '{string_value}': {e}"
        ))
    })?;
    append_bytes_ordered(buffer, &bytes_value, decreasing)
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

#[inline]
fn append_null_ordered(buffer: &mut Vec<u8>, nulls_last: bool) -> Result<()> {
    if nulls_last {
        ssformat::append_null_ordered_last(buffer);
    } else {
        ssformat::append_null_ordered_first(buffer);
    }
    Ok(())
}

#[inline]
fn append_not_null_marker_ordered(buffer: &mut Vec<u8>, nulls_last: bool) -> Result<()> {
    if nulls_last {
        ssformat::append_not_null_marker_null_ordered_last(buffer);
    } else {
        ssformat::append_not_null_marker_null_ordered_first(buffer);
    }
    Ok(())
}

#[cfg(test)]
mod golden_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Type;
    use crate::model::key_recipe::Part;
    use crate::value::ToValue;
    use prost_types::{ListValue, Value as ProstValue, value::Kind as ProstValueKind};

    fn sample_recipe(parts: Vec<Part>) -> KeyRecipe {
        let mut all_parts = vec![Part::new().set_tag(1u32)];
        all_parts.extend(parts);
        KeyRecipe::new().set_part(all_parts)
    }

    fn string_part(order: Order) -> Part {
        Part::new()
            .set_order(order)
            .set_type(Type::default().set_code(TypeCode::String))
    }

    #[test]
    fn encode_key_from_recipe_invalid_recipe_returns_err() {
        let empty_recipe = KeyRecipe::new();
        let error = encode_key_from_recipe(&empty_recipe, &[])
            .expect_err("empty recipe should return error");
        assert!(
            error.to_string().contains("must have at least one part"),
            "unexpected error message: {error}"
        );

        let no_tag_recipe = KeyRecipe::new().set_part(vec![string_part(Order::Ascending)]);
        let error2 = encode_key_from_recipe(&no_tag_recipe, &["alpha".to_value()])
            .expect_err("recipe without leading tag should return error");
        assert!(
            error2
                .to_string()
                .contains("must start with a table or index tag"),
            "unexpected error message: {error2}"
        );
    }

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
            .expect_err("exceeding K_MAX_FIELD_TAG should return error");
        assert!(
            error.to_string().contains("Invalid tag value"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_string_ascending_and_descending() {
        let asc_recipe = sample_recipe(vec![string_part(Order::Ascending)]);
        let desc_recipe = sample_recipe(vec![string_part(Order::Descending)]);
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

        let recipe = sample_recipe(vec![part]);
        let values = vec!["12345".to_value()];

        let encoded =
            encode_key_from_recipe(&recipe, &values).expect("int64 string encoding should succeed");
        assert!(!encoded.is_empty(), "encoded buffer should not be empty");
    }

    #[test]
    fn encode_key_from_recipe_float_number_encoding() {
        let asc_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);
        let desc_recipe = sample_recipe(vec![
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
        let recipe = sample_recipe(vec![
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
    fn encode_key_from_recipe_bytes_base64_fast_and_slow_paths() {
        let part = Part::new()
            .set_order(Order::Ascending)
            .set_type(Type::default().set_code(TypeCode::Bytes));
        let recipe = sample_recipe(vec![part]);

        // 1. Fast path (<= 512 bytes: stack-allocated buffer decoding)
        let fast_base64 = BASE64_STANDARD.encode(b"hello spanner fast path");
        let fast_encoded = encode_key_from_recipe(&recipe, &[fast_base64.to_value()])
            .expect("fast path base64 bytes encoding should succeed");
        assert!(
            !fast_encoded.is_empty(),
            "fast path encoded buffer should not be empty"
        );

        // 2. Slow path (> 512 bytes: heap fallback decoding)
        let large_payload = vec![0xAB_u8; 1024];
        let slow_base64 = BASE64_STANDARD.encode(&large_payload);
        let slow_encoded = encode_key_from_recipe(&recipe, &[slow_base64.to_value()])
            .expect("slow path base64 bytes encoding should succeed");
        assert!(
            slow_encoded.len() > fast_encoded.len(),
            "slow path encoded buffer should be larger"
        );

        // 3. Error path (invalid base64 string should fail cleanly)
        let invalid_err = encode_key_from_recipe(&recipe, &["!!!not-base64!!!".to_value()])
            .expect_err("invalid base64 string should return error");
        assert!(
            invalid_err
                .to_string()
                .contains("Failed to decode base64 Bytes"),
            "unexpected error message: {invalid_err}"
        );
    }

    #[test]
    fn encode_key_from_recipe_strict_type_validation_returns_err() {
        // 1. INT64 column must reject non-string values (e.g. Bool or Number)
        let int64_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Int64)),
        ]);
        let err1 = encode_key_from_recipe(&int64_recipe, &[true.to_value()])
            .expect_err("Bool value for INT64 column should return error");
        assert!(
            err1.to_string()
                .contains("Type mismatch: expected String value for INT64 column"),
            "unexpected error message: {err1}"
        );

        // 2. FLOAT64 column must reject regular number strings ("123.45")
        let float_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);
        let err2 = encode_key_from_recipe(&float_recipe, &["123.45".to_value()])
            .expect_err("regular number string for FLOAT64 column should return error");
        assert!(
            err2.to_string()
                .contains("only NaN and Infinity strings permitted"),
            "unexpected error message: {err2}"
        );

        // 3. BOOL column must reject non-bool values
        let bool_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Bool)),
        ]);
        let err3 = encode_key_from_recipe(&bool_recipe, &["true".to_value()])
            .expect_err("String value for BOOL column should return error");
        assert!(
            err3.to_string()
                .contains("Type mismatch: expected Bool value for BOOL column"),
            "unexpected error message: {err3}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unspecified_or_missing_type_returns_err() {
        // 1. TypeCode::Unspecified must be rejected
        let unspecified_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Unspecified)),
        ]);
        let err1 = encode_key_from_recipe(&unspecified_recipe, &["alpha".to_value()])
            .expect_err("TypeCode::Unspecified should return error");
        assert!(
            err1.to_string()
                .contains("TypeCode::Unspecified is not permitted"),
            "unexpected error message: {err1}"
        );

        // 2. Missing type definition (None) must be rejected
        let missing_type_recipe = sample_recipe(vec![Part::new().set_order(Order::Ascending)]);
        let err2 = encode_key_from_recipe(&missing_type_recipe, &["alpha".to_value()])
            .expect_err("missing type definition should return error");
        assert!(
            err2.to_string().contains("missing type definition"),
            "unexpected error message: {err2}"
        );
    }

    #[test]
    fn encode_key_from_recipe_bool_encoding() {
        let asc_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Bool)),
        ]);
        let desc_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Descending)
                .set_type(Type::default().set_code(TypeCode::Bool)),
        ]);
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
        let first_recipe = sample_recipe(vec![
            Part::new()
                .set_null_order(NullOrder::NullsFirst)
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let last_recipe = sample_recipe(vec![
            Part::new()
                .set_null_order(NullOrder::NullsLast)
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
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
        let recipe = sample_recipe(vec![string_part(Order::Ascending)]);
        let error = encode_key_from_recipe(&recipe, &[])
            .expect_err("missing column values should return error");
        assert!(
            error.to_string().contains("Not enough column values"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_not_null_violation_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
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
        let recipe = sample_recipe(vec![string_part(Order::Ascending)]);
        let values = vec![Value(ProstValue {
            kind: Some(ProstValueKind::ListValue(ListValue::default())),
        })];
        let error = encode_key_from_recipe(&recipe, &values)
            .expect_err("unsupported value kind should return error");
        assert!(
            error
                .to_string()
                .contains("Type mismatch: expected String value"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unsupported_type_float32_returns_err() {
        let recipe = sample_recipe(vec![
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
        let recipe = sample_recipe(vec![
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
    fn encode_key_from_recipe_unsupported_type_timestamp_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Timestamp)),
        ]);
        let values = vec!["2026-08-05T00:00:00Z".to_value()];
        let error = encode_key_from_recipe(&recipe, &values)
            .expect_err("Timestamp should return error until binary ssformat is implemented");
        assert!(
            error
                .to_string()
                .contains("is not yet supported for key recipe encoding"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unsupported_type_date_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Date)),
        ]);
        let values = vec!["2026-08-05".to_value()];
        let error = encode_key_from_recipe(&recipe, &values)
            .expect_err("Date should return error until binary ssformat is implemented");
        assert!(
            error
                .to_string()
                .contains("is not yet supported for key recipe encoding"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unspecified_null_order_defaults_correctly() {
        let asc_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::Unspecified)
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let desc_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Descending)
                .set_null_order(NullOrder::Unspecified)
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let values = vec![Value::null()];

        let asc_encoded =
            encode_key_from_recipe(&asc_recipe, &values).expect("asc null encoding should succeed");
        let desc_encoded = encode_key_from_recipe(&desc_recipe, &values)
            .expect("desc null encoding should succeed");
        assert_ne!(
            asc_encoded, desc_encoded,
            "ascending and descending unspecified null orders must differ"
        );
    }

    #[test]
    fn encode_key_from_recipe_into_scratch_buffer() {
        let recipe = sample_recipe(vec![string_part(Order::Ascending)]);
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

    #[test]
    fn encode_key_from_recipe_unsupported_type_uuid_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Uuid)),
        ]);
        let values = vec!["123e4567-e89b-12d3-a456-426614174000".to_value()];
        let error = encode_key_from_recipe(&recipe, &values)
            .expect_err("Uuid should return error until binary ssformat is implemented");
        assert!(
            error
                .to_string()
                .contains("is not yet supported for key recipe encoding"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unsupported_type_enum_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Enum)),
        ]);
        let values = vec!["VALUE_1".to_value()];
        let error = encode_key_from_recipe(&recipe, &values)
            .expect_err("Enum should return error until binary ssformat is implemented");
        assert!(
            error
                .to_string()
                .contains("is not yet supported for key recipe encoding"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unsupported_type_code_other_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Array)),
        ]);
        let values = vec!["[]".to_value()];
        let error = encode_key_from_recipe(&recipe, &values)
            .expect_err("Array should return error as unsupported type");
        assert!(
            error
                .to_string()
                .contains("is not supported for key recipe encoding"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_invalid_int64_string_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Int64)),
        ]);
        let values = vec!["not_an_int".to_value()];
        let error = encode_key_from_recipe(&recipe, &values)
            .expect_err("non-numeric int64 string should return error");
        assert!(
            error
                .to_string()
                .contains("Failed to parse Int64 from string"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unknown_null_order() {
        let recipe_null = sample_recipe(vec![
            Part::new()
                .set_null_order(NullOrder::from(99))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let null_encoded = encode_key_from_recipe(&recipe_null, &[Value::null()])
            .expect("UnknownValue null order should succeed for null");
        assert!(
            !null_encoded.is_empty(),
            "null encoded buffer should not be empty"
        );

        let value_encoded = encode_key_from_recipe(&recipe_null, &["spanner".to_value()])
            .expect("UnknownValue null order should succeed for non-null value");
        assert!(
            value_encoded.len() > null_encoded.len(),
            "non-null value should include prefix marker byte and content"
        );
    }

    #[test]
    fn encode_key_from_recipe_type_mismatches_all_columns_returns_err() {
        // 1. STRING column rejects bool value
        let string_recipe = sample_recipe(vec![string_part(Order::Ascending)]);
        let err1 = encode_key_from_recipe(&string_recipe, &[true.to_value()])
            .expect_err("Bool value for STRING column should fail");
        assert!(
            err1.to_string()
                .contains("Type mismatch: expected String value for STRING column"),
            "unexpected error message: {err1}"
        );

        // 2. BYTES column rejects bool value
        let bytes_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Bytes)),
        ]);
        let err2 = encode_key_from_recipe(&bytes_recipe, &[true.to_value()])
            .expect_err("Bool value for BYTES column should fail");
        assert!(
            err2.to_string()
                .contains("Type mismatch: expected base64 String value for BYTES column"),
            "unexpected error message: {err2}"
        );

        // 3. FLOAT64 column rejects bool value (neither String nor f64 number)
        let float_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);
        let err3 = encode_key_from_recipe(&float_recipe, &[true.to_value()])
            .expect_err("Bool value for FLOAT64 column should fail");
        assert!(
            err3.to_string().contains(
                "Type mismatch: expected Number or special String value for FLOAT64 column"
            ),
            "unexpected error message: {err3}"
        );
    }

    #[test]
    fn encode_key_from_recipe_into_truncates_buffer_on_error() {
        let recipe = sample_recipe(vec![
            string_part(Order::Ascending),
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Date)),
        ]);
        let values = vec!["spanner".to_value(), "2026-08-05".to_value()];
        let mut buffer = b"existing_prefix".to_vec();
        let initial_len = buffer.len();

        let err = encode_key_from_recipe_into(&recipe, &values, &mut buffer)
            .expect_err("second column Date should fail");
        assert_eq!(
            buffer.len(),
            initial_len,
            "buffer must be truncated back to its initial length on error: {err}"
        );
        assert_eq!(
            &buffer[..],
            b"existing_prefix",
            "existing buffer contents must remain untouched"
        );
    }
}
