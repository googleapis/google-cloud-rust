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
use crate::error::internal_error;
use crate::model::key_recipe::Part;
use crate::model::key_recipe::part::{NullOrder, Order};
use crate::model::{KeyRecipe, TypeCode};
use crate::routing::{ssformat, temporal, uuid};
use crate::value::{Kind, Value};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use std::collections::BTreeMap;

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
        return Err(internal_error(
            "Invalid KeyRecipe: must have at least one part",
        ));
    }
    if recipe.part[0].tag == 0 {
        return Err(internal_error(
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

        if part.random() == Some(&true) {
            let decreasing = matches!(part.order, Order::Descending);
            if let Err(e) = encode_random_part(buffer, part, decreasing) {
                buffer.truncate(initial_len);
                return Err(e);
            }
            continue;
        }

        if let Some(constant_val) = part.value() {
            let resolved_value =
                match resolve_struct_field_json(constant_val, &part.struct_identifiers) {
                    Ok(v) => v,
                    Err(e) => {
                        buffer.truncate(initial_len);
                        return Err(e);
                    }
                };
            if let Err(e) = encode_json_part(buffer, part, resolved_value) {
                buffer.truncate(initial_len);
                return Err(e);
            }
            continue;
        }

        let value = match values_iter.next() {
            Some(v) => v,
            None => {
                buffer.truncate(initial_len);
                return Err(internal_error(
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

/// Encodes a Spanner routing key (`Vec<u8>`) from a SQL [`KeyRecipe`] and query parameters.
///
/// Evaluates each part of the recipe in order:
/// - If `part.tag != 0`, the tag number is appended to the binary key.
/// - If `part.tag == 0`:
///   - If `part.random()` is `Some(&true)`, generates a pseudo-random positive 63-bit integer and encodes it.
///   - If `part.identifier()` is present, resolves the parameter by name (case-insensitively).
///     If `part.struct_identifiers` is present, traverses nested struct `ListValue` elements.
///
/// # Caller Fallback Contract
/// If encoding returns an error (for example, due to a missing parameter or unsupported type),
/// callers (`LocationRouter` / `DatabaseClient`) MUST catch the error and silently fall back to
/// default routing rather than failing the user's RPC.
pub(crate) fn encode_key_from_query_params(
    recipe: &KeyRecipe,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<u8>> {
    let mut buffer = Vec::with_capacity(recipe.part.len().saturating_mul(16));
    encode_key_from_query_params_into(recipe, params, &mut buffer)?;
    Ok(buffer)
}

/// Encodes a Spanner routing key from a SQL [`KeyRecipe`] and query parameters directly into
/// an existing output buffer.
///
/// This avoids new heap allocations when callers reuse a scratch buffer across RPCs on hot paths.
///
/// ### Recipe Evaluation Flow:
/// A [`KeyRecipe`] for a SQL query consists of:
/// 1. Table or index composite tags (`part.tag != 0`), which identify the table/index partition namespace.
/// 2. Column value parts (`part.tag == 0`), which resolve values from SQL query parameters.
///
/// # Caller Fallback Contract
/// If encoding returns an error (for example, due to a missing parameter or unsupported type),
/// callers (`LocationRouter` / `DatabaseClient`) MUST catch the error and silently fall back to
/// default routing rather than failing the user's RPC.
pub(crate) fn encode_key_from_query_params_into(
    recipe: &KeyRecipe,
    params: &BTreeMap<String, Value>,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    if recipe.part.is_empty() {
        return Err(internal_error(
            "Invalid KeyRecipe: must have at least one part",
        ));
    }
    if recipe.part[0].tag == 0 {
        return Err(internal_error(
            "Invalid KeyRecipe: must start with a table or index tag",
        ));
    }

    let initial_len = buffer.len();

    for part in &recipe.part {
        // In Spanner's KeyRecipe proto definition, `tag` is a u32:
        // - Non-zero tag (> 0): composite tag identifying the table or index prefix namespace.
        // - Zero tag (== 0): key column value to be extracted from query parameters and encoded.
        if part.tag != 0 {
            if let Err(e) = ssformat::append_composite_tag(buffer, part.tag) {
                buffer.truncate(initial_len);
                return Err(e);
            }
            continue;
        }

        if let Err(e) = encode_query_part(buffer, part, params) {
            buffer.truncate(initial_len);
            return Err(e);
        }
    }

    Ok(())
}

/// Evaluates a single query recipe part against query parameters and appends it to `buffer`.
fn encode_query_part(
    buffer: &mut Vec<u8>,
    part: &Part,
    params: &BTreeMap<String, Value>,
) -> Result<()> {
    let decreasing = matches!(part.order, Order::Descending);

    // 1. Random partition root tag:
    // In Spanner tables/queries with distributed random root sharding, the client router
    // generates a pseudo-random positive 63-bit integer to pick a candidate root tablet.
    if part.random() == Some(&true) {
        return encode_random_part(buffer, part, decreasing);
    }

    // 2. Constant literal value:
    // Some recipes embed hardcoded literal constants directly in the schema definition.
    // Borrows directly from &serde_json::Value with zero allocations and full 64-bit integer precision.
    if let Some(constant_val) = part.value() {
        let resolved_value = resolve_struct_field_json(constant_val, &part.struct_identifiers)?;
        return encode_json_part(buffer, part, resolved_value);
    }

    // 3. Resolve root parameter by identifier (case-insensitive lookup matching Spanner SQL semantics):
    let identifier = part
        .identifier()
        .ok_or_else(|| internal_error("Invalid KeyRecipe part: missing parameter identifier"))?;

    let param_value = lookup_query_param(params, identifier).ok_or_else(|| {
        internal_error(format!(
            "Missing query parameter '{identifier}' required by key recipe"
        ))
    })?;

    // 4. Drill down into nested struct fields if struct_identifiers is present:
    let resolved_value = resolve_struct_field(param_value, &part.struct_identifiers)?;

    // 5. Encode the resolved column value (handling sort order, null ordering, and type serialization):
    encode_part(buffer, part, resolved_value)
}

/// Drills down into nested struct fields within a constant JSON value without cloning.
///
/// In Cloud Spanner:
/// - When a query recipe part targets a field inside a struct (e.g. `WHERE (id, role) = (1, 'ADMIN')`),
///   the recipe provides a `struct_identifiers` index path (e.g., `[0]` for `id` and `[1]` for `role`).
/// - In JSON wire format, Spanner represents `STRUCT` data as positional JSON arrays (`[1, "ADMIN"]`).
/// - This helper walks the `struct_identifiers` index path positionally into the nested JSON array,
///   returning a borrowed reference to the target leaf column value without copying or allocating.
///
/// If `struct_identifiers` is empty, `param_value` is returned unchanged.
fn resolve_struct_field_json<'a>(
    param_value: &'a serde_json::Value,
    struct_identifiers: &[i32],
) -> Result<&'a serde_json::Value> {
    let mut current = param_value;
    for &struct_index in struct_identifiers {
        if struct_index < 0 {
            return Err(internal_error(format!(
                "Invalid negative struct index {struct_index} in key recipe part"
            )));
        }
        let array = current.as_array().ok_or_else(|| {
            internal_error("Expected Struct array for struct parameter traversal")
        })?;
        current = array.get(struct_index as usize).ok_or_else(|| {
            internal_error(format!(
                "Struct field index {struct_index} out of bounds (len {})",
                array.len()
            ))
        })?;
    }
    Ok(current)
}

/// Evaluates a pseudo-random positive 63-bit integer for random root sharded tables/queries.
fn encode_random_part(buffer: &mut Vec<u8>, part: &Part, decreasing: bool) -> Result<()> {
    let type_code = check_supported_key_type(part)?;
    if !matches!(type_code, TypeCode::Int64) {
        return Err(internal_error(
            "Random key recipe part must have TypeCode::Int64",
        ));
    }
    let random_value = rand::random::<i64>() & i64::MAX;
    append_int64_ordered(buffer, random_value, decreasing)
}

/// Evaluates a constant JSON value directly against the recipe part without heap allocations.
fn encode_json_part(buffer: &mut Vec<u8>, part: &Part, value: &serde_json::Value) -> Result<()> {
    let type_code = check_supported_key_type(part)?;
    let decreasing = matches!(part.order, Order::Descending);

    if value.is_null() {
        return encode_null_part(buffer, &part.null_order, decreasing);
    }

    encode_not_null_marker(buffer, &part.null_order, decreasing)?;

    match *type_code {
        TypeCode::Bool => encode_json_bool_part(buffer, value, decreasing),
        TypeCode::Int64 => encode_json_int64_part(buffer, value, decreasing),
        TypeCode::Float64 => encode_json_float64_part(buffer, value, decreasing),
        TypeCode::String => encode_json_string_part(buffer, value, decreasing),
        TypeCode::Bytes => encode_json_bytes_part(buffer, value, decreasing),
        ref other => Err(internal_error(format!(
            "Unsupported TypeCode {other:?} for key recipe encoding",
        ))),
    }
}

/// Evaluates a boolean constant JSON value (`BOOL`).
fn encode_json_bool_part(
    buffer: &mut Vec<u8>,
    value: &serde_json::Value,
    decreasing: bool,
) -> Result<()> {
    let boolean_value = value
        .as_bool()
        .ok_or_else(|| internal_error("Type mismatch: expected Bool value for BOOL column"))?;
    append_bool_ordered(buffer, boolean_value, decreasing)
}

/// Evaluates an integer constant JSON value (`INT64`).
fn encode_json_int64_part(
    buffer: &mut Vec<u8>,
    value: &serde_json::Value,
    decreasing: bool,
) -> Result<()> {
    let integer_value = if let Some(string_value) = value.as_str() {
        string_value.parse::<i64>().map_err(|error| {
            internal_error(format!(
                "Failed to parse Int64 from string '{string_value}': {error}"
            ))
        })?
    } else if let Some(integer_number) = value.as_i64() {
        integer_number
    } else {
        return Err(internal_error(
            "Type mismatch: expected String or Integer value for INT64 column",
        ));
    };
    append_int64_ordered(buffer, integer_value, decreasing)
}

/// Evaluates a floating-point constant JSON value (`FLOAT64`).
fn encode_json_float64_part(
    buffer: &mut Vec<u8>,
    value: &serde_json::Value,
    decreasing: bool,
) -> Result<()> {
    let float_value = if let Some(string_value) = value.as_str() {
        match string_value {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            _ => {
                return Err(internal_error(format!(
                    "Type mismatch: invalid FLOAT64 string '{string_value}' (only NaN and Infinity strings permitted)"
                )));
            }
        }
    } else if let Some(number_value) = value.as_f64() {
        number_value
    } else {
        return Err(internal_error(
            "Type mismatch: expected Number or special String value for FLOAT64 column",
        ));
    };
    append_double_ordered(buffer, float_value, decreasing)
}

/// Evaluates a string constant JSON value (`STRING`).
fn encode_json_string_part(
    buffer: &mut Vec<u8>,
    value: &serde_json::Value,
    decreasing: bool,
) -> Result<()> {
    let string_value = value
        .as_str()
        .ok_or_else(|| internal_error("Type mismatch: expected String value for STRING column"))?;
    append_string_ordered(buffer, string_value, decreasing)
}

/// Evaluates a byte array constant JSON value (`BYTES`).
fn encode_json_bytes_part(
    buffer: &mut Vec<u8>,
    value: &serde_json::Value,
    decreasing: bool,
) -> Result<()> {
    let string_value = value.as_str().ok_or_else(|| {
        internal_error("Type mismatch: expected base64 String value for BYTES column")
    })?;
    let mut stack_buffer = [0u8; 512];
    if let Ok(length) = BASE64_STANDARD.decode_slice(string_value, &mut stack_buffer) {
        return append_bytes_ordered(buffer, &stack_buffer[..length], decreasing);
    }
    let bytes_value = BASE64_STANDARD.decode(string_value).map_err(|error| {
        internal_error(format!(
            "Failed to decode base64 Bytes from string '{string_value}': {error}"
        ))
    })?;
    append_bytes_ordered(buffer, &bytes_value, decreasing)
}

/// Drills down into nested struct parameter fields according to `struct_identifiers`.
///
/// In Cloud Spanner:
/// - When a query parameter is a `STRUCT` (such as `@user` in `WHERE (id, email) = (@user.id, @user.email)`),
///   its value is transmitted on the wire as a [`ListValue`](crate::value::List) where each element
///   represents a struct field by position.
/// - The recipe's `struct_identifiers` contains the sequence of 0-based field indices needed to reach
///   the target leaf column value (e.g., `[1, 0]` for `@user.address.city`).
///
/// If `struct_identifiers` is empty, `param_value` is returned unchanged.
fn resolve_struct_field<'a>(
    param_value: &'a Value,
    struct_identifiers: &[i32],
) -> Result<&'a Value> {
    let mut current = param_value;
    for &struct_index in struct_identifiers {
        if struct_index < 0 {
            return Err(internal_error(format!(
                "Invalid negative struct index {struct_index} in key recipe part"
            )));
        }
        let list = current.try_as_list().ok_or_else(|| {
            internal_error("Expected Struct ListValue for struct parameter traversal")
        })?;
        current = list.get(struct_index as usize).ok_or_else(|| {
            internal_error(format!(
                "Struct field index {struct_index} out of bounds (len {})",
                list.len()
            ))
        })?;
    }
    Ok(current)
}

/// Looks up a query parameter by name in `params`, supporting case-insensitive lookup.
fn lookup_query_param<'a>(
    params: &'a BTreeMap<String, Value>,
    identifier: &str,
) -> Option<&'a Value> {
    params.get(identifier).or_else(|| {
        params
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(identifier))
            .map(|(_, value)| value)
    })
}

/// Verifies that the recipe part's data type is supported by this key recipe encoder.
///
/// According to [Spanner documentation](https://docs.cloud.google.com/spanner/docs/reference/standard-sql/data-types#valid_key_column_types),
/// all data types are valid key column types except for `FLOAT32`, `ARRAY`, `JSON`, and `STRUCT`.
///
/// Supported types include `BOOL`, `INT64`, `FLOAT64`, `STRING`, `BYTES`, `DATE`, `TIMESTAMP`,
/// `UUID`, and `ENUM`.
///
/// Note: `NUMERIC` is not supported for Storage Specification key encoding.
/// Unsupported types explicitly return an error so that requests gracefully fall back
/// to default routing rather than emitting invalid shard keys.
fn check_supported_key_type(part: &Part) -> Result<&TypeCode> {
    let part_type = part
        .r#type
        .as_ref()
        .ok_or_else(|| internal_error("Invalid KeyRecipe part: missing type definition"))?;
    match &part_type.code {
        TypeCode::Bool
        | TypeCode::Int64
        | TypeCode::Float64
        | TypeCode::String
        | TypeCode::Bytes
        | TypeCode::Date
        | TypeCode::Timestamp
        | TypeCode::Uuid
        | TypeCode::Enum => Ok(&part_type.code),
        TypeCode::Unspecified => Err(internal_error(
            "Invalid KeyRecipe part: TypeCode::Unspecified is not permitted",
        )),
        unsupported => Err(internal_error(format!(
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
        TypeCode::Int64 | TypeCode::Enum => encode_int64_part(buffer, value, decreasing),
        TypeCode::Float64 => encode_float64_part(buffer, value, decreasing),
        TypeCode::String => encode_string_part(buffer, value, decreasing),
        TypeCode::Bytes => encode_bytes_part(buffer, value, decreasing),
        TypeCode::Date => temporal::encode_date_part(buffer, value, decreasing),
        TypeCode::Timestamp => temporal::encode_timestamp_part(buffer, value, decreasing),
        TypeCode::Uuid => uuid::encode_uuid_part(buffer, value, decreasing),
        ref other => Err(internal_error(format!(
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
        NullOrder::NotNull => Err(internal_error(
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
    let boolean_value = value
        .try_as_bool()
        .ok_or_else(|| internal_error("Type mismatch: expected Bool value for BOOL column"))?;
    append_bool_ordered(buffer, boolean_value, decreasing)
}

/// Evaluates an integer or enum column value (`INT64` or `ENUM`).
fn encode_int64_part(buffer: &mut Vec<u8>, value: &Value, decreasing: bool) -> Result<()> {
    let string_value = value.try_as_string().ok_or_else(|| {
        internal_error("Type mismatch: expected String value for INT64 or ENUM column")
    })?;
    let integer_value = string_value.parse::<i64>().map_err(|e| {
        internal_error(format!(
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
                return Err(internal_error(format!(
                    "Type mismatch: invalid FLOAT64 string '{string_value}' (only NaN and Infinity strings permitted)",
                )));
            }
        };
        return append_double_ordered(buffer, num, decreasing);
    }
    let num = value.try_as_f64().ok_or_else(|| {
        internal_error("Type mismatch: expected Number or special String value for FLOAT64 column")
    })?;
    append_double_ordered(buffer, num, decreasing)
}

/// Evaluates a string column value (`STRING`).
fn encode_string_part(buffer: &mut Vec<u8>, value: &Value, decreasing: bool) -> Result<()> {
    let string_value = value
        .try_as_string()
        .ok_or_else(|| internal_error("Type mismatch: expected String value for STRING column"))?;
    append_string_ordered(buffer, string_value, decreasing)
}

/// Evaluates a byte array column value (`BYTES`).
fn encode_bytes_part(buffer: &mut Vec<u8>, value: &Value, decreasing: bool) -> Result<()> {
    let string_value = value.try_as_string().ok_or_else(|| {
        internal_error("Type mismatch: expected base64 String value for BYTES column")
    })?;
    let mut stack_buffer = [0u8; 512];
    if let Ok(len) = BASE64_STANDARD.decode_slice(string_value, &mut stack_buffer) {
        return append_bytes_ordered(buffer, &stack_buffer[..len], decreasing);
    }
    let bytes_value = BASE64_STANDARD.decode(string_value).map_err(|e| {
        internal_error(format!(
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
                .contains("Type mismatch: expected String value for INT64 or ENUM column"),
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
    fn encode_key_from_recipe_timestamp_encoding() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::Timestamp)),
        ]);
        let values = vec!["1970-01-01T00:00:00Z".to_value()];
        let encoded = encode_key_from_recipe(&recipe, &values)
            .expect("Timestamp key encoding should succeed");
        let mut expected = Vec::new();
        ssformat::append_composite_tag(&mut expected, 1).expect("tag 1");
        temporal::encode_timestamp_part(&mut expected, &values[0], false)
            .expect("timestamp part encoding");
        assert_eq!(encoded, expected, "Timestamp key encoding mismatch");
    }

    #[test]
    fn encode_key_from_recipe_date_encoding() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::Date)),
        ]);
        let values = vec!["1970-01-01".to_value()];
        let encoded =
            encode_key_from_recipe(&recipe, &values).expect("Date key encoding should succeed");
        let mut expected = Vec::new();
        ssformat::append_composite_tag(&mut expected, 1).expect("tag 1");
        temporal::encode_date_part(&mut expected, &values[0], false).expect("date part encoding");
        assert_eq!(encoded, expected, "Date key encoding mismatch");
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
    fn encode_key_from_recipe_uuid_encoding() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::Uuid)),
        ]);
        let values = vec!["01234567-89ab-cdef-0123-456789abcdef".to_value()];
        let encoded =
            encode_key_from_recipe(&recipe, &values).expect("UUID key encoding should succeed");
        let mut expected = Vec::new();
        ssformat::append_composite_tag(&mut expected, 1).expect("tag 1");
        uuid::encode_uuid_part(&mut expected, &values[0], false).expect("uuid part encoding");
        assert_eq!(encoded, expected, "UUID key encoding mismatch");
    }

    #[test]
    fn encode_key_from_recipe_enum_encoding() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::Enum)),
        ]);
        let values = vec!["42".to_value()];
        let encoded =
            encode_key_from_recipe(&recipe, &values).expect("Enum key encoding should succeed");
        let mut expected = Vec::new();
        ssformat::append_composite_tag(&mut expected, 1).expect("tag 1");
        ssformat::append_int64_increasing(&mut expected, 42);
        assert_eq!(encoded, expected, "Enum key encoding mismatch");
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

        // 4. INT64 column rejects bool value
        let int64_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Int64)),
        ]);
        let err4 = encode_key_from_recipe(&int64_recipe, &[true.to_value()])
            .expect_err("Bool value for INT64 column should fail");
        assert!(
            err4.to_string()
                .contains("Type mismatch: expected String value for INT64 or ENUM column"),
            "unexpected error message: {err4}"
        );

        // 5. DATE column rejects bool value
        let date_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Date)),
        ]);
        let err5 = encode_key_from_recipe(&date_recipe, &[true.to_value()])
            .expect_err("Bool value for DATE column should fail");
        assert!(
            err5.to_string()
                .contains("Type mismatch: expected ISO 8601 String value for DATE column"),
            "unexpected error message: {err5}"
        );

        // 6. TIMESTAMP column rejects bool value
        let ts_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Timestamp)),
        ]);
        let err6 = encode_key_from_recipe(&ts_recipe, &[true.to_value()])
            .expect_err("Bool value for TIMESTAMP column should fail");
        assert!(
            err6.to_string()
                .contains("Type mismatch: expected RFC 3339 String value for TIMESTAMP column"),
            "unexpected error message: {err6}"
        );

        // 7. UUID column rejects bool value
        let uuid_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Uuid)),
        ]);
        let err7 = encode_key_from_recipe(&uuid_recipe, &[true.to_value()])
            .expect_err("Bool value for UUID column should fail");
        assert!(
            err7.to_string()
                .contains("Type mismatch: expected String value for UUID column"),
            "unexpected error message: {err7}"
        );

        // 8. ENUM column rejects bool value
        let enum_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Enum)),
        ]);
        let err8 = encode_key_from_recipe(&enum_recipe, &[true.to_value()])
            .expect_err("Bool value for ENUM column should fail");
        assert!(
            err8.to_string()
                .contains("Type mismatch: expected String value for INT64 or ENUM column"),
            "unexpected error message: {err8}"
        );
    }

    #[test]
    fn encode_key_from_recipe_into_truncates_buffer_on_error() {
        let recipe = sample_recipe(vec![
            string_part(Order::Ascending),
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Array)),
        ]);
        let values = vec!["spanner".to_value(), "invalid".to_value()];
        let mut buffer = b"existing_prefix".to_vec();
        let initial_len = buffer.len();

        let err = encode_key_from_recipe_into(&recipe, &values, &mut buffer)
            .expect_err("second column Array should fail");
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

    #[test]
    fn encode_key_from_query_params_simple_parameters() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NullsFirst)
                .set_identifier("p1")
                .set_type(Type::default().set_code(TypeCode::String)),
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NullsFirst)
                .set_identifier("p0")
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let mut params = BTreeMap::new();
        params.insert("p0".to_string(), "foo".to_value());
        params.insert("p1".to_string(), "bar".to_value());

        let encoded = encode_key_from_query_params(&recipe, &params)
            .expect("query parameter encoding should succeed");
        assert!(
            !encoded.is_empty(),
            "encoded query parameter buffer must not be empty"
        );

        let direct_encoded = encode_key_from_recipe(&recipe, &["bar".to_value(), "foo".to_value()])
            .expect("direct recipe encoding should succeed");
        assert_eq!(
            encoded, direct_encoded,
            "query parameter encoding must match direct recipe encoding"
        );
    }

    #[test]
    fn encode_key_from_query_params_case_insensitive() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_identifier("userId")
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let mut params = BTreeMap::new();
        params.insert("USERID".to_string(), "user_12345".to_value());

        let encoded = encode_key_from_query_params(&recipe, &params)
            .expect("case-insensitive USERID param should resolve");

        let direct_encoded = encode_key_from_recipe(&recipe, &["user_12345".to_value()])
            .expect("direct recipe encoding should succeed");
        assert_eq!(encoded, direct_encoded, "param matching mismatch");
    }

    #[test]
    fn encode_key_from_query_params_nested_struct_traversal() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_identifier("user")
                .set_struct_identifiers(vec![1, 0])
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        // user = [ "user_123", [ "Seattle", "98101" ] ]
        let address = vec!["Seattle".to_value(), "98101".to_value()].to_value();
        let user_struct = vec!["user_123".to_value(), address].to_value();

        let mut params = BTreeMap::new();
        params.insert("user".to_string(), user_struct);

        let encoded = encode_key_from_query_params(&recipe, &params)
            .expect("nested struct traversal should resolve 'Seattle'");

        let direct_encoded = encode_key_from_recipe(&recipe, &["Seattle".to_value()])
            .expect("direct recipe encoding should succeed");
        assert_eq!(
            encoded, direct_encoded,
            "struct traversal result must match 'Seattle'"
        );
    }

    #[test]
    fn encode_key_from_query_params_random_root() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_random(true)
                .set_type(Type::default().set_code(TypeCode::Int64)),
        ]);

        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let encoded = encode_key_from_query_params(&recipe, &empty_params)
            .expect("random root should encode positive integer");
        assert!(
            encoded.len() > 2,
            "random root encoding should contain tag and integer bytes"
        );
    }

    #[test]
    fn encode_key_from_query_params_missing_param_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NullsFirst)
                .set_identifier("p1")
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let mut params = BTreeMap::new();
        params.insert("p0".to_string(), "foo".to_value());

        let error = encode_key_from_query_params(&recipe, &params)
            .expect_err("missing p1 parameter should return error");
        assert!(
            error.to_string().contains("Missing query parameter 'p1'"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_struct_index_out_of_bounds_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_identifier("user")
                .set_struct_identifiers(vec![5])
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let user_struct = vec!["only_element".to_value()].to_value();
        let mut params = BTreeMap::new();
        params.insert("user".to_string(), user_struct);

        let error = encode_key_from_query_params(&recipe, &params)
            .expect_err("out of bounds struct index should return error");
        assert!(
            error
                .to_string()
                .contains("Struct field index 5 out of bounds"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_not_a_struct_list_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_identifier("user")
                .set_struct_identifiers(vec![0])
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let mut params = BTreeMap::new();
        params.insert("user".to_string(), "scalar_string".to_value());

        let error = encode_key_from_query_params(&recipe, &params)
            .expect_err("non-list struct parameter should return error");
        assert!(
            error
                .to_string()
                .contains("Expected Struct ListValue for struct parameter traversal"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_value() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(wkt::Value::String("fixed_shard".to_string())))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let encoded = encode_key_from_query_params(&recipe, &empty_params)
            .expect("constant value part should encode without parameters");

        let direct_encoded = encode_key_from_recipe(&recipe, &["fixed_shard".to_value()])
            .expect("direct recipe encoding should succeed");
        assert_eq!(encoded, direct_encoded, "constant value encoding mismatch");
    }

    #[test]
    fn encode_key_from_query_params_constant_int64_large_precision() {
        let large_id: i64 = 9_000_000_000_000_000_001;
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(wkt::Value::String(large_id.to_string())))
                .set_type(Type::default().set_code(TypeCode::Int64)),
        ]);

        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let encoded = encode_key_from_query_params(&recipe, &empty_params)
            .expect("large int64 constant value should encode with full precision");

        let direct_encoded = encode_key_from_recipe(&recipe, &[large_id.to_value()])
            .expect("direct recipe encoding should succeed");
        assert_eq!(
            encoded, direct_encoded,
            "large int64 constant value mismatch"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_struct_traversal() {
        let constant_struct = serde_json::json!(["ignored", "target_constant_leaf"]);
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_struct_identifiers(vec![1])
                .set_value(Box::new(constant_struct))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let encoded = encode_key_from_query_params(&recipe, &empty_params)
            .expect("constant struct traversal should resolve leaf value");

        let direct_encoded = encode_key_from_recipe(&recipe, &["target_constant_leaf".to_value()])
            .expect("direct recipe encoding should succeed");
        assert_eq!(
            encoded, direct_encoded,
            "constant struct leaf encoding mismatch"
        );
    }

    #[test]
    fn encode_key_from_query_params_random_root_descending() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Descending)
                .set_null_order(NullOrder::NotNull)
                .set_random(true)
                .set_type(Type::default().set_code(TypeCode::Int64)),
        ]);

        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let encoded = encode_key_from_query_params(&recipe, &empty_params)
            .expect("descending random root should encode successfully");
        assert!(
            encoded.len() > 2,
            "descending random root encoding should contain tag and integer bytes"
        );
    }

    #[test]
    fn encode_key_from_query_params_null_param_respects_null_order() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NullsLast)
                .set_identifier("p0")
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let mut params = BTreeMap::new();
        params.insert("p0".to_string(), Value::null());

        let encoded = encode_key_from_query_params(&recipe, &params)
            .expect("null parameter encoding should succeed");

        let direct_encoded = encode_key_from_recipe(&recipe, &[Value::null()])
            .expect("direct recipe encoding for null should succeed");
        assert_eq!(encoded, direct_encoded, "null parameter encoding mismatch");
    }

    #[test]
    fn encode_key_from_query_params_into_scratch_buffer() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_identifier("p0")
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let mut params = BTreeMap::new();
        params.insert("p0".to_string(), "spanner".to_value());

        let mut buffer = Vec::with_capacity(32);
        encode_key_from_query_params_into(&recipe, &params, &mut buffer)
            .expect("encoding into reusable buffer should succeed");
        assert!(!buffer.is_empty(), "buffer must not be empty");

        let direct_encoded = encode_key_from_recipe(&recipe, &["spanner".to_value()])
            .expect("direct recipe encoding should succeed");
        assert_eq!(buffer, direct_encoded, "scratch buffer mismatch");
    }

    #[test]
    fn encode_key_from_recipe_random_root_does_not_consume_values() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_random(true)
                .set_type(Type::default().set_code(TypeCode::Int64)),
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let values = vec!["alice".to_value()];
        let encoded = encode_key_from_recipe(&recipe, &values)
            .expect("random root should not consume from values slice");
        assert!(
            encoded.len() > 8,
            "encoded key should contain table tag, random integer, and string value"
        );
    }

    #[test]
    fn encode_key_from_recipe_constant_value_does_not_consume_values() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(wkt::Value::String("fixed_shard".to_string())))
                .set_type(Type::default().set_code(TypeCode::String)),
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let values = vec!["alice".to_value()];
        let encoded = encode_key_from_recipe(&recipe, &values)
            .expect("constant value part should not consume from values slice");
        assert!(
            encoded.len() > 10,
            "encoded key should contain table tag, fixed_shard constant, and alice string"
        );
    }

    #[test]
    fn encode_key_from_query_params_random_root_invalid_type_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_random(true)
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);

        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let error = encode_key_from_query_params(&recipe, &empty_params)
            .expect_err("random root with TypeCode::String should fail");
        assert!(
            error
                .to_string()
                .contains("Random key recipe part must have TypeCode::Int64"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_invalid_recipe_returns_err() {
        let empty_recipe = KeyRecipe::new();
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let error = encode_key_from_query_params(&empty_recipe, &empty_params)
            .expect_err("empty recipe should return error");
        assert!(
            error.to_string().contains("must have at least one part"),
            "unexpected error: {error}"
        );

        let no_tag_recipe = KeyRecipe::new().set_part(vec![string_part(Order::Ascending)]);
        let error_no_tag = encode_key_from_query_params(&no_tag_recipe, &empty_params)
            .expect_err("recipe without leading tag should return error");
        assert!(
            error_no_tag
                .to_string()
                .contains("must start with a table or index tag"),
            "unexpected error: {error_no_tag}"
        );
    }

    #[test]
    fn encode_key_from_query_params_missing_identifier_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let error = encode_key_from_query_params(&recipe, &empty_params)
            .expect_err("missing identifier in non-random/non-constant part should fail");
        assert!(
            error.to_string().contains("missing parameter identifier"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_negative_struct_index_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_identifier("user")
                .set_struct_identifiers(vec![-1])
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let mut params = BTreeMap::new();
        params.insert("user".to_string(), vec!["alice".to_value()].to_value());
        let error = encode_key_from_query_params(&recipe, &params)
            .expect_err("negative struct index should return error");
        assert!(
            error
                .to_string()
                .contains("Invalid negative struct index -1"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_json_struct_negative_index_returns_err() {
        let constant_struct = serde_json::json!(["val1", "val2"]);
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_struct_identifiers(vec![-2])
                .set_value(Box::new(constant_struct))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let error = encode_key_from_query_params(&recipe, &empty_params)
            .expect_err("negative struct index on constant JSON should return error");
        assert!(
            error
                .to_string()
                .contains("Invalid negative struct index -2"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_json_struct_not_an_array_returns_err() {
        let constant_val = serde_json::json!("scalar_string");
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_struct_identifiers(vec![0])
                .set_value(Box::new(constant_val))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let error = encode_key_from_query_params(&recipe, &empty_params)
            .expect_err("struct traversal on non-array constant should return error");
        assert!(
            error
                .to_string()
                .contains("Expected Struct array for struct parameter traversal"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_json_struct_index_out_of_bounds_returns_err() {
        let constant_val = serde_json::json!(["only_one"]);
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_struct_identifiers(vec![5])
                .set_value(Box::new(constant_val))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let error = encode_key_from_query_params(&recipe, &empty_params)
            .expect_err("out of bounds struct index on constant JSON should return error");
        assert!(
            error
                .to_string()
                .contains("Struct field index 5 out of bounds"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_json_bool() {
        let asc_true = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(true)))
                .set_type(Type::default().set_code(TypeCode::Bool)),
        ]);
        let desc_false = sample_recipe(vec![
            Part::new()
                .set_order(Order::Descending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(false)))
                .set_type(Type::default().set_code(TypeCode::Bool)),
        ]);
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();

        let encoded_true = encode_key_from_query_params(&asc_true, &empty_params)
            .expect("bool true constant encoding should succeed");
        let direct_true = encode_key_from_recipe(&asc_true, &[true.to_value()])
            .expect("direct bool true encoding should succeed");
        assert_eq!(encoded_true, direct_true);

        let encoded_false = encode_key_from_query_params(&desc_false, &empty_params)
            .expect("bool false constant encoding should succeed");
        let direct_false = encode_key_from_recipe(&desc_false, &[false.to_value()])
            .expect("direct bool false encoding should succeed");
        assert_eq!(encoded_false, direct_false);

        // Type mismatch for bool
        let invalid_bool_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!("not_a_bool")))
                .set_type(Type::default().set_code(TypeCode::Bool)),
        ]);
        let error = encode_key_from_query_params(&invalid_bool_recipe, &empty_params)
            .expect_err("non-bool JSON value should fail");
        assert!(
            error
                .to_string()
                .contains("Type mismatch: expected Bool value"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_json_int64_number_and_errors() {
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();

        // Integer as JSON number
        let int_number_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(42)))
                .set_type(Type::default().set_code(TypeCode::Int64)),
        ]);
        let encoded = encode_key_from_query_params(&int_number_recipe, &empty_params)
            .expect("integer JSON number encoding should succeed");
        let direct = encode_key_from_recipe(&int_number_recipe, &[42i64.to_value()])
            .expect("direct int64 encoding should succeed");
        assert_eq!(encoded, direct);

        // Invalid integer string
        let invalid_str_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!("not_a_number")))
                .set_type(Type::default().set_code(TypeCode::Int64)),
        ]);
        let error_str = encode_key_from_query_params(&invalid_str_recipe, &empty_params)
            .expect_err("invalid integer string should fail");
        assert!(
            error_str
                .to_string()
                .contains("Failed to parse Int64 from string"),
            "unexpected error: {error_str}"
        );

        // Invalid JSON type (boolean for INT64)
        let invalid_type_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(true)))
                .set_type(Type::default().set_code(TypeCode::Int64)),
        ]);
        let error_type = encode_key_from_query_params(&invalid_type_recipe, &empty_params)
            .expect_err("boolean for INT64 should fail");
        assert!(
            error_type
                .to_string()
                .contains("Type mismatch: expected String or Integer value for INT64 column"),
            "unexpected error: {error_type}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_json_float64() {
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();

        // 1. JSON number
        let float_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(123.456)))
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);
        let encoded = encode_key_from_query_params(&float_recipe, &empty_params)
            .expect("float number constant encoding should succeed");
        let direct = encode_key_from_recipe(&float_recipe, &[123.456_f64.to_value()])
            .expect("direct float64 encoding should succeed");
        assert_eq!(encoded, direct);

        // 2. Special float strings (NaN, Infinity, -Infinity)
        for special_str in &["NaN", "Infinity", "-Infinity"] {
            let special_recipe = sample_recipe(vec![
                Part::new()
                    .set_order(Order::Ascending)
                    .set_null_order(NullOrder::NotNull)
                    .set_value(Box::new(serde_json::json!(special_str)))
                    .set_type(Type::default().set_code(TypeCode::Float64)),
            ]);
            let encoded_special = encode_key_from_query_params(&special_recipe, &empty_params)
                .expect("special float string should encode");
            assert!(!encoded_special.is_empty());
        }

        // 3. Invalid float string
        let invalid_float_str = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!("invalid_float")))
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);
        let error_str = encode_key_from_query_params(&invalid_float_str, &empty_params)
            .expect_err("invalid float string should fail");
        assert!(
            error_str
                .to_string()
                .contains("Type mismatch: invalid FLOAT64 string"),
            "unexpected error: {error_str}"
        );

        // 4. Invalid JSON type (boolean for FLOAT64)
        let invalid_type = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(true)))
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);
        let error_type = encode_key_from_query_params(&invalid_type, &empty_params)
            .expect_err("boolean for FLOAT64 should fail");
        assert!(
            error_type.to_string().contains(
                "Type mismatch: expected Number or special String value for FLOAT64 column"
            ),
            "unexpected error: {error_type}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_json_string_type_mismatch_returns_err() {
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let invalid_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(12345)))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let error = encode_key_from_query_params(&invalid_recipe, &empty_params)
            .expect_err("number for STRING column should fail");
        assert!(
            error
                .to_string()
                .contains("Type mismatch: expected String value for STRING column"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_json_bytes() {
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();

        // 1. Small base64 payload (exercises stack buffer)
        let small_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!("aGVsbG8=")))
                .set_type(Type::default().set_code(TypeCode::Bytes)),
        ]);
        let encoded_small = encode_key_from_query_params(&small_recipe, &empty_params)
            .expect("small base64 bytes encoding should succeed");
        let direct_small = encode_key_from_recipe(&small_recipe, &[b"hello".to_value()])
            .expect("direct bytes encoding should succeed");
        assert_eq!(encoded_small, direct_small);

        // 2. Large base64 payload > 512 bytes (exercises heap fallback)
        let large_payload = vec![0xABu8; 600];
        let large_base64 = BASE64_STANDARD.encode(&large_payload);
        let large_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(large_base64)))
                .set_type(Type::default().set_code(TypeCode::Bytes)),
        ]);
        let encoded_large = encode_key_from_query_params(&large_recipe, &empty_params)
            .expect("large base64 bytes encoding should succeed");
        let direct_large = encode_key_from_recipe(&large_recipe, &[large_payload.to_value()])
            .expect("direct large bytes encoding should succeed");
        assert_eq!(encoded_large, direct_large);

        // 3. Invalid base64 string
        let invalid_base64_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!("%%%not_base64%%%")))
                .set_type(Type::default().set_code(TypeCode::Bytes)),
        ]);
        let error_base64 = encode_key_from_query_params(&invalid_base64_recipe, &empty_params)
            .expect_err("invalid base64 string should fail");
        assert!(
            error_base64
                .to_string()
                .contains("Failed to decode base64 Bytes from string"),
            "unexpected error: {error_base64}"
        );

        // 4. Non-string JSON value for BYTES
        let non_str_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(123)))
                .set_type(Type::default().set_code(TypeCode::Bytes)),
        ]);
        let error_non_str = encode_key_from_query_params(&non_str_recipe, &empty_params)
            .expect_err("non-string for BYTES should fail");
        assert!(
            error_non_str
                .to_string()
                .contains("Type mismatch: expected base64 String value for BYTES column"),
            "unexpected error: {error_non_str}"
        );
    }

    #[test]
    fn encode_key_from_query_params_constant_json_null_and_markers() {
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();

        // 1. Null constant with NullsFirst
        let nulls_first = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NullsFirst)
                .set_value(Box::new(serde_json::Value::Null))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let encoded_first = encode_key_from_query_params(&nulls_first, &empty_params)
            .expect("nulls first constant null should succeed");

        // 2. Null constant with NullsLast
        let nulls_last = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NullsLast)
                .set_value(Box::new(serde_json::Value::Null))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let encoded_last = encode_key_from_query_params(&nulls_last, &empty_params)
            .expect("nulls last constant null should succeed");
        assert_ne!(encoded_first, encoded_last);

        // 3. Null constant with NotNull order -> error
        let not_null_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::Value::Null))
                .set_type(Type::default().set_code(TypeCode::String)),
        ]);
        let error = encode_key_from_query_params(&not_null_recipe, &empty_params)
            .expect_err("null value for NOT NULL constant part should fail");
        assert!(
            error
                .to_string()
                .contains("NULL value provided for NOT NULL"),
            "unexpected error: {error}"
        );

        // 4. Non-null constant with NullsFirst/NullsLast/Unspecified (exercises not-null markers)
        for null_order in &[
            NullOrder::NullsFirst,
            NullOrder::NullsLast,
            NullOrder::Unspecified,
        ] {
            let marker_recipe = sample_recipe(vec![
                Part::new()
                    .set_order(Order::Ascending)
                    .set_null_order(null_order.clone())
                    .set_value(Box::new(serde_json::json!("value")))
                    .set_type(Type::default().set_code(TypeCode::String)),
            ]);
            let encoded_marker = encode_key_from_query_params(&marker_recipe, &empty_params)
                .expect("non-null constant with marker should succeed");
            assert!(!encoded_marker.is_empty());
        }
    }

    #[test]
    fn encode_key_from_query_params_constant_json_unsupported_type_returns_err() {
        let empty_params: BTreeMap<String, Value> = BTreeMap::new();
        let unsupported_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_value(Box::new(serde_json::json!(["elem1"])))
                .set_type(Type::default().set_code(TypeCode::Array)),
        ]);
        let error = encode_key_from_query_params(&unsupported_recipe, &empty_params)
            .expect_err("unsupported constant type should fail");
        assert!(
            error
                .to_string()
                .contains("is not supported for key recipe encoding"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_float64_special_strings() {
        for special_str in &["NaN", "Infinity", "-Infinity"] {
            let recipe = sample_recipe(vec![
                Part::new()
                    .set_order(Order::Ascending)
                    .set_null_order(NullOrder::NotNull)
                    .set_type(Type::default().set_code(TypeCode::Float64)),
            ]);
            let encoded = encode_key_from_recipe(&recipe, &[(*special_str).to_value()])
                .expect("special float64 string should encode successfully");
            assert!(!encoded.is_empty());
        }

        // Invalid float string
        let invalid_recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::Float64)),
        ]);
        let error = encode_key_from_recipe(&invalid_recipe, &["invalid_float".to_value()])
            .expect_err("invalid float64 string should fail");
        assert!(
            error
                .to_string()
                .contains("Type mismatch: invalid FLOAT64 string"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_bytes_invalid_base64_and_long_payload() {
        // Large payload > 512 bytes to exercise heap fallback in encode_bytes_part
        let large_payload = vec![0xDEu8; 600];
        let large_base64 = BASE64_STANDARD.encode(&large_payload);
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull)
                .set_type(Type::default().set_code(TypeCode::Bytes)),
        ]);
        let encoded = encode_key_from_recipe(&recipe, &[large_base64.to_value()])
            .expect("large base64 payload should encode successfully");
        assert!(encoded.len() > 600);

        // Invalid base64 string
        let error = encode_key_from_recipe(&recipe, &["%%%invalid_base64%%%".to_value()])
            .expect_err("invalid base64 string should fail");
        assert!(
            error
                .to_string()
                .contains("Failed to decode base64 Bytes from string"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_missing_type_definition_returns_err() {
        let recipe = sample_recipe(vec![Part::new().set_order(Order::Ascending)]);
        let error = encode_key_from_recipe(&recipe, &["test".to_value()])
            .expect_err("part with missing type should fail");
        assert!(
            error.to_string().contains("missing type definition"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn encode_key_from_recipe_unspecified_type_code_returns_err() {
        let recipe = sample_recipe(vec![
            Part::new()
                .set_order(Order::Ascending)
                .set_type(Type::default().set_code(TypeCode::Unspecified)),
        ]);
        let error = encode_key_from_recipe(&recipe, &["test".to_value()])
            .expect_err("TypeCode::Unspecified should fail");
        assert!(
            error
                .to_string()
                .contains("TypeCode::Unspecified is not permitted"),
            "unexpected error: {error}"
        );
    }
}
