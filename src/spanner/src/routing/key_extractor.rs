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

//! Routing key extraction for Spanner client requests.
//!
//! Provides helpers to extract and encode binary routing keys from [`KeySet`]s and [`ReadRequest`]s
//! using cached [`KeyRecipe`]s stored in [`KeyRecipeCache`].

// TODO(#6236): Remove dead_code allowance once key extractor is integrated into DatabaseClient / Read operations.
#![allow(dead_code)]

use crate::Result;
use crate::key::{Endpoint, KeySet};
use crate::model::KeyRecipe;
use crate::read::ReadRequest;
use crate::routing::key_recipe::encode_key_from_recipe_into;
use crate::routing::key_recipe_cache::KeyRecipeCache;

/// Extracts and encodes a binary routing key (`Vec<u8>`) from a [`KeyRecipe`] and [`KeySet`].
///
/// Returns:
/// - `Ok(Some(routing_key))` if a valid point key or start range key was found and encoded.
/// - `Ok(None)` if the `KeySet` is empty or matches all keys (`key_set.all == true`), meaning no
///   single partition routing key can be determined.
/// - `Err(e)` if encoding failed (e.g. unsupported key column type or missing column value).
///
/// # Caller Fallback Contract
/// If encoding returns an error or `None`, callers (`LocationRouter` / `DatabaseClient`) MUST
/// gracefully fall back to default routing rather than failing the user's RPC.
pub(crate) fn extract_key_from_key_set(
    recipe: &KeyRecipe,
    key_set: &KeySet,
) -> Result<Option<Vec<u8>>> {
    if key_set.all || (key_set.keys.is_empty() && key_set.ranges.is_empty()) {
        return Ok(None);
    }
    let mut buffer = Vec::with_capacity(recipe.part.len().saturating_mul(16));
    if !extract_key_from_key_set_into(recipe, key_set, &mut buffer)? {
        return Ok(None);
    }
    Ok(Some(buffer))
}

/// Extracts and encodes a binary routing key from a [`KeyRecipe`] and [`KeySet`] directly into an
/// existing output buffer.
///
/// Returns:
/// - `Ok(true)` if a routing key was successfully extracted and written to `buffer`.
/// - `Ok(false)` if the `KeySet` is empty or matches all keys, leaving `buffer` unchanged.
/// - `Err(e)` if encoding failed, in which case `buffer` is truncated back to its initial length.
pub(crate) fn extract_key_from_key_set_into(
    recipe: &KeyRecipe,
    key_set: &KeySet,
    buffer: &mut Vec<u8>,
) -> Result<bool> {
    if key_set.all {
        return Ok(false);
    }

    // 1. If point keys are present, encode the first point key tuple.
    if let Some(first_key) = key_set.keys.first()
        && !first_key.values.is_empty()
    {
        encode_key_from_recipe_into(recipe, &first_key.values, buffer)?;
        return Ok(true);
    }

    // 2. If no point keys are present, check the first key range.
    if let Some(first_range) = key_set.ranges.first() {
        let start_key = match &first_range.start {
            Endpoint::Closed(key) | Endpoint::Open(key) => key,
        };
        if !start_key.values.is_empty() {
            encode_key_from_recipe_into(recipe, &start_key.values, buffer)?;
            return Ok(true);
        }
    }

    // 3. Unbounded or empty KeySet has no single partition routing key.
    Ok(false)
}

/// Resolves the table or index [`KeyRecipe`] from [`KeyRecipeCache`] and encodes the routing key for a read operation.
///
/// If `index` is `Some` and non-empty, the index recipe is looked up; otherwise the table recipe is used.
/// Returns `None` if:
/// - The `key_set` is `KeySet::all()` or empty.
/// - No recipe is present in the cache.
/// - Key encoding returned an error.
pub(crate) fn extract_read_routing_key(
    key_recipe_cache: &KeyRecipeCache,
    table: &str,
    index: Option<&str>,
    key_set: &KeySet,
) -> Option<Vec<u8>> {
    if key_set.all || (key_set.keys.is_empty() && key_set.ranges.is_empty()) {
        return None;
    }
    let recipe = match index.filter(|name| !name.is_empty()) {
        Some(index_name) => key_recipe_cache.get_index_recipe(index_name)?,
        None => key_recipe_cache.get_table_recipe(table)?,
    };
    extract_key_from_key_set(&recipe, key_set).ok().flatten()
}

/// Resolves the recipe from [`KeyRecipeCache`] and encodes the routing key for a [`ReadRequest`].
pub(crate) fn extract_read_request_routing_key(
    key_recipe_cache: &KeyRecipeCache,
    request: &ReadRequest,
) -> Option<Vec<u8>> {
    extract_read_routing_key(
        key_recipe_cache,
        &request.table,
        request.index.as_deref(),
        &request.keys,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key;
    use crate::key::{Key, KeyRange};
    use crate::model::Type;
    use crate::model::TypeCode;
    use crate::model::key_recipe::Part;
    use crate::model::key_recipe::part::{NullOrder, Order};
    use crate::routing::key_recipe::encode_key_from_recipe;
    use crate::value::ToValue;

    fn sample_table_recipe(table_name: &str, parts: Vec<Part>) -> KeyRecipe {
        let mut all_parts = vec![Part::new().set_tag(1_u32)];
        all_parts.extend(parts);
        KeyRecipe::new()
            .set_table_name(table_name.to_string())
            .set_part(all_parts)
    }

    fn sample_index_recipe(index_name: &str, parts: Vec<Part>) -> KeyRecipe {
        let mut all_parts = vec![Part::new().set_tag(2_u32)];
        all_parts.extend(parts);
        KeyRecipe::new()
            .set_index_name(index_name.to_string())
            .set_part(all_parts)
    }

    fn int64_part(order: Order) -> Part {
        Part::new()
            .set_order(order)
            .set_null_order(NullOrder::NullsFirst)
            .set_type(Type::default().set_code(TypeCode::Int64))
    }

    fn string_part(order: Order) -> Part {
        Part::new()
            .set_order(order)
            .set_null_order(NullOrder::NullsFirst)
            .set_type(Type::default().set_code(TypeCode::String))
    }

    #[test]
    fn extract_key_from_key_set_point_key() {
        let recipe = sample_table_recipe(
            "Users",
            vec![int64_part(Order::Ascending), string_part(Order::Ascending)],
        );
        let key_set = KeySet::builder()
            .add_key(key![42_i64, "Alice"])
            .add_key(key![43_i64, "Bob"])
            .build();

        let routing_key = extract_key_from_key_set(&recipe, &key_set)
            .expect("extraction should succeed")
            .expect("should return some routing key");

        let expected_key =
            encode_key_from_recipe(&recipe, &[42_i64.to_value(), "Alice".to_value()])
                .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_key_from_key_set_closed_open_range() {
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let key_set = KeySet::builder()
            .add_range(KeyRange::closed_open(key![100_i64], key![200_i64]))
            .build();

        let routing_key = extract_key_from_key_set(&recipe, &key_set)
            .expect("extraction should succeed")
            .expect("should return some routing key");

        let expected_key = encode_key_from_recipe(&recipe, &[100_i64.to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_key_from_key_set_open_closed_range() {
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let key_set = KeySet::builder()
            .add_range(KeyRange::open_closed(key![100_i64], key![200_i64]))
            .build();

        let routing_key = extract_key_from_key_set(&recipe, &key_set)
            .expect("extraction should succeed")
            .expect("should return some routing key");

        let expected_key = encode_key_from_recipe(&recipe, &[100_i64.to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_key_from_key_set_closed_closed_range() {
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let key_set = KeySet::builder()
            .add_range(KeyRange::closed_closed(key![150_i64], key![250_i64]))
            .build();

        let routing_key = extract_key_from_key_set(&recipe, &key_set)
            .expect("extraction should succeed")
            .expect("should return some routing key");

        let expected_key = encode_key_from_recipe(&recipe, &[150_i64.to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_key_from_key_set_open_open_range() {
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let key_set = KeySet::builder()
            .add_range(KeyRange::open_open(key![175_i64], key![275_i64]))
            .build();

        let routing_key = extract_key_from_key_set(&recipe, &key_set)
            .expect("extraction should succeed")
            .expect("should return some routing key");

        let expected_key = encode_key_from_recipe(&recipe, &[175_i64.to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_key_from_key_set_point_key_over_range_precedence() {
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let key_set = KeySet::builder()
            .add_range(KeyRange::closed_open(key![500_i64], key![600_i64]))
            .add_key(key![42_i64])
            .build();

        let routing_key = extract_key_from_key_set(&recipe, &key_set)
            .expect("extraction should succeed")
            .expect("should return point key");

        let expected_key = encode_key_from_recipe(&recipe, &[42_i64.to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_key_from_key_set_unbounded_range_returns_none() {
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let key_set = KeySet::builder()
            .add_range(KeyRange::closed_open(Key::default(), key![200_i64]))
            .build();

        let routing_key = extract_key_from_key_set(&recipe, &key_set)
            .expect("unbounded start range should succeed without error");
        assert!(routing_key.is_none());
    }

    #[test]
    fn extract_key_from_key_set_all_returns_none() {
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let key_set = KeySet::all();

        let routing_key = extract_key_from_key_set(&recipe, &key_set)
            .expect("KeySet::all() should succeed without error");
        assert!(routing_key.is_none());
    }

    #[test]
    fn extract_key_from_key_set_empty_returns_none() {
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let key_set = KeySet::default();

        let routing_key = extract_key_from_key_set(&recipe, &key_set)
            .expect("empty KeySet should succeed without error");
        assert!(routing_key.is_none());
    }

    #[test]
    fn extract_key_from_key_set_into_buffer() {
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let key_set = KeySet::from(key![55_i64]);

        let mut buffer = Vec::with_capacity(32);
        let extracted = extract_key_from_key_set_into(&recipe, &key_set, &mut buffer)
            .expect("encoding into buffer should succeed");
        assert!(extracted);
        assert!(!buffer.is_empty());

        let expected_key = encode_key_from_recipe(&recipe, &[55_i64.to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(buffer, expected_key);
    }

    #[test]
    fn extract_key_from_key_set_error_preserves_buffer() {
        // Recipe expects 2 columns (int64, string)
        let recipe = sample_table_recipe(
            "Users",
            vec![int64_part(Order::Ascending), string_part(Order::Ascending)],
        );
        // Provide only 1 column value, which will trigger an error in encode_key_from_recipe_into
        let key_set = KeySet::from(key![42_i64]);

        let mut buffer = b"prefix_data".to_vec();
        let initial_len = buffer.len();

        let error = extract_key_from_key_set_into(&recipe, &key_set, &mut buffer)
            .expect_err("insufficient column values should return error");
        assert!(
            error.to_string().contains("Not enough column values"),
            "unexpected error message: {error}"
        );
        assert_eq!(
            buffer.len(),
            initial_len,
            "buffer must be restored to initial length on error"
        );
        assert_eq!(&buffer[..], b"prefix_data");
    }

    #[test]
    fn extract_read_routing_key_table_lookup() {
        let cache = KeyRecipeCache::new();
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        cache.insert(recipe.clone());

        let key_set = KeySet::from(key![777_i64]);
        let routing_key = extract_read_routing_key(&cache, "Users", None, &key_set)
            .expect("should find table recipe and extract key");

        let expected_key = encode_key_from_recipe(&recipe, &[777_i64.to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_read_routing_key_index_lookup() {
        let cache = KeyRecipeCache::new();
        let table_recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        let index_recipe = sample_index_recipe("UsersByEmail", vec![string_part(Order::Ascending)]);
        cache.insert(table_recipe);
        cache.insert(index_recipe.clone());

        let key_set = KeySet::from(key!["alice@example.com"]);
        let routing_key = extract_read_routing_key(&cache, "Users", Some("UsersByEmail"), &key_set)
            .expect("should find index recipe and extract key");

        let expected_key = encode_key_from_recipe(&index_recipe, &["alice@example.com".to_value()])
            .expect("direct index encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_read_routing_key_empty_string_index_falls_back_to_table() {
        let cache = KeyRecipeCache::new();
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        cache.insert(recipe.clone());

        let key_set = KeySet::from(key![888_i64]);
        let routing_key = extract_read_routing_key(&cache, "Users", Some(""), &key_set)
            .expect("empty string index should fall back to table recipe");

        let expected_key = encode_key_from_recipe(&recipe, &[888_i64.to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_read_routing_key_missing_recipe_returns_none() {
        let cache = KeyRecipeCache::new();
        let key_set = KeySet::from(key![100_i64]);

        let routing_key = extract_read_routing_key(&cache, "NonExistentTable", None, &key_set);
        assert!(routing_key.is_none());
    }

    #[test]
    fn extract_read_routing_key_missing_index_recipe_returns_none() {
        let cache = KeyRecipeCache::new();
        let table_recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        cache.insert(table_recipe);

        let key_set = KeySet::from(key!["alice@example.com"]);
        let routing_key =
            extract_read_routing_key(&cache, "Users", Some("NonExistentIndex"), &key_set);
        assert!(routing_key.is_none());
    }

    #[test]
    fn extract_read_routing_key_encoding_error_returns_none() {
        let cache = KeyRecipeCache::new();
        // Recipe expects 2 columns
        let recipe = sample_table_recipe(
            "Users",
            vec![int64_part(Order::Ascending), string_part(Order::Ascending)],
        );
        cache.insert(recipe);

        // KeySet has only 1 column
        let key_set = KeySet::from(key![100_i64]);

        let routing_key = extract_read_routing_key(&cache, "Users", None, &key_set);
        assert!(
            routing_key.is_none(),
            "encoding error must gracefully fall back to None"
        );
    }

    #[test]
    fn extract_read_routing_key_all_short_circuits() {
        let cache = KeyRecipeCache::new();
        let recipe = sample_table_recipe("Users", vec![int64_part(Order::Ascending)]);
        cache.insert(recipe);

        let routing_key = extract_read_routing_key(&cache, "Users", None, &KeySet::all());
        assert!(routing_key.is_none());
    }

    #[test]
    fn extract_read_request_routing_key_end_to_end() {
        let cache = KeyRecipeCache::new();
        let recipe = sample_table_recipe("Accounts", vec![string_part(Order::Ascending)]);
        cache.insert(recipe.clone());

        let read_request = ReadRequest::builder("Accounts", vec!["Balance"])
            .with_keys(key!["acc_999"])
            .build();

        let routing_key = extract_read_request_routing_key(&cache, &read_request)
            .expect("should extract key from ReadRequest");

        let expected_key = encode_key_from_recipe(&recipe, &["acc_999".to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_read_request_routing_key_with_index() {
        let cache = KeyRecipeCache::new();
        let index_recipe =
            sample_index_recipe("AccountsByNumber", vec![string_part(Order::Ascending)]);
        cache.insert(index_recipe.clone());

        let read_request = ReadRequest::builder("Accounts", vec!["Balance"])
            .with_index("AccountsByNumber", key!["acc_888"])
            .build();

        let routing_key = extract_read_request_routing_key(&cache, &read_request)
            .expect("should extract key from ReadRequest with index");

        let expected_key = encode_key_from_recipe(&index_recipe, &["acc_888".to_value()])
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }
}
