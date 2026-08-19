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
//! Provides helpers to extract and encode binary routing keys from [`KeySet`]s, [`ReadRequest`]s,
//! and [`Mutation`]s using cached [`KeyRecipe`]s stored in [`KeyRecipeCache`].

// TODO(#6236): Remove dead_code allowance once key extractor is integrated into DatabaseClient / Read / Write operations.
#![allow(dead_code)]

use crate::Result;
use crate::key::{Endpoint, KeySet};
use crate::model::mutation::Operation as ProtoOperation;
use crate::model::{KeyRecipe, KeySet as ProtoKeySet, Mutation as ProtoMutation};
use crate::mutation::{InternalMutation, Mutation};
use crate::read::ReadRequest;
use crate::routing::key_recipe::{
    encode_key_from_columns_and_values_into, encode_key_from_json_columns_and_values_into,
    encode_key_from_json_recipe_into, encode_key_from_recipe_into,
};
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

/// Extracts and encodes a binary routing key (`Vec<u8>`) from a [`KeyRecipe`] and [`Mutation`].
///
/// Supports `Insert`, `Update`, `InsertOrUpdate`, `Replace` (mapping recipe column identifiers
/// to mutation column positions case-insensitively), and `Delete` (delegating to [`KeySet`] extraction).
///
/// Returns:
/// - `Ok(Some(routing_key))` if a valid point key or start range key was found and encoded.
/// - `Ok(None)` if the mutation does not contain a single partition key (e.g. empty mutation or `all: true`).
/// - `Err(e)` if encoding failed (e.g. unsupported column type or missing key column).
pub(crate) fn extract_key_from_mutation(
    recipe: &KeyRecipe,
    mutation: &Mutation,
) -> Result<Option<Vec<u8>>> {
    let mut buffer = Vec::with_capacity(recipe.part.len().saturating_mul(16));
    if !extract_key_from_mutation_into(recipe, mutation, &mut buffer)? {
        return Ok(None);
    }
    Ok(Some(buffer))
}

/// Extracts and encodes a binary routing key from a [`KeyRecipe`] and [`Mutation`] directly into an
/// existing output buffer.
pub(crate) fn extract_key_from_mutation_into(
    recipe: &KeyRecipe,
    mutation: &Mutation,
    buffer: &mut Vec<u8>,
) -> Result<bool> {
    match &mutation.inner {
        InternalMutation::Insert(write)
        | InternalMutation::Update(write)
        | InternalMutation::InsertOrUpdate(write)
        | InternalMutation::Replace(write) => {
            if write.columns.is_empty() || write.values.is_empty() {
                return Ok(false);
            }
            encode_key_from_columns_and_values_into(recipe, &write.columns, &write.values, buffer)?;
            Ok(true)
        }
        InternalMutation::Delete(delete) => {
            extract_key_from_key_set_into(recipe, &delete.key_set, buffer)
        }
    }
}

/// Extracts and encodes a binary routing key (`Vec<u8>`) from a [`KeyRecipe`] and protobuf [`ProtoMutation`].
pub(crate) fn extract_key_from_proto_mutation(
    recipe: &KeyRecipe,
    mutation: &ProtoMutation,
) -> Result<Option<Vec<u8>>> {
    let mut buffer = Vec::with_capacity(recipe.part.len().saturating_mul(16));
    if !extract_key_from_proto_mutation_into(recipe, mutation, &mut buffer)? {
        return Ok(None);
    }
    Ok(Some(buffer))
}

/// Extracts and encodes a binary routing key from a [`KeyRecipe`] and protobuf [`ProtoMutation`] directly
/// into an existing output buffer.
pub(crate) fn extract_key_from_proto_mutation_into(
    recipe: &KeyRecipe,
    mutation: &ProtoMutation,
    buffer: &mut Vec<u8>,
) -> Result<bool> {
    match &mutation.operation {
        Some(ProtoOperation::Insert(write))
        | Some(ProtoOperation::Update(write))
        | Some(ProtoOperation::InsertOrUpdate(write))
        | Some(ProtoOperation::Replace(write)) => {
            if write.columns.is_empty() || write.values.is_empty() {
                return Ok(false);
            }
            let first_row = match write.values.first() {
                Some(row) if !row.is_empty() => row,
                _ => return Ok(false),
            };
            encode_key_from_json_columns_and_values_into(
                recipe,
                &write.columns,
                first_row,
                buffer,
            )?;
            Ok(true)
        }
        Some(ProtoOperation::Delete(delete)) => {
            let Some(key_set) = &delete.key_set else {
                return Ok(false);
            };
            extract_key_from_proto_key_set_into(recipe, key_set, buffer)
        }
        Some(ProtoOperation::Send(send)) => {
            extract_key_from_proto_key_list_into(recipe, send.key.as_deref(), buffer)
        }
        Some(ProtoOperation::Ack(ack)) => {
            extract_key_from_proto_key_list_into(recipe, ack.key.as_deref(), buffer)
        }
        None => Ok(false),
    }
}

/// Extracts a routing key from a protobuf key value list into `buffer` if non-empty.
fn extract_key_from_proto_key_list_into(
    recipe: &KeyRecipe,
    key_list: Option<&[serde_json::Value]>,
    buffer: &mut Vec<u8>,
) -> Result<bool> {
    let Some(key_list) = key_list else {
        return Ok(false);
    };
    if key_list.is_empty() {
        return Ok(false);
    }
    encode_key_from_json_recipe_into(recipe, key_list, buffer)?;
    Ok(true)
}

/// Extracts a routing key from a protobuf [`ProtoKeySet`].
fn extract_key_from_proto_key_set_into(
    recipe: &KeyRecipe,
    key_set: &ProtoKeySet,
    buffer: &mut Vec<u8>,
) -> Result<bool> {
    if key_set.all {
        return Ok(false);
    }
    if let Some(first_key) = key_set.keys.first()
        && extract_key_from_proto_key_list_into(recipe, Some(first_key.as_slice()), buffer)?
    {
        return Ok(true);
    }
    if let Some(first_range) = key_set.ranges.first() {
        if extract_key_from_proto_key_list_into(
            recipe,
            first_range.start_closed().map(|values| values.as_slice()),
            buffer,
        )? {
            return Ok(true);
        }
        if extract_key_from_proto_key_list_into(
            recipe,
            first_range.start_open().map(|values| values.as_slice()),
            buffer,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Returns the target table name of a [`Mutation`].
pub(crate) fn extract_mutation_table_name(mutation: &Mutation) -> &str {
    match &mutation.inner {
        InternalMutation::Insert(write)
        | InternalMutation::Update(write)
        | InternalMutation::InsertOrUpdate(write)
        | InternalMutation::Replace(write) => &write.table,
        InternalMutation::Delete(delete) => &delete.table,
    }
}

/// Returns the target table or queue name of a protobuf [`ProtoMutation`].
pub(crate) fn extract_proto_mutation_table_name(mutation: &ProtoMutation) -> Option<&str> {
    match &mutation.operation {
        Some(ProtoOperation::Insert(write))
        | Some(ProtoOperation::Update(write))
        | Some(ProtoOperation::InsertOrUpdate(write))
        | Some(ProtoOperation::Replace(write)) => Some(&write.table),
        Some(ProtoOperation::Delete(delete)) => Some(&delete.table),
        Some(ProtoOperation::Send(send)) => Some(&send.queue),
        Some(ProtoOperation::Ack(ack)) => Some(&ack.queue),
        None => None,
    }
}

/// Trait abstracting mutation types (high-level [`Mutation`] and protobuf [`ProtoMutation`])
/// for routing key extraction and cache lookup.
pub(crate) trait ExtractableMutation {
    /// Returns the target table or queue name.
    fn table_name(&self) -> Option<&str>;

    /// Extracts and encodes the routing key using the provided [`KeyRecipe`].
    fn extract_key(&self, recipe: &KeyRecipe) -> Result<Option<Vec<u8>>>;

    /// Extracts and encodes the routing key into `buffer` using the provided [`KeyRecipe`].
    fn extract_key_into(&self, recipe: &KeyRecipe, buffer: &mut Vec<u8>) -> Result<bool>;
}

impl ExtractableMutation for Mutation {
    fn table_name(&self) -> Option<&str> {
        Some(extract_mutation_table_name(self))
    }

    fn extract_key(&self, recipe: &KeyRecipe) -> Result<Option<Vec<u8>>> {
        extract_key_from_mutation(recipe, self)
    }

    fn extract_key_into(&self, recipe: &KeyRecipe, buffer: &mut Vec<u8>) -> Result<bool> {
        extract_key_from_mutation_into(recipe, self, buffer)
    }
}

impl ExtractableMutation for ProtoMutation {
    fn table_name(&self) -> Option<&str> {
        extract_proto_mutation_table_name(self)
    }

    fn extract_key(&self, recipe: &KeyRecipe) -> Result<Option<Vec<u8>>> {
        extract_key_from_proto_mutation(recipe, self)
    }

    fn extract_key_into(&self, recipe: &KeyRecipe, buffer: &mut Vec<u8>) -> Result<bool> {
        extract_key_from_proto_mutation_into(recipe, self, buffer)
    }
}

/// Resolves the table recipe from [`KeyRecipeCache`] and encodes the routing key for a mutation.
pub(crate) fn extract_mutation_routing_key<M: ExtractableMutation>(
    key_recipe_cache: &KeyRecipeCache,
    mutation: &M,
) -> Option<Vec<u8>> {
    let table = mutation.table_name()?;
    let recipe = key_recipe_cache.get_table_recipe(table)?;
    mutation.extract_key(&recipe).ok().flatten()
}

/// Iterates through a slice of mutations and extracts the routing key from the first
/// eligible mutation whose table recipe exists in [`KeyRecipeCache`].
///
/// ### Mutation Selection Heuristics:
/// This method performs sequential extraction over candidate mutations. When routing complex
/// multi-mutation commit requests in [`LocationRouter`], specialized selection heuristics
/// (such as prioritizing non-insert mutations or selecting the largest insert mutation)
/// may be evaluated prior to calling this extraction helper.
pub(crate) fn extract_mutations_routing_key<M: ExtractableMutation>(
    key_recipe_cache: &KeyRecipeCache,
    mutations: &[M],
) -> Option<Vec<u8>> {
    for mutation in mutations {
        if let Some(routing_key) = extract_mutation_routing_key(key_recipe_cache, mutation) {
            return Some(routing_key);
        }
    }
    None
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
    use crate::model::mutation::{
        Ack as ProtoAck, Delete as ProtoDelete, Send as ProtoSend, Write as ProtoWrite,
    };
    use crate::model::{KeyRange as ProtoKeyRange, KeySet as ProtoKeySet};
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

    fn sample_table_recipe_with_identifiers(
        table_name: &str,
        parts: Vec<(Part, &str)>,
    ) -> KeyRecipe {
        let mut all_parts = vec![Part::new().set_tag(50020_u32), Part::new().set_tag(1_u32)];
        for (part, identifier) in parts {
            all_parts.push(part.set_identifier(identifier.to_string()));
        }
        KeyRecipe::new()
            .set_table_name(table_name.to_string())
            .set_part(all_parts)
    }

    #[test]
    fn extract_key_from_mutation_insert_case_insensitive_and_reordered() {
        let recipe = sample_table_recipe_with_identifiers(
            "Users",
            vec![
                (int64_part(Order::Ascending), "user_id"),
                (string_part(Order::Ascending), "user_name"),
            ],
        );
        let mutation = Mutation::new_insert_builder("Users")
            .set("ExtraCol")
            .to("extra")
            .set("USER_NAME")
            .to("Alice")
            .set("USER_ID")
            .to(1001_i64)
            .build();

        let routing_key = extract_key_from_mutation(&recipe, &mutation)
            .expect("extraction should succeed")
            .expect("should return some routing key");

        let mut expected_key = Vec::new();
        encode_key_from_recipe_into(
            &recipe,
            &[1001_i64.to_value(), "Alice".to_value()],
            &mut expected_key,
        )
        .expect("direct encoding should succeed");

        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_key_from_mutation_update_and_replace() {
        let recipe = sample_table_recipe_with_identifiers(
            "Users",
            vec![(int64_part(Order::Ascending), "k")],
        );

        let update_mutation = Mutation::new_update_builder("Users")
            .set("k")
            .to(80_i64)
            .set("v")
            .to("val")
            .build();
        let update_key = extract_key_from_mutation(&recipe, &update_mutation)
            .expect("update extraction should succeed")
            .expect("should return routing key");

        let replace_mutation = Mutation::new_replace_builder("Users")
            .set("k")
            .to(80_i64)
            .build();
        let replace_key = extract_key_from_mutation(&recipe, &replace_mutation)
            .expect("replace extraction should succeed")
            .expect("should return routing key");

        let insert_or_update_mutation = Mutation::new_insert_or_update_builder("Users")
            .set("k")
            .to(80_i64)
            .build();
        let insert_or_update_key = extract_key_from_mutation(&recipe, &insert_or_update_mutation)
            .expect("insert_or_update extraction should succeed")
            .expect("should return routing key");

        assert_eq!(update_key, replace_key);
        assert_eq!(update_key, insert_or_update_key);
    }

    #[test]
    fn extract_key_from_mutation_delete() {
        let recipe = sample_table_recipe_with_identifiers(
            "Users",
            vec![(int64_part(Order::Ascending), "k")],
        );
        let mutation = Mutation::delete("Users", KeySet::from(key![80_i64]));

        let routing_key = extract_key_from_mutation(&recipe, &mutation)
            .expect("delete extraction should succeed")
            .expect("should return routing key");

        let mut expected_key = Vec::new();
        encode_key_from_recipe_into(&recipe, &[80_i64.to_value()], &mut expected_key)
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_key_from_mutation_missing_column_returns_error() {
        let recipe = sample_table_recipe_with_identifiers(
            "Users",
            vec![
                (int64_part(Order::Ascending), "user_id"),
                (string_part(Order::Ascending), "user_name"),
            ],
        );
        let mutation = Mutation::new_insert_builder("Users")
            .set("user_id")
            .to(1001_i64)
            .build();

        let result = extract_key_from_mutation(&recipe, &mutation);
        assert!(result.is_err(), "missing key column should return Err");
    }

    #[test]
    fn extract_mutations_routing_key_first_eligible_mutation() {
        let cache = KeyRecipeCache::new();
        let recipe = sample_table_recipe_with_identifiers(
            "Users",
            vec![(int64_part(Order::Ascending), "user_id")],
        );
        cache.insert(recipe.clone());

        // First mutation is for an uncached table, second is for cached "Users".
        let mutation1 = Mutation::new_insert_builder("UncachedTable")
            .set("id")
            .to(1)
            .build();
        let mutation2 = Mutation::new_insert_builder("Users")
            .set("user_id")
            .to(2002_i64)
            .build();

        let routing_key = extract_mutations_routing_key(&cache, &[mutation1, mutation2])
            .expect("should extract key from second eligible mutation");

        let mut expected_key = Vec::new();
        encode_key_from_recipe_into(&recipe, &[2002_i64.to_value()], &mut expected_key)
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_key_from_proto_mutation_write() {
        let recipe = sample_table_recipe_with_identifiers(
            "SimpleMutations",
            vec![(int64_part(Order::Ascending), "k")],
        );

        let mut write = ProtoWrite::new();
        write.table = "SimpleMutations".to_string();
        write.columns = vec!["k".to_string()];
        let row = vec![serde_json::Value::String("80".to_string())];
        write.values = vec![row];

        let proto_mutation = ProtoMutation::new().set_insert(write);

        let routing_key = extract_key_from_proto_mutation(&recipe, &proto_mutation)
            .expect("proto extraction should succeed")
            .expect("should return routing key");

        let mut expected_key = Vec::new();
        encode_key_from_recipe_into(&recipe, &[80_i64.to_value()], &mut expected_key)
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_mutations_routing_key_proto() {
        let cache = KeyRecipeCache::new();
        let recipe = sample_table_recipe_with_identifiers(
            "Users",
            vec![(int64_part(Order::Ascending), "user_id")],
        );
        cache.insert(recipe.clone());

        let mut write1 = ProtoWrite::new();
        write1.table = "UncachedTable".to_string();
        write1.columns = vec!["id".to_string()];
        write1.values = vec![vec![serde_json::json!(1)]];

        let mut write2 = ProtoWrite::new();
        write2.table = "Users".to_string();
        write2.columns = vec!["user_id".to_string()];
        write2.values = vec![vec![serde_json::Value::String("2002".to_string())]];

        let mutation1 = ProtoMutation::new().set_insert(write1);
        let mutation2 = ProtoMutation::new().set_insert(write2);

        let routing_key = extract_mutations_routing_key(&cache, &[mutation1, mutation2])
            .expect("should extract key from second eligible proto mutation");

        let mut expected_key = Vec::new();
        encode_key_from_recipe_into(&recipe, &[2002_i64.to_value()], &mut expected_key)
            .expect("direct encoding should succeed");
        assert_eq!(routing_key, expected_key);
    }

    #[test]
    fn extract_mutation_table_name_all_operations() {
        let insert_mutation = Mutation::new_insert_builder("Users")
            .set("id")
            .to(1)
            .build();
        assert_eq!(extract_mutation_table_name(&insert_mutation), "Users");

        let update_mutation = Mutation::new_update_builder("Orders")
            .set("id")
            .to(1)
            .build();
        assert_eq!(extract_mutation_table_name(&update_mutation), "Orders");

        let insert_or_update = Mutation::new_insert_or_update_builder("Items")
            .set("id")
            .to(1)
            .build();
        assert_eq!(extract_mutation_table_name(&insert_or_update), "Items");

        let replace_mutation = Mutation::new_replace_builder("Products")
            .set("id")
            .to(1)
            .build();
        assert_eq!(extract_mutation_table_name(&replace_mutation), "Products");

        let delete_mutation = Mutation::delete("Accounts", KeySet::all());
        assert_eq!(extract_mutation_table_name(&delete_mutation), "Accounts");
    }

    #[test]
    fn extract_proto_mutation_table_name_all_operations() {
        let mut write = ProtoWrite::new();
        write.table = "Users".to_string();
        let insert_mutation = ProtoMutation::new().set_insert(write.clone());
        assert_eq!(
            extract_proto_mutation_table_name(&insert_mutation),
            Some("Users")
        );

        let update_mutation = ProtoMutation::new().set_update(write.clone());
        assert_eq!(
            extract_proto_mutation_table_name(&update_mutation),
            Some("Users")
        );

        let insert_or_update = ProtoMutation::new().set_insert_or_update(write.clone());
        assert_eq!(
            extract_proto_mutation_table_name(&insert_or_update),
            Some("Users")
        );

        let replace_mutation = ProtoMutation::new().set_replace(write);
        assert_eq!(
            extract_proto_mutation_table_name(&replace_mutation),
            Some("Users")
        );

        let mut delete = ProtoDelete::new();
        delete.table = "Accounts".to_string();
        let delete_mutation = ProtoMutation::new().set_delete(delete);
        assert_eq!(
            extract_proto_mutation_table_name(&delete_mutation),
            Some("Accounts")
        );

        let mut send = ProtoSend::new();
        send.queue = "MyQueue".to_string();
        let send_mutation = ProtoMutation::new().set_send(send);
        assert_eq!(
            extract_proto_mutation_table_name(&send_mutation),
            Some("MyQueue")
        );

        let mut ack = ProtoAck::new();
        ack.queue = "MyAckQueue".to_string();
        let ack_mutation = ProtoMutation::new().set_ack(ack);
        assert_eq!(
            extract_proto_mutation_table_name(&ack_mutation),
            Some("MyAckQueue")
        );

        let empty_mutation = ProtoMutation::new();
        assert_eq!(extract_proto_mutation_table_name(&empty_mutation), None);
    }

    #[test]
    fn extract_key_from_mutation_delete_ranges_and_all() {
        let recipe = sample_table_recipe_with_identifiers(
            "Users",
            vec![(int64_part(Order::Ascending), "k")],
        );

        // Delete all returns Ok(None)
        let delete_all = Mutation::delete("Users", KeySet::all());
        assert_eq!(
            extract_key_from_mutation(&recipe, &delete_all).expect("delete all should succeed"),
            None
        );

        // Delete empty KeySet returns Ok(None)
        let delete_empty = Mutation::delete("Users", KeySet::builder().build());
        assert_eq!(
            extract_key_from_mutation(&recipe, &delete_empty).expect("delete empty should succeed"),
            None
        );

        // Delete closed_open range
        let delete_closed_open = Mutation::delete(
            "Users",
            KeySet::builder()
                .add_range(KeyRange::closed_open(key![100_i64], key![200_i64]))
                .build(),
        );
        let key = extract_key_from_mutation(&recipe, &delete_closed_open)
            .expect("delete closed_open should succeed")
            .expect("should return key");
        let mut expected_key = Vec::new();
        encode_key_from_recipe_into(&recipe, &[100_i64.to_value()], &mut expected_key)
            .expect("direct encode should succeed");
        assert_eq!(key, expected_key);

        // Delete open_closed range
        let delete_open_closed = Mutation::delete(
            "Users",
            KeySet::builder()
                .add_range(KeyRange::open_closed(key![100_i64], key![200_i64]))
                .build(),
        );
        let key = extract_key_from_mutation(&recipe, &delete_open_closed)
            .expect("delete open_closed should succeed")
            .expect("should return key");
        assert_eq!(key, expected_key);

        // High-level write mutation with empty columns/values returns Ok(None)
        let empty_insert = Mutation::new_insert_builder("Users").build();
        assert_eq!(
            extract_key_from_mutation(&recipe, &empty_insert)
                .expect("empty high-level write should succeed"),
            None
        );
    }

    #[test]
    fn extract_key_from_proto_mutation_edge_cases() {
        let recipe = sample_table_recipe_with_identifiers(
            "Users",
            vec![(int64_part(Order::Ascending), "k")],
        );

        // Proto write with empty values returns Ok(None)
        let mut write_empty_values = ProtoWrite::new();
        write_empty_values.table = "Users".to_string();
        write_empty_values.columns = vec!["k".to_string()];
        write_empty_values.values = vec![];
        let proto_empty = ProtoMutation::new().set_insert(write_empty_values);
        assert_eq!(
            extract_key_from_proto_mutation(&recipe, &proto_empty)
                .expect("empty write values should succeed"),
            None
        );

        // Proto write with empty first row returns Ok(None)
        let mut write_empty_first_row = ProtoWrite::new();
        write_empty_first_row.table = "Users".to_string();
        write_empty_first_row.columns = vec!["k".to_string()];
        write_empty_first_row.values = vec![vec![]];
        let proto_empty_row = ProtoMutation::new().set_insert(write_empty_first_row);
        assert_eq!(
            extract_key_from_proto_mutation(&recipe, &proto_empty_row)
                .expect("empty first row should succeed"),
            None
        );

        // Proto delete with None key_set returns Ok(None)
        let mut delete_no_keyset = ProtoDelete::new();
        delete_no_keyset.table = "Users".to_string();
        let proto_delete_no_keyset = ProtoMutation::new().set_delete(delete_no_keyset);
        assert_eq!(
            extract_key_from_proto_mutation(&recipe, &proto_delete_no_keyset)
                .expect("delete without keyset should succeed"),
            None
        );

        // Proto delete with all: true returns Ok(None)
        let mut delete_all = ProtoDelete::new();
        delete_all.table = "Users".to_string();
        let mut keyset_all = ProtoKeySet::new();
        keyset_all.all = true;
        delete_all.key_set = Some(keyset_all);
        let proto_delete_all = ProtoMutation::new().set_delete(delete_all);
        assert_eq!(
            extract_key_from_proto_mutation(&recipe, &proto_delete_all)
                .expect("delete all should succeed"),
            None
        );

        // Proto delete with ranges start_closed
        let mut delete_range = ProtoDelete::new();
        delete_range.table = "Users".to_string();
        let mut keyset_range = ProtoKeySet::new();
        let proto_range = ProtoKeyRange::new()
            .set_start_closed(vec![serde_json::Value::String("50".to_string())]);
        keyset_range.ranges = vec![proto_range];
        delete_range.key_set = Some(keyset_range);
        let proto_delete_range = ProtoMutation::new().set_delete(delete_range);
        let key = extract_key_from_proto_mutation(&recipe, &proto_delete_range)
            .expect("delete range start_closed should succeed")
            .expect("should return key");
        let mut expected_key = Vec::new();
        encode_key_from_recipe_into(&recipe, &[50_i64.to_value()], &mut expected_key)
            .expect("direct encode should succeed");
        assert_eq!(key, expected_key);

        // Proto delete with ranges start_open
        let mut delete_range_open = ProtoDelete::new();
        delete_range_open.table = "Users".to_string();
        let mut keyset_range_open = ProtoKeySet::new();
        let proto_range_open =
            ProtoKeyRange::new().set_start_open(vec![serde_json::Value::String("60".to_string())]);
        keyset_range_open.ranges = vec![proto_range_open];
        delete_range_open.key_set = Some(keyset_range_open);
        let proto_delete_range_open = ProtoMutation::new().set_delete(delete_range_open);
        let key = extract_key_from_proto_mutation(&recipe, &proto_delete_range_open)
            .expect("delete range start_open should succeed")
            .expect("should return key");
        let mut expected_key_open = Vec::new();
        encode_key_from_recipe_into(&recipe, &[60_i64.to_value()], &mut expected_key_open)
            .expect("direct encode should succeed");
        assert_eq!(key, expected_key_open);

        // Proto Send with None key returns Ok(None)
        let mut send_none = ProtoSend::new();
        send_none.queue = "Queue".to_string();
        let proto_send_none = ProtoMutation::new().set_send(send_none);
        assert_eq!(
            extract_key_from_proto_mutation(&recipe, &proto_send_none)
                .expect("send without key should succeed"),
            None
        );

        // Proto Send with empty key returns Ok(None)
        let mut send_empty = ProtoSend::new();
        send_empty.queue = "Queue".to_string();
        send_empty.key = Some(vec![]);
        let proto_send_empty = ProtoMutation::new().set_send(send_empty);
        assert_eq!(
            extract_key_from_proto_mutation(&recipe, &proto_send_empty)
                .expect("send with empty key should succeed"),
            None
        );

        // Proto Ack with None key returns Ok(None)
        let mut ack_none = ProtoAck::new();
        ack_none.queue = "Queue".to_string();
        let proto_ack_none = ProtoMutation::new().set_ack(ack_none);
        assert_eq!(
            extract_key_from_proto_mutation(&recipe, &proto_ack_none)
                .expect("ack without key should succeed"),
            None
        );

        // Proto Ack with empty key returns Ok(None)
        let mut ack_empty = ProtoAck::new();
        ack_empty.queue = "Queue".to_string();
        ack_empty.key = Some(vec![]);
        let proto_ack_empty = ProtoMutation::new().set_ack(ack_empty);
        assert_eq!(
            extract_key_from_proto_mutation(&recipe, &proto_ack_empty)
                .expect("ack with empty key should succeed"),
            None
        );

        // Proto mutation with operation == None returns Ok(None)
        let proto_empty_op = ProtoMutation::new();
        assert_eq!(
            extract_key_from_proto_mutation(&recipe, &proto_empty_op)
                .expect("empty operation should succeed"),
            None
        );
    }

    #[test]
    fn extract_mutation_routing_key_edge_cases() {
        let cache = KeyRecipeCache::new();

        // Empty mutations slice returns None
        let empty_mutations: Vec<Mutation> = Vec::new();
        assert_eq!(
            extract_mutations_routing_key(&cache, &empty_mutations),
            None
        );

        // Mutation for uncached table returns None
        let mutation = Mutation::new_insert_builder("Uncached")
            .set("id")
            .to(1)
            .build();
        assert_eq!(extract_mutation_routing_key(&cache, &mutation), None);

        // Mutation for cached table with encoding error returns None
        let unsupported_recipe = sample_table_recipe_with_identifiers(
            "Users",
            vec![(
                Part::new()
                    .set_order(Order::Ascending)
                    .set_null_order(NullOrder::NotNull)
                    .set_type(Type::default().set_code(TypeCode::Array)),
                "id",
            )],
        );
        cache.insert(unsupported_recipe);
        let user_mutation = Mutation::new_insert_builder("Users")
            .set("id")
            .to(1)
            .build();
        assert_eq!(extract_mutation_routing_key(&cache, &user_mutation), None);
    }
}
