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

use crate::model::CacheUpdate;
use crate::model::KeyRecipe;
use crate::model::key_recipe::Part;
use crate::routing::key_range_cache::{KeyRangeCache, RangeMode};
use crate::routing::key_recipe::RecipeValue;
use crate::routing::key_recipe_cache::KeyRecipeCache;
use crate::routing::ssformat;
use crate::routing::textproto_test_utils::{
    EndKeyType, FinderRequest, ParsedKeyRange, ParsedKeySet, ParsedReadRequest, ParsedRoutingHint,
    ParsedSqlRequest, ParsedTabletUid, StartKeyType, json_to_spanner_value,
    parse_finder_golden_textproto,
};
use crate::value::Value;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TargetRange {
    start: Vec<u8>,
    limit: Vec<u8>,
    approximate: bool,
}

impl TargetRange {
    fn new(start: Vec<u8>, limit: Vec<u8>, approximate: bool) -> Self {
        Self {
            start,
            limit,
            approximate,
        }
    }

    fn is_point(&self) -> bool {
        self.limit.is_empty()
    }

    fn merge_from(&mut self, other: TargetRange) {
        if self.start.is_empty() || (!other.start.is_empty() && other.start < self.start) {
            self.start = other.start.clone();
        }
        if other.is_point() && (self.limit.is_empty() || other.start >= self.limit) {
            self.limit = ssformat::make_prefix_successor(&other.start);
        } else if other.limit > self.limit {
            self.limit = other.limit;
        }
        self.approximate |= other.approximate;
    }
}

#[test]
fn golden_conformance_finder() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let textproto_path = Path::new(manifest_dir).join("src/routing/testdata/finder_test.textproto");
    let textproto = fs::read_to_string(&textproto_path)
        .expect("failed to load Spanner golden testdata from finder_test.textproto");

    let cases = parse_finder_golden_textproto(&textproto);
    assert_eq!(
        cases.len(),
        36,
        "finder_test.textproto must parse all 36 Spanner golden test cases"
    );

    let finder = TestFinder::new();
    let mut executed_events = 0;

    for case in &cases {
        finder.reset();
        finder.key_range_cache.use_deterministic_random();

        for event in &case.events {
            // Apply unhealthy servers (cleared and replaced per event, matching Java and Go)
            {
                let mut unhealthy = finder
                    .unhealthy_servers
                    .write()
                    .expect("poisoned unhealthy lock");
                unhealthy.clear();
                for server in &event.unhealthy_servers {
                    unhealthy.insert(server.clone());
                }
            }

            // Apply cache update if present
            if let Some(update) = &event.cache_update {
                finder.apply_cache_update(update);
            }

            // Execute request
            if let Some(request) = &event.request {
                executed_events += 1;
                let (actual_server, actual_hint) = match request {
                    FinderRequest::Read(read_req) => finder.find_server_read(read_req),
                    FinderRequest::Sql(sql_req) => finder.find_server_sql(sql_req),
                };

                if let Some(expected_srv) = &event.expected_server {
                    assert_eq!(
                        actual_server.as_deref(),
                        Some(expected_srv.as_str()),
                        "server mismatch in case {}, event {}",
                        case.name,
                        event.name
                    );
                }

                if let Some(expected_hint) = &event.expected_hint {
                    assert_hints_match(&case.name, &event.name, expected_hint, &actual_hint);
                }
            }
        }
    }

    assert_eq!(
        executed_events, 203,
        "Expected all 203 query/read events across 36 test cases to execute successfully, ran {executed_events}"
    );
}

fn assert_hints_match(
    case_name: &str,
    event_name: &str,
    expected: &ParsedRoutingHint,
    actual: &ParsedRoutingHint,
) {
    assert_eq!(
        actual, expected,
        "RoutingHint mismatch in case {case_name}, event {event_name}"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyType {
    FullKey,
    Prefix,
    PrefixSuccessor,
}

fn encode_single_value_part(
    ss_key: &mut Vec<u8>,
    part: &Part,
    value: &str,
    is_successor: bool,
) -> bool {
    let mut buf = Vec::new();
    let val = Value::from(value);
    if val.encode_into(&mut buf, part).is_err() {
        return false;
    }
    if is_successor {
        let succ = ssformat::make_prefix_successor(&buf);
        ss_key.extend_from_slice(&succ);
    } else {
        ss_key.extend_from_slice(&buf);
    }
    true
}

fn encode_key_internal(
    recipe: &KeyRecipe,
    values: &[String],
    key_type: KeyType,
    is_index: bool,
) -> Option<TargetRange> {
    let mut ss_key = Vec::new();
    let mut parts_count = 0;
    let mut value_index = 0;
    let mut state_end_of_keys = false;

    for part in &recipe.part {
        if part.tag != 0 {
            let _ = ssformat::append_composite_tag(&mut ss_key, part.tag);
            parts_count += 1;
        } else if part.random() == Some(&true) {
            ssformat::append_int64_increasing(&mut ss_key, 0);
            parts_count += 1;
        } else if let Some(constant_val) = part.value() {
            let val = json_to_spanner_value(constant_val.as_ref());
            let _ = val.encode_into(&mut ss_key, part);
            parts_count += 1;
        } else if value_index < values.len() {
            let is_last_val = value_index + 1 == values.len();
            let is_succ = is_last_val && key_type == KeyType::PrefixSuccessor;
            let success =
                encode_single_value_part(&mut ss_key, part, &values[value_index], is_succ);
            if success {
                parts_count += 1;
                value_index += 1;
            } else {
                state_end_of_keys = true;
                break;
            }
        } else {
            state_end_of_keys = true;
            break;
        }
    }

    let start = ss_key.clone();
    let mut limit = Vec::new();
    let mut approximate = false;

    if is_index && key_type != KeyType::PrefixSuccessor {
        limit = ssformat::make_prefix_successor(&start);
    } else if (key_type == KeyType::Prefix && parts_count != recipe.part.len())
        || state_end_of_keys
        || (key_type == KeyType::PrefixSuccessor && is_index)
    {
        approximate = true;
    }

    Some(TargetRange::new(start, limit, approximate))
}

fn key_range_to_target_range(
    recipe: &KeyRecipe,
    range: &ParsedKeyRange,
    is_index: bool,
) -> TargetRange {
    let start = match &range.start {
        StartKeyType::Closed(values) => {
            encode_key_internal(recipe, values, KeyType::Prefix, is_index).unwrap_or_default()
        }
        StartKeyType::Open(values) => {
            encode_key_internal(recipe, values, KeyType::PrefixSuccessor, is_index)
                .unwrap_or_default()
        }
        StartKeyType::Unspecified => {
            let mut target =
                encode_key_internal(recipe, &[], KeyType::Prefix, is_index).unwrap_or_default();
            target.approximate = true;
            target
        }
    };

    let limit = match &range.end {
        EndKeyType::Closed(values) => {
            encode_key_internal(recipe, values, KeyType::PrefixSuccessor, is_index)
                .unwrap_or_default()
        }
        EndKeyType::Open(values) => {
            encode_key_internal(recipe, values, KeyType::Prefix, is_index).unwrap_or_default()
        }
        EndKeyType::Unspecified => TargetRange {
            start: Vec::new(),
            limit: Vec::new(),
            approximate: true,
        },
    };

    let limit_key = if limit.start.is_empty() && (start.approximate || limit.approximate) {
        ssformat::make_prefix_successor(&start.start)
    } else {
        limit.start
    };

    TargetRange::new(
        start.start,
        limit_key,
        start.approximate || limit.approximate,
    )
}

fn key_set_to_target_range(
    recipe: &KeyRecipe,
    key_set: &ParsedKeySet,
    is_index: bool,
) -> TargetRange {
    if key_set.all {
        return key_range_to_target_range(
            recipe,
            &ParsedKeyRange {
                start: StartKeyType::Unspecified,
                end: EndKeyType::Unspecified,
            },
            is_index,
        );
    }

    if key_set.keys.len() == 1 && key_set.ranges.is_empty() {
        return encode_key_internal(
            recipe,
            &key_set.keys[0],
            if is_index {
                KeyType::Prefix
            } else {
                KeyType::FullKey
            },
            is_index,
        )
        .unwrap_or_default();
    }

    let mut target = TargetRange::default();
    for point_key in &key_set.keys {
        if let Some(t) = encode_key_internal(recipe, point_key, KeyType::FullKey, is_index) {
            target.merge_from(t);
        }
    }
    for range in &key_set.ranges {
        target.merge_from(key_range_to_target_range(recipe, range, is_index));
    }
    target
}

fn query_params_to_target_range(
    recipe: &KeyRecipe,
    params: &BTreeMap<String, serde_json::Value>,
) -> Option<TargetRange> {
    let mut ss_key = Vec::new();
    let mut parts_count = 0;
    let mut state_end_of_keys = false;

    // Normalizing param keys case-insensitively
    let lowercase_params: HashMap<String, &serde_json::Value> = params
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v))
        .collect();

    for part in &recipe.part {
        if part.tag != 0 {
            let _ = ssformat::append_composite_tag(&mut ss_key, part.tag);
            parts_count += 1;
        } else if part.random() == Some(&true) {
            ssformat::append_int64_increasing(&mut ss_key, 0);
            parts_count += 1;
        } else if let Some(constant_val) = part.value() {
            let val = json_to_spanner_value(constant_val.as_ref());
            let _ = val.encode_into(&mut ss_key, part);
            parts_count += 1;
        } else if let Some(id) = part.identifier() {
            if let Some(json_val) = lowercase_params.get(&id.to_ascii_lowercase()) {
                if matches!(
                    json_val,
                    serde_json::Value::String(_)
                        | serde_json::Value::Number(_)
                        | serde_json::Value::Bool(_)
                        | serde_json::Value::Null
                ) {
                    let val = json_to_spanner_value(json_val);
                    let _ = val.encode_into(&mut ss_key, part);
                    parts_count += 1;
                } else {
                    state_end_of_keys = true;
                    break;
                }
            } else {
                state_end_of_keys = true;
                break;
            }
        } else {
            state_end_of_keys = true;
            break;
        }
    }

    let start = ss_key.clone();
    let mut limit = Vec::new();
    let mut approximate = false;

    if parts_count != recipe.part.len() || state_end_of_keys {
        approximate = true;
        limit = ssformat::make_prefix_successor(&start);
    } else if recipe.index_name().is_some() {
        limit = ssformat::make_prefix_successor(&start);
    }

    Some(TargetRange::new(start, limit, approximate))
}

type ReadShapeKey = (String, String, Vec<String>);

struct TestFinder {
    database_id: AtomicU64,
    next_operation_uid: AtomicU64,
    read_shapes: RwLock<HashMap<ReadShapeKey, u64>>,
    sql_shapes: RwLock<HashMap<String, u64>>,
    schema_generation: RwLock<Bytes>,
    key_range_cache: KeyRangeCache,
    key_recipe_cache: KeyRecipeCache,
    unhealthy_servers: RwLock<HashSet<String>>,
}

impl TestFinder {
    fn new() -> Self {
        Self {
            database_id: AtomicU64::new(0),
            next_operation_uid: AtomicU64::new(1),
            read_shapes: RwLock::new(HashMap::new()),
            sql_shapes: RwLock::new(HashMap::new()),
            schema_generation: RwLock::new(Bytes::new()),
            key_range_cache: KeyRangeCache::new(),
            key_recipe_cache: KeyRecipeCache::new(),
            unhealthy_servers: RwLock::new(HashSet::new()),
        }
    }

    fn reset(&self) {
        self.database_id.store(0, Ordering::Relaxed);
        self.next_operation_uid.store(1, Ordering::Relaxed);
        self.read_shapes
            .write()
            .expect("poisoned read shapes lock")
            .clear();
        self.sql_shapes
            .write()
            .expect("poisoned sql shapes lock")
            .clear();
        *self
            .schema_generation
            .write()
            .expect("poisoned schema lock") = Bytes::new();
        self.key_range_cache.clear();
        self.key_recipe_cache.clear();
        self.unhealthy_servers
            .write()
            .expect("poisoned unhealthy lock")
            .clear();
    }

    fn apply_cache_update(&self, update: &CacheUpdate) {
        if update.database_id != 0 {
            let current_db = self.database_id.load(Ordering::Relaxed);
            if current_db != 0 && current_db != update.database_id {
                self.read_shapes
                    .write()
                    .expect("poisoned read shapes lock")
                    .clear();
                self.sql_shapes
                    .write()
                    .expect("poisoned sql shapes lock")
                    .clear();
                self.key_range_cache.clear();
                self.key_recipe_cache.clear();
                *self
                    .schema_generation
                    .write()
                    .expect("poisoned schema lock") = Bytes::new();
            }
            self.database_id
                .store(update.database_id, Ordering::Relaxed);
        }

        if let Some(recipes) = &update.key_recipes {
            if !recipes.schema_generation.is_empty() {
                let mut schema_gen = self
                    .schema_generation
                    .write()
                    .expect("poisoned schema lock");
                if *schema_gen != recipes.schema_generation {
                    self.key_recipe_cache.clear();
                    *schema_gen = recipes.schema_generation.clone();
                }
            }
            for recipe in &recipes.recipe {
                self.key_recipe_cache.insert(recipe.clone());
            }
        }

        self.key_range_cache.add_ranges(update);
    }

    fn find_server_read(&self, req: &ParsedReadRequest) -> (Option<String>, ParsedRoutingHint) {
        let mut hint = ParsedRoutingHint::default();

        let op_uid = {
            let key = (req.table.clone(), req.index.clone(), req.columns.clone());
            let mut shapes = self.read_shapes.write().expect("poisoned read shapes lock");
            if let Some(&uid) = shapes.get(&key) {
                uid
            } else {
                let uid = self.next_operation_uid.fetch_add(1, Ordering::Relaxed);
                shapes.insert(key, uid);
                uid
            }
        };
        hint.operation_uid = Some(op_uid);

        let db_id = self.database_id.load(Ordering::Relaxed);
        if db_id != 0 {
            hint.database_id = Some(db_id);
        }

        let schema_gen = self
            .schema_generation
            .read()
            .expect("poisoned schema lock")
            .clone();
        if !schema_gen.is_empty() {
            hint.schema_generation = Some(schema_gen.to_vec());
        }

        let is_index = !req.index.is_empty();
        let recipe = if is_index {
            self.key_recipe_cache.get_index_recipe(&req.index)
        } else {
            self.key_recipe_cache.get_table_recipe(&req.table)
        };

        if let Some(recipe) = recipe {
            let target_range = key_set_to_target_range(&recipe, &req.key_set, is_index);
            if !target_range.start.is_empty() {
                hint.key = Some(target_range.start);
            }
            if !target_range.limit.is_empty() {
                hint.limit_key = Some(target_range.limit);
            }
        }

        // Query KeyRangeCache with RangeMode::CoveringSplit
        let selected_server = if let Some(search_key) = &hint.key {
            let search_limit = hint.limit_key.as_deref().unwrap_or(&[]);
            if let Some(range) =
                self.key_range_cache
                    .find_range(search_key, search_limit, RangeMode::CoveringSplit)
            {
                hint.group_uid = Some(range.group_uid);
                hint.split_id = Some(range.split_id);
                hint.key = Some(range.start_key.to_vec());
                hint.limit_key = Some(range.limit_key.to_vec());

                // Select tablet excluding unhealthy servers
                let unhealthy = self
                    .unhealthy_servers
                    .read()
                    .expect("poisoned unhealthy lock");
                let tablet = self
                    .key_range_cache
                    .select_tablet(&range, req.prefer_leader);

                if let Some(t) = tablet {
                    if unhealthy.contains(&t.server_address) {
                        hint.skipped_tablet_uids.push(ParsedTabletUid {
                            tablet_uid: t.tablet_uid,
                            incarnation: if t.incarnation.is_empty() {
                                None
                            } else {
                                Some(t.incarnation.to_vec())
                            },
                        });
                        None
                    } else {
                        hint.tablet_uid = Some(t.tablet_uid);
                        Some(t.server_address)
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        (selected_server, hint)
    }

    fn find_server_sql(&self, req: &ParsedSqlRequest) -> (Option<String>, ParsedRoutingHint) {
        let mut hint = ParsedRoutingHint::default();

        let op_uid = {
            let mut shapes = self.sql_shapes.write().expect("poisoned sql shapes lock");
            if let Some(&uid) = shapes.get(&req.sql) {
                uid
            } else {
                let uid = self.next_operation_uid.fetch_add(1, Ordering::Relaxed);
                shapes.insert(req.sql.clone(), uid);
                uid
            }
        };
        hint.operation_uid = Some(op_uid);

        let db_id = self.database_id.load(Ordering::Relaxed);
        if db_id != 0 {
            hint.database_id = Some(db_id);
        }

        let schema_gen = self
            .schema_generation
            .read()
            .expect("poisoned schema lock")
            .clone();
        if !schema_gen.is_empty() {
            hint.schema_generation = Some(schema_gen.to_vec());
        }

        let recipe = self.key_recipe_cache.get_query_recipe(op_uid);
        if let Some(recipe) = recipe
            && let Some(target_range) = query_params_to_target_range(&recipe, &req.params)
        {
            if !target_range.start.is_empty() {
                hint.key = Some(target_range.start);
            }
            if !target_range.limit.is_empty() {
                hint.limit_key = Some(target_range.limit);
            }
        }

        // Query KeyRangeCache with RangeMode::PickRandom
        let selected_server = if let Some(search_key) = &hint.key {
            let search_limit = hint.limit_key.as_deref().unwrap_or(&[]);
            if let Some(range) =
                self.key_range_cache
                    .find_range(search_key, search_limit, RangeMode::PickRandom)
            {
                hint.group_uid = Some(range.group_uid);
                hint.split_id = Some(range.split_id);
                hint.key = Some(range.start_key.to_vec());
                hint.limit_key = Some(range.limit_key.to_vec());

                // Select tablet excluding unhealthy servers
                let unhealthy = self
                    .unhealthy_servers
                    .read()
                    .expect("poisoned unhealthy lock");
                let tablet = self
                    .key_range_cache
                    .select_tablet(&range, req.prefer_leader);

                if let Some(t) = tablet {
                    if unhealthy.contains(&t.server_address) {
                        hint.skipped_tablet_uids.push(ParsedTabletUid {
                            tablet_uid: t.tablet_uid,
                            incarnation: if t.incarnation.is_empty() {
                                None
                            } else {
                                Some(t.incarnation.to_vec())
                            },
                        });
                        None
                    } else {
                        hint.tablet_uid = Some(t.tablet_uid);
                        Some(t.server_address)
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        (selected_server, hint)
    }
}

#[test]
fn test_query_params_with_null_param() {
    use crate::model::TypeCode;
    use crate::model::key_recipe::part::Order;

    let part1 = Part::new()
        .set_tag(50020u32)
        .set_type(crate::model::Type::new().set_code(TypeCode::Int64))
        .set_identifier("col1")
        .set_order(Order::Ascending);
    let recipe = KeyRecipe::new().set_part(vec![part1]);

    let mut params = BTreeMap::new();
    params.insert("col1".to_string(), serde_json::Value::Null);

    let target = query_params_to_target_range(&recipe, &params)
        .expect("query_params_to_target_range should succeed with null parameter");
    assert!(!target.start.is_empty());
}
