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

use crate::key::{Key, KeySet};
use crate::model::Mutation as ProtoMutation;
use crate::model::mutation::{Operation as ProtoOperation, Write as ProtoWrite};
use crate::mutation::{Mutation, WriteBuilder};
use crate::routing::key_extractor::{extract_key_from_mutation, extract_key_from_proto_mutation};
use crate::routing::key_recipe::{encode_key_from_query_params, encode_key_from_recipe};
use crate::routing::textproto_test_utils::{json_to_spanner_value, parse_recipe_golden_textproto};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

fn populate_builder(mut builder: WriteBuilder, write: &ProtoWrite) -> Option<Mutation> {
    let first_row = write.values.first()?;
    for (column, value) in write.columns.iter().zip(first_row.iter()) {
        builder = builder.set(column).to(json_to_spanner_value(value));
    }
    Some(builder.build())
}

fn proto_mutation_to_high_level_mutation(proto: &ProtoMutation) -> Option<Mutation> {
    match &proto.operation {
        Some(ProtoOperation::Insert(write)) => {
            populate_builder(Mutation::new_insert_builder(&write.table), write)
        }
        Some(ProtoOperation::Update(write)) => {
            populate_builder(Mutation::new_update_builder(&write.table), write)
        }
        Some(ProtoOperation::InsertOrUpdate(write)) => {
            populate_builder(Mutation::new_insert_or_update_builder(&write.table), write)
        }
        Some(ProtoOperation::Replace(write)) => {
            populate_builder(Mutation::new_replace_builder(&write.table), write)
        }
        Some(ProtoOperation::Delete(delete)) => {
            let key_set = delete.key_set.as_ref()?;
            let first_key = key_set.keys.first()?;
            let mut spanner_keys = Vec::new();
            for value in first_key {
                spanner_keys.push(json_to_spanner_value(value));
            }
            Some(Mutation::delete(
                &delete.table,
                KeySet::from(Key::new(spanner_keys)),
            ))
        }
        _ => None,
    }
}

#[test]
fn golden_conformance_supported_types() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("routing")
        .join("testdata")
        .join("recipe_test.textproto");

    let textproto = fs::read_to_string(&path)
        .expect("failed to load Spanner golden testdata from recipe_test.textproto");

    let cases = parse_recipe_golden_textproto(&textproto);
    assert_eq!(
        cases.len(),
        38,
        "recipe_test.textproto must parse all 38 Spanner golden test cases"
    );

    // Execute golden conformance tests for all supported key column data types, table mutations, and SQL query parameter cases.
    // Full table key sets and composite struct resolution will be enabled in subsequent pull requests.
    let supported_test_prefixes = [
        "DataTypeTest_BOOL",
        "DataTypeTest_INT64",
        "DataTypeTest_FLOAT64",
        "DataTypeTest_STRING",
        "DataTypeTest_BYTES",
        "DataTypeTest_DATE",
        "DataTypeTest_TIMESTAMP",
        "DataTypeTest_UUID",
        "DataTypeTest_ENUM",
        "NotNull",
        "NullsLast",
        "MultiPart",
        "Interleaved",
        "GeneratedKeyColumns",
        "QueryEncoding",
        "SimpleMutations",
        "QueueMutations",
    ];

    let mut tests_per_prefix: HashMap<&'static str, usize> =
        supported_test_prefixes.iter().map(|&p| (p, 0)).collect();

    let mut executed_tests = 0;

    for case in &cases {
        let matching_prefix = supported_test_prefixes
            .iter()
            .copied()
            .find(|&prefix| case.name.starts_with(prefix));

        let Some(prefix) = matching_prefix else {
            continue;
        };

        for (index, test) in case.tests.iter().enumerate() {
            // In Spanner's `recipe_test.textproto`, tests marked `approximate: true` represent
            // cases where an invalid value type was provided or partial prefixes were tested.
            // These will be verified against the TargetRange fallback router in a subsequent pull request.
            if test.approximate {
                continue;
            }

            let encoded = if let Some(params) = &test.query_params {
                match encode_key_from_query_params(&case.recipe, params) {
                    Ok(bytes) => bytes,
                    Err(e) => panic!(
                        "Golden query test case {} index {} failed encoding: {}",
                        case.name, index, e
                    ),
                }
            } else if let Some(mutation) = &test.mutation {
                // When convertible, verify that our high-level crate::mutation::Mutation
                // extractor produces the exact same routing key as the proto extractor.
                if let Some(high_level_mutation) = proto_mutation_to_high_level_mutation(mutation) {
                    let high_level_key =
                        extract_key_from_mutation(&case.recipe, &high_level_mutation)
                            .expect("extract_key_from_mutation should succeed")
                            .expect("extract_key_from_mutation should return routing key");
                    assert_eq!(
                        &high_level_key, &test.start,
                        "Mismatch in high-level mutation golden test case {} at index {}: expected {:?}, got {:?}",
                        case.name, index, test.start, high_level_key
                    );
                }

                match extract_key_from_proto_mutation(&case.recipe, mutation) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => panic!(
                        "Golden mutation test case {} index {} returned None routing key",
                        case.name, index
                    ),
                    Err(e) => panic!(
                        "Golden mutation test case {} index {} failed encoding: {}",
                        case.name, index, e
                    ),
                }
            } else {
                match encode_key_from_recipe(&case.recipe, &test.values) {
                    Ok(bytes) => bytes,
                    Err(e) => panic!(
                        "Golden test case {} index {} failed encoding: {}",
                        case.name, index, e
                    ),
                }
            };

            assert_eq!(
                &encoded, &test.start,
                "Mismatch in golden test case {} at test index {}: expected {:?}, got {:?}",
                case.name, index, test.start, encoded
            );
            executed_tests += 1;
            *tests_per_prefix
                .get_mut(prefix)
                .expect("matching prefix must exist in prefix counter map") += 1;
        }
    }

    // Verify that every single supported prefix actually executed tests (prevents dead prefixes).
    for (prefix, count) in &tests_per_prefix {
        assert!(
            *count > 0,
            "Golden test prefix '{prefix}' was configured as supported but executed 0 tests! \
             Every supported prefix in the test harness must execute at least one test."
        );
    }

    assert_eq!(
        executed_tests, 95,
        "Expected exactly 95 golden test vectors for supported types, executed {executed_tests}"
    );
}
