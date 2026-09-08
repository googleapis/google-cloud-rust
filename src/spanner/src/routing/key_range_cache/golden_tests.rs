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

use crate::model::Tablet;
use crate::routing::key_range_cache::{CachedRange, KeyRangeCache};
use crate::routing::textproto_test_utils::{
    RangeCacheQueryTest, parse_range_cache_golden_textproto,
};
use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;

fn assert_matched_range(
    range: &CachedRange,
    tablet: Option<&Tablet>,
    case_name: &str,
    test_index: usize,
    test: &RangeCacheQueryTest,
) {
    if let Some(expected_group_uid) = test.expected_result.group_uid {
        assert_eq!(
            range.group_uid, expected_group_uid,
            "Group UID mismatch in test case {case_name} test index {test_index}"
        );
    }
    if let Some(expected_split_id) = test.expected_result.split_id {
        assert_eq!(
            range.split_id, expected_split_id,
            "Split ID mismatch in test case {case_name} test index {test_index}"
        );
    }
    if let Some(expected_key) = &test.expected_result.key {
        assert_eq!(
            range.start_key.as_ref(),
            expected_key.as_slice(),
            "Start key mismatch in test case {case_name} test index {test_index}"
        );
    }
    if let Some(expected_limit_key) = &test.expected_result.limit_key {
        assert_eq!(
            range.limit_key.as_ref(),
            expected_limit_key.as_slice(),
            "Limit key mismatch in test case {case_name} test index {test_index}"
        );
    }
    if let Some(expected_server) = &test.expected_server {
        let actual_server = tablet
            .map(|tablet_entry| tablet_entry.server_address.as_str())
            .unwrap_or("");
        assert_eq!(
            actual_server, expected_server,
            "Server address mismatch in test case {case_name} test index {test_index}"
        );
    }
    if let Some(expected_tablet_uid) = test.expected_result.tablet_uid {
        assert_eq!(
            tablet.map(|tablet_entry| tablet_entry.tablet_uid),
            Some(expected_tablet_uid),
            "Tablet UID mismatch in test case {case_name} test index {test_index}"
        );
    }
}

fn assert_unmatched_range(case_name: &str, test_index: usize, test: &RangeCacheQueryTest) {
    assert!(
        test.expected_server.is_none(),
        "Expected server {:?} but got no range match in test case {case_name} test index {test_index}",
        test.expected_server
    );
    assert!(
        test.expected_result.tablet_uid.is_none(),
        "Expected tablet {:?} but got no range match in test case {case_name} test index {test_index}",
        test.expected_result.tablet_uid
    );
}

fn run_single_golden_test(
    cache: &KeyRangeCache,
    case_name: &str,
    test_index: usize,
    test: &RangeCacheQueryTest,
) {
    let min_entries = test.min_cache_entries_for_random_pick.unwrap_or(1000);
    cache
        .min_cache_entries_for_random_pick
        .store(min_entries, Ordering::Relaxed);

    let search_key = test.key.as_deref().unwrap_or(&[]);
    let search_limit = test.limit_key.as_deref().unwrap_or(&[]);
    let matched_range = cache.find_range(search_key, search_limit, test.range_mode);

    if let Some(range) = matched_range {
        let tablet = cache.select_tablet(&range, test.leader);
        assert_matched_range(&range, tablet.as_ref(), case_name, test_index, test);
    } else {
        assert_unmatched_range(case_name, test_index, test);
    }
}

#[test]
fn golden_conformance_range_cache() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("routing")
        .join("testdata")
        .join("range_cache_test.textproto");

    let textproto = fs::read_to_string(&path)
        .expect("failed to load Spanner golden testdata from range_cache_test.textproto");

    let cases = parse_range_cache_golden_textproto(&textproto);
    assert_eq!(
        cases.len(),
        13,
        "range_cache_test.textproto must parse all 13 Spanner golden test cases"
    );

    let mut executed_tests = 0;

    for case in &cases {
        // Directed read options will be enabled in its dedicated PR.
        if case.name == "directed_read_options" {
            continue;
        }

        let cache = KeyRangeCache::new();
        cache.use_deterministic_random();

        for step in &case.steps {
            if let Some(update) = &step.update {
                cache.add_ranges(update);
            }

            for (test_index, test) in step.tests.iter().enumerate() {
                run_single_golden_test(&cache, &case.name, test_index, test);
                executed_tests += 1;
            }
        }
    }

    assert_eq!(
        executed_tests, 45,
        "Expected exactly 45 golden test executions (excluding directed read options), ran {executed_tests}"
    );
}
