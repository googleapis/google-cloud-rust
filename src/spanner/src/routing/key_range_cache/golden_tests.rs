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

use crate::model::tablet::Role;
use crate::model::{CacheUpdate, Group, Range, Tablet};
use crate::routing::key_range_cache::{CachedRange, KeyRangeCache, RangeMode};
use crate::routing::textproto_test_utils::{extract_value, unescape_bytes};
use bytes::Bytes;
use std::fs;
use std::iter::Peekable;
use std::path::Path;
use std::sync::atomic::Ordering;

#[derive(Debug)]
struct RangeCacheTestCase {
    name: String,
    steps: Vec<ParsedStep>,
}

#[derive(Debug)]
struct ParsedStep {
    update: Option<CacheUpdate>,
    tests: Vec<ParsedQueryTest>,
}

#[derive(Debug)]
struct ParsedQueryTest {
    key: Option<Vec<u8>>,
    limit_key: Option<Vec<u8>>,
    min_cache_entries_for_random_pick: Option<usize>,
    range_mode: RangeMode,
    leader: bool,
    expected_result: ExpectedResult,
    expected_server: Option<String>,
}

#[derive(Debug, Default)]
struct ExpectedResult {
    key: Option<Vec<u8>>,
    limit_key: Option<Vec<u8>>,
    group_uid: Option<u64>,
    split_id: Option<u64>,
    tablet_uid: Option<u64>,
}

fn parse_tablet_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> Tablet {
    let mut tablet_uid = 0u64;
    let mut server_address = String::new();
    let mut location = String::new();
    let mut role = Role::Unspecified;
    let mut incarnation = Bytes::new();
    let mut distance = 0u32;
    let mut skip = false;
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some(string_value) = extract_value(trimmed, "tablet_uid:") {
            tablet_uid = string_value.parse::<u64>().unwrap_or(0);
        } else if let Some(string_value) = extract_value(trimmed, "server_address:") {
            server_address = string_value.to_string();
        } else if let Some(string_value) = extract_value(trimmed, "location:") {
            location = string_value.to_string();
        } else if let Some(string_value) = extract_value(trimmed, "role:") {
            role = match string_value {
                "READ_WRITE" => Role::ReadWrite,
                "READ_ONLY" => Role::ReadOnly,
                _ => Role::Unspecified,
            };
        } else if let Some(string_value) = extract_value(trimmed, "incarnation:") {
            incarnation = Bytes::from(unescape_bytes(string_value));
        } else if let Some(string_value) = extract_value(trimmed, "distance:") {
            distance = string_value.parse::<u32>().unwrap_or(0);
        } else if let Some(string_value) = extract_value(trimmed, "skip:") {
            skip = string_value == "true";
        }
    }

    Tablet {
        tablet_uid,
        server_address,
        location,
        role,
        incarnation,
        distance,
        skip,
        _unknown_fields: Default::default(),
    }
}

fn parse_group_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> Group {
    let mut group_uid = 0u64;
    let mut generation = Bytes::new();
    let mut leader_index = -1i32;
    let mut tablets = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("tablets {") {
            tablets.push(parse_tablet_block(lines));
        } else if let Some(string_value) = extract_value(trimmed, "group_uid:") {
            group_uid = string_value.parse::<u64>().unwrap_or(0);
        } else if let Some(string_value) = extract_value(trimmed, "generation:") {
            generation = Bytes::from(unescape_bytes(string_value));
        } else if let Some(string_value) = extract_value(trimmed, "leader_index:") {
            leader_index = string_value.parse::<i32>().unwrap_or(-1);
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    Group {
        group_uid,
        generation,
        tablets,
        leader_index,
        _unknown_fields: Default::default(),
    }
}

fn parse_range_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> Range {
    let mut start_key = Bytes::new();
    let mut limit_key = Bytes::new();
    let mut group_uid = 0u64;
    let mut split_id = 0u64;
    let mut generation = Bytes::new();
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some(string_value) = extract_value(trimmed, "start_key:") {
            start_key = Bytes::from(unescape_bytes(string_value));
        } else if let Some(string_value) = extract_value(trimmed, "limit_key:") {
            limit_key = Bytes::from(unescape_bytes(string_value));
        } else if let Some(string_value) = extract_value(trimmed, "group_uid:") {
            group_uid = string_value.parse::<u64>().unwrap_or(0);
        } else if let Some(string_value) = extract_value(trimmed, "split_id:") {
            split_id = string_value.parse::<u64>().unwrap_or(0);
        } else if let Some(string_value) = extract_value(trimmed, "generation:") {
            generation = Bytes::from(unescape_bytes(string_value));
        }
    }

    Range {
        start_key,
        limit_key,
        group_uid,
        split_id,
        generation,
        _unknown_fields: Default::default(),
    }
}

fn parse_update_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> CacheUpdate {
    let mut ranges = Vec::new();
    let mut groups = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("range {") {
            ranges.push(parse_range_block(lines));
        } else if trimmed.starts_with("group {") {
            groups.push(parse_group_block(lines));
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    CacheUpdate {
        range: ranges,
        group: groups,
        ..Default::default()
    }
}

fn parse_result_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> ExpectedResult {
    let mut result = ExpectedResult::default();
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some(string_value) = extract_value(trimmed, "key:") {
            result.key = Some(unescape_bytes(string_value));
        } else if let Some(string_value) = extract_value(trimmed, "limit_key:") {
            result.limit_key = Some(unescape_bytes(string_value));
        } else if let Some(string_value) = extract_value(trimmed, "group_uid:") {
            result.group_uid = string_value.parse::<u64>().ok();
        } else if let Some(string_value) = extract_value(trimmed, "split_id:") {
            result.split_id = string_value.parse::<u64>().ok();
        } else if let Some(string_value) = extract_value(trimmed, "tablet_uid:") {
            result.tablet_uid = string_value.parse::<u64>().ok();
        }
    }

    result
}

fn parse_test_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> ParsedQueryTest {
    let mut key = None;
    let mut limit_key = None;
    let mut min_cache_entries_for_random_pick = None;
    let mut range_mode = RangeMode::CoveringSplit;
    let mut leader = false;
    let mut expected_result = ExpectedResult::default();
    let mut expected_server = None;
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("result {") {
            expected_result = parse_result_block(lines);
        } else if trimmed.starts_with("directed_read_options {") {
            let mut sub_depth = 1;
            for sub_line in lines.by_ref() {
                let sub_trimmed = sub_line.trim();
                if sub_trimmed.ends_with('{') {
                    sub_depth += 1;
                } else if sub_trimmed == "}" {
                    sub_depth -= 1;
                    if sub_depth == 0 {
                        break;
                    }
                }
            }
        } else if let Some(string_value) = extract_value(trimmed, "key:") {
            key = Some(unescape_bytes(string_value));
        } else if let Some(string_value) = extract_value(trimmed, "limit_key:") {
            limit_key = Some(unescape_bytes(string_value));
        } else if let Some(string_value) =
            extract_value(trimmed, "min_cache_entries_for_random_pick:")
        {
            min_cache_entries_for_random_pick = string_value.parse::<usize>().ok();
        } else if let Some(string_value) = extract_value(trimmed, "range_mode:") {
            if string_value == "PICK_RANDOM" {
                range_mode = RangeMode::PickRandom;
            }
        } else if let Some(string_value) = extract_value(trimmed, "leader:") {
            leader = string_value == "true";
        } else if let Some(string_value) = extract_value(trimmed, "server:") {
            expected_server = Some(string_value.to_string());
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    ParsedQueryTest {
        key,
        limit_key,
        min_cache_entries_for_random_pick,
        range_mode,
        leader,
        expected_result,
        expected_server,
    }
}

fn parse_step_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> ParsedStep {
    let mut update = None;
    let mut tests = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("update {") {
            update = Some(parse_update_block(lines));
        } else if trimmed.starts_with("test {") {
            tests.push(parse_test_block(lines));
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    ParsedStep { update, tests }
}

fn parse_test_case_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Option<RangeCacheTestCase> {
    let mut name = String::new();
    let mut steps = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if let Some(case_name) = extract_value(trimmed, "name:") {
            name = case_name.to_string();
        } else if trimmed.starts_with("step {") {
            steps.push(parse_step_block(lines));
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    if !name.is_empty() {
        Some(RangeCacheTestCase { name, steps })
    } else {
        None
    }
}

fn parse_range_cache_golden_textproto(content: &str) -> Vec<RangeCacheTestCase> {
    let mut cases = Vec::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        if line.trim().starts_with("test_case {")
            && let Some(case) = parse_test_case_block(&mut lines)
        {
            cases.push(case);
        }
    }

    cases
}

fn assert_matched_range(
    range: &CachedRange,
    tablet: Option<&Tablet>,
    case_name: &str,
    test_index: usize,
    test: &ParsedQueryTest,
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

fn assert_unmatched_range(case_name: &str, test_index: usize, test: &ParsedQueryTest) {
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
    test: &ParsedQueryTest,
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
