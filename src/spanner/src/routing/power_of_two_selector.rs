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

//! Power of 2 Random Choices (P2C) candidate selection algorithm.
//!
//! Provides candidate selection algorithms to balance traffic load across multiple options
//! while preferring lower-cost or lower-latency candidates.

// TODO(location-aware-routing): Remove allow(dead_code) once integrated into LocationRouter and KeyRangeCache.
#![allow(dead_code)]

use rand::RngExt;
use rand::rng;
use std::cmp::Ordering;
use std::fmt::Debug;

/// Candidate selector implementing the "Power of 2 Random Choices" (P2C) strategy.
///
/// Samples 2 distinct random candidates without replacement from an eligible candidate slice
/// and selects the candidate with the lower cost score. This balances load across healthy
/// candidates and prevents thundering herds to a single lowest-latency node.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PowerOfTwoSelector;

impl PowerOfTwoSelector {
    /// Creates a new `PowerOfTwoSelector`.
    pub(crate) fn new() -> Self {
        Self
    }

    /// Selects the index of a candidate from `candidates` using the provided random number generator.
    ///
    /// - Returns `None` if `candidates` is empty.
    /// - Returns `Some(0)` if `candidates` contains exactly 1 candidate (without invoking `score_lookup`).
    /// - Samples 2 distinct random candidates without replacement and chooses the one with the lower score.
    /// - In case of a tie in score, the first sampled candidate is preferred.
    pub(crate) fn select_with_rng<T, S, F, R>(
        &self,
        candidates: &[T],
        mut score_lookup: F,
        rng: &mut R,
    ) -> Option<usize>
    where
        F: FnMut(&T) -> S,
        S: PartialOrd,
        R: RngExt + ?Sized,
    {
        if candidates.is_empty() {
            return None;
        }
        if candidates.len() == 1 {
            return Some(0);
        }

        let total_candidates = candidates.len();
        let first_index = rng.random_range(0..total_candidates);
        let mut second_index = rng.random_range(0..total_candidates - 1);
        if second_index >= first_index {
            second_index += 1;
        }

        let first_score = score_lookup(&candidates[first_index]);
        let second_score = score_lookup(&candidates[second_index]);

        let is_first_preferred = match first_score.partial_cmp(&second_score) {
            Some(Ordering::Less | Ordering::Equal) => true,
            Some(Ordering::Greater) => false,
            // If comparison is undefined (e.g. NaN floating-point numbers), prefer the valid score.
            None => first_score.partial_cmp(&first_score).is_some(),
        };

        if is_first_preferred {
            return Some(first_index);
        }
        Some(second_index)
    }

    /// Selects the index of a candidate using the thread-local random number generator.
    pub(crate) fn select_index<T, S, F>(&self, candidates: &[T], score_lookup: F) -> Option<usize>
    where
        F: FnMut(&T) -> S,
        S: PartialOrd,
    {
        let mut local_rng = rng();
        self.select_with_rng(candidates, score_lookup, &mut local_rng)
    }

    /// Selects a candidate reference from the given slice based on the scoring function.
    pub(crate) fn select<'a, T, S, F>(&self, candidates: &'a [T], score_lookup: F) -> Option<&'a T>
    where
        F: FnMut(&T) -> S,
        S: PartialOrd,
    {
        let selected_index = self.select_index(candidates, score_lookup)?;
        candidates.get(selected_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use std::collections::HashMap;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(
            PowerOfTwoSelector: Send,
            Sync,
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Default
        );

        let selector: PowerOfTwoSelector = Default::default();
        let cloned_selector = selector;
        assert_eq!(
            selector, cloned_selector,
            "selector must equal its copy/clone"
        );
    }

    #[test]
    fn empty_candidates_returns_none() {
        let selector = PowerOfTwoSelector::new();
        let candidates: [String; 0] = [];

        let selected_index = selector.select_index(&candidates, |_| 10.0);
        assert_eq!(
            selected_index, None,
            "empty candidate slice must return None for select_index"
        );

        let selected_ref = selector.select(&candidates, |_| 10.0);
        assert_eq!(
            selected_ref, None,
            "empty candidate slice must return None for select"
        );
    }

    #[test]
    fn single_candidate_returns_first() {
        let selector = PowerOfTwoSelector::new();
        let candidates = vec!["10.0.0.1:15000"];

        let mut lookup_invoked = false;
        let selected_index = selector.select_index(&candidates, |_| {
            lookup_invoked = true;
            50.0
        });
        assert_eq!(
            selected_index,
            Some(0),
            "single candidate must return index 0"
        );
        assert!(
            !lookup_invoked,
            "score lookup must not be called when only 1 candidate exists"
        );

        let selected_ref = selector.select(&candidates, |_| 50.0);
        assert_eq!(
            selected_ref,
            Some(&"10.0.0.1:15000"),
            "single candidate must return reference to element 0"
        );
    }

    #[test]
    fn two_candidates_picks_lower_score() {
        let selector = PowerOfTwoSelector::new();
        let candidates = vec!["fast_endpoint", "slow_endpoint"];

        let score_map: HashMap<&str, f64> = [("fast_endpoint", 10.0), ("slow_endpoint", 100.0)]
            .into_iter()
            .collect();

        // With 2 candidates, both are sampled (since indices are 0 and 1).
        // The faster endpoint (10.0) must always win.
        for _ in 0..50 {
            let selected = selector.select(&candidates, |candidate| {
                *score_map
                    .get(candidate)
                    .expect("candidate score must exist in map")
            });
            assert_eq!(
                selected,
                Some(&"fast_endpoint"),
                "fast endpoint with lower score must be selected"
            );
        }
    }

    #[test]
    fn integer_and_tuple_multi_criteria_scoring() {
        let selector = PowerOfTwoSelector::new();

        struct Node {
            name: &'static str,
            distance: u32,
            active_requests: usize,
        }

        let candidates = vec![
            Node {
                name: "local_idle",
                distance: 0,
                active_requests: 0,
            },
            Node {
                name: "local_busy",
                distance: 0,
                active_requests: 5,
            },
            Node {
                name: "remote_idle",
                distance: 1,
                active_requests: 0,
            },
        ];

        // Multi-criteria tuple scoring: (distance, active_requests)
        for _ in 0..20 {
            let selected = selector
                .select(&candidates, |node| (node.distance, node.active_requests))
                .expect("selection succeeds");
            assert_ne!(
                selected.name, "remote_idle",
                "remote node must not beat local nodes"
            );
        }
    }

    #[test]
    fn nan_floating_point_score_handling() {
        let selector = PowerOfTwoSelector::new();
        let candidates = vec!["valid_score", "nan_score"];

        // Test symmetric comparison: valid score must always defeat NaN score
        for _ in 0..50 {
            let selected = selector.select(&candidates, |candidate| match *candidate {
                "valid_score" => 10.0,
                "nan_score" => f64::NAN,
                _ => unreachable!(),
            });

            assert_eq!(
                selected,
                Some(&"valid_score"),
                "valid finite score must beat NaN score"
            );
        }

        // When both scores are NaN, selection still returns a valid candidate index
        let nan_candidates = vec!["nan_1", "nan_2"];
        let selected = selector.select(&nan_candidates, |_| f64::NAN);
        assert!(
            selected == Some(&"nan_1") || selected == Some(&"nan_2"),
            "when both scores are NaN, selection must still return a valid candidate"
        );
    }

    #[test]
    fn tie_breaking_picks_sampled_candidate() {
        let selector = PowerOfTwoSelector::new();
        let candidates = vec!["endpoint_a", "endpoint_b"];

        // Both candidates have identical scores
        let selected = selector.select(&candidates, |_| 42.0);
        assert!(
            selected == Some(&"endpoint_a") || selected == Some(&"endpoint_b"),
            "tie breaking must select one of the valid candidates"
        );
    }

    #[test]
    fn deterministic_seeded_rng_selection() {
        let selector = PowerOfTwoSelector::new();
        let candidates = vec!["endpoint_0", "endpoint_1", "endpoint_2", "endpoint_3"];

        // Endpoint 3 has the lowest score (10.0)
        let scores = [100.0, 50.0, 80.0, 10.0];

        let mut seeded_rng = SmallRng::seed_from_u64(12345);
        let selected_index = selector
            .select_with_rng(
                &candidates,
                |candidate| {
                    let index = candidates
                        .iter()
                        .position(|c| c == candidate)
                        .expect("candidate in list");
                    scores[index]
                },
                &mut seeded_rng,
            )
            .expect("selection must succeed");

        assert!(
            selected_index < candidates.len(),
            "selected index must be valid"
        );
    }

    #[test]
    fn mutable_closure_score_lookup() {
        let selector = PowerOfTwoSelector::new();
        let candidates = vec!["endpoint_0", "endpoint_1", "endpoint_2"];
        let mut lookup_count = 0usize;

        let selected = selector.select(&candidates, |_| {
            lookup_count += 1;
            25.0
        });

        assert!(selected.is_some(), "selection must succeed");
        assert_eq!(
            lookup_count, 2,
            "P2C must evaluate exactly 2 candidate scores for 3 candidates"
        );
    }

    #[test]
    fn distribution_across_multiple_replicas() {
        let selector = PowerOfTwoSelector::new();
        let candidates = vec!["node_0", "node_1", "node_2", "node_3"];
        // Pairs of nodes with low (10.0) and moderate (20.0) scores.
        let scores = [10.0, 10.0, 20.0, 20.0];

        let mut selection_counts = vec![0usize; candidates.len()];
        let total_iterations = 10_000;

        for _ in 0..total_iterations {
            let selected_index = selector
                .select_index(&candidates, |candidate| {
                    let index = candidates
                        .iter()
                        .position(|c| c == candidate)
                        .expect("candidate in list");
                    scores[index]
                })
                .expect("selection must succeed for non-empty list");

            selection_counts[selected_index] += 1;
        }

        // Lower-score nodes (0 and 1) should be selected more frequently than higher-score nodes (2 and 3)
        assert!(
            selection_counts[0] > selection_counts[2],
            "low-score node_0 ({}) must be selected more often than higher-score node_2 ({})",
            selection_counts[0],
            selection_counts[2]
        );
        assert!(
            selection_counts[1] > selection_counts[3],
            "low-score node_1 ({}) must be selected more often than higher-score node_3 ({})",
            selection_counts[1],
            selection_counts[3]
        );

        // Every candidate node should receive traffic when tied or paired with an equal or worse score
        for (index, count) in selection_counts.iter().enumerate() {
            assert!(
                *count > 0,
                "node_{index} should receive at least some traffic under P2C"
            );
        }
    }
}
