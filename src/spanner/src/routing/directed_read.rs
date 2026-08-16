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

//! Replica matching and candidate selection for `DirectedReadOptions`.
//!
//! Evaluates whether candidate `Tablet` replicas satisfy caller-specified
//! `DirectedReadOptions` (`IncludeReplicas` / `ExcludeReplicas`) based on
//! geographical location and replica roles (`ReadOnly` / `ReadWrite`).
//!
//! # Failover Semantics
//!
//! Note that `IncludeReplicas.auto_failover_disabled` is evaluated at the RPC
//! retry and dispatch layer when no routable candidate endpoint is available,
//! rather than during basic replica filtering.

#![allow(dead_code)]

use crate::model::directed_read_options::ReplicaSelection;
use crate::model::directed_read_options::Replicas;
use crate::model::directed_read_options::replica_selection::Type;
use crate::model::tablet::Role;
use crate::model::{DirectedReadOptions, Tablet};

/// Maximum distance for a replica to be considered local (within the same region or metro).
pub(crate) const MAX_LOCAL_REPLICA_DISTANCE: u32 = 5;

/// Returns `true` if `tablet` satisfies the criteria in `selection`.
///
/// # Matching Rules
/// - **Location**: An empty `selection.location` matches any location. If non-empty,
///   `selection.location` must equal `tablet.location`.
/// - **Type / Role**:
///   - `Type::ReadWrite` matches `Role::ReadWrite` and `Role::Unspecified`.
///   - `Type::ReadOnly` matches `Role::ReadOnly`.
///   - `Type::Unspecified` or unknown enum variants match all replica roles.
pub(crate) fn matches_replica_selection(tablet: &Tablet, selection: &ReplicaSelection) -> bool {
    if !selection.location.is_empty() && selection.location != tablet.location {
        return false;
    }
    match selection.r#type {
        Type::ReadWrite => tablet.role == Role::ReadWrite || tablet.role == Role::Unspecified,
        Type::ReadOnly => tablet.role == Role::ReadOnly,
        // When type is not specified (e.g. selecting by location alone, like "location:us-east1"),
        // or has a forward-compatible unknown variant, all replica roles in that location match.
        Type::Unspecified | Type::UnknownValue(_) => true,
    }
}

/// Returns `true` if `tablet` satisfies the given directed read options.
///
/// # Fallback & Selection Semantics
/// - If `options` is `None` or has no replica configuration set, falls back to the default
///   local region/metro distance rule: `tablet.distance <= MAX_LOCAL_REPLICA_DISTANCE`.
/// - `Replicas::IncludeReplicas`: Matches if `tablet` satisfies **any** selector in the include
///   list (an empty include list matches no tablets).
/// - `Replicas::ExcludeReplicas`: Matches if `tablet` satisfies **none** of the selectors in the
///   exclude list (an empty exclude list matches all tablets).
pub(crate) fn matches_directed_read_options(
    tablet: &Tablet,
    options: Option<&DirectedReadOptions>,
) -> bool {
    let Some(options) = options else {
        return tablet.distance <= MAX_LOCAL_REPLICA_DISTANCE;
    };
    let Some(replicas) = &options.replicas else {
        return tablet.distance <= MAX_LOCAL_REPLICA_DISTANCE;
    };

    match replicas {
        Replicas::IncludeReplicas(include) => include
            .replica_selections
            .iter()
            .any(|selection| matches_replica_selection(tablet, selection)),
        Replicas::ExcludeReplicas(exclude) => !exclude
            .replica_selections
            .iter()
            .any(|selection| matches_replica_selection(tablet, selection)),
    }
}

/// Returns an iterator yielding tablets that are routable and match `options`.
pub(crate) fn filter_tablets_by_directed_read<'a>(
    tablets: impl IntoIterator<Item = &'a Tablet>,
    options: Option<&DirectedReadOptions>,
) -> impl Iterator<Item = &'a Tablet> {
    tablets.into_iter().filter(move |tablet| {
        !tablet.skip
            && !tablet.server_address.is_empty()
            && matches_directed_read_options(tablet, options)
    })
}

/// Selects candidate tablet references in the lowest available distance tier that satisfy
/// `options`.
///
/// If `prefer_leader` is `true`, and the designated leader is routable and satisfies `options`,
/// returns a single-element vector containing that leader. Otherwise, finds all matching routable
/// tablets and returns those matching the minimum distance.
pub(crate) fn select_eligible_tablets_for_directed_read<'a>(
    tablets: &'a [Tablet],
    leader_index: Option<usize>,
    prefer_leader: bool,
    options: Option<&DirectedReadOptions>,
) -> Vec<&'a Tablet> {
    if prefer_leader
        && let Some(leader) = leader_index.and_then(|index| tablets.get(index))
        && !leader.skip
        && !leader.server_address.is_empty()
        && options
            .and_then(|options| options.replicas.as_ref())
            .is_none_or(|_| matches_directed_read_options(leader, options))
    {
        return vec![leader];
    }

    let mut minimum_distance = u32::MAX;
    // Pre-allocate capacity up to 4 elements (standard Spanner Paxos groups typically have 3-4 replicas,
    // and fewer will share the exact minimum distance tier), avoiding heap reallocations.
    let mut candidates = Vec::with_capacity(tablets.len().min(4));

    for tablet in filter_tablets_by_directed_read(tablets, options) {
        if tablet.distance < minimum_distance {
            minimum_distance = tablet.distance;
            candidates.clear();
            candidates.push(tablet);
            continue;
        }
        if tablet.distance == minimum_distance {
            candidates.push(tablet);
        }
    }

    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::directed_read_options::{ExcludeReplicas, IncludeReplicas};
    use bytes::Bytes;

    fn make_test_tablet(
        tablet_uid: u64,
        location: &'static str,
        role: Role,
        distance: u32,
    ) -> Tablet {
        Tablet {
            tablet_uid,
            server_address: format!("localhost:800{tablet_uid}"),
            location: location.to_string(),
            role,
            incarnation: Bytes::from_static(b"1"),
            distance,
            skip: false,
            _unknown_fields: Default::default(),
        }
    }

    #[test]
    fn matches_replica_selection_by_location() {
        let tablet = make_test_tablet(1, "us-east1", Role::ReadOnly, 2);

        // Matching location
        let selection_match = ReplicaSelection {
            location: "us-east1".to_string(),
            r#type: Type::Unspecified,
            _unknown_fields: Default::default(),
        };
        assert!(
            matches_replica_selection(&tablet, &selection_match),
            "expected tablet in us-east1 to match selection for us-east1"
        );

        // Mismatched location
        let selection_mismatch = ReplicaSelection {
            location: "us-west1".to_string(),
            r#type: Type::Unspecified,
            _unknown_fields: Default::default(),
        };
        assert!(
            !matches_replica_selection(&tablet, &selection_mismatch),
            "expected tablet in us-east1 to reject selection for us-west1"
        );

        // Empty location matches any location
        let selection_empty_location = ReplicaSelection {
            location: String::new(),
            r#type: Type::Unspecified,
            _unknown_fields: Default::default(),
        };
        assert!(
            matches_replica_selection(&tablet, &selection_empty_location),
            "expected empty location selector to match any tablet location"
        );
    }

    #[test]
    fn matches_replica_selection_by_type() {
        let tablet_ro = make_test_tablet(1, "us-east1", Role::ReadOnly, 2);
        let tablet_rw = make_test_tablet(2, "us-east1", Role::ReadWrite, 2);
        let tablet_unspecified = make_test_tablet(3, "us-east1", Role::Unspecified, 2);

        let selection_ro = ReplicaSelection {
            location: String::new(),
            r#type: Type::ReadOnly,
            _unknown_fields: Default::default(),
        };
        let selection_rw = ReplicaSelection {
            location: String::new(),
            r#type: Type::ReadWrite,
            _unknown_fields: Default::default(),
        };

        // ReadOnly selection
        assert!(
            matches_replica_selection(&tablet_ro, &selection_ro),
            "expected ReadOnly tablet to match ReadOnly selection"
        );
        assert!(
            !matches_replica_selection(&tablet_rw, &selection_ro),
            "expected ReadWrite tablet to reject ReadOnly selection"
        );
        assert!(
            !matches_replica_selection(&tablet_unspecified, &selection_ro),
            "expected Unspecified tablet to reject ReadOnly selection"
        );

        // ReadWrite selection (matches both ReadWrite and Unspecified roles)
        assert!(
            !matches_replica_selection(&tablet_ro, &selection_rw),
            "expected ReadOnly tablet to reject ReadWrite selection"
        );
        assert!(
            matches_replica_selection(&tablet_rw, &selection_rw),
            "expected ReadWrite tablet to match ReadWrite selection"
        );
        assert!(
            matches_replica_selection(&tablet_unspecified, &selection_rw),
            "expected Unspecified role tablet to match ReadWrite selection"
        );
    }

    #[test]
    fn matches_directed_read_options_default_locality() {
        let tablet_local = make_test_tablet(1, "us-central1", Role::ReadWrite, 0);
        let tablet_boundary = make_test_tablet(2, "us-central1", Role::ReadOnly, 5);
        let tablet_remote = make_test_tablet(3, "europe-west1", Role::ReadOnly, 10);

        // When options is None, distance <= 5 is accepted
        assert!(
            matches_directed_read_options(&tablet_local, None),
            "expected local tablet (distance 0) to match default locality"
        );
        assert!(
            matches_directed_read_options(&tablet_boundary, None),
            "expected boundary tablet (distance 5) to match default locality"
        );
        assert!(
            !matches_directed_read_options(&tablet_remote, None),
            "expected remote tablet (distance 10) to reject default locality"
        );

        // When options has no replicas variant, default locality applies
        let options_empty = DirectedReadOptions::default();
        assert!(
            matches_directed_read_options(&tablet_local, Some(&options_empty)),
            "expected local tablet to match empty DirectedReadOptions"
        );
        assert!(
            matches_directed_read_options(&tablet_boundary, Some(&options_empty)),
            "expected boundary tablet to match empty DirectedReadOptions"
        );
        assert!(
            !matches_directed_read_options(&tablet_remote, Some(&options_empty)),
            "expected remote tablet to reject empty DirectedReadOptions"
        );
    }

    #[test]
    fn matches_directed_read_options_include_replicas() {
        let tablet_east = make_test_tablet(1, "us-east1", Role::ReadOnly, 10);
        let tablet_west = make_test_tablet(2, "us-west1", Role::ReadOnly, 10);

        let options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    r#type: Type::ReadOnly,
                    _unknown_fields: Default::default(),
                }],
                auto_failover_disabled: false,
                _unknown_fields: Default::default(),
            }))),
            _unknown_fields: Default::default(),
        };

        assert!(
            matches_directed_read_options(&tablet_east, Some(&options)),
            "expected us-east1 tablet to match IncludeReplicas for us-east1"
        );
        assert!(
            !matches_directed_read_options(&tablet_west, Some(&options)),
            "expected us-west1 tablet to reject IncludeReplicas for us-east1"
        );
    }

    #[test]
    fn matches_directed_read_options_include_replicas_multiple_selectors() {
        let tablet_east = make_test_tablet(1, "us-east1", Role::ReadOnly, 10);
        let tablet_central = make_test_tablet(2, "us-central1", Role::ReadWrite, 10);
        let tablet_west = make_test_tablet(3, "us-west1", Role::ReadOnly, 10);

        let options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![
                    ReplicaSelection {
                        location: "us-east1".to_string(),
                        r#type: Type::ReadOnly,
                        _unknown_fields: Default::default(),
                    },
                    ReplicaSelection {
                        location: "us-central1".to_string(),
                        r#type: Type::ReadWrite,
                        _unknown_fields: Default::default(),
                    },
                ],
                auto_failover_disabled: false,
                _unknown_fields: Default::default(),
            }))),
            _unknown_fields: Default::default(),
        };

        assert!(
            matches_directed_read_options(&tablet_east, Some(&options)),
            "expected us-east1 ReadOnly to match multi-selector IncludeReplicas"
        );
        assert!(
            matches_directed_read_options(&tablet_central, Some(&options)),
            "expected us-central1 ReadWrite to match multi-selector IncludeReplicas"
        );
        assert!(
            !matches_directed_read_options(&tablet_west, Some(&options)),
            "expected us-west1 to reject multi-selector IncludeReplicas"
        );
    }

    #[test]
    fn matches_directed_read_options_exclude_replicas() {
        let tablet_east = make_test_tablet(1, "us-east1", Role::ReadOnly, 2);
        let tablet_west = make_test_tablet(2, "us-west1", Role::ReadOnly, 2);

        let options = DirectedReadOptions {
            replicas: Some(Replicas::ExcludeReplicas(Box::new(ExcludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    r#type: Type::ReadOnly,
                    _unknown_fields: Default::default(),
                }],
                _unknown_fields: Default::default(),
            }))),
            _unknown_fields: Default::default(),
        };

        // us-east1 is excluded
        assert!(
            !matches_directed_read_options(&tablet_east, Some(&options)),
            "expected us-east1 tablet to be excluded by ExcludeReplicas"
        );
        // us-west1 is NOT excluded
        assert!(
            matches_directed_read_options(&tablet_west, Some(&options)),
            "expected us-west1 tablet to pass ExcludeReplicas"
        );
    }

    #[test]
    fn filter_tablets_by_directed_read_skips_unroutable() {
        let mut tablet_skipped = make_test_tablet(1, "us-east1", Role::ReadOnly, 2);
        tablet_skipped.skip = true;

        let mut tablet_empty_addr = make_test_tablet(2, "us-east1", Role::ReadOnly, 2);
        tablet_empty_addr.server_address.clear();

        let tablet_valid = make_test_tablet(3, "us-east1", Role::ReadOnly, 2);

        let tablets = vec![tablet_skipped, tablet_empty_addr, tablet_valid];
        let filtered: Vec<_> = filter_tablets_by_directed_read(&tablets, None).collect();

        assert_eq!(
            filtered.len(),
            1,
            "expected only 1 routable tablet after filtering"
        );
        assert_eq!(
            filtered[0].tablet_uid, 3,
            "expected tablet UID 3 to be the sole routable tablet"
        );
    }

    #[test]
    fn select_eligible_tablets_for_directed_read_prefers_leader_when_matching() {
        let leader = make_test_tablet(1, "us-east1", Role::ReadWrite, 2);
        let replica = make_test_tablet(2, "us-east1", Role::ReadOnly, 1);
        let tablets = vec![leader, replica];

        let options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    r#type: Type::Unspecified,
                    _unknown_fields: Default::default(),
                }],
                auto_failover_disabled: false,
                _unknown_fields: Default::default(),
            }))),
            _unknown_fields: Default::default(),
        };

        // When prefer_leader is true and leader matches options -> returns leader
        let eligible =
            select_eligible_tablets_for_directed_read(&tablets, Some(0), true, Some(&options));
        assert_eq!(
            eligible.len(),
            1,
            "expected exactly 1 eligible tablet when matching leader is preferred"
        );
        assert_eq!(
            eligible[0].tablet_uid, 1,
            "expected leader (tablet UID 1) to be selected"
        );

        // When options exclude ReadWrite (Type::ReadOnly only), leader is bypassed
        let options_ro_only = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    r#type: Type::ReadOnly,
                    _unknown_fields: Default::default(),
                }],
                auto_failover_disabled: false,
                _unknown_fields: Default::default(),
            }))),
            _unknown_fields: Default::default(),
        };

        let eligible_ro = select_eligible_tablets_for_directed_read(
            &tablets,
            Some(0),
            true,
            Some(&options_ro_only),
        );
        assert_eq!(
            eligible_ro.len(),
            1,
            "expected exactly 1 eligible tablet when leader does not match"
        );
        assert_eq!(
            eligible_ro[0].tablet_uid, 2,
            "expected ReadOnly replica (tablet UID 2) to be selected over mismatched leader"
        );
    }

    #[test]
    fn select_eligible_tablets_for_directed_read_prefers_remote_leader_when_no_options() {
        let leader_remote = make_test_tablet(1, "us-central1", Role::ReadWrite, 10);
        let replica_local = make_test_tablet(2, "us-east1", Role::ReadOnly, 2);
        let tablets = vec![leader_remote, replica_local];

        // When prefer_leader is true and options is None (e.g. write/read-write transaction),
        // the remote leader must be selected directly to avoid forwarding hops.
        let eligible = select_eligible_tablets_for_directed_read(&tablets, Some(0), true, None);
        assert_eq!(
            eligible.len(),
            1,
            "expected remote leader to be selected when prefer_leader is true and options is None"
        );
        assert_eq!(
            eligible[0].tablet_uid, 1,
            "expected leader (tablet UID 1) to be returned despite remote distance"
        );

        // When options is Some(DirectedReadOptions::default()) (replicas is None, semantically identical to None),
        // the remote leader must also be selected directly.
        let default_options = DirectedReadOptions::default();
        let eligible_default_options = select_eligible_tablets_for_directed_read(
            &tablets,
            Some(0),
            true,
            Some(&default_options),
        );
        assert_eq!(
            eligible_default_options.len(),
            1,
            "expected remote leader to be selected when options has replicas as None"
        );
        assert_eq!(
            eligible_default_options[0].tablet_uid, 1,
            "expected leader (tablet UID 1) with default options"
        );
    }

    #[test]
    fn select_eligible_tablets_partitions_lowest_distance() {
        let tablet1 = make_test_tablet(1, "us-east1", Role::ReadOnly, 8);
        let tablet2 = make_test_tablet(2, "us-east1", Role::ReadOnly, 4);
        let tablet3 = make_test_tablet(3, "us-east1", Role::ReadOnly, 4);
        let tablets = vec![tablet1, tablet2, tablet3];

        let options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    r#type: Type::ReadOnly,
                    _unknown_fields: Default::default(),
                }],
                auto_failover_disabled: false,
                _unknown_fields: Default::default(),
            }))),
            _unknown_fields: Default::default(),
        };

        let eligible =
            select_eligible_tablets_for_directed_read(&tablets, None, false, Some(&options));
        assert_eq!(
            eligible.len(),
            2,
            "expected 2 tied candidate tablets at minimum distance 4"
        );
        assert_eq!(
            eligible[0].tablet_uid, 2,
            "expected tablet UID 2 in lowest distance tier"
        );
        assert_eq!(
            eligible[1].tablet_uid, 3,
            "expected tablet UID 3 in lowest distance tier"
        );
    }

    #[test]
    fn matches_replica_selection_unspecified_type_matches_any_role() {
        let tablet_ro = make_test_tablet(1, "us-east1", Role::ReadOnly, 2);
        let tablet_rw = make_test_tablet(2, "us-east1", Role::ReadWrite, 2);
        let tablet_unspecified = make_test_tablet(3, "us-east1", Role::Unspecified, 2);

        let selection = ReplicaSelection {
            location: "us-east1".to_string(),
            r#type: Type::Unspecified,
            _unknown_fields: Default::default(),
        };

        assert!(
            matches_replica_selection(&tablet_ro, &selection),
            "expected Unspecified type to match ReadOnly tablet"
        );
        assert!(
            matches_replica_selection(&tablet_rw, &selection),
            "expected Unspecified type to match ReadWrite tablet"
        );
        assert!(
            matches_replica_selection(&tablet_unspecified, &selection),
            "expected Unspecified type to match Unspecified role tablet"
        );
    }

    #[test]
    fn matches_directed_read_options_empty_selections() {
        let tablet = make_test_tablet(1, "us-east1", Role::ReadOnly, 2);

        // Empty IncludeReplicas includes nothing
        let options_empty_include = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: Vec::new(),
                auto_failover_disabled: false,
                _unknown_fields: Default::default(),
            }))),
            _unknown_fields: Default::default(),
        };
        assert!(
            !matches_directed_read_options(&tablet, Some(&options_empty_include)),
            "expected empty IncludeReplicas to match no tablets"
        );

        // Empty ExcludeReplicas excludes nothing
        let options_empty_exclude = DirectedReadOptions {
            replicas: Some(Replicas::ExcludeReplicas(Box::new(ExcludeReplicas {
                replica_selections: Vec::new(),
                _unknown_fields: Default::default(),
            }))),
            _unknown_fields: Default::default(),
        };
        assert!(
            matches_directed_read_options(&tablet, Some(&options_empty_exclude)),
            "expected empty ExcludeReplicas to match all tablets"
        );
    }

    #[test]
    fn select_eligible_tablets_empty_input_or_all_excluded() {
        assert!(
            select_eligible_tablets_for_directed_read(&[], None, false, None).is_empty(),
            "expected empty tablets slice to yield empty candidate vector"
        );

        let tablet = make_test_tablet(1, "us-east1", Role::ReadOnly, 2);
        let options_exclude_all = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-west1".to_string(),
                    r#type: Type::Unspecified,
                    _unknown_fields: Default::default(),
                }],
                auto_failover_disabled: false,
                _unknown_fields: Default::default(),
            }))),
            _unknown_fields: Default::default(),
        };

        let tablets = [tablet];
        let eligible = select_eligible_tablets_for_directed_read(
            &tablets,
            None,
            false,
            Some(&options_exclude_all),
        );
        assert!(
            eligible.is_empty(),
            "expected candidate vector to be empty when all tablets are excluded"
        );
    }

    #[test]
    fn matches_replica_selection_unknown_type_matches_any_role() {
        let tablet_ro = make_test_tablet(1, "us-east1", Role::ReadOnly, 2);
        let unknown_type: Type = serde_json::from_str(r#""UNKNOWN_FUTURE_TYPE""#)
            .expect("deserialization of unknown enum value must succeed");

        let selection = ReplicaSelection {
            location: "us-east1".to_string(),
            r#type: unknown_type,
            _unknown_fields: Default::default(),
        };

        assert!(
            matches_replica_selection(&tablet_ro, &selection),
            "expected unknown enum type to match any tablet role"
        );
    }

    #[test]
    fn select_eligible_tablets_unroutable_leader_falls_through_to_replicas() {
        let mut leader_skipped = make_test_tablet(1, "us-east1", Role::ReadWrite, 2);
        leader_skipped.skip = true;
        let replica = make_test_tablet(2, "us-east1", Role::ReadOnly, 3);
        let tablets = [leader_skipped, replica];

        // When leader is skipped, prefer_leader falls through to the routable replica
        let eligible = select_eligible_tablets_for_directed_read(&tablets, Some(0), true, None);
        assert_eq!(
            eligible.len(),
            1,
            "expected 1 eligible replica when leader is skipped"
        );
        assert_eq!(
            eligible[0].tablet_uid, 2,
            "expected routable replica (tablet UID 2) to be selected"
        );

        let mut leader_empty_addr = make_test_tablet(1, "us-east1", Role::ReadWrite, 2);
        leader_empty_addr.server_address.clear();
        let replica2 = make_test_tablet(2, "us-east1", Role::ReadOnly, 3);
        let tablets2 = [leader_empty_addr, replica2];

        // When leader address is empty, prefer_leader falls through to the routable replica
        let eligible2 = select_eligible_tablets_for_directed_read(&tablets2, Some(0), true, None);
        assert_eq!(
            eligible2.len(),
            1,
            "expected 1 eligible replica when leader address is empty"
        );
        assert_eq!(
            eligible2[0].tablet_uid, 2,
            "expected routable replica (tablet UID 2) to be selected"
        );
    }

    #[test]
    fn select_eligible_tablets_out_of_bounds_leader_index_falls_through_safely() {
        let replica = make_test_tablet(1, "us-east1", Role::ReadOnly, 3);
        let tablets = [replica];

        // Out-of-bounds leader_index: Some(99) on 1-element slice
        let eligible = select_eligible_tablets_for_directed_read(&tablets, Some(99), true, None);
        assert_eq!(
            eligible.len(),
            1,
            "expected out-of-bounds leader_index to safely fall through to routable replica"
        );
        assert_eq!(
            eligible[0].tablet_uid, 1,
            "expected candidate replica (tablet UID 1) to be returned"
        );
    }
}
