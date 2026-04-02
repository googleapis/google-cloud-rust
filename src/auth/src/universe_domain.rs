// Copyright 2025 Google LLC
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

use crate::constants::DEFAULT_UNIVERSE_DOMAIN;
use crate::credentials::Credentials;

/// Returns `true` if the given universe domain is the Default Google Universe (GDU).
///
/// This serves as a feature gate for capabilities that are only supported in the GDU
/// (e.g., `googleapis.com`). For example, Regional Access Boundaries should be disabled,
/// and User Account credentials should return an error when running outside the GDU.
pub(crate) fn is_default_universe_domain(universe_domain: Option<String>) -> bool {
    match universe_domain {
        Some(ud) => ud == DEFAULT_UNIVERSE_DOMAIN,
        None => true,
    }
}

pub(crate) async fn resolve(cred: &Credentials) -> String {
    let cred_universe = cred.universe_domain().await;
    cred_universe
        .as_deref()
        .unwrap_or(DEFAULT_UNIVERSE_DOMAIN)
        .to_string()
}
