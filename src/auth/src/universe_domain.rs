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
use crate::errors::CredentialsError;

pub(crate) fn is_default_universe_domain<S: Into<String>>(universe_domain: Option<S>) -> bool {
    let universe_domain = universe_domain.map(|s| s.into());
    match universe_domain {
        Some(ud) => ud == DEFAULT_UNIVERSE_DOMAIN,
        None => true,
    }
}

pub(crate) async fn resolve(cred: &Credentials) -> Result<String, CredentialsError> {
    let cred_universe = cred.universe_domain().await;
    Ok(cred_universe
        .as_deref()
        .unwrap_or(DEFAULT_UNIVERSE_DOMAIN)
        .to_string())
}
