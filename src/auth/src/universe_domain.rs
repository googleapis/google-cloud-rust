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

use crate::credentials::Credentials;
use crate::errors::CredentialsError;
use crate::constants::DEFAULT_UNIVERSE_DOMAIN;

// TODO: how to make this internal only ?
pub async fn resolve(
    config_universe: Option<&str>,
    cred: &Credentials,
) -> Result<String, CredentialsError> {
    resolve_internal(config_universe, cred, true).await
}

#[allow(dead_code)]
pub(crate) async fn resolve_without_env(
    config_universe: Option<&str>,
    cred: &Credentials,
) -> Result<String, CredentialsError> {
    resolve_internal(config_universe, cred, false).await
}

async fn resolve_internal(
    config_universe: Option<&str>,
    cred: &Credentials,
    use_env: bool,
) -> Result<String, CredentialsError> {
    let env_universe = if use_env {
        std::env::var("GOOGLE_CLOUD_UNIVERSE_DOMAIN").ok()
    } else {
        None
    };
    let cred_universe = cred.universe_domain().await;

    let universe_domain = config_universe
        .or(cred_universe.as_deref())
        .or(env_universe.as_deref())
        .unwrap_or(DEFAULT_UNIVERSE_DOMAIN)
        .to_string();

    // Verification
    if let Some(config_universe) = config_universe {
        if let Some(cred_universe) = &cred_universe {
            if config_universe != cred_universe {
                return Err(crate::errors::non_retryable_from_str(format!(
                    "Universe domain mismatch: client configured with '{}' but credentials found '{}'",
                    config_universe, cred_universe
                )));
            }
        }
    }

    Ok(universe_domain)
}
