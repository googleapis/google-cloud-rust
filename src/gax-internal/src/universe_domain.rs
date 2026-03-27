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

use google_cloud_auth::credentials::Credentials;
use google_cloud_auth::errors::CredentialsError;

const DEFAULT_UNIVERSE_DOMAIN: &str = "googleapis.com";

pub(crate) fn is_default_universe_domain<S: Into<String>>(universe_domain: Option<S>) -> bool {
    let universe_domain = universe_domain.map(|s| s.into());
    match universe_domain {
        Some(ud) => ud == DEFAULT_UNIVERSE_DOMAIN,
        None => true,
    }
}

pub(crate) async fn resolve(
    universe_domain_client_override: Option<&str>,
    cred: &Credentials,
) -> Result<String, CredentialsError> {
    let env_universe = std::env::var("GOOGLE_CLOUD_UNIVERSE_DOMAIN").ok();
    let cred_universe = cred.universe_domain().await;

    let universe_domain = universe_domain_client_override
        .or(cred_universe.as_deref())
        .or(env_universe.as_deref())
        .unwrap_or(DEFAULT_UNIVERSE_DOMAIN)
        .to_string();

    let cred_universe = cred_universe.as_deref().unwrap_or(DEFAULT_UNIVERSE_DOMAIN);

    if universe_domain != cred_universe {
        return Err(CredentialsError::from_msg(
            false,
            format!(
                "The configured universe domain ({}) does not match the universe domain found in the credentials ({}). If you haven't configured the universe domain explicitly, `googleapis.com` is the default.",
                universe_domain, cred_universe
            ),
        ));
    }

    Ok(universe_domain)
}
