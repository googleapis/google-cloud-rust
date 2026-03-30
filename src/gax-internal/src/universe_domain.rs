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

pub(crate) const DEFAULT_UNIVERSE_DOMAIN: &str = "googleapis.com";
const UNIVERSE_DOMAIN_VAR: &str = "GOOGLE_CLOUD_UNIVERSE_DOMAIN";

pub(crate) async fn resolve(
    universe_domain_client_override: Option<&str>,
    cred: &Credentials,
) -> Result<String, CredentialsError> {
    let env_universe = std::env::var(UNIVERSE_DOMAIN_VAR).ok();
    let cred_universe = cred.universe_domain().await;

    let universe_domain = env_universe
        .as_deref()
        .or(universe_domain_client_override)
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
#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider};
    use http::{Extensions, HeaderMap};
    use scoped_env::ScopedEnv;
    use serial_test::serial;
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;
    type AuthResult<T> = std::result::Result<T, CredentialsError>;

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsProvider for Credentials {
            async fn headers(&self, extensions: Extensions) -> AuthResult<CacheableResource<HeaderMap>>;
            async fn universe_domain(&self) -> Option<String>;
        }
    }

    #[tokio::test]
    #[test_case(None, None, None, Ok(DEFAULT_UNIVERSE_DOMAIN); "default")]
    #[test_case(Some("universe.com"), None, Some("universe.com"), Ok("universe.com"); "env var only")]
    #[test_case(None, Some("universe.com"), Some("universe.com"), Ok("universe.com"); "client override only")]
    #[test_case(Some("universe.com"), Some("universe.com"), Some("universe.com"), Ok("universe.com"); "all")]
    #[test_case(None, None, Some("universe.com"), Err(CredentialsError::from_msg(false, "universe domain mismatch")); "credentials only")]
    #[test_case(None, Some("test.com"), Some("universe.com"), Err(CredentialsError::from_msg(false, "universe domain mismatch")); "client override mismatch")]
    #[test_case( Some("test.com"), None, Some("universe.com"), Err(CredentialsError::from_msg(false, "universe domain mismatch")); "env var override mismatch")]
    #[serial]
    async fn universe_domain_resolve(
        env_domain: Option<&str>,
        client_override: Option<&str>,
        cred_domain: Option<&str>,
        expected: Result<&str, CredentialsError>,
    ) -> TestResult {
        let _env = match env_domain {
            Some(domain) => ScopedEnv::set("GOOGLE_CLOUD_UNIVERSE_DOMAIN", domain),
            None => ScopedEnv::remove("GOOGLE_CLOUD_UNIVERSE_DOMAIN"),
        };
        let mut provider = MockCredentials::new();
        let cred_domain = cred_domain.clone().map(|s| s.to_string());
        provider
            .expect_universe_domain()
            .returning(move || cred_domain.clone());
        let cred = Credentials::from(provider);

        let universe_domain = resolve(client_override, &cred).await;
        let expected = expected.map(|s| s.to_string());
        match (universe_domain, expected) {
            (Ok(got), Ok(expected)) => {
                assert_eq!(got, expected, "{got:?}");
                Ok(())
            }
            (Err(_), Err(_)) => Ok(()),
            (got, expected) => panic!("Expected {:?}, got {:?}", expected, got),
        }
    }
}
