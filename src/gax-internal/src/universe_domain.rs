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

use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::client_builder::{Error, Result};

pub(crate) const DEFAULT_UNIVERSE_DOMAIN: &str = "googleapis.com";
const UNIVERSE_DOMAIN_VAR: &str = "GOOGLE_CLOUD_UNIVERSE_DOMAIN";

pub(crate) async fn resolve(
    universe_domain_client_override: Option<&str>,
    cred: &Credentials,
) -> Result<String> {
    let env_universe = std::env::var(UNIVERSE_DOMAIN_VAR).ok();
    let cred_universe = cred.universe_domain().await;
    let cred_universe = cred_universe.as_deref().unwrap_or(DEFAULT_UNIVERSE_DOMAIN);
    let client_universe = env_universe
        .as_deref()
        .or(universe_domain_client_override)
        .unwrap_or(DEFAULT_UNIVERSE_DOMAIN);

    if !cred_universe.eq(client_universe) {
        return Err(Error::universe_domain_mismatch(
            client_universe,
            cred_universe,
        ));
    }

    Ok(client_universe.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider};
    use google_cloud_auth::errors::CredentialsError;
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

    fn mock_credentials(universe_domain: Option<&str>) -> Credentials {
        let mut provider = MockCredentials::new();
        let universe_domain = universe_domain.map(|s| s.to_string());
        provider
            .expect_universe_domain()
            .returning(move || universe_domain.clone());
        Credentials::from(provider)
    }

    #[tokio::test]
    #[test_case(None, None, None, DEFAULT_UNIVERSE_DOMAIN; "default")]
    #[test_case(Some("universe.com"), None, Some("universe.com"), "universe.com"; "env var only")]
    #[test_case(None, Some("universe.com"), Some("universe.com"), "universe.com"; "client override only")]
    #[test_case(Some("universe.com"), Some("universe.com"), Some("universe.com"), "universe.com"; "all")]
    #[serial]
    async fn universe_domain_resolve_success(
        env_domain: Option<&str>,
        client_override: Option<&str>,
        cred_domain: Option<&str>,
        expected: &str,
    ) -> TestResult {
        let _env = match env_domain {
            Some(domain) => ScopedEnv::set(UNIVERSE_DOMAIN_VAR, domain),
            None => ScopedEnv::remove(UNIVERSE_DOMAIN_VAR),
        };
        let cred = mock_credentials(cred_domain);

        let universe_domain = resolve(client_override, &cred).await?;
        assert_eq!(universe_domain.as_str(), expected, "{universe_domain:?}");

        Ok(())
    }

    #[tokio::test]
    #[test_case(None, None, Some("universe.com"); "credentials only")]
    #[test_case(None, Some("test.com"), Some("universe.com"); "client override mismatch")]
    #[test_case( Some("test.com"), None, Some("universe.com"); "env var override mismatch")]
    #[test_case(None, Some("universe.com"), None; "client override only mismatch")]
    #[serial]
    async fn universe_domain_resolve_failure(
        env_domain: Option<&str>,
        client_override: Option<&str>,
        cred_domain: Option<&str>,
    ) -> TestResult {
        let _env = match env_domain {
            Some(domain) => ScopedEnv::set(UNIVERSE_DOMAIN_VAR, domain),
            None => ScopedEnv::remove(UNIVERSE_DOMAIN_VAR),
        };
        let cred = mock_credentials(cred_domain);

        let err = resolve(client_override, &cred).await.unwrap_err();
        assert!(err.is_universe_domain_mismatch(), "{err:?}");

        Ok(())
    }
}
