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

use crate::constants::DEFAULT_UNIVERSE_DOMAIN;
use crate::credentials::Credentials;

/// Returns `true` if the given universe domain is the Default Google Universe (GDU).
///
/// This serves as a feature gate for capabilities that are only supported in the GDU
/// (e.g., `googleapis.com`). For example, Regional Access Boundaries should be disabled,
/// and User Account credentials should return an error when running outside the GDU.
#[allow(dead_code)]
pub(crate) fn is_default_universe_domain(universe_domain: Option<&str>) -> bool {
    match universe_domain {
        Some(ud) => ud == DEFAULT_UNIVERSE_DOMAIN,
        None => true,
    }
}

#[allow(dead_code)]
pub(crate) async fn resolve(cred: &Credentials) -> String {
    let cred_universe = cred.universe_domain().await;
    cred_universe
        .as_deref()
        .unwrap_or(DEFAULT_UNIVERSE_DOMAIN)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::credentials::tests::MockCredentials;
    use test_case::test_case;

    #[tokio::test]
    async fn test_resolve_default() {
        let mut mock = MockCredentials::new();
        mock.expect_universe_domain().return_const(None);
        let cred = Credentials::from(mock);
        let result = resolve(&cred).await;
        assert_eq!(result, DEFAULT_UNIVERSE_DOMAIN);
    }

    #[tokio::test]
    async fn test_resolve_custom() {
        let mut mock = MockCredentials::new();
        mock.expect_universe_domain()
            .return_const(Some("some-universe-domain.com".into()));
        let cred = Credentials::from(mock);
        let result = resolve(&cred).await;
        assert_eq!(result, "some-universe-domain.com");
    }

    #[test_case(None, true)]
    #[test_case(Some(DEFAULT_UNIVERSE_DOMAIN), true)]
    #[test_case(Some("some-universe-domain.com"), false)]
    fn test_is_default_universe_domain(universe_domain: Option<&str>, expected: bool) {
        assert_eq!(is_default_universe_domain(universe_domain), expected);
    }
}
