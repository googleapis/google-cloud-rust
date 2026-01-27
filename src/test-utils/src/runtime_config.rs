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

use anyhow::Result;

const PROJECT_VAR: &str = "GOOGLE_CLOUD_PROJECT";
const ACCOUNT_VAR: &str = "GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT";
const REGION_VAR: &str = "GOOGLE_CLOUD_RUST_TEST_REGION";
const ZONE_VAR: &str = "GOOGLE_CLOUD_RUST_TEST_ZONE";
const DEFAULT_REGION: &str = "us-central1";
const DEFAULT_ZONE: &str = "us-central1-a";

/// Returns the project id used for the integration tests.
pub fn project_id() -> Result<String> {
    std::env::var(PROJECT_VAR).map_err(anyhow::Error::from)
}

/// Returns an existing, but disabled service account.
pub fn test_service_account() -> Result<String> {
    std::env::var(ACCOUNT_VAR).map_err(anyhow::Error::from)
}

/// Returns the preferred region id used for the integration tests.
pub fn region_id() -> String {
    std::env::var(REGION_VAR)
        .ok()
        .unwrap_or(DEFAULT_REGION.to_string())
}

/// Returns the preferred zone id used for the integration tests.
pub fn zone_id() -> String {
    std::env::var(ZONE_VAR)
        .ok()
        .unwrap_or(DEFAULT_ZONE.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use scoped_env::ScopedEnv;
    use serial_test::serial;

    #[serial]
    #[test]
    fn project() {
        let _env = ScopedEnv::remove(PROJECT_VAR);
        let got = project_id();
        assert!(got.is_err(), "{got:?}");
        let _env = ScopedEnv::set(PROJECT_VAR, "abc");
        let got = project_id();
        assert!(got.as_ref().is_ok_and(|v| v == "abc"), "{got:?}");
    }

    #[serial]
    #[test]
    fn account() {
        let _env = ScopedEnv::remove(ACCOUNT_VAR);
        let got = test_service_account();
        assert!(got.is_err(), "{got:?}");
        let _env = ScopedEnv::set(ACCOUNT_VAR, "abc");
        let got = test_service_account();
        assert!(got.as_ref().is_ok_and(|v| v == "abc"), "{got:?}");
    }

    #[test]
    fn region_is_prefix() {
        assert!(
            DEFAULT_ZONE.strip_prefix(DEFAULT_REGION).is_some(),
            "default region ({DEFAULT_REGION}) should be a prefix of the default zone ({DEFAULT_ZONE})"
        );
    }

    #[serial]
    #[test]
    fn region() {
        let _env = ScopedEnv::remove(REGION_VAR);
        let got = region_id();
        assert_eq!(got, DEFAULT_REGION);
        let _env = ScopedEnv::set(REGION_VAR, "abc");
        let got = region_id();
        assert_eq!(got, "abc");
    }

    #[serial]
    #[test]
    fn zone() {
        let _env = ScopedEnv::remove(ZONE_VAR);
        let got = zone_id();
        assert_eq!(got, DEFAULT_ZONE);
        let _env = ScopedEnv::set(ZONE_VAR, "abc");
        let got = zone_id();
        assert_eq!(got, "abc");
    }
}
