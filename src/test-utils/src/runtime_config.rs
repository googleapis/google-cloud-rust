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

/// Returns the project id used for the integration tests.
pub fn project_id() -> Result<String> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    Ok(project_id)
}

/// Returns an existing, but disabled service account.
pub fn test_service_account() -> Result<String> {
    let value = std::env::var("GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT")?;
    Ok(value)
}

/// Returns the preferred region id used for the integration tests.
pub fn region_id() -> String {
    std::env::var("GOOGLE_CLOUD_RUST_TEST_REGION")
        .ok()
        .unwrap_or("us-central1".to_string())
}

/// Returns the preferred zone id used for the integration tests.
pub fn zone_id() -> String {
    std::env::var("GOOGLE_CLOUD_RUST_TEST_REGION")
        .ok()
        .unwrap_or("us-central1-a".to_string())
}
