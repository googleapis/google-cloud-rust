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

//! Examples showing how to credentials for fetching tokens.

// ANCHOR: use
use google_cloud_auth::credentials::mds_credential::MDSCredentialBuilder;
use google_cloud_auth::credentials::Credential;
// ANCHOR_END: use

// ANCHOR: build-mds-credentials
pub fn build_mds_credentials() -> Credential {
    let mds_endpoint = "http://metadata.google.internal/computeMetadata/v1";
    let cloud_scopes = vec!["https://www.googleapis.com/auth/cloud-platform"];
    let universe_domain = "googleapis.com";
    MDSCredentialBuilder::default()
        .endpoint(mds_endpoint)
        .quota_project_id("sample-quota-project")
        .scopes(cloud_scopes)
        .universe_domain(universe_domain)
        .build()
}
// ANCHOR_END: build-mds-credentials
