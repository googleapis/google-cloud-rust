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

use anyhow::bail;
use google_cloud_auth::credentials::Builder as CredentialsBuilder;
use google_cloud_gax::{options::RequestOptions, response::Response};
use google_cloud_gax_internal::{http::ReqwestClient, options::ClientConfig};
use serde_json::Value;

/// Verify the google_cloud_gax_internal crate is minimally functional.
///
/// The caller may need to configure the default crypto provider.
pub async fn run() -> anyhow::Result<()> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    let mut config = ClientConfig::default();
    config.cred = Some(CredentialsBuilder::default().build()?);

    let client = ReqwestClient::new(config, "https://secretmanager.googleapis.com").await?;
    let builder = client.builder(
        http::Method::GET,
        format!("/v1/projects/{project_id}/secrets"),
    );
    let body: Value = client
        .execute(builder, None::<Value>, RequestOptions::default())
        .await?
        .into_body();
    let Some(items) = body.get("secrets").and_then(|i| i.as_array()) else {
        bail!("the response should include an array of items: {body:?}")
    };
    assert!(
        !items.is_empty(),
        "the response item list should not be empty: {body:?}"
    );

    Ok(())
}
