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

// [START rust_auth_request_id_token] ANCHOR: all
// [START rust_auth_request_id_token_parameters] ANCHOR: request_id_token_parameters
// # Parameters
// * `audience`: The audience for the ID token.
pub async fn sample(audience: &str) -> anyhow::Result<String> {
    // [END rust_auth_request_id_token_parameters] ANCHOR_END: request_id_token_parameters
    // [START rust_auth_request_id_token_use] ANCHOR: request_id_token_use
    use google_cloud_auth::credentials::idtoken::Builder;
    // [END rust_auth_request_id_token_use] ANCHOR_END: request_id_token_use

    // [START rust_auth_request_id_token_credentials] ANCHOR: request_id_token_credentials
    let credentials = Builder::new(audience).build()?;
    // [END rust_auth_request_id_token_credentials] ANCHOR_END: request_id_token_credentials

    // [START rust_auth_request_id_token_call] ANCHOR: request_id_token_call
    let id_token = credentials.id_token().await?;
    println!("ID Token: {id_token:?}");
    // [END rust_auth_request_id_token_call] ANCHOR_END: request_id_token_call
    Ok(id_token)
}

// [START request_id_token_send] ANCHOR: request_id_token_send
// # Parameters
// * `target_url`: The receiving service target URL.
// * `credentials`: The IDTokenCredentials to use for authentication.
pub async fn api_call_with_id_token(
    target_url: &str,
    credentials: &google_cloud_auth::credentials::idtoken::IDTokenCredentials,
) -> anyhow::Result<()> {
    use reqwest;

    let id_token = credentials.id_token().await?;
    let client = reqwest::Client::new();
    client.get(target_url).bearer_auth(id_token).send().await?;

    Ok(())
}
// [END request_id_token_send] ANCHOR_END: request_id_token_send
// [END rust_auth_request_id_token] ANCHOR_END: all
