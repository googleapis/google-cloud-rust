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

    // [START rust_auth_request_id_token_client] ANCHOR: request_id_token_client
    let client = Builder::new(audience).build()?;
    // [END rust_auth_request_id_token_client] ANCHOR_END: request_id_token_client

    // [START rust_auth_request_id_token_call] ANCHOR: request_id_token_call
    let id_token = client.id_token().await?;
    println!("ID Token: {id_token:?}");
    // [END rust_auth_request_id_token_call] ANCHOR_END: request_id_token_call
    Ok(id_token)
}

pub async fn send_id_token(id_token: &str) -> anyhow::Result<()> {
    // [START request_id_token_send] ANCHOR: request_id_token_send
    use reqwest;

    let client = reqwest::Client::new();
    let target_url = format!("{audience}/api/method");
    client.get(target_url)
        .bearer_auth(id_token)
        .send()
        .await?;
    // [END request_id_token_send] ANCHOR_END: request_id_token_send
    Ok(())
}
// [END rust_auth_request_id_token] ANCHOR_END: all