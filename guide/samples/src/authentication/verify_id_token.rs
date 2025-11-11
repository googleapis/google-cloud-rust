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

// [START rust_auth_verify_id_token] ANCHOR: all
// [START rust_auth_id_verify_token_parameters] ANCHOR: verify_id_token_parameters
// # Parameters
// * `token`: The ID token string to verify.
// * `audience`: The expected audience of the ID token.
pub async fn sample(token: &str, audience: &str) -> anyhow::Result<()> {
    // [END rust_auth_id_verify_token_parameters] ANCHOR_END: verify_id_token_parameters
    // [START rust_auth_verify_id_token_use] ANCHOR: verify_id_token_use
    use google_cloud_auth::credentials::idtoken::verifier::Builder as IdTokenVerifierBuilder;
    // [END rust_auth_verify_id_token_use] ANCHOR_END: verify_id_token_use

    // [START rust_auth_id_verify_token_verifier] ANCHOR: verify_id_token_verifier
    let verifier = IdTokenVerifierBuilder::new(audience).build();
    // [END rust_auth_id_verify_token_verifier] ANCHOR_END: verify_id_token_verifier

    // [START rust_auth_id_verify_token_verify_call] ANCHOR: verify_id_token_verify_call
    let claims = verifier.verify(token).await?;
    println!("ID Token claims {claims:?}");
    // [END rust_auth_id_verify_token_verify_call] ANCHOR_END: verify_id_token_verify_call
    Ok(())
}
// [END rust_auth_id_verify_token] ANCHOR_END: all
