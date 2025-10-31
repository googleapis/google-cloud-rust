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

// [START rust_auth_api_key] ANCHOR: rust_auth_api_key
// [START rust_auth_api_key_parameter] ANCHOR: rust_auth_api_key_parameter
pub async fn sample(key: &str) -> anyhow::Result<()> {
    // [END rust_auth_api_key_parameter] ANCHOR_END: rust_auth_api_key_parameter
    // [START rust_auth_api_key_use] ANCHOR: rust_auth_api_key_use
    use google_cloud_auth::credentials::api_key_credentials::Builder as ApiKeyCredentialsBuilder;
    use google_cloud_language_v2::client::LanguageService;
    use google_cloud_language_v2::model::{Document, document::Type};
    // [END rust_auth_api_key_use] ANCHOR_END: rust_auth_api_key_use

    // [START rust_auth_api_key_credentials] ANCHOR: rust_auth_api_key_credentials
    let credentials = ApiKeyCredentialsBuilder::new(key).build();
    // [END rust_auth_api_key_credentials] ANCHOR_END: rust_auth_api_key_credentials
    // [START rust_auth_api_key_client] ANCHOR: rust_auth_api_key_client
    let client = LanguageService::builder()
        .with_credentials(credentials)
        .build()
        .await?;
    // [END rust_auth_api_key_client] ANCHOR_END: rust_auth_api_key_client

    // [START rust_auth_api_key_call] ANCHOR: rust_auth_api_key_call
    let response = client
        .analyze_sentiment()
        .set_document(
            Document::new()
                .set_content("Hello World!")
                .set_type(Type::PlainText),
        )
        .send()
        .await?;
    println!("response={response:?}");
    // [END rust_auth_api_key_call] ANCHOR_END: rust_auth_api_key_call
    Ok(())
}
// [END rust_auth_api_key] ANCHOR_END: rust_auth_api_key
