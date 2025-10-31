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

// [START rust_auth_impersonation] ANCHOR: rust_auth_impersonation
// [START rust_auth_impersonation_parameter] ANCHOR: rust_auth_impersonation_parameter
/// # Parameters
/// * `target_principal`: the email or unique id of the target service account.
///   For example: `my-service-account@my-project.iam.gserviceaccount.com`.
pub async fn sample(target_principal: &str) -> anyhow::Result<()> {
    // [END rust_auth_impersonation_parameter] ANCHOR_END: rust_auth_impersonation_parameter
    // [START rust_auth_impersonation_use] ANCHOR: rust_auth_impersonation_use
    use google_cloud_auth::credentials::Builder as AdcCredentialsBuilder;
    use google_cloud_auth::credentials::impersonated::Builder as ImpersonatedCredentialsBuilder;
    use google_cloud_language_v2::client::LanguageService;
    use google_cloud_language_v2::model::{Document, document::Type};
    // [END rust_auth_impersonation_use] ANCHOR_END: rust_auth_impersonation_use

    // [START rust_auth_impersonation_credentials] ANCHOR: rust_auth_impersonation_credentials
    let credentials = ImpersonatedCredentialsBuilder::from_source_credentials(
        AdcCredentialsBuilder::default().build()?,
    )
    .with_target_principal(target_principal)
    .build()?;
    // [END rust_auth_impersonation_credentials] ANCHOR_END: rust_auth_impersonation_credentials
    // [START rust_auth_impersonation_client] ANCHOR: rust_auth_impersonation_client
    let client = LanguageService::builder()
        .with_credentials(credentials)
        .build()
        .await?;
    // [END rust_auth_impersonation_client] ANCHOR_END: rust_auth_impersonation_client

    // [START rust_auth_impersonation_call] ANCHOR: rust_auth_impersonation_call
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
    // [END rust_auth_impersonation_call] ANCHOR_END: rust_auth_impersonation_call
    Ok(())
}
// [END rust_auth_impersonation] ANCHOR_END: rust_auth_impersonation
