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

// [START rust_auth_adc] ANCHOR: rust_auth_adc
pub async fn sample() -> anyhow::Result<()> {
    // [START rust_auth_adc_use] ANCHOR: rust_auth_adc_use
    use google_cloud_language_v2::client::LanguageService;
    use google_cloud_language_v2::model::{Document, document::Type};
    // [END rust_auth_adc_use] ANCHOR_END: rust_auth_adc_use

    // [START rust_auth_adc_client] ANCHOR: rust_auth_adc_client
    let client = LanguageService::builder().build().await?;
    // [END rust_auth_adc_client] ANCHOR_END: rust_auth_adc_client

    // [START rust_auth_adc_call] ANCHOR: rust_auth_adc_call
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
    // [END rust_auth_adc_call] ANCHOR_END: rust_auth_adc_call
    Ok(())
}
// [END rust_auth_adc] ANCHOR_END: rust_auth_adc
