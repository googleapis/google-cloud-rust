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

// [START rust_text_prompt] ANCHOR: text-prompt
use google_cloud_aiplatform_v1::client::PredictionService;
use google_cloud_aiplatform_v1::model::{Content, Part};

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    // [START rust_text_prompt_client] ANCHOR: text-prompt-client
    let client = PredictionService::builder().build().await?;
    // [END rust_text_prompt_client] ANCHOR_END: text-prompt-client

    // [START rust_text_prompt_model] ANCHOR: text-prompt-model
    const MODEL: &str = "gemini-2.0-flash-001";
    let model = format!("projects/{project_id}/locations/global/publishers/google/models/{MODEL}");
    // [END rust_text_prompt_model] ANCHOR_END: text-prompt-model

    // [START rust_text_prompt_request] ANCHOR: text-prompt-request
    let response = client
        .generate_content()
        .set_model(&model)
        .set_contents([Content::new()
            .set_role("user")
            .set_parts([Part::new().set_text(
                "What's a good name for a flower shop that specializes in selling bouquets of dried flowers?",
            )])])
        .send()
        .await;
    // [END rust_text_prompt_request] ANCHOR_END: text-prompt-request
    // [START rust_text_prompt_response] ANCHOR: text-prompt-response
    println!("RESPONSE = {response:#?}");
    // [END rust_text_prompt_response] ANCHOR_END: text-prompt-response

    Ok(())
}
// [END rust_text_prompt] ANCHOR_END: text-prompt
