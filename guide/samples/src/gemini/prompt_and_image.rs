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

// [START rust_prompt_and_image] ANCHOR: prompt-and-image
use google_cloud_aiplatform_v1::client::PredictionService;
use google_cloud_aiplatform_v1::model::{Content, FileData, Part};

pub async fn prompt_and_image(project_id: &str) -> anyhow::Result<()> {
    // [START rust_prompt_and_image_client] ANCHOR: prompt-and-image-client
    let client = PredictionService::builder().build().await?;
    // [END rust_prompt_and_image_client] ANCHOR_END: prompt-and-image-client

    // [START rust_prompt_and_image_model] ANCHOR: prompt-and-image-model
    const MODEL: &str = "gemini-2.0-flash-001";
    let model = format!("projects/{project_id}/locations/global/publishers/google/models/{MODEL}");
    // [END rust_prompt_and_image_model] ANCHOR_END: prompt-and-image-model

    // [START rust_prompt_and_image_request] ANCHOR: prompt-and-image-request
    let response = client
        .generate_content()
        .set_model(&model)
        .set_contents([Content::new().set_role("user").set_parts([
            // [START rust_prompt_and_image_image_part] ANCHOR: prompt-and-image-image-part
            Part::new().set_file_data(
                FileData::new()
                    .set_mime_type("image/jpeg")
                    .set_file_uri("gs://generativeai-downloads/images/scones.jpg"),
            ),
            // [END rust_prompt_and_image_image_part] ANCHOR_END: prompt-and-image-image-part
            // [START rust_prompt_and_image_prompt_part] ANCHOR: prompt-and-image-prompt-part
            Part::new().set_text("Describe this picture."),
            // [END rust_prompt_and_image_prompt_part] ANCHOR_END: prompt-and-image-prompt-part
        ])])
        .send()
        .await;
    // [END rust_prompt_and_image_request] ANCHOR_END: prompt-and-image-request
    // [START rust_prompt_and_image_response] ANCHOR: prompt-and-image-response
    println!("RESPONSE = {response:#?}");
    // [END rust_prompt_and_image_response] ANCHOR_END: prompt-and-image-response

    Ok(())
}
// [END rust_prompt_and_image] ANCHOR_END: prompt-and-image
