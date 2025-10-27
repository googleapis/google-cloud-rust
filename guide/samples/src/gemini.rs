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

//! Examples showing how to use the Vertex AI Gemini API.

// ANCHOR: text-prompt
pub async fn text_prompt(project_id: &str) -> anyhow::Result<()> {
    // ANCHOR: text-prompt-client
    use google_cloud_aiplatform_v1 as vertexai;
    let client = vertexai::client::PredictionService::builder()
        .build()
        .await?;
    // ANCHOR_END: text-prompt-client

    // ANCHOR: text-prompt-model
    const MODEL: &str = "gemini-2.0-flash-001";
    let model = format!("projects/{project_id}/locations/global/publishers/google/models/{MODEL}");
    // ANCHOR_END: text-prompt-model

    // ANCHOR: text-prompt-request
    let response = client
        .generate_content().set_model(&model)
        .set_contents([vertexai::model::Content::new().set_role("user").set_parts([
            vertexai::model::Part::new().set_text("What's a good name for a flower shop that specializes in selling bouquets of dried flowers?"),
        ])])
        .send()
        .await;
    // ANCHOR_END: text-prompt-request
    // ANCHOR: text-prompt-response
    println!("RESPONSE = {response:#?}");
    // ANCHOR_END: text-prompt-response

    Ok(())
}
// ANCHOR_END: text-prompt

// ANCHOR: prompt-and-image
pub async fn prompt_and_image(project_id: &str) -> anyhow::Result<()> {
    // ANCHOR: prompt-and-image-client
    use google_cloud_aiplatform_v1 as vertexai;
    let client = vertexai::client::PredictionService::builder()
        .build()
        .await?;
    // ANCHOR_END: prompt-and-image-client

    // ANCHOR: prompt-and-image-model
    const MODEL: &str = "gemini-2.0-flash-001";
    let model = format!("projects/{project_id}/locations/global/publishers/google/models/{MODEL}");
    // ANCHOR_END: prompt-and-image-model

    // ANCHOR: prompt-and-image-request
    let response = client
        .generate_content()
        .set_model(&model)
        .set_contents(
            [vertexai::model::Content::new().set_role("user").set_parts([
                // ANCHOR: prompt-and-image-image-part
                vertexai::model::Part::new().set_file_data(
                    vertexai::model::FileData::new()
                        .set_mime_type("image/jpeg")
                        .set_file_uri("gs://generativeai-downloads/images/scones.jpg"),
                ),
                // ANCHOR_END: prompt-and-image-image-part
                // ANCHOR: prompt-and-image-prompt-part
                vertexai::model::Part::new().set_text("Describe this picture."),
                // ANCHOR_END: prompt-and-image-prompt-part
            ])],
        )
        .send()
        .await;
    // ANCHOR_END: prompt-and-image-request
    // ANCHOR: prompt-and-image-response
    println!("RESPONSE = {response:#?}");
    // ANCHOR_END: prompt-and-image-response

    Ok(())
}
// ANCHOR_END: prompt-and-image
