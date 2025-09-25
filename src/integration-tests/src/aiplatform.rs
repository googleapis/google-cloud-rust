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

use crate::Result;

pub async fn locational_endpoint() -> Result<()> {
    let project_id = crate::project_id()?;
    let location_id = "us-central1";
    let client = aiplatform::client::ModelService::builder()
        .with_endpoint(format!("https://{location_id}-aiplatform.googleapis.com"))
        .with_tracing()
        .build()
        .await?;

    tracing::info!("Listing models in {location_id}...");
    let models = client
        .list_models()
        .set_parent(format!("projects/{project_id}/locations/{location_id}"))
        .send()
        .await?;
    tracing::info!("Successfully listed models in {location_id}: {models:#?}");

    Ok(())
}
