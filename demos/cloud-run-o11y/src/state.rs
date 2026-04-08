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

use std::time::Duration;

use super::args::Args;
use google_cloud_aiplatform_v1::client::PredictionService;
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
use google_cloud_storage::client::StorageControl;

const MODEL: &str = "gemini-2.5-flash";

#[derive(Clone, Debug)]
pub struct AppState {
    prediction_service: PredictionService,
    model: String,
    storage_control: StorageControl,
}

impl AppState {
    pub async fn new(args: Args, credentials: Credentials) -> anyhow::Result<Self> {
        let builder = PredictionService::builder()
            .with_credentials(credentials.clone())
            .with_retry_policy(
                Aip194Strict
                    .continue_on_too_many_requests()
                    .with_time_limit(Duration::from_secs(60)),
            )
            .with_tracing();
        let (builder, model) = if let Some(region) = args.regional.as_ref() {
            let model = format!(
                "projects/{}/locations/{region}/publishers/google/models/{MODEL}",
                args.project_id
            );
            let builder =
                builder.with_endpoint(format!("https://{region}-aiplatform.googleapis.com"));
            (builder, model)
        } else {
            let model = format!(
                "projects/{}/locations/global/publishers/google/models/{MODEL}",
                args.project_id
            );
            (builder, model)
        };

        let prediction_service = builder.build().await?;
        let storage_control = StorageControl::builder()
            .with_credentials(credentials.clone())
            .with_tracing()
            .build()
            .await?;
        Ok(Self {
            prediction_service,
            model,
            storage_control,
        })
    }

    pub fn prediction_service(&self) -> &PredictionService {
        &self.prediction_service
    }

    pub fn storage_control(&self) -> &StorageControl {
        &self.storage_control
    }

    pub fn model(&self) -> &str {
        self.model.as_str()
    }
}
