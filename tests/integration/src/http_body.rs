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

use anyhow::Result;
use bytes::Bytes;
use google_cloud_aiplatform_v1::client::PredictionService;
use google_cloud_api::model::HttpBody;
use google_cloud_test_utils::runtime_config::{project_id, region_id};

/// Verify `google.api.HttpBody` requests and responses work against a live
/// service.
///
/// `RawPredict` sends the `HttpBody` payload as the raw request body and
/// returns the raw response body. If the client serializes the wrapper
/// request message instead, the service rejects the request with
/// `instances is empty`.
pub async fn raw_predict() -> Result<()> {
    let project_id = project_id()?;
    let location_id = region_id();
    let client = PredictionService::builder()
        .with_endpoint(format!("https://{location_id}-aiplatform.googleapis.com"))
        .with_tracing()
        .build()
        .await?;

    let response = client
        .raw_predict()
        .set_endpoint(format!(
            "projects/{project_id}/locations/{location_id}/publishers/google/models/text-embedding-005"
        ))
        .set_http_body(
            HttpBody::new()
                .set_content_type("application/json")
                .set_data(Bytes::from_static(
                    br#"{"instances": [{"content": "hello world"}]}"#,
                )),
        )
        .send()
        .await?;

    tracing::info!(
        "raw_predict response: content_type = {:?}, body_len = {}",
        response.content_type,
        response.data.len()
    );
    assert!(
        response.content_type.starts_with("application/json"),
        "{response:?}"
    );
    let parsed: serde_json::Value = serde_json::from_slice(&response.data)?;
    assert!(parsed.get("predictions").is_some(), "{parsed}");
    Ok(())
}
