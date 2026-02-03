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

use anyhow::Result;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
use google_cloud_storage::client::StorageControl;
use google_cloud_telcoautomation_v1::client::TelcoAutomation;
use google_cloud_test_utils::resource_names::{random_bucket_id, random_workflow_id};
use google_cloud_test_utils::runtime_config::{project_id, region_id};
use google_cloud_workflows_v1::client::Workflows;

pub async fn error_details_http() -> Result<()> {
    let project_id = project_id()?;
    let region_id = region_id();
    let client = TelcoAutomation::builder()
        .with_tracing()
        .with_retry_policy(AlwaysRetry.with_attempt_limit(2))
        .build()
        .await?;

    let response = client
        .list_orchestration_clusters()
        .set_parent(format!("projects/{project_id}/locations/{region_id}"))
        .send()
        .await;
    let err = response
        .expect_err("expect an error, the service should be disabled in integration test projects");
    assert!(
        err.status().map(|s| !s.details.is_empty()).unwrap_or(false),
        "expected at least some error details {err:?}"
    );

    Ok(())
}

pub async fn error_details_grpc() -> Result<()> {
    let client = StorageControl::builder().with_tracing().build().await?;
    let err = client
        .get_bucket()
        .set_name("malformed/_/bucket/name")
        .send()
        .await
        .expect_err("should fail");
    assert!(
        err.status().map(|s| !s.details.is_empty()).unwrap_or(false),
        "expected at least some error details {err:?}"
    );

    Ok(())
}

pub async fn check_code_for_http() -> Result<()> {
    let project_id = project_id()?;
    let location_id = region_id();
    let workflow_id = random_workflow_id();
    let workflow_name =
        format!("projects/{project_id}/locations/{location_id}/workflows/{workflow_id}");
    let client = Workflows::builder().with_tracing().build().await?;

    match client.get_workflow().set_name(&workflow_name).send().await {
        Ok(g) => panic!("unexpected success {g:?}"),
        Err(e) => match e.status() {
            None => panic!("expected service error, got {e:?}"),
            Some(status) => {
                let want = Code::NotFound;
                assert_eq!(status.code, want, "{e:?}");
                tracing::info!("service error = {e}");
            }
        },
    }

    Ok(())
}

pub async fn check_code_for_grpc() -> Result<()> {
    let bucket_id = random_bucket_id();
    let bucket_name = format!("projects/_/buckets/{bucket_id}");
    let client = StorageControl::builder().with_tracing().build().await?;

    match client.get_bucket().set_name(&bucket_name).send().await {
        Ok(g) => panic!("unexpected success {g:?}"),
        Err(e) => match e.status() {
            None => panic!("expected service error, got {e:?}"),
            Some(status) => {
                let want = Code::NotFound;
                assert_eq!(status.code, want, "{e:?}");
                tracing::info!("service error = {e}");
            }
        },
    };

    Ok(())
}
