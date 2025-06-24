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
use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};

pub async fn run(builder: ta::builder::telco_automation::ClientBuilder) -> Result<()> {
    // Enable a basic subscriber. Useful to troubleshoot problems and visually
    // verify tracing is doing something.
    #[cfg(feature = "log-integration-tests")]
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let project_id = crate::project_id()?;
    let region_id = crate::region_id();
    let client = builder
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
        !err.status().map(|s| s.details.is_empty()).unwrap_or(false),
        "expected at least some error details {err:?}"
    );

    Ok(())
}

pub async fn check_code_for_http(builder: wf::builder::workflows::ClientBuilder) -> Result<()> {
    #[cfg(feature = "log-integration-tests")]
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let project_id = crate::project_id()?;
    let location_id = crate::region_id();
    let workflow_id = crate::random_workflow_id();
    let workflow_name =
        format!("projects/{project_id}/locations/{location_id}/workflows/{workflow_id}");
    let client = builder.build().await?;

    match client.get_workflow().set_name(&workflow_name).send().await {
        Ok(g) => panic!("unexpected success {g:?}"),
        Err(e) => match e.status() {
            None => panic!("expected service error, got {e:?}"),
            Some(status) => {
                let want = gax::error::rpc::Code::NotFound;
                assert_eq!(status.code, want, "{e:?}");
                tracing::info!("service error = {e}");
            }
        },
    }

    Ok(())
}

pub async fn check_code_for_grpc(builder: storage_control::client::ClientBuilder) -> Result<()> {
    #[cfg(feature = "log-integration-tests")]
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let bucket_id = crate::random_bucket_id();
    let bucket_name = format!("projects/_/buckets/{bucket_id}");
    let client = builder.build().await?;

    match client.get_bucket().set_name(&bucket_name).send().await {
        Ok(g) => panic!("unexpected success {g:?}"),
        Err(e) => match e.status() {
            None => panic!("expected service error, got {e:?}"),
            Some(status) => {
                let want = gax::error::rpc::Code::NotFound;
                assert_eq!(status.code, want, "{e:?}");
                tracing::info!("service error = {e}");
            }
        },
    };

    Ok(())
}
