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
use gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use gax::options::RequestOptionsBuilder;
use gax::paginator::{ItemPaginator, Paginator};
use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
use std::time::Duration;
use wf::Poller;

// Verify enum query parameters are serialized correctly.
pub async fn list(builder: wfe::builder::executions::ClientBuilder) -> Result<()> {
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

    // Create a workflow so we can list its executions. We rely on the other
    // workflows integration tests to delete it if something fails or crashes
    // in this test.
    let parent = create_test_workflow().await?;
    let client = builder.build().await?;

    // Create an execution with a label. The label is not returned for the `BASIC` view.
    let start = client
        .create_execution(&parent)
        .set_execution(wfe::model::Execution::new().set_labels([("test-label", "test-value")]))
        .send()
        .await?;
    tracing::info!("start was successful={start:?}");

    // The execution list using the `BASIC` view.
    let mut executions = client
        .list_executions(&parent)
        .set_view(wfe::model::ExecutionView::Basic)
        .paginator()
        .await
        .items();

    while let Some(execution) = executions.next().await {
        let execution = execution?;
        tracing::info!("list item={execution:?}");
        assert!(execution.labels.is_empty(), "{execution:?}");
    }

    // The execution list using the `FULL` view.
    let mut executions = client
        .list_executions(&parent)
        .set_view(wfe::model::ExecutionView::Full)
        .paginator()
        .await
        .items();

    while let Some(execution) = executions.next().await {
        let execution = execution?;
        tracing::info!("list item={execution:?}");
        assert!(!execution.labels.is_empty(), "{execution:?}");
    }

    delete_test_workflow(parent).await
}

async fn delete_test_workflow(name: String) -> Result<()> {
    let client = workflow_client().await?;
    client.delete_workflow(name).poller().until_done().await?;
    Ok(())
}

async fn create_test_workflow() -> Result<String> {
    let project_id = crate::project_id()?;
    let location_id = crate::region_id();
    let workflows_runner = crate::workflows_runner()?;
    let client = workflow_client().await?;

    let source_contents = r###"# Test only workflow
main:
    steps:
        - sayHello:
            return: Hello World
"###;
    let source_code = wf::model::workflow::SourceCode::SourceContents(source_contents.to_string());
    let workflow_id = crate::random_workflow_id();

    tracing::info!("Start create_workflow() LRO and poll it to completion");
    let response = client
        .create_workflow(format!("projects/{project_id}/locations/{location_id}"))
        .set_workflow_id(&workflow_id)
        .set_workflow(
            wf::model::Workflow::new()
                .set_labels([("integration-test", "true")])
                .set_service_account(&workflows_runner)
                .set_source_code(source_code),
        )
        .with_polling_backoff_policy(test_backoff())
        .poller()
        .until_done()
        .await?;
    tracing::info!("create LRO finished, name={}", &response.name);

    Ok(response.name)
}

async fn workflow_client() -> Result<wf::client::Workflows> {
    wf::client::Workflows::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_time_limit(Duration::from_secs(15))
                .with_attempt_limit(5),
        )
        .build()
        .await
}

fn test_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(100))
        .with_maximum_delay(Duration::from_secs(1))
        .build()
        .unwrap()
}
