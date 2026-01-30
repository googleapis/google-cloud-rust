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
use gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use gax::options::RequestOptionsBuilder;
use gax::paginator::ItemPaginator as _;
use google_cloud_longrunning::model::operation::Result as OperationResult;
use google_cloud_lro::{Poller, PollingResult};
use google_cloud_test_utils::resource_names::random_workflow_id;
use google_cloud_test_utils::runtime_config::{project_id, region_id, test_service_account};
use google_cloud_workflows_v1::client::Workflows;
use google_cloud_workflows_v1::model::{OperationMetadata, Workflow, workflow::SourceCode};
use std::time::Duration;

pub async fn until_done() -> Result<()> {
    let project_id = project_id()?;
    let location_id = region_id();
    let workflows_runner = test_service_account()?;

    let client = Workflows::builder().with_tracing().build().await?;
    cleanup_stale_workflows(&client, &project_id, &location_id).await?;

    let source_contents = r###"# Test only workflow
main:
    steps:
        - sayHello:
            return: Hello World
"###;
    let source_code = SourceCode::SourceContents(source_contents.to_string());
    let workflow_id = random_workflow_id();

    println!("\n\nStart create_workflow() LRO and poll it to completion");
    let response = client
        .create_workflow()
        .set_parent(format!("projects/{project_id}/locations/{location_id}"))
        .set_workflow_id(&workflow_id)
        .set_workflow(
            Workflow::new()
                .set_labels([("integration-test", "true")])
                .set_service_account(&workflows_runner)
                .set_source_code(source_code),
        )
        .with_polling_backoff_policy(test_backoff())
        .poller()
        .until_done()
        .await?;
    println!("    create LRO finished, response={response:?}");

    println!("\n\nStart delete_workflow() LRO and poll it to completion");
    client
        .delete_workflow()
        .set_name(format!(
            "projects/{project_id}/locations/{location_id}/workflows/{workflow_id}"
        ))
        .poller()
        .until_done()
        .await?;
    println!("    delete LRO finished");

    Ok(())
}

pub async fn explicit_loop() -> Result<()> {
    let project_id = project_id()?;
    let location_id = region_id();
    let workflows_runner = test_service_account()?;

    let client = Workflows::builder().with_tracing().build().await?;
    cleanup_stale_workflows(&client, &project_id, &location_id).await?;

    let source_contents = r###"# Test only workflow
main:
    steps:
        - sayHello:
            return: Hello World
"###;
    let source_code = SourceCode::SourceContents(source_contents.to_string());
    let workflow_id = random_workflow_id();

    println!("\n\nStart create_workflow() LRO and poll it to completion");
    let mut create = client
        .create_workflow()
        .set_parent(format!("projects/{project_id}/locations/{location_id}"))
        .set_workflow_id(&workflow_id)
        .set_workflow(
            Workflow::new()
                .set_labels([("integration-test", "true")])
                .set_service_account(&workflows_runner)
                .set_source_code(source_code),
        )
        .poller();
    let mut backoff = Duration::from_millis(100);
    while let Some(status) = create.poll().await {
        match status {
            PollingResult::PollingError(e) => {
                println!("    error polling create LRO, continuing {e}");
            }
            PollingResult::InProgress(m) => {
                println!("    create LRO still in progress, metadata={m:?}");
            }
            PollingResult::Completed(r) => match r {
                Err(e) => {
                    println!("    create LRO finished with error={e}\n\n");
                    return Err(anyhow::Error::from(e));
                }
                Ok(m) => {
                    println!("    create LRO finished with success={m:?}\n\n");
                }
            },
        }
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2);
    }

    println!("\n\nStart delete_workflow() LRO and poll it to completion");
    let mut delete = client
        .delete_workflow()
        .set_name(format!(
            "projects/{project_id}/locations/{location_id}/workflows/{workflow_id}"
        ))
        .poller();
    let mut backoff = Duration::from_millis(100);
    while let Some(status) = delete.poll().await {
        match status {
            PollingResult::PollingError(e) => {
                println!("    error polling delete LRO, continuing {e:?}");
            }
            PollingResult::InProgress(m) => {
                println!("    delete LRO still in progress, metadata={m:?}");
            }
            PollingResult::Completed(Ok(_)) => {
                println!("    delete LRO finished successfully");
            }
            PollingResult::Completed(Err(e)) => {
                println!("    delete LRO finished with an error {e}");
                return Err(anyhow::Error::from(e));
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2);
    }

    Ok(())
}

fn test_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(100))
        .with_maximum_delay(Duration::from_secs(1))
        .build()
        .expect("test policy values should succeed")
}

async fn cleanup_stale_workflows(
    client: &Workflows,
    project_id: &str,
    location_id: &str,
) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut paginator = client
        .list_workflows()
        .set_parent(format!("projects/{project_id}/locations/{location_id}"))
        .by_item();
    let mut stale_workflows = Vec::new();
    while let Some(workflow) = paginator.next().await {
        let item = workflow?;
        if item
            .labels
            .get("integration-test")
            .is_some_and(|v| v == "true")
            && item.create_time.is_some_and(|v| v < stale_deadline)
        {
            stale_workflows.push(item.name);
        }
    }
    let pending = stale_workflows
        .iter()
        .map(|name| {
            client
                .delete_workflow()
                .set_name(name)
                .poller()
                .until_done()
        })
        .collect::<Vec<_>>();

    futures::future::join_all(pending)
        .await
        .into_iter()
        .zip(stale_workflows)
        .for_each(|(r, name)| println!("{name} = {r:?}"));

    Ok(())
}

pub async fn manual(
    project_id: String,
    region_id: String,
    workflow_id: String,
    workflow: Workflow,
) -> Result<()> {
    let client = Workflows::builder().with_tracing().build().await?;

    println!("\n\nStart create_workflow() LRO and poll it to completion");
    let create = client
        .create_workflow()
        .set_parent(format!("projects/{project_id}/locations/{region_id}"))
        .set_workflow_id(&workflow_id)
        .set_workflow(workflow)
        .send()
        .await?;
    if create.done {
        let result = create
            .result
            .ok_or_else(|| anyhow::Error::msg("service error: done with missing result "))?;
        match result {
            OperationResult::Error(status) => {
                println!("LRO completed with error {status:?}");
                let status = gax::error::rpc::Status::from(*status);
                return Err(anyhow::Error::from(gax::error::Error::service(status)));
            }
            OperationResult::Response(any) => {
                println!("LRO completed successfully {any:?}");
                let response = any.to_msg::<Workflow>();
                println!("LRO completed response={response:?}");
                return Ok(());
            }
            _ => panic!("unexpected branch"),
        }
    }
    let name = create.name;
    loop {
        let operation = client.get_operation().set_name(name.clone()).send().await?;
        if !operation.done {
            println!("operation is pending {operation:?}");
            if let Some(any) = operation.metadata {
                match any.to_msg::<OperationMetadata>() {
                    Err(_) => {
                        println!("    cannot extract expected metadata from {any:?}");
                    }
                    Ok(metadata) => {
                        println!("    metadata={metadata:?}");
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        let result = create
            .result
            .ok_or_else(|| anyhow::Error::msg("service error: done with missing result "))?;
        match result {
            OperationResult::Error(status) => {
                println!("LRO completed with error {status:?}");
                let status = gax::error::rpc::Status::from(&*status);
                return Err(anyhow::Error::from(gax::error::Error::service(status)));
            }
            OperationResult::Response(any) => {
                println!("LRO completed successfully {any:?}");
                let response = any.to_msg::<Workflow>();
                println!("LRO completed response={response:?}");
                return Ok(());
            }
            _ => panic!("unexpected branch"),
        }
    }
}
