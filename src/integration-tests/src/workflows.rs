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
use gax::{error::Error, options::RequestOptionsBuilder};
use rand::{distr::Alphanumeric, Rng};
use std::time::Duration;
use wf::Poller;

pub const WORKFLOW_ID_LENGTH: usize = 64;

pub async fn until_done(config: Option<gax::options::ClientConfig>) -> Result<()> {
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
    let location_id = crate::region_id();
    let workflows_runner = crate::workflows_runner()?;

    // We could simplify the code, but we want to test both ::new_with_config()
    // and ::new().
    let client = if let Some(config) = config {
        wf::client::Workflows::new_with_config(config).await?
    } else {
        wf::client::Workflows::new().await?
    };
    cleanup_stale_workflows(&client, &project_id, &location_id).await?;

    let source_contents = r###"# Test only workflow
main:
    steps:
        - sayHello:
            return: Hello World
"###;
    let source_code = wf::model::workflow::SourceCode::SourceContents(source_contents.to_string());
    let prefix = "wf-";
    let workflow_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        // Workflow ids must start with a letter, we use `wf-` as a prefix to
        // this requirement (see below).
        .take(WORKFLOW_ID_LENGTH - prefix.len())
        .map(char::from)
        .collect();
    let workflow_id = format!("{prefix}{workflow_id}");

    println!("\n\nStart create_workflow() LRO and poll it to completion");
    let response = client
        .create_workflow(format!("projects/{project_id}/locations/{location_id}"))
        .set_workflow_id(&workflow_id)
        .set_workflow(
            wf::model::Workflow::new()
                .set_labels([("integration-test", "true")])
                .set_call_log_level(wf::model::workflow::call_log_level::LOG_ERRORS_ONLY)
                .set_state(wf::model::workflow::state::UNAVAILABLE)
                .set_service_account(&workflows_runner)
                .set_source_code(source_code),
        )
        .with_polling_backoff_policy(test_backoff()?)
        .poller()
        .until_done()
        .await?;
    println!("    create LRO finished, response={response:?}");

    println!("\n\nStart delete_workflow() LRO and poll it to completion");
    let delete = client
        .delete_workflow(format!(
            "projects/{project_id}/locations/{location_id}/workflows/{workflow_id}"
        ))
        .poller()
        .until_done()
        .await?;
    println!("    delete LRO finished, response={delete:?}");

    Ok(())
}

pub async fn explicit_loop(config: Option<gax::options::ClientConfig>) -> Result<()> {
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
    let location_id = crate::region_id();
    let workflows_runner = crate::workflows_runner()?;

    // We could simplify the code, but we want to test both ::new_with_config()
    // and ::new().
    let client = if let Some(config) = config {
        wf::client::Workflows::new_with_config(config).await?
    } else {
        wf::client::Workflows::new().await?
    };
    cleanup_stale_workflows(&client, &project_id, &location_id).await?;

    let source_contents = r###"# Test only workflow
main:
    steps:
        - sayHello:
            return: Hello World
"###;
    let source_code = wf::model::workflow::SourceCode::SourceContents(source_contents.to_string());
    let prefix = "wf-";
    let workflow_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        // Workflow ids must start with a letter, we use `wf-` as a prefix to
        // this requirement (see below).
        .take(WORKFLOW_ID_LENGTH - prefix.len())
        .map(char::from)
        .collect();
    let workflow_id = format!("{prefix}{workflow_id}");

    println!("\n\nStart create_workflow() LRO and poll it to completion");
    let mut create = client
        .create_workflow(format!("projects/{project_id}/locations/{location_id}"))
        .set_workflow_id(&workflow_id)
        .set_workflow(
            wf::model::Workflow::new()
                .set_labels([("integration-test", "true")])
                .set_call_log_level(wf::model::workflow::call_log_level::LOG_ERRORS_ONLY)
                .set_state(wf::model::workflow::state::UNAVAILABLE)
                .set_service_account(&workflows_runner)
                .set_source_code(source_code),
        )
        .poller();
    let mut backoff = Duration::from_millis(100);
    while let Some(status) = create.poll().await {
        match status {
            wf::PollingResult::PollingError(e) => {
                println!("    error polling create LRO, continuing {e}");
            }
            wf::PollingResult::InProgress(m) => {
                println!("    create LRO still in progress, metadata={m:?}");
            }
            wf::PollingResult::Completed(r) => match r {
                Err(e) => {
                    println!("    create LRO finished with error={e}\n\n");
                    return Err(e);
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
        .delete_workflow(format!(
            "projects/{project_id}/locations/{location_id}/workflows/{workflow_id}"
        ))
        .poller();
    let mut backoff = Duration::from_millis(100);
    while let Some(status) = delete.poll().await {
        match status {
            wf::PollingResult::PollingError(e) => {
                println!("    error polling delete LRO, continuing {e:?}");
            }
            wf::PollingResult::InProgress(m) => {
                println!("    delete LRO still in progress, metadata={m:?}");
            }
            wf::PollingResult::Completed(r) => {
                println!("    delete LRO finished, result={r:?}");
                let _ = r?;
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2);
    }

    Ok(())
}

fn test_backoff() -> Result<ExponentialBackoff> {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(100))
        .with_maximum_delay(Duration::from_secs(1))
        .build()
}

async fn cleanup_stale_workflows(
    client: &wf::client::Workflows,
    project_id: &str,
    location_id: &str,
) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(Error::other)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut stream = client
        .list_workflows(format!("projects/{project_id}/locations/{location_id}"))
        .stream()
        .await
        .items();
    let mut stale_workflows = Vec::new();
    while let Some(workflow) = stream.next().await {
        let item = workflow?;
        if let Some("true") = item.labels.get("integration-test").map(String::as_str) {
            if let Some(true) = item.create_time.map(|v| v < stale_deadline) {
                stale_workflows.push(item.name);
            }
        }
    }
    let pending = stale_workflows
        .iter()
        .map(|name| client.delete_workflow(name).poller().until_done())
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
    workflow: wf::model::Workflow,
) -> Result<()> {
    let client = wf::client::Workflows::new().await?;

    println!("\n\nStart create_workflow() LRO and poll it to completion");
    let create = client
        .create_workflow(format!("projects/{project_id}/locations/{region_id}"))
        .set_workflow_id(&workflow_id)
        .set_workflow(workflow)
        .send()
        .await?;
    if create.done {
        use longrunning::model::operation::Result as LR;
        let result = create
            .result
            .ok_or("service error: done with missing result ")
            .map_err(Error::other)?;
        match result {
            LR::Error(status) => {
                println!("LRO completed with error {status:?}");
                let err = gax::error::ServiceError::from(*status);
                return Err(Error::rpc(err));
            }
            LR::Response(any) => {
                println!("LRO completed successfully {any:?}");
                let response = any
                    .try_into_message::<wf::model::Workflow>()
                    .map_err(Error::other);
                println!("LRO completed response={response:?}");
                return Ok(());
            }
            _ => panic!("unexpected branch"),
        }
    }
    let name = create.name;
    loop {
        let operation = client.get_operation(name.clone()).send().await?;
        if !operation.done {
            println!("operation is pending {operation:?}");
            if let Some(any) = operation.metadata {
                match any.try_into_message::<wf::model::OperationMetadata>() {
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
        use longrunning::model::operation::Result as LR;
        let result = create
            .result
            .ok_or("service error: done with missing result ")
            .map_err(Error::other)?;
        match result {
            LR::Error(status) => {
                println!("LRO completed with error {status:?}");
                let err = gax::error::ServiceError::from(*status);
                return Err(Error::rpc(err));
            }
            LR::Response(any) => {
                println!("LRO completed successfully {any:?}");
                let response = any
                    .try_into_message::<wf::model::Workflow>()
                    .map_err(Error::other);
                println!("LRO completed response={response:?}");
                return Ok(());
            }
            _ => panic!("unexpected branch"),
        }
    }
}
