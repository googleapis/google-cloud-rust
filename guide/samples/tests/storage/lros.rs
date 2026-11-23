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

// [START rust_storage_lros_use] ANCHOR: use
use anyhow::anyhow;
use google_cloud_longrunning as longrunning;
use google_cloud_storage::client::StorageControl;
// [END rust_storage_lros_use] ANCHOR_END: use

// [START rust_storage_lros_manual] ANCHOR: manual
// [START rust_storage_lros_manual_arguments] ANCHOR: manual-arguments
pub async fn manual(bucket: &str, folder: &str, dest: &str) -> anyhow::Result<()> {
    // [END rust_storage_lros_manual_arguments] ANCHOR_END: manual-arguments
    use google_cloud_storage::model::Folder;
    use google_cloud_storage::model::RenameFolderMetadata;

    // [START rust_storage_lros_client] ANCHOR: client
    let client = StorageControl::builder().build().await?;
    // [END rust_storage_lros_client] ANCHOR_END: client

    // [START rust_storage_lros_request_builder] ANCHOR: request-builder
    let operation = client
        .rename_folder()
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // [END rust_storage_lros_request_builder] ANCHOR_END: request-builder
        // [START rust_storage_lros_send] ANCHOR: send
        .send()
        .await?;
    // [END rust_storage_lros_send] ANCHOR_END: send
    println!("LRO started, response={operation:?}");

    // [START rust_storage_lros_manual_loop] ANCHOR: manual-loop
    let mut operation = operation;
    // [START rust_storage_lros_manual_if_done] ANCHOR: manual-if-done
    let response: anyhow::Result<Folder> = loop {
        if operation.done {
            // [END rust_storage_lros_manual_if_done] ANCHOR_END: manual-if-done
            // [START rust_storage_lros_manual_match_none] ANCHOR: manual-match-none
            match &operation.result {
                None => {
                    break Err(anyhow!("missing result for finished operation"));
                }
                // [END rust_storage_lros_manual_match_none] ANCHOR_END: manual-match-none
                // [START rust_storage_lros_manual_match_error] ANCHOR: manual-match-error
                Some(r) => {
                    break match r {
                        longrunning::model::operation::Result::Error(s) => {
                            Err(anyhow!("operation completed with error {s:?}"))
                        }
                        // [END rust_storage_lros_manual_match_error] ANCHOR_END: manual-match-error
                        // [START rust_storage_lros_manual_match_success] ANCHOR: manual-match-success
                        longrunning::model::operation::Result::Response(any) => {
                            let response = any.to_msg::<Folder>()?;
                            Ok(response)
                        }
                        // [END rust_storage_lros_manual_match_success] ANCHOR_END: manual-match-success
                        // [START rust_storage_lros_manual_match_default] ANCHOR: manual-match-default
                        _ => Err(anyhow!("unexpected result branch {r:?}")),
                        // [END rust_storage_lros_manual_match_default] ANCHOR_END: manual-match-default
                    };
                }
            }
        }
        // [START rust_storage_lros_manual_metadata] ANCHOR: manual-metadata
        if let Some(any) = &operation.metadata {
            let metadata = any.to_msg::<RenameFolderMetadata>()?;
            println!("LRO in progress, metadata={metadata:?}");
        }
        // [END rust_storage_lros_manual_metadata] ANCHOR_END: manual-metadata
        // [START rust_storage_lros_manual_backoff] ANCHOR: manual-backoff
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // [END rust_storage_lros_manual_backoff] ANCHOR_END: manual-backoff
        // [START rust_storage_lros_manual_poll_again] ANCHOR: manual-poll-again
        if let Ok(attempt) = client
            .get_operation()
            .set_name(&operation.name)
            .send()
            .await
        {
            operation = attempt;
        }
        // [END rust_storage_lros_manual_poll_again] ANCHOR_END: manual-poll-again
    };
    // [END rust_storage_lros_manual_loop] ANCHOR_END: manual-loop
    println!("LRO completed, response={response:?}");

    Ok(())
}
// [END rust_storage_lros_manual] ANCHOR_END: manual

// [START rust_storage_lros_automatic] ANCHOR: automatic
pub async fn automatic(bucket: &str, folder: &str, dest: &str) -> anyhow::Result<()> {
    // [START rust_storage_lros_automatic_use] ANCHOR: automatic-use
    use google_cloud_lro::Poller;
    // [END rust_storage_lros_automatic_use] ANCHOR_END: automatic-use

    let client = StorageControl::builder().build().await?;

    // [START rust_storage_lros_automatic_prepare] ANCHOR: automatic-prepare
    let response = client
        .rename_folder()
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // [END rust_storage_lros_automatic_prepare] ANCHOR_END: automatic-prepare
        // [START rust_storage_lros_automatic_print] ANCHOR: automatic-print
        // [START rust_storage_lros_automatic_poller_until_done] ANCHOR: automatic-poller-until-done
        .poller()
        .until_done()
        .await?;
    // [END rust_storage_lros_automatic_poller_until_done] ANCHOR_END: automatic-poller-until-done

    println!("LRO completed, response={response:?}");
    // [END rust_storage_lros_automatic_print] ANCHOR_END: automatic-print

    Ok(())
}
// [END rust_storage_lros_automatic] ANCHOR_END: automatic

// [START rust_storage_lros_polling] ANCHOR: polling
pub async fn polling(bucket: &str, folder: &str, dest: &str) -> anyhow::Result<()> {
    // [START rust_storage_lros_polling_use] ANCHOR: polling-use
    use google_cloud_lro::{Poller, PollingResult};
    // [END rust_storage_lros_polling_use] ANCHOR_END: polling-use

    let client = StorageControl::builder().build().await?;

    let mut poller = client
        .rename_folder()
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // [START rust_storage_lros_polling_poller] ANCHOR: polling-poller
        .poller();
    // [END rust_storage_lros_polling_poller] ANCHOR_END: polling-poller

    // [START rust_storage_lros_polling_loop] ANCHOR: polling-loop
    while let Some(p) = poller.poll().await {
        match p {
            PollingResult::Completed(r) => {
                println!("LRO completed, response={r:?}");
            }
            PollingResult::InProgress(m) => {
                println!("LRO in progress, metadata={m:?}");
            }
            PollingResult::PollingError(e) => {
                println!("Transient error polling the LRO: {e}");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    // [END rust_storage_lros_polling_loop] ANCHOR_END: polling-loop

    Ok(())
}
// [END rust_storage_lros_polling] ANCHOR_END: polling

pub async fn test(control: &StorageControl, bucket: &str) -> anyhow::Result<()> {
    for id in ["manual/", "automatic/", "polling/"] {
        let folder = control
            .create_folder()
            .set_parent(bucket)
            .set_folder_id(id)
            .send()
            .await?;
        println!("created folder {id}: {folder:?}");
    }
    let bucket_id = bucket.strip_prefix("projects/_/buckets/").ok_or(anyhow!(
        "bad bucket name format {bucket}, should start with `projects/_/buckets/`"
    ))?;
    println!("running manual LRO example");
    manual(bucket_id, "manual", "manual-renamed").await?;
    println!("running automatic LRO example");
    automatic(bucket_id, "automatic", "automatic-renamed").await?;
    println!("running automatic LRO with polling example");
    polling(bucket_id, "polling", "polling-renamed").await?;
    Ok(())
}
