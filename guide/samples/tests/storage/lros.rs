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

// ANCHOR: use
use anyhow::anyhow;
use google_cloud_longrunning as longrunning;
use google_cloud_storage::client::StorageControl;
// ANCHOR_END: use

// ANCHOR: manual
// ANCHOR: client
pub async fn manual(bucket: &str, folder: &str, dest: &str) -> anyhow::Result<()> {
    use google_cloud_storage::model::Folder;
    use google_cloud_storage::model::RenameFolderMetadata;

    let client = StorageControl::builder().build().await?;
    // ANCHOR_END: client

    // ANCHOR: request-builder
    let operation = client
        .rename_folder()
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // ANCHOR_END: request-builder
        // ANCHOR: send
        .send()
        .await?;
    // ANCHOR_END: send
    println!("LRO started, response={operation:?}");

    // ANCHOR: manual-loop
    let mut operation = operation;
    // ANCHOR: manual-if-done
    let response: anyhow::Result<Folder> = loop {
        if operation.done {
            // ANCHOR_END: manual-if-done
            // ANCHOR: manual-match-none
            match &operation.result {
                None => {
                    break Err(anyhow!("missing result for finished operation"));
                }
                // ANCHOR_END: manual-match-none
                // ANCHOR: manual-match-error
                Some(r) => {
                    break match r {
                        longrunning::model::operation::Result::Error(s) => {
                            Err(anyhow!("operation completed with error {s:?}"))
                        }
                        // ANCHOR_END: manual-match-error
                        // ANCHOR: manual-match-success
                        longrunning::model::operation::Result::Response(any) => {
                            let response = any.to_msg::<Folder>()?;
                            Ok(response)
                        }
                        // ANCHOR_END: manual-match-success
                        // ANCHOR: manual-match-default
                        _ => Err(anyhow!("unexpected result branch {r:?}")),
                        // ANCHOR_END: manual-match-default
                    };
                }
            }
        }
        // ANCHOR: manual-metadata
        if let Some(any) = &operation.metadata {
            let metadata = any.to_msg::<RenameFolderMetadata>()?;
            println!("LRO in progress, metadata={metadata:?}");
        }
        // ANCHOR_END: manual-metadata
        // ANCHOR: manual-backoff
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // ANCHOR_END: manual-backoff
        // ANCHOR: manual-poll-again
        if let Ok(attempt) = client
            .get_operation()
            .set_name(&operation.name)
            .send()
            .await
        {
            operation = attempt;
        }
        // ANCHOR_END: manual-poll-again
    };
    // ANCHOR_END: manual-loop
    println!("LRO completed, response={response:?}");

    Ok(())
}
// ANCHOR_END: manual

// ANCHOR: automatic
pub async fn automatic(bucket: &str, folder: &str, dest: &str) -> anyhow::Result<()> {
    // ANCHOR: automatic-use
    use google_cloud_lro::Poller;
    // ANCHOR_END: automatic-use

    let client = StorageControl::builder().build().await?;

    // ANCHOR: automatic-prepare
    let response = client
        .rename_folder()
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // ANCHOR_END: automatic-prepare
        // ANCHOR: automatic-print
        // ANCHOR: automatic-poller-until-done
        .poller()
        .until_done()
        .await?;
    // ANCHOR_END: automatic-poller-until-done

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: automatic-print

    Ok(())
}
// ANCHOR_END: automatic

// ANCHOR: polling
pub async fn polling(bucket: &str, folder: &str, dest: &str) -> anyhow::Result<()> {
    // ANCHOR: polling-use
    use google_cloud_lro::{Poller, PollingResult};
    // ANCHOR_END: polling-use

    let client = StorageControl::builder().build().await?;

    let mut poller = client
        .rename_folder()
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // ANCHOR: polling-poller
        .poller();
    // ANCHOR_END: polling-poller

    // ANCHOR: polling-loop
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
    // ANCHOR_END: polling-loop

    Ok(())
}
// ANCHOR_END: polling

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
