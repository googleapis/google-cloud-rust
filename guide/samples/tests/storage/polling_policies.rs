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

//! Examples showing how to configure the polling policies.

use anyhow::{Result, anyhow};
// ANCHOR: use
use google_cloud_storage::client::StorageControl;
// ANCHOR_END: use

// ANCHOR: client-backoff
pub async fn client_backoff(bucket: &str, folder: &str, dest: &str) -> Result<()> {
    // ANCHOR: client-backoff-use
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    // ANCHOR_END: client-backoff-use
    use google_cloud_lro::Poller;
    use std::time::Duration;

    // ANCHOR: client-backoff-client
    let client = StorageControl::builder()
        .with_polling_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_millis(250))
                .with_maximum_delay(Duration::from_secs(10))
                .build()?,
        )
        .build()
        .await?;
    // ANCHOR_END: client-backoff-client

    // ANCHOR: client-backoff-builder
    let response = client
        .rename_folder()
        // ANCHOR_END: client-backoff-builder
        // ANCHOR: client-backoff-prepare
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // ANCHOR_END: client-backoff-prepare
        // ANCHOR: client-backoff-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: client-backoff-print

    Ok(())
}
// ANCHOR_END: client-backoff

// ANCHOR: rpc-backoff
pub async fn rpc_backoff(bucket: &str, folder: &str, dest: &str) -> Result<()> {
    // ANCHOR: rpc-backoff-use
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use std::time::Duration;
    // ANCHOR_END: rpc-backoff-use
    // ANCHOR: rpc-backoff-builder-trait
    use google_cloud_gax::options::RequestOptionsBuilder;
    // ANCHOR_END: rpc-backoff-builder-trait
    use google_cloud_lro::Poller;

    // ANCHOR: rpc-backoff-client
    let client = StorageControl::builder().build().await?;
    // ANCHOR_END: rpc-backoff-client

    // ANCHOR: rpc-backoff-builder
    let response = client
        .rename_folder()
        // ANCHOR_END: rpc-backoff-builder
        // ANCHOR: rpc-backoff-rpc-polling-backoff
        .with_polling_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_millis(250))
                .with_maximum_delay(Duration::from_secs(10))
                .build()?,
        )
        // ANCHOR_END: rpc-backoff-rpc-polling-backoff
        // ANCHOR: rpc-backoff-prepare
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // ANCHOR_END: rpc-backoff-prepare
        // ANCHOR: rpc-backoff-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: rpc-backoff-print

    Ok(())
}
// ANCHOR_END: rpc-backoff

// ANCHOR: client-errors
pub async fn client_errors(bucket: &str, folder: &str, dest: &str) -> Result<()> {
    // ANCHOR: client-errors-use
    use google_cloud_gax::polling_error_policy::Aip194Strict;
    use google_cloud_gax::polling_error_policy::PollingErrorPolicyExt;
    use google_cloud_gax::retry_policy;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use std::time::Duration;
    // ANCHOR_END: client-errors-use
    use google_cloud_lro::Poller;

    // ANCHOR: client-errors-client
    let builder = StorageControl::builder().with_polling_error_policy(
        Aip194Strict
            .with_attempt_limit(100)
            .with_time_limit(Duration::from_secs(300)),
    );
    // ANCHOR_END: client-errors-client

    // ANCHOR: client-errors-client-retry
    let client = builder
        .with_retry_policy(
            retry_policy::Aip194Strict
                .with_attempt_limit(100)
                .with_time_limit(Duration::from_secs(300)),
        )
        .build()
        .await?;
    // ANCHOR_END: client-errors-client-retry

    // ANCHOR: client-errors-builder
    let response = client
        .rename_folder()
        // ANCHOR_END: client-errors-builder
        // ANCHOR: client-errors-prepare
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // ANCHOR_END: client-errors-prepare
        // ANCHOR: client-errors-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: client-errors-print

    Ok(())
}
// ANCHOR_END: client-errors

// ANCHOR: rpc-errors
pub async fn rpc_errors(bucket: &str, folder: &str, dest: &str) -> Result<()> {
    // ANCHOR: rpc-errors-use
    use google_cloud_gax::polling_error_policy::Aip194Strict;
    use google_cloud_gax::polling_error_policy::PollingErrorPolicyExt;
    use google_cloud_gax::retry_policy;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use std::time::Duration;
    // ANCHOR_END: rpc-errors-use
    // ANCHOR: rpc-errors-builder-trait
    use google_cloud_gax::options::RequestOptionsBuilder;
    // ANCHOR_END: rpc-errors-builder-trait
    use google_cloud_lro::Poller;

    // ANCHOR: rpc-errors-client
    let client = StorageControl::builder()
        .with_retry_policy(
            retry_policy::Aip194Strict
                .with_attempt_limit(100)
                .with_time_limit(Duration::from_secs(300)),
        )
        .build()
        .await?;
    // ANCHOR_END: rpc-errors-client

    // ANCHOR: rpc-errors-builder
    let response = client
        .rename_folder()
        // ANCHOR_END: rpc-errors-builder
        // ANCHOR: rpc-errors-rpc-polling-errors
        .with_polling_error_policy(
            Aip194Strict
                .with_attempt_limit(100)
                .with_time_limit(Duration::from_secs(300)),
        )
        // ANCHOR_END: rpc-errors-rpc-polling-errors
        // ANCHOR: rpc-errors-prepare
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // ANCHOR_END: rpc-errors-prepare
        // ANCHOR: rpc-errors-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: rpc-errors-print

    Ok(())
}
// ANCHOR_END: rpc-errors

pub async fn test(control: &StorageControl, bucket: &str) -> Result<()> {
    for id in [
        "client-backoff/",
        "rpc-backoff/",
        "client-errors/",
        "rpc-errors/",
    ] {
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
    println!("running client_backoff example");
    client_backoff(bucket_id, "client-backoff", "client-backoff-renamed").await?;
    println!("running rpc_backoff example");
    rpc_backoff(bucket_id, "rpc-backoff", "rpc-backoff-renamed").await?;
    println!("running client_errors example");
    client_errors(bucket_id, "client-errors", "client-errors-renamed").await?;
    println!("running rpc_errors example");
    rpc_errors(bucket_id, "rpc-errors", "rpc-errors-renamed").await?;
    Ok(())
}
