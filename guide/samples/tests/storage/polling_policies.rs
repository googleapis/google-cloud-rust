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
// [START rust_storage_polling_policies_use] ANCHOR: use
use google_cloud_storage::client::StorageControl;
// [END rust_storage_polling_policies_use] ANCHOR_END: use

// [START rust_storage_polling_policies_client_backoff] ANCHOR: client-backoff
pub async fn client_backoff(bucket: &str, folder: &str, dest: &str) -> Result<()> {
    // [START rust_storage_polling_policies_client_backoff_use] ANCHOR: client-backoff-use
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    // [END rust_storage_polling_policies_client_backoff_use] ANCHOR_END: client-backoff-use
    use google_cloud_lro::Poller;
    use std::time::Duration;

    // [START rust_storage_polling_policies_client_backoff_client] ANCHOR: client-backoff-client
    let client = StorageControl::builder()
        .with_polling_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_millis(250))
                .with_maximum_delay(Duration::from_secs(10))
                .build()?,
        )
        .build()
        .await?;
    // [END rust_storage_polling_policies_client_backoff_client] ANCHOR_END: client-backoff-client

    // [START rust_storage_polling_policies_client_backoff_builder] ANCHOR: client-backoff-builder
    let response = client
        .rename_folder()
        // [END rust_storage_polling_policies_client_backoff_builder] ANCHOR_END: client-backoff-builder
        // [START rust_storage_polling_policies_client_backoff_prepare] ANCHOR: client-backoff-prepare
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // [END rust_storage_polling_policies_client_backoff_prepare] ANCHOR_END: client-backoff-prepare
        // [START rust_storage_polling_policies_client_backoff_print] ANCHOR: client-backoff-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // [END rust_storage_polling_policies_client_backoff_print] ANCHOR_END: client-backoff-print

    Ok(())
}
// [END rust_storage_polling_policies_client_backoff] ANCHOR_END: client-backoff

// [START rust_storage_polling_policies_rpc_backoff] ANCHOR: rpc-backoff
pub async fn rpc_backoff(bucket: &str, folder: &str, dest: &str) -> Result<()> {
    // [START rust_storage_polling_policies_rpc_backoff_use] ANCHOR: rpc-backoff-use
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use std::time::Duration;
    // [END rust_storage_polling_policies_rpc_backoff_use] ANCHOR_END: rpc-backoff-use
    // [START rust_storage_polling_policies_rpc_backoff_builder_trait] ANCHOR: rpc-backoff-builder-trait
    use google_cloud_gax::options::RequestOptionsBuilder;
    // [END rust_storage_polling_policies_rpc_backoff_builder_trait] ANCHOR_END: rpc-backoff-builder-trait
    use google_cloud_lro::Poller;

    // [START rust_storage_polling_policies_rpc_backoff_client] ANCHOR: rpc-backoff-client
    let client = StorageControl::builder().build().await?;
    // [END rust_storage_polling_policies_rpc_backoff_client] ANCHOR_END: rpc-backoff-client

    // [START rust_storage_polling_policies_rpc_backoff_builder] ANCHOR: rpc-backoff-builder
    let response = client
        .rename_folder()
        // [END rust_storage_polling_policies_rpc_backoff_builder] ANCHOR_END: rpc-backoff-builder
        // [START rust_storage_polling_policies_rpc_backoff_rpc_polling_backoff] ANCHOR: rpc-backoff-rpc-polling-backoff
        .with_polling_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_millis(250))
                .with_maximum_delay(Duration::from_secs(10))
                .build()?,
        )
        // [END rust_storage_polling_policies_rpc_backoff_rpc_polling_backoff] ANCHOR_END: rpc-backoff-rpc-polling-backoff
        // [START rust_storage_polling_policies_rpc_backoff_prepare] ANCHOR: rpc-backoff-prepare
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // [END rust_storage_polling_policies_rpc_backoff_prepare] ANCHOR_END: rpc-backoff-prepare
        // [START rust_storage_polling_policies_rpc_backoff_print] ANCHOR: rpc-backoff-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // [END rust_storage_polling_policies_rpc_backoff_print] ANCHOR_END: rpc-backoff-print

    Ok(())
}
// [END rust_storage_polling_policies_rpc_backoff] ANCHOR_END: rpc-backoff

// [START rust_storage_polling_policies_client_errors] ANCHOR: client-errors
pub async fn client_errors(bucket: &str, folder: &str, dest: &str) -> Result<()> {
    // [START rust_storage_polling_policies_client_errors_use] ANCHOR: client-errors-use
    use google_cloud_gax::polling_error_policy::Aip194Strict;
    use google_cloud_gax::polling_error_policy::PollingErrorPolicyExt;
    use google_cloud_gax::retry_policy;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use std::time::Duration;
    // [END rust_storage_polling_policies_client_errors_use] ANCHOR_END: client-errors-use
    use google_cloud_lro::Poller;

    // [START rust_storage_polling_policies_client_errors_client] ANCHOR: client-errors-client
    let builder = StorageControl::builder().with_polling_error_policy(
        Aip194Strict
            .with_attempt_limit(100)
            .with_time_limit(Duration::from_secs(300)),
    );
    // [END rust_storage_polling_policies_client_errors_client] ANCHOR_END: client-errors-client

    // [START rust_storage_polling_policies_client_errors_client_retry] ANCHOR: client-errors-client-retry
    let client = builder
        .with_retry_policy(
            retry_policy::Aip194Strict
                .with_attempt_limit(100)
                .with_time_limit(Duration::from_secs(300)),
        )
        .build()
        .await?;
    // [END rust_storage_polling_policies_client_errors_client_retry] ANCHOR_END: client-errors-client-retry

    // [START rust_storage_polling_policies_client_errors_builder] ANCHOR: client-errors-builder
    let response = client
        .rename_folder()
        // [END rust_storage_polling_policies_client_errors_builder] ANCHOR_END: client-errors-builder
        // [START rust_storage_polling_policies_client_errors_prepare] ANCHOR: client-errors-prepare
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // [END rust_storage_polling_policies_client_errors_prepare] ANCHOR_END: client-errors-prepare
        // [START rust_storage_polling_policies_client_errors_print] ANCHOR: client-errors-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // [END rust_storage_polling_policies_client_errors_print] ANCHOR_END: client-errors-print

    Ok(())
}
// [END rust_storage_polling_policies_client_errors] ANCHOR_END: client-errors

// [START rust_storage_polling_policies_rpc_errors] ANCHOR: rpc-errors
pub async fn rpc_errors(bucket: &str, folder: &str, dest: &str) -> Result<()> {
    // [START rust_storage_polling_policies_rpc_errors_use] ANCHOR: rpc-errors-use
    use google_cloud_gax::polling_error_policy::Aip194Strict;
    use google_cloud_gax::polling_error_policy::PollingErrorPolicyExt;
    use google_cloud_gax::retry_policy;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use std::time::Duration;
    // [END rust_storage_polling_policies_rpc_errors_use] ANCHOR_END: rpc-errors-use
    // [START rust_storage_polling_policies_rpc_errors_builder_trait] ANCHOR: rpc-errors-builder-trait
    use google_cloud_gax::options::RequestOptionsBuilder;
    // [END rust_storage_polling_policies_rpc_errors_builder_trait] ANCHOR_END: rpc-errors-builder-trait
    use google_cloud_lro::Poller;

    // [START rust_storage_polling_policies_rpc_errors_client] ANCHOR: rpc-errors-client
    let client = StorageControl::builder()
        .with_retry_policy(
            retry_policy::Aip194Strict
                .with_attempt_limit(100)
                .with_time_limit(Duration::from_secs(300)),
        )
        .build()
        .await?;
    // [END rust_storage_polling_policies_rpc_errors_client] ANCHOR_END: rpc-errors-client

    // [START rust_storage_polling_policies_rpc_errors_builder] ANCHOR: rpc-errors-builder
    let response = client
        .rename_folder()
        // [END rust_storage_polling_policies_rpc_errors_builder] ANCHOR_END: rpc-errors-builder
        // [START rust_storage_polling_policies_rpc_errors_rpc_polling_errors] ANCHOR: rpc-errors-rpc-polling-errors
        .with_polling_error_policy(
            Aip194Strict
                .with_attempt_limit(100)
                .with_time_limit(Duration::from_secs(300)),
        )
        // [END rust_storage_polling_policies_rpc_errors_rpc_polling_errors] ANCHOR_END: rpc-errors-rpc-polling-errors
        // [START rust_storage_polling_policies_rpc_errors_prepare] ANCHOR: rpc-errors-prepare
        .set_name(format!("projects/_/buckets/{bucket}/folders/{folder}"))
        .set_destination_folder_id(dest)
        // [END rust_storage_polling_policies_rpc_errors_prepare] ANCHOR_END: rpc-errors-prepare
        // [START rust_storage_polling_policies_rpc_errors_print] ANCHOR: rpc-errors-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // [END rust_storage_polling_policies_rpc_errors_print] ANCHOR_END: rpc-errors-print

    Ok(())
}
// [END rust_storage_polling_policies_rpc_errors] ANCHOR_END: rpc-errors

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
