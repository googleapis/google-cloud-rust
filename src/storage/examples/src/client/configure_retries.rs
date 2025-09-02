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

// [START storage_configure_retries]
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::retry_policy::RetryableErrors;
use std::time::Duration;

pub async fn sample(bucket_id: &str) -> anyhow::Result<()> {
    // Retries all operations for up to 5 minutes, including any backoff time.
    let retry_policy = RetryableErrors.with_time_limit(Duration::from_secs(60 * 5));
    // On error, it backs off for a random delay between [1, 3] seconds, then [3,
    // 9] seconds, then [9, 27] seconds, etc. The backoff time never grows larger
    // than 1 minute.
    let backoff_policy = ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_secs(1))
        .with_maximum_delay(Duration::from_secs(60))
        .with_scaling(3)
        .build()?;

    let control = StorageControl::builder()
        .with_retry_policy(retry_policy)
        .with_backoff_policy(backoff_policy)
        .build()
        .await?;
    // Use the `StorageControl` client as usual:
    let bucket = control
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    println!("Bucket {bucket_id} metadata is {bucket:?}");

    // Retries all operations for up to 5 attempts.
    let retry_policy = RetryableErrors.with_attempt_limit(5);
    // On error, it backs off for a random delay between [1, 3] seconds, then [3,
    // 9] seconds, then [9, 27] seconds, etc. The backoff time never grows larger
    // than 1 minute.
    let backoff_policy = ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_secs(1))
        .with_maximum_delay(Duration::from_secs(60))
        .with_scaling(3)
        .build()?;

    const NAME: &str = "hello-world.txt";
    let client = Storage::builder()
        .with_retry_policy(retry_policy)
        .with_backoff_policy(backoff_policy)
        .build()
        .await?;
    // Use the `Storage` client as usual:
    let reader = client
        .read_object(format!("projects/_/buckets/{bucket_id}"), NAME)
        .send()
        .await?;
    println!("Object highlights: {:?}", reader.object());

    Ok(())
}
// [END storage_configure_retries]
