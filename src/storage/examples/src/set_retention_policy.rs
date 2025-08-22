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

// [START storage_set_retention_policy]
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::bucket::RetentionPolicy;
use google_cloud_wkt::{Duration, FieldMask};

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    retention_period: i64,
) -> anyhow::Result<()> {
    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    let metageneration = bucket.metageneration;
    let retention_policy =
        RetentionPolicy::new().set_retention_duration(Duration::new(retention_period, 0)?);
    let bucket = client
        .update_bucket()
        .set_bucket(bucket.set_retention_policy(retention_policy))
        .set_if_metageneration_match(metageneration)
        .set_update_mask(FieldMask::default().set_paths(["retention_policy"]))
        .send()
        .await?;
    println!(
        "Retention policy for bucket {} set to: {:?}",
        bucket_id, bucket.retention_policy
    );
    Ok(())
}
// [END storage_set_retention_policy]
