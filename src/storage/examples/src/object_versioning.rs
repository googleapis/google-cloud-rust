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

use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::bucket::Versioning;
use google_cloud_wkt::FieldMask;

// [START storage_view_versioning_status]
pub async fn view_versioning_status(
    client: &StorageControl,
    bucket_id: &str,
) -> anyhow::Result<()> {
    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    if let Some(versioning) = bucket.versioning {
        if versioning.enabled {
            println!("Versioning is enabled for bucket {bucket_id}");
        } else {
            println!("Versioning is disabled for bucket {bucket_id}");
        }
    } else {
        println!("Versioning is not configured for bucket {bucket_id}");
    }
    Ok(())
}
// [END storage_view_versioning_status]

// [START storage_enable_versioning]
pub async fn enable_versioning(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    let metageneration = bucket.metageneration;
    let bucket = client
        .update_bucket()
        .set_bucket(bucket.set_versioning(Versioning::new().set_enabled(true)))
        .set_if_metageneration_match(metageneration)
        .set_update_mask(FieldMask::default().set_paths(["versioning"]))
        .send()
        .await?;
    println!("Versioning enabled for bucket {bucket_id}: {bucket:?}");
    Ok(())
}
// [END storage_enable_versioning]

// [START storage_disable_versioning]
pub async fn disable_versioning(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    let metageneration = bucket.metageneration;
    let bucket = client
        .update_bucket()
        .set_bucket(bucket.set_versioning(Versioning::new().set_enabled(false)))
        .set_if_metageneration_match(metageneration)
        .set_update_mask(FieldMask::default().set_paths(["versioning"]))
        .send()
        .await?;
    println!("Versioning disabled for bucket {bucket_id}: {bucket:?}");
    Ok(())
}
// [END storage_disable_versioning]
