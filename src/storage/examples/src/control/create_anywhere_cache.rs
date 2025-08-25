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

// [START storage_control_create_anywhere_cache]
use google_cloud_lro::Poller;
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::AnywhereCache;

pub async fn sample(client: &StorageControl, bucket_id: &str, zone: &str) -> anyhow::Result<()> {
    let cache = client
        .create_anywhere_cache()
        .set_parent(format!("projects/_/buckets/{bucket_id}"))
        .set_anywhere_cache(AnywhereCache::new().set_zone(zone).set_name(format!(
            "projects/_/buckets/{bucket_id}/anywhereCaches/{zone}"
        )))
        .poller()
        .until_done()
        .await?;
    println!("Created anywhere cache: {cache:?}");
    Ok(())
}
// [END storage_control_create_anywhere_cache]
