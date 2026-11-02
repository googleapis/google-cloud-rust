// Copyright 2026 Google LLC
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

// [START storage_control_create_rapid_cache]
use google_cloud_lro::Poller;
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::RapidCache;

pub async fn sample(client: &StorageControl, bucket_id: &str, zone: &str) -> anyhow::Result<()> {
    // Creating a Rapid Cache starts a long-running operation. This waits
    // until the operation completes and returns the new cache.
    let cache = client
        .create_rapid_cache()
        .set_parent(format!("projects/_/buckets/{bucket_id}"))
        .set_rapid_cache(
            RapidCache::new()
                .set_zone(zone)
                .set_cache_type("rapid-cache-ultra"),
        )
        .poller()
        .until_done()
        .await?;
    println!("Created Rapid Cache: {}", cache.name);
    Ok(())
}
// [END storage_control_create_rapid_cache]
