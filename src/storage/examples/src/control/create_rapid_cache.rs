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
use google_cloud_wkt::Duration;

pub async fn sample(client: &StorageControl, bucket_id: &str, zone: &str) -> anyhow::Result<()> {
    // The Rapid Cache ID must be the same as the zone of the cache.
    let name = format!("projects/_/buckets/{bucket_id}/rapidCaches/{zone}");
    // Creating a Rapid Cache starts a long-running operation. This waits
    // until the operation completes and returns the new cache.
    let cache = client
        .create_rapid_cache()
        .set_parent(format!("projects/_/buckets/{bucket_id}"))
        .set_rapid_cache(
            RapidCache::new()
                .set_name(name)
                .set_zone(zone)
                .set_cache_type("rapid-cache-ultra")
                // Cache entries expire after 24 hours (86400 seconds).
                .set_ttl(Duration::clamp(86400, 0)),
        )
        .poller()
        .until_done()
        .await?;
    println!("Created Rapid Cache: {}", cache.name);
    println!("State: {}", cache.state);
    Ok(())
}
// [END storage_control_create_rapid_cache]
