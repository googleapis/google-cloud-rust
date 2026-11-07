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

// [START storage_control_update_rapid_cache]
use google_cloud_lro::Poller;
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::RapidCache;
use google_cloud_wkt::{Duration, FieldMask};

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    cache_id: &str,
) -> anyhow::Result<()> {
    // The cache entry TTL must be in whole seconds, between 1 hour and 7 days.
    let ttl_seconds = 172800;
    let rapid_cache = RapidCache::new()
        .set_name(format!(
            "projects/_/buckets/{bucket_id}/rapidCaches/{cache_id}"
        ))
        .set_ttl(Duration::clamp(ttl_seconds, 0));
    // Only the fields listed in the update mask are changed.
    let cache = client
        .update_rapid_cache()
        .set_rapid_cache(rapid_cache)
        .set_update_mask(FieldMask::default().set_paths(["ttl"]))
        .poller()
        .until_done()
        .await?;
    println!("Updated Rapid Cache: {}", cache.name);
    if let Some(ttl) = cache.ttl {
        println!("TTL: {} seconds", ttl.seconds());
    }
    Ok(())
}
// [END storage_control_update_rapid_cache]
