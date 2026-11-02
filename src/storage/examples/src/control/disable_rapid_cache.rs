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

// [START storage_control_disable_rapid_cache]
use google_cloud_storage::client::StorageControl;

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    cache_id: &str,
) -> anyhow::Result<()> {
    let name = format!("projects/_/buckets/{bucket_id}/rapidCaches/{cache_id}");
    // Disabling a Rapid Cache starts a long-running operation. The cache moves
    // to the "disabled" state within seconds, but the operation only completes
    // once the cache has been fully removed, which can take more than 30
    // minutes. This sample does not wait for the operation to complete; use
    // `get_rapid_cache()` to check the cache state.
    let operation = client.disable_rapid_cache().set_name(&name).send().await?;
    println!("Disabling Rapid Cache: {name}");
    println!("Operation: {}", operation.name);
    Ok(())
}
// [END storage_control_disable_rapid_cache]
