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
    // Disabling a Rapid Cache starts a long-running operation. The cache state
    // changes to "disabled" within seconds, but the operation only completes
    // once the cache is fully removed. That can take over an hour, longer if
    // the bucket has a lot of data, so waiting for the operation (for example
    // with `.poller().until_done()`) may look like it hangs. Until the cache is
    // fully removed, deleting the bucket fails with `409 Conflict`.
    //
    // This sample starts the operation and returns without waiting for it.
    // Use `get_rapid_cache()` to check that the state is "disabled".
    let name = format!("projects/_/buckets/{bucket_id}/rapidCaches/{cache_id}");
    let operation = client.disable_rapid_cache().set_name(&name).send().await?;
    println!("Disabling Rapid Cache: {name}");
    println!("Operation: {}", operation.name);
    Ok(())
}
// [END storage_control_disable_rapid_cache]
