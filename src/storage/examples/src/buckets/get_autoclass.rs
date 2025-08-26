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

// [START storage_get_autoclass]
use google_cloud_storage::client::StorageControl;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    if let Some(autoclass) = bucket.autoclass {
        println!(
            "Autoclass for bucket {} is enabled={}, terminal_storage_class={:?}, toggled_time={:?}",
            bucket_id, autoclass.enabled, autoclass.terminal_storage_class, autoclass.toggle_time
        );
    } else {
        println!("Autoclass is not set for bucket {bucket_id}.");
    }
    Ok(())
}
// [END storage_get_autoclass]
