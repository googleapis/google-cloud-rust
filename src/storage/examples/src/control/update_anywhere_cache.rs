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

// [START storage_control_update_anywhere_cache]
use google_cloud_lro::Poller;
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::AnywhereCache;
use google_cloud_wkt::FieldMask;

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    cache_id: &str,
) -> anyhow::Result<()> {
    let anywhere_cache = AnywhereCache::new()
        .set_name(format!(
            "projects/_/buckets/{}/anywhereCaches/{}",
            bucket_id, cache_id
        ))
        .set_admission_policy("ADMIT_ALL".to_string());
    let operation = client
        .update_anywhere_cache()
        .set_anywhere_cache(anywhere_cache)
        .set_update_mask(FieldMask::default().set_paths(["admission_policy"]))
        .poller()
        .until_done()
        .await?;
    println!("Updated anywhere cache: {:?}", operation);
    Ok(())
}
// [END storage_control_update_anywhere_cache]
