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

// [START storage_get_bucket_class_and_location]
// [START storage_get_bucket_labels]
// [START storage_get_bucket_metadata]
// [START storage_get_public_access_prevention]
// [START storage_get_rpo]
// [START storage_get_uniform_bucket_level_access]
use google_cloud_storage::client::StorageControl;

pub async fn get_bucket_metadata(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    println!("successfully obtained bucket metadata {bucket:?}");
    Ok(())
}
// [END storage_get_uniform_bucket_level_access]
// [END storage_get_rpo]
// [END storage_get_public_access_prevention]
// [END storage_get_bucket_metadata]
// [END storage_get_bucket_labels]
// [END storage_get_bucket_class_and_location]
