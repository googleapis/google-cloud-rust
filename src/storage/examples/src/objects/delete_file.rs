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

// [START storage_delete_file]
use google_cloud_storage::client::StorageControl;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    const NAME: &str = "deleted-object-name";
    client
        .delete_object()
        .set_bucket(format!("projects/_/buckets/{bucket_id}"))
        .set_object(NAME)
        // Consider .set_generation() to make request idempotent
        .send()
        .await?;
    println!("successfully deleted object {NAME} in bucket {bucket_id}");
    Ok(())
}
// [END storage_delete_file]
