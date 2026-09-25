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

// [START storage_control_delete_folder_recursive]
use google_cloud_lro::Poller;
use google_cloud_storage::client::StorageControl;

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    folder_id: &str,
) -> anyhow::Result<()> {
    // The storage bucket path uses the global access pattern, in which "_"
    // denotes this bucket exists in the global namespace.
    let folder_name = format!("projects/_/buckets/{bucket_id}/folders/{folder_id}");

    // Start a delete folder recursive operation and poll until it completes.
    client
        .delete_folder_recursive()
        .set_name(&folder_name)
        .poller()
        .until_done()
        .await?;

    println!("Deleted folder recursively: {folder_name}");
    Ok(())
}
// [END storage_control_delete_folder_recursive]
