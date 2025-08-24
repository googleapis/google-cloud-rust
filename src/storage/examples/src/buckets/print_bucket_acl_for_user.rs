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

// [START storage_print_bucket_acl_for_user]
use google_cloud_storage::client::StorageControl;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    // For other scopes see:
    //     https://cloud.google.com/storage/docs/access-control/lists#scopes
    const NAME: &str = "allAuthenticatedUsers";

    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    if let Some(entry) = bucket.acl.iter().find(|x| x.entity == NAME) {
        println!("found ACL entry for {NAME} in bucket {bucket_id}: {entry:?}");
    } else {
        println!("ACL entry for {NAME} not found in bucket {bucket_id}");
    }
    Ok(())
}
// [END storage_print_bucket_acl_for_user]
