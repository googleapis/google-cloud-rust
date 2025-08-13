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

// [START storage_create_bucket]
use google_cloud_storage::{client::StorageControl, model::Bucket};

pub async fn create_bucket(
    client: &StorageControl,
    project_id: &str,
    bucket_id: &str,
) -> anyhow::Result<()> {
    let bucket = client
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(Bucket::new().set_project(format!("projects/{project_id}")))
        .send()
        .await?;
    println!("successfully created bucket {bucket:?}");
    Ok(())
}
// [END storage_create_bucket]
