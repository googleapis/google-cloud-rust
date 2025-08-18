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

// [START storage_list_files_with_prefix]
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_storage::client::StorageControl;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    const PREFIX: &str = "prefixes/are-not-always/folders-";
    let mut objects = client
        .list_objects()
        .set_parent(format!("projects/_/buckets/{bucket_id}"))
        .set_prefix(PREFIX)
        .by_item();
    println!("listing objects in bucket {bucket_id} with prefix {PREFIX}");
    while let Some(object) = objects.next().await.transpose()? {
        println!("{object:?}");
    }
    println!("DONE");
    Ok(())
}
// [END storage_list_files_with_prefix]
