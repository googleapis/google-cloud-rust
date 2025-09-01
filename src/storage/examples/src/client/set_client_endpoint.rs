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

// [START storage_set_client_endpoint]
use google_cloud_storage::client::Storage;
use google_cloud_storage::client::StorageControl;

pub async fn sample(bucket_id: &str) -> anyhow::Result<()> {
    const NAME: &str = "hello-world.txt";
    let client = Storage::builder()
        .with_endpoint("https://www.googleapis.com")
        .build()
        .await?;
    // Use the `Storage` client as usual:
    let reader = client
        .read_object(format!("projects/_/buckets/{bucket_id}"), NAME)
        .send()
        .await?;
    println!("Object highlights: {:?}", reader.object());

    let control = StorageControl::builder()
        .with_endpoint("https://www.googleapis.com")
        .build()
        .await?;
    // Use the `StorageControl` client as usual:
    let bucket = control
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    println!("Bucket {bucket_id} metadata is {bucket:?}");

    Ok(())
}
// [END storage_set_client_endpoint]
