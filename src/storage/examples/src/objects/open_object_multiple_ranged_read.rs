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

// [START storage_open_object_multiple_ranged_read]
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;

pub async fn sample(client: &Storage, bucket: &str, object: &str) -> anyhow::Result<()> {
    let bucket_name = format!("projects/_/buckets/{bucket}");

    // 1. Open the connection and read the first range.
    let (descriptor, mut reader) = client
        .open_object(bucket_name, object)
        .send_and_read(ReadRange::head(5))
        .await?;

    let mut content = Vec::new();
    while let Some(data) = reader.next().await.transpose()? {
        content.extend_from_slice(&data);
    }
    println!(
        "Read first range from {object} in {bucket}. Content: {:?}",
        String::from_utf8_lossy(&content)
    );

    // 2. Read another range.
    let mut reader = descriptor.read_range(ReadRange::segment(5, 5)).await;

    let mut content = Vec::new();
    while let Some(data) = reader.next().await.transpose()? {
        content.extend_from_slice(&data);
    }
    println!(
        "Read second range from {object} in {bucket}. Content: {:?}",
        String::from_utf8_lossy(&content)
    );

    println!("Metadata: {:?}", descriptor.object());
    Ok(())
}
// [END storage_open_object_multiple_ranged_read]
