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
use tokio::fs::File;
use tokio::io::AsyncWriteExt; // Required for write_all

pub async fn sample(
    client: &Storage,
    bucket: &str,
    object: &str,
    destination_file_name: &str,
) -> anyhow::Result<()> {
    let bucket_name = format!("projects/_/buckets/{bucket}");

    // 1. Open the connection and request the first 5 bytes.
    let (descriptor, mut reader) = client
        .open_object(bucket_name, object)
        .send_and_read(ReadRange::head(5))
        .await?;

    let file_path = format!("{}_part1", destination_file_name);
    let mut file = File::create(&file_path).await?;
    while let Some(data) = reader.next().await.transpose()? {
        file.write_all(&data).await?;
    }
    file.sync_all().await?;
    println!("Downloaded the first 5 bytes of object {object} in bucket {bucket} to {file_path}.");

    // 2. Request the last 2 bytes using the existing `descriptor`.
    let mut reader = descriptor.read_range(ReadRange::tail(2)).await;

    let file_path = format!("{}_part2", destination_file_name);
    let mut file = File::create(&file_path).await?;
    while let Some(data) = reader.next().await.transpose()? {
        file.write_all(&data).await?;
    }
    file.sync_all().await?;
    println!("Downloaded the last 2 bytes of object {object} in bucket {bucket} to {file_path}.");

    println!("Metadata: {:?}", descriptor.object());
    Ok(())
}
// [END storage_open_object_multiple_ranged_read]
