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

    // 1. Open the connection and request the first byte range. Read 1,024 bytes starting from offset 0.
    let (descriptor, mut reader1) = client
        .open_object(bucket_name, object)
        .send_and_read(ReadRange::segment(0, 1024))
        .await?;

    // Create the file and stream the first discrete range directly to disk.
    let file1_path = format!("{}_part1", destination_file_name);
    let mut file1 = File::create(&file1_path).await?;
    while let Some(data) = reader1.next().await.transpose()? {
        file1.write_all(&data).await?;
    }
    file1.sync_all().await?;
    println!("Downloaded bytes 0-1023 of object {object} in bucket {bucket} to {file1_path}.");

    // 2. Request a second byte range using the existing `descriptor`.
    // Read 2,048 bytes starting from offset 5000.
    // Because the connection is already open, this read will have much lower latency.
    let mut reader2 = descriptor.read_range(ReadRange::segment(5000, 2048)).await;

    // Create the file and stream the second discrete range directly to disk.
    let file2_path = format!("{}_part2", destination_file_name);
    let mut file2 = File::create(&file2_path).await?;
    while let Some(data) = reader2.next().await.transpose()? {
        file2.write_all(&data).await?;
    }
    file2.sync_all().await?;
    println!("Downloaded bytes 5000-7047 of object {object} in bucket {bucket} to {file2_path}.");

    println!("Metadata: {:?}", descriptor.object());
    Ok(())
}
// [END storage_open_object_multiple_ranged_read]
