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

// [START storage_read_appendable_object_tail]
use bytes::Bytes;
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use std::time::Duration;
use tokio::time::sleep;

pub async fn sample(client: &Storage, bucket: &str, object: &str) -> Result<(), anyhow::Error> {
    let bucket_name = format!("projects/_/buckets/{bucket}");
    let mut writer = client
        .open_appendable_object(&bucket_name, object)
        .send()
        .await?;

    let mut bytes_read: u64 = 0;
    for i in 0..2 {
        let content = format!("More data for tail example, iteration {i}\n");
        writer.append(Bytes::from(content)).await?;
        // After a flush, the data is visible to readers.
        writer.flush().await?;

        // Read everything from the last offset to the current end of the object.
        let (_metadata, mut reader) = client
            .open_object(&bucket_name, object)
            .send_and_read(ReadRange::offset(bytes_read))
            .await?;
        while let Some(chunk) = reader.next().await.transpose()? {
            print!("{}", String::from_utf8_lossy(&chunk));
            bytes_read += chunk.len() as u64;
        }

        sleep(Duration::from_secs(1)).await;
    }

    // Close the writer without finalizing the object.
    writer.close().await?;
    Ok(())
}
// [END storage_read_appendable_object_tail]
