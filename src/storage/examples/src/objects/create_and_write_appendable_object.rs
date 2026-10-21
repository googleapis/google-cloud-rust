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

// [START storage_create_and_write_appendable_object]
use bytes::Bytes;
use google_cloud_storage::client::Storage;

pub async fn sample(client: &Storage, bucket: &str, object: &str) -> Result<(), anyhow::Error> {
    let mut writer = client
        .open_appendable_object(format!("projects/_/buckets/{bucket}"), object)
        .send()
        .await?;

    writer.append(Bytes::from("Some data\n")).await?;

    // Flush the buffered data to the service. After a flush, the data is visible to readers.
    let persisted_size = writer.flush().await?;
    println!("Flush completed. Persisted size is now {persisted_size}");

    // The writer is still open. We can write more data.
    writer.append(Bytes::from("Some more data\n")).await?;

    // Close the writer without finalizing the object.
    let persisted_size = writer.close().await?;
    println!("Created appendable object {object} in bucket {bucket} with {persisted_size} bytes");
    Ok(())
}
// [END storage_create_and_write_appendable_object]
