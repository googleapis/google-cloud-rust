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

// [START storage_download_file_into_memory]
use google_cloud_storage::client::Storage;

pub async fn sample(client: &Storage, bucket: &str) -> Result<(), anyhow::Error> {
    const NAME: &str = "object-to-download.txt";
    let mut reader = client
        .read_object(format!("projects/_/buckets/{bucket}"), NAME)
        .send()
        .await?;

    let mut content = Vec::new();
    while let Some(data) = reader.next().await.transpose()? {
        content.extend_from_slice(&data);
    }

    println!(
        "Downloaded {} bytes of object {NAME} in bucket {bucket}.",
        content.len()
    );
    Ok(())
}
// [END storage_download_file_into_memory]
