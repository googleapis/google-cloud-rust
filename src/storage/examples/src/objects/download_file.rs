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

// [START storage_download_file]
use google_cloud_storage::client::Storage;
use tokio::io::AsyncWriteExt;

pub async fn sample(
    client: &Storage,
    bucket: &str,
    object: &str,
    file_path: &str,
) -> Result<(), anyhow::Error> {
    let mut reader = client
        .read_object(format!("projects/_/buckets/{bucket}"), object)
        .send()
        .await?;

    let mut file = tokio::fs::File::create(file_path).await?;
    while let Some(data) = reader.next().await.transpose()? {
        file.write_all(&data).await?;
    }
    file.flush().await?;

    println!("Downloaded {object} in bucket {bucket} to {file_path}.");
    Ok(())
}
// [END storage_download_file]
