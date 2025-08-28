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

// [START storage_download_byte_range]
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;

pub async fn sample(
    client: &Storage,
    bucket: &str,
    object: &str,
    start: u64,
    end: u64,
) -> Result<(), anyhow::Error> {
    let count = end - start + 1;
    let mut reader = client
        .read_object(format!("projects/_/buckets/{bucket}"), object)
        .set_read_range(ReadRange::segment(start, count))
        .send()
        .await?;

    let mut content = Vec::new();
    while let Some(data) = reader.next().await.transpose()? {
        content.extend_from_slice(&data);
    }

    println!(
        "Downloaded {} bytes from {start} to {end} of object {object} in bucket {bucket}.",
        content.len()
    );
    Ok(())
}
// [END storage_download_byte_range]
