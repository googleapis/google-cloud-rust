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

// [START storage_download_encrypted_file]
use base64::{Engine as _, engine::general_purpose};
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::KeyAes256;
use google_cloud_storage::read_object::ReadObjectResponse;

pub async fn sample(
    client: &Storage,
    bucket: &str,
    object: &str,
    encryption_key: &str,
) -> Result<(), anyhow::Error> {
    let raw_key_bytes = general_purpose::STANDARD.decode(encryption_key)?;

    let mut reader = client
        .read_object(format!("projects/_/buckets/{bucket}"), object)
        .set_key(KeyAes256::new(&raw_key_bytes)?)
        .send()
        .await?;

    while let Some(data) = reader.next().await.transpose()? {
        println!("downloaded {} bytes", data.len())
    }

    println!(
        "Downloaded {} in bucket {} with key={}.",
        object, bucket, encryption_key
    );
    Ok(())
}
// [END storage_download_encrypted_file]
