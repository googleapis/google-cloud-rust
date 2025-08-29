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

// [START storage_upload_from_memory]
use google_cloud_storage::client::Storage;

pub async fn sample(client: &Storage, bucket: &str) -> Result<(), anyhow::Error> {
    const NAME: &str = "object-to-upload.txt";
    let data: bytes::Bytes = bytes::Bytes::from("Hello, world!");
    let _result = client
        .write_object(format!("projects/_/buckets/{bucket}"), NAME, data)
        .send_unbuffered()
        .await?;

    println!("Uploaded to {NAME} in bucket {bucket} from memory.");
    Ok(())
}
// [END storage_upload_from_memory]
