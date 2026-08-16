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

// [START storage_open_appendable_object_pause_resume]
use bytes::Bytes;
use google_cloud_storage::client::Storage;

pub async fn sample(client: &Storage, bucket: &str, object: &str) -> Result<(), anyhow::Error> {
    let mut writer = client
        .open_appendable_object(format!("projects/_/buckets/{bucket}"), object)
        .send()
        .await?;

    writer.append(Bytes::from("Part 1")).await?;
    let generation = writer.generation();
    writer.close().await?;

    let mut resumed_writer = client
        .reopen_appendable_object(format!("projects/_/buckets/{bucket}"), object, generation)
        .send()
        .await?;

    resumed_writer.append(Bytes::from("Part 2")).await?;
    let metadata = resumed_writer.finalize().await?;

    println!("Appended data across multiple sessions to {object} in bucket {bucket}: {metadata:?}");
    Ok(())
}
// [END storage_open_appendable_object_pause_resume]
