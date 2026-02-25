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

// [START storage_open_multiple_objects_ranged_read]
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use tokio::task::JoinSet;

pub async fn sample(client: &Storage, bucket: &str, object_names: &[&str]) -> anyhow::Result<()> {
    let mut set = JoinSet::new();
    let bucket_name = format!("projects/_/buckets/{bucket}");

    for object in object_names {
        let client = client.clone();
        let bucket_name = bucket_name.clone();
        let object = object.to_string();

        set.spawn(async move {
            let (_descriptor, mut reader) = client
                .open_object(bucket_name, &object)
                .send_and_read(ReadRange::segment(5, 5))
                .await?;

            let mut content = Vec::new();
            while let Some(data) = reader.next().await.transpose()? {
                content.extend_from_slice(&data);
            }

            Ok::<(String, Vec<u8>), anyhow::Error>((object, content))
        });
    }

    while let Some(result) = set.join_next().await {
        let (object, content) = result??;
        println!(
            "Downloaded first 10 bytes of object {object} in bucket {bucket}. Content: {:?}",
            String::from_utf8_lossy(&content)
        );
    }

    Ok(())
}
// [END storage_open_multiple_objects_ranged_read]
