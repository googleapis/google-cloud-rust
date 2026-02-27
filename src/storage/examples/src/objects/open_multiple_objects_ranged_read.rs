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
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use std::collections::HashMap;
use tokio::task::JoinSet;

pub async fn sample(client: &Storage, bucket: &str, object_names: &[&str]) -> anyhow::Result<()> {
    let bucket_name = format!("projects/_/buckets/{bucket}");

    async fn read_footer(
        client: &Storage,
        bucket_name: &str,
        object_name: &str,
    ) -> anyhow::Result<(ObjectDescriptor, Vec<u8>, String)> {
        // Reading the last 8 bytes is common in Parquet and other column-oriented formats
        // with a footer.
        let (descriptor, mut reader) = client
            .open_object(bucket_name, object_name)
            .send_and_read(ReadRange::tail(8))
            .await?;

        let mut footer = Vec::new();
        while let Some(data) = reader.next().await.transpose()? {
            footer.extend_from_slice(&data);
        }
        Ok((descriptor, footer, object_name.to_string()))
    }

    let mut set = JoinSet::new();
    for o in object_names {
        let client = client.clone();
        let bucket_name = bucket_name.clone();
        let object_name = o.to_string();
        set.spawn(async move { read_footer(&client, &bucket_name, &object_name).await });
    }

    let mut descriptors = HashMap::new();
    while let Some(result) = set.join_next().await {
        let (descriptor, footer, object_name) = result??;
        println!("The footer for object {object_name} is {footer:?}");
        descriptors.insert(object_name, (descriptor, footer));
    }

    // Use the descriptors in the map to read more ranges.

    Ok(())
}
// [END storage_open_multiple_objects_ranged_read]
