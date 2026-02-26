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
    async fn  read_footer(client: &Storage, bucket: &str, object: &str)
        -> anyhow::Result<(ObjectDescriptor, Vec<u8>)>
    {
        // Reading the last 8 bytes is common in Parquet and other column-oriented formats
        // with a footer.
        let (descriptor, mut reader) = client.open_object(bucket_name.clone(), object)
            .send_and_read(ReadRange::tail(8))
            .await?;
        let mut footer = Vec::new();
        while let Some(data) = reader.next().await.transpose()? {
            content.extend_from_slice(&data);
        }
        Ok((descriptor, footer, object.to_string()))
    };
    
    object_names.iter().for_each(|o| set.spawn(read_footer(client, &bucket_name, o.as_str())));
    let mut descriptors = HashMap::new();
    while let Some(result) = set.join_next().await {
        let (descriptor, footer, name) = result??;
        println!("The footer for {name} is {footer:?}");
        descriptors.insert(name, (descriptor, footer));
    }
    Ok(())
}
// [END storage_open_multiple_objects_ranged_read]
