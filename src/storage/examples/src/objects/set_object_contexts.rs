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

// [START storage_set_object_contexts]
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::ObjectContexts;
use google_cloud_wkt::FieldMask;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    const NAME: &str = "object-with-contexts";
    let object = client
        .get_object()
        .set_bucket(format!("projects/_/buckets/{bucket_id}"))
        .set_object(NAME)
        .send()
        .await?;

    let metageneration = object.metageneration;
    let mut custom = object.contexts.clone().unwrap_or_default().custom;
    custom.entry("example".to_string()).or_default().value = "true".into();

    let updated = client
        .update_object()
        .set_if_metageneration_match(metageneration)
        .set_object(object.set_contexts(ObjectContexts::new().set_custom(custom)))
        .set_update_mask(FieldMask::default().set_paths(["contexts.custom.example"]))
        .send()
        .await?;
    println!(
        "successfully set contexts for object {NAME} in bucket {bucket_id}: {:?}",
        updated.contexts
    );
    Ok(())
}
// [END storage_set_object_contexts]
