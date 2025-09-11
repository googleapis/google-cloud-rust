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

// [START storage_change_file_storage_class]
use google_cloud_storage::builder_ext::RewriteObjectExt;
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::Object;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    const NAME: &str = "update-storage-class";
    let updated = client
        .rewrite_object()
        .set_source_bucket(format!("projects/_/buckets/{bucket_id}"))
        .set_source_object(NAME)
        .set_destination_bucket(format!("projects/_/buckets/{bucket_id}"))
        .set_destination_name(NAME)
        // For other valid storage classes, refer to the documentation:
        // https://cloud.google.com/storage/docs/storage-classes
        .set_destination(Object::new().set_storage_class("NEARLINE"))
        .rewrite_until_done()
        .await?;
    println!(
        "successfully updated storage class for object {NAME} in bucket {bucket_id}: {updated:?}"
    );
    Ok(())
}
// [END storage_change_file_storage_class]
