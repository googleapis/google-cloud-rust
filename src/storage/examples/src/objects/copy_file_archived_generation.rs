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

// [START storage_copy_file_archived_generation]
use google_cloud_storage::builder_ext::RewriteObjectExt;
use google_cloud_storage::client::StorageControl;

pub async fn sample(
    client: &StorageControl,
    source_bucket_id: &str,
    dest_bucket_id: &str,
    generation: i64,
) -> anyhow::Result<()> {
    const SOURCE_NAME: &str = "object-generation-to-copy";
    const DEST_NAME: &str = "copied-object";
    let copied = client
        .rewrite_object()
        .set_source_bucket(format!("projects/_/buckets/{source_bucket_id}"))
        .set_source_object(SOURCE_NAME)
        .set_source_generation(generation)
        .set_destination_bucket(format!("projects/_/buckets/{dest_bucket_id}"))
        .set_destination_name(DEST_NAME)
        .rewrite_until_done()
        .await?;
    println!(
        "successfully copied {source_bucket_id}/{SOURCE_NAME} to {dest_bucket_id}/{DEST_NAME}: {copied:?}"
    );
    Ok(())
}
// [END storage_copy_file_archived_generation]
