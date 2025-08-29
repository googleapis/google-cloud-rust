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

// [START storage_delete_file]
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::{Object, compose_object_request::SourceObject};

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    const COMPOSE_SOURCE_1: &str = "compose-source-object-1";
    const COMPOSE_SOURCE_2: &str = "compose-source-object-2";
    const DESTINATION_NAME: &str = "compose-destination-object";
    let object = client
        .compose_object()
        .set_destination(
            Object::new()
                .set_bucket(format!("projects/_/buckets/{bucket_id}"))
                .set_name(DESTINATION_NAME),
        )
        .set_source_objects([
            SourceObject::new().set_name(COMPOSE_SOURCE_1),
            SourceObject::new().set_name(COMPOSE_SOURCE_2),
        ])
        // Consider .set_generation() to make request idempotent
        .send()
        .await?;
    println!(
        "successfully composed {COMPOSE_SOURCE_1} and {COMPOSE_SOURCE_2} into object {object:?}"
    );
    Ok(())
}
// [END storage_delete_file]
