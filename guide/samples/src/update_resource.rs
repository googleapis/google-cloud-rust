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

// [START rust_update_resource_field] ANCHOR: update-field
use google_cloud_secretmanager_v1::client::SecretManagerService;
use google_cloud_secretmanager_v1::model::replication::Automatic;
use google_cloud_secretmanager_v1::model::{Replication, Secret};
use google_cloud_wkt::FieldMask;
use std::collections::HashMap;

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    // [START rust_update_resource_create] ANCHOR: create
    let client = SecretManagerService::builder().build().await?;

    let secret = client
        .create_secret()
        .set_parent(format!("projects/{project_id}"))
        .set_secret_id("your-secret")
        .set_secret(
            Secret::new().set_replication(Replication::new().set_automatic(Automatic::new())),
        )
        .send()
        .await?;
    println!("CREATE = {secret:?}");
    // [END rust_update_resource_create] ANCHOR_END: create

    // [START rust_update_resource_update] ANCHOR: update
    let tag = |mut labels: HashMap<_, _>, msg: &str| {
        labels.insert("updated".to_string(), msg.to_string());
        labels
    };

    let update = client
        .update_secret()
        .set_secret(
            Secret::new()
                .set_name(&secret.name)
                .set_etag(secret.etag)
                .set_labels(tag(secret.labels, "your-label"))
                .set_annotations(tag(secret.annotations, "your-annotations")),
        )
        // [START rust_update_resource_set_update_mask] ANCHOR: set-update-mask
        .set_update_mask(FieldMask::default().set_paths(["annotations", "labels"]))
        // [END rust_update_resource_set_update_mask] ANCHOR_END: set-update-mask
        .send()
        .await?;
    println!("UPDATE = {update:?}");
    // [END rust_update_resource_update] ANCHOR_END: update

    Ok(())
}
// [END rust_update_resource_field] ANCHOR_END: update-field
