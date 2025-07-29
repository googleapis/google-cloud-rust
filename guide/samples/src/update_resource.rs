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

use google_cloud_secretmanager_v1::model;
use google_cloud_wkt;

// ANCHOR: update-field
pub type Result = std::result::Result<(), Box<dyn std::error::Error>>;

pub async fn update_field(project_id: &str) -> Result {
    use google_cloud_secretmanager_v1::client::SecretManagerService;
    
    // ANCHOR: create
    let client = SecretManagerService::builder().build().await?;
    
    let secret = client
        .create_secret()
        .set_parent(format!("projects/{project_id}"))
        .set_secret_id("your-secret")
        .set_secret(model::Secret::new().set_replication(
            model::Replication::new().set_automatic(model::replication::Automatic::new()),
        ))
        .send()
        .await?;
    println!("CREATE = {secret:?}");
    // ANCHOR_END: create
    
    // ANCHOR: update
    let tag = |mut map: std::collections::HashMap<String, String>, msg: &str| {
        map.insert("updated".to_string(), msg.to_string());
        map
    };
    
    let update = client
        .update_secret()
        .set_secret(
            model::Secret::new()
                .set_name(&secret.name)
                .set_etag(secret.etag)
                .set_labels(tag(secret.labels.clone(), "your-label"))
                .set_annotations(tag(secret.annotations.clone(), "your-annotations")),
        )
        // ANCHOR: set-update-mask
        .set_update_mask(google_cloud_wkt::FieldMask::default().set_paths(["annotations", "labels"]))
        // ANCHOR_END: set-update-mask
        .send()
        .await?;
    println!("UPDATE = {update:?}");
    // ANCHOR_END: update
    
    Ok(())
}
// ANCHOR_END: update-field
