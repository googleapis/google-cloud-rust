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

// [START storage_make_public]
use google_cloud_storage::{client::StorageControl, model::ObjectAccessControl};
use google_cloud_wkt::FieldMask;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    const NAME: &str = "object-to-update";
    let object = client
        .get_object()
        .set_bucket(format!("projects/_/buckets/{bucket_id}"))
        .set_object(NAME)
        .send()
        .await?;
    const ENTITY: &str = "allUsers";
    const ROLE: &str = "READER";
    let mut acl = object.acl.clone();
    let entry = acl.iter_mut().find(|x| x.entity == ENTITY);
    match entry {
        None => acl.push(ObjectAccessControl::new().set_entity(ENTITY).set_role(ROLE)),
        Some(e) => e.role = ROLE.into(),
    };
    let metageneration = object.metageneration;
    let updated = client
        .update_object()
        .set_if_metageneration_match(metageneration)
        .set_object(object.set_acl(acl))
        .set_update_mask(FieldMask::default().set_paths(["acl"]))
        .send()
        .await?;
    println!("successfully made public object {NAME} in bucket {bucket_id}: {updated:?}");
    Ok(())
}
// [END storage_make_public]
