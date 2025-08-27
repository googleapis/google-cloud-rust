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

// [START storage_add_bucket_default_owner]
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::ObjectAccessControl;
use google_cloud_wkt::FieldMask;

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    user_email: &str,
) -> anyhow::Result<()> {
    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    let metageneration = bucket.metageneration;
    let mut acls = bucket.default_object_acl.clone();
    acls.push(
        ObjectAccessControl::new()
            .set_entity(format!("user-{}", user_email))
            .set_role("OWNER"),
    );

    let bucket = client
        .update_bucket()
        .set_bucket(bucket.set_default_object_acl(acls))
        .set_if_metageneration_match(metageneration)
        .set_update_mask(FieldMask::default().set_paths(["defaultObjectAcl"]))
        .send()
        .await?;
    println!("Successfully added default owner {user_email} to bucket {bucket_id}");
    println!("{:?}", bucket.default_object_acl);
    Ok(())
}
// [END storage_add_bucket_default_owner]
