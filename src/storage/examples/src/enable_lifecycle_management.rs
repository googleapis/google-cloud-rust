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

// [START storage_enable_lifecycle_management]
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::bucket::Lifecycle;
use google_cloud_storage::model::{
    Bucket,
    bucket::lifecycle::{
        Rule,
        rule::{Action, Condition},
    },
};
use google_cloud_wkt::FieldMask;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    let metageneration = bucket.metageneration;
    let rule = Rule::new()
        .set_action(Action::new().set_type("Delete"))
        .set_condition(Condition::new().set_age_days(30));
    let lifecycle = Lifecycle::new().set_rule(vec![rule]);
    let bucket = client
        .update_bucket()
        .set_bucket(Bucket::new().set_lifecycle(lifecycle))
        .set_if_metageneration_match(metageneration)
        .set_update_mask(FieldMask::default().set_paths(["lifecycle"]))
        .send()
        .await?;
    println!("Lifecycle management enabled for bucket {bucket_id}: {bucket:?}");
    Ok(())
}
// [END storage_enable_lifecycle_management]
