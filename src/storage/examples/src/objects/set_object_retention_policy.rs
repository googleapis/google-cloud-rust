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

// [START storage_set_object_retention_policy]
use google_cloud_storage::{client::StorageControl, model::object::Retention};
use google_cloud_wkt::{FieldMask, Timestamp};
use std::time::SystemTime;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    const NAME: &str = "object-to-update";
    let object = client
        .get_object()
        .set_bucket(format!("projects/_/buckets/{bucket_id}"))
        .set_object(NAME)
        .send()
        .await?;

    let now = Timestamp::try_from(SystemTime::now())?;
    let then = Timestamp::clamp(now.seconds() + 24 * 60 * 60, now.nanos());
    let metageneration = object.metageneration;
    let updated = client
        .update_object()
        .set_if_metageneration_match(metageneration)
        .set_object(
            object.set_retention(
                Retention::new()
                    .set_mode("UNLOCKED")
                    .set_retain_until_time(then),
            ),
        )
        .set_override_unlocked_retention(true)
        .set_update_mask(FieldMask::default().set_paths(["retention"]))
        .send()
        .await?;
    println!("successfully set retention for object {NAME} in bucket {bucket_id}: {updated:?}");
    Ok(())
}
// [END storage_set_object_retention_policy]
