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

// [START storage_set_bucket_encryption_enforcement]
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::bucket::Encryption;
use google_cloud_storage::model::bucket::encryption::GoogleManagedEncryptionEnforcementConfig;
use google_cloud_wkt::FieldMask;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    // Note that this example uses Google-managed encryption keys (GMEK) with FullyRestricted.
    // More options are available, please consult the documentation.
    let gmek_config =
        GoogleManagedEncryptionEnforcementConfig::new().set_restriction_mode("FullyRestricted");

    let encryption =
        Encryption::new().set_google_managed_encryption_enforcement_config(gmek_config);

    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;

    let metageneration = bucket.metageneration;

    // Update the bucket, using a field mask to only update the modified configuration.
    let updated_bucket = client
        .update_bucket()
        .set_bucket(bucket.set_encryption(encryption))
        .set_if_metageneration_match(metageneration)
        .set_update_mask(
            FieldMask::default()
                .set_paths(["encryption.google_managed_encryption_enforcement_config"]),
        )
        .send()
        .await?;

    println!(
        "Updated encryption enforcement on bucket {bucket_id}: {:?}",
        updated_bucket.encryption
    );

    Ok(())
}
// [END storage_set_bucket_encryption_enforcement]
