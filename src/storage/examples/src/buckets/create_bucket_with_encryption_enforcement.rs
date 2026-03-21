// Copyright 2026 Google LLC
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

// [START storage_create_bucket_with_encryption_enforcement]
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::Bucket;
use google_cloud_storage::model::bucket::Encryption;
use google_cloud_storage::model::bucket::encryption::GoogleManagedEncryptionEnforcementConfig;

pub async fn sample(
    client: &StorageControl,
    project_id: &str,
    bucket_id: &str,
) -> anyhow::Result<()> {
    // Note that this example uses Google-managed encryption keys (GMEK) with FullyRestricted.
    // More options are available, please consult the documentation.
    let gmek_config =
        GoogleManagedEncryptionEnforcementConfig::new().set_restriction_mode("FullyRestricted");

    let encryption =
        Encryption::new().set_google_managed_encryption_enforcement_config(gmek_config);

    let bucket = client
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(
            Bucket::new()
                .set_project(format!("projects/{project_id}"))
                .set_encryption(encryption),
        )
        .send()
        .await?;

    println!("Successfully created bucket {bucket_id} with encryption enforcement: {bucket:?}");

    Ok(())
}
// [END storage_create_bucket_with_encryption_enforcement]
