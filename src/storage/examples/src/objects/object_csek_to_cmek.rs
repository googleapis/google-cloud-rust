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

// [START storage_object_csek_to_cmek]
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::CommonObjectRequestParams;
use google_cloud_storage::model_ext::KeyAes256;

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    object_id: &str,
    csek_key: KeyAes256,
    kms_key: &str,
) -> anyhow::Result<()> {
    let csek: CommonObjectRequestParams = csek_key.into();
    let mut builder = client
        .rewrite_object()
        .set_source_bucket(format!("projects/_/buckets/{bucket_id}"))
        .set_source_object(object_id)
        .set_copy_source_encryption_algorithm(csek.encryption_algorithm)
        .set_copy_source_encryption_key_bytes(csek.encryption_key_bytes)
        .set_copy_source_encryption_key_sha256_bytes(csek.encryption_key_sha256_bytes)
        .set_destination_bucket(format!("projects/_/buckets/{bucket_id}"))
        .set_destination_name(object_id)
        .set_destination_kms_key(kms_key.to_string());

    // For more details on this loop, see the "Rewriting objects" section of the
    // user guide:
    // https://googleapis.github.io/google-cloud-rust/storage/rewrite_object.html
    let updated = loop {
        let resp = builder.clone().send().await?;
        if resp.done {
            break resp.resource;
        }
        builder = builder.set_rewrite_token(resp.rewrite_token);
    };

    println!(
        "successfully rotated encryption key for object {object_id} in bucket {bucket_id} from CSEK to CMEK: {updated:?}"
    );
    Ok(())
}
// [END storage_object_csek_to_cmek]
