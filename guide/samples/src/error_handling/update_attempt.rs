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

// [START rust_update_attempt] ANCHOR: update-attempt
use google_cloud_secretmanager_v1::client::SecretManagerService;
use google_cloud_secretmanager_v1::model::{SecretPayload, SecretVersion};

pub(crate) async fn update_attempt(
    client: &SecretManagerService,
    project_id: &str,
    secret_id: &str,
    data: Vec<u8>,
) -> anyhow::Result<SecretVersion> {
    let checksum = crc32c::crc32c(&data) as i64;
    let version = client
        .add_secret_version()
        .set_parent(format!("projects/{project_id}/secrets/{secret_id}"))
        .set_payload(
            SecretPayload::new()
                .set_data(data)
                .set_data_crc32c(checksum),
        )
        .send()
        .await?;
    Ok(version)
}
// [END rust_update_attempt] ANCHOR_END: update-attempt
