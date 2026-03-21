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

// [BEGIN rust_occ_loop]
use google_cloud_gax::error::rpc::Code;
use google_cloud_iam_v1::model::Binding;
use google_cloud_iam_v1::model::Policy;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use google_cloud_wkt::FieldMask;

/// Executes an Optimistic Concurrency Control (OCC) loop to safely update a resource.
///
/// This function demonstrates the core Read-Modify-Write-Retry pattern. It uses the secret manager
/// service and a hard-coded role. The principles apply to any other service or role.
///
/// # Parameters
/// * `project_id` The Google Cloud Project ID (e.g., "my-project-123").
/// * `secret_id` The Google Cloud Project ID (e.g., "my-secret").
/// * `member` The member to add (e.g., "user:user@example.com").
///
/// # Returns
/// The new IAM policy.
pub async fn sample(project_id: &str, secret_id: &str, member: &str) -> anyhow::Result<Policy> {
    const ROLE: &str = "roles/secretmanager.secretAccessor";
    const ATTEMPTS: u32 = 5;

    let secret_name = format!("projects/{project_id}/secrets/{secret_id}");
    let client = SecretManagerService::builder().build().await?;
    for _attempt in 0..ATTEMPTS {
        let mut current = client
            .get_iam_policy()
            .set_resource(&secret_name)
            .send()
            .await?;

        match current.bindings.iter_mut().find(|b| b.role == ROLE) {
            None => current
                .bindings
                .push(Binding::new().set_role(ROLE).set_members([member])),
            Some(b) => {
                if b.members.iter().find(|m| *m == member).is_some() {
                    return Ok(current);
                }
                b.members.push(member.to_string());
            }
        };
        let updated = client
            .set_iam_policy()
            .set_resource(&secret_name)
            .set_policy(current)
            .set_update_mask(FieldMask::default().set_paths(["bindings"]))
            .send()
            .await;
        match updated {
            Ok(p) => return Ok(p),
            Err(e)
                if e.status().is_some_and(|s| {
                    s.code == Code::Aborted || s.code == Code::FailedPrecondition
                }) =>
            {
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
    anyhow::bail!("could not set IAM policy after {ATTEMPTS} attempts")
}
// [END rust_occ_loop]
