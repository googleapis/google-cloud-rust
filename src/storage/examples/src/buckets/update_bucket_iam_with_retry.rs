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

// [START storage_update_bucket_iam_with_retry]
use google_cloud_iam_v1::model::Binding;
use google_cloud_storage::client::StorageControl;

/// Safely adds an IAM member to a bucket using OCC loop to handle concurrent updates.
///
/// This example demonstrates how to use `update_iam_policy_with_retry` to safely
/// modify IAM policies even when multiple processes are updating them concurrently.
/// The OCC (Optimistic Concurrency Control) loop automatically retries on conflicts.
///
/// # Example Usage
///
/// ```ignore
/// let client = StorageControl::builder().build().await?;
/// sample(&client, "my-bucket", "roles/storage.admin", "user:alice@example.com").await?;
/// ```
#[allow(dead_code)]
pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    role: &str,
    member: &str,
) -> anyhow::Result<()> {
    let resource = format!("projects/_/buckets/{bucket_id}");

    // Clone values for use inside the closure
    let role_clone = role.to_string();
    let member_clone = member.to_string();
    let bucket_id_clone = bucket_id.to_string();

    // Use OCC loop to safely add IAM member
    let updated_policy = client.update_iam_policy_with_retry(resource, move |mut policy| {
        // Check if member already has this role
        let already_exists = policy.bindings
            .iter()
            .any(|b| { b.role == role_clone && b.members.contains(&member_clone) });

        if !already_exists {
            // Add new binding if it doesn't exist
            policy.bindings.push(Binding::new().set_role(&role_clone).set_members([&member_clone]));
            Ok(Some(policy))
        } else {
            // Member already has this role, no changes needed
            println!(
                "Member {member_clone} already has role {role_clone} on bucket {bucket_id_clone}"
            );
            Ok(None)
        }
    }).await?;

    println!(
        "Successfully added binding for {member} to bucket {bucket_id} policy: {updated_policy:?}"
    );
    Ok(())
}
// [END storage_update_bucket_iam_with_retry]
