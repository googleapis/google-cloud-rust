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

// [START storage_remove_bucket_iam_member]
use google_cloud_storage::client::StorageControl;

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    role: &str,
    member: &str,
) -> anyhow::Result<()> {
    let mut policy = client
        .get_iam_policy()
        .set_resource(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    if let Some(binding) = policy.bindings.iter_mut().find(|b| b.role == role) {
        binding.members.retain(|m| m != member);
    }
    policy.bindings.retain(|b| !b.members.is_empty());
    let updated_policy = client
        .set_iam_policy()
        .set_resource(format!("projects/_/buckets/{bucket_id}"))
        .set_policy(policy)
        .send()
        .await?;
    println!("Successfully removed {member} with role {role} from bucket {bucket_id}");
    println!("The updated policy is: {:?}", updated_policy);
    Ok(())
}
// [END storage_remove_bucket_iam_member]
