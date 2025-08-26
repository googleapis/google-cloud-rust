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

// [START storage_remove_bucket_conditional_iam_binding]
use google_cloud_iam_v1::model::GetPolicyOptions;
use google_cloud_storage::client::StorageControl;

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    role: &str,
    title: &str,
) -> anyhow::Result<()> {
    let mut policy = client
        .get_iam_policy()
        .set_resource(format!("projects/_/buckets/{bucket_id}"))
        .set_options(GetPolicyOptions::new().set_requested_policy_version(3))
        .send()
        .await?;
    policy.version = 3;
    policy.bindings.retain(|b| {
        if b.role == role {
            if let Some(condition) = &b.condition {
                return condition.title != title;
            }
        }
        true
    });
    let updated_policy = client
        .set_iam_policy()
        .set_resource(format!("projects/_/buckets/{bucket_id}"))
        .set_policy(policy)
        .send()
        .await?;
    println!("Successfully removed conditional IAM binding from bucket {bucket_id}");
    println!("The updated policy is: {:?}", updated_policy);
    Ok(())
}
// [END storage_remove_bucket_conditional_iam_binding]
