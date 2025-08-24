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

// [START storage_add_bucket_conditional_iam_binding]
use google_cloud_iam_v1::model::{Binding, GetPolicyOptions};
use google_cloud_storage::client::StorageControl;
use gtype::model::Expr;

pub async fn sample(
    client: &StorageControl,
    bucket_id: &str,
    role: &str,
    member: &str,
    condition_title: &str,
    condition_description: &str,
    condition_expression: &str,
) -> anyhow::Result<()> {
    let options = GetPolicyOptions::new().set_requested_policy_version(3);
    let mut policy = client
        .get_iam_policy()
        .set_resource(format!("projects/_/buckets/{bucket_id}"))
        .set_options(options)
        .send()
        .await?;
    policy.version = 3;
    let mut binding = Binding::new()
        .set_role(role)
        .set_members(vec![member.to_string()]);
    let mut condition = Expr::new()
        .set_expression(condition_expression)
        .set_title(condition_title);
    if !condition_description.is_empty() {
        condition = condition.set_description(condition_description);
    }
    binding = binding.set_condition(condition);
    policy.bindings.push(binding);
    let updated_policy = client
        .set_iam_policy()
        .set_resource(format!("projects/_/buckets/{bucket_id}"))
        .set_policy(policy)
        .send()
        .await?;
    println!(
        "Successfully added conditional IAM binding to bucket {}",
        bucket_id
    );
    println!("The updated policy is: {:?}", updated_policy);
    Ok(())
}
// [END storage_add_bucket_conditional_iam_binding]
