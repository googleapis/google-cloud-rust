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

use google_cloud_compute_v1::client::Instances;
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_lro::Poller;
use google_cloud_storage::client::StorageControl;

pub mod compute_images_list;
pub mod compute_images_list_page;
pub mod compute_instances_create;
pub mod compute_instances_delete;
pub mod compute_instances_list_all;
pub mod compute_instances_operation_check;
pub mod compute_usage_report_get;
pub mod compute_usage_report_set;

pub async fn cleanup_stale_instances(client: &Instances, project_id: &str) -> anyhow::Result<()> {
    use google_cloud_wkt::Timestamp;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline =
        Timestamp::new(stale_deadline.as_secs() as i64, 0).expect("48 hours fits in wkt Timestamp");

    let mut items = client
        .aggregated_list()
        .set_project(project_id)
        .set_return_partial_success(true)
        .by_item();
    while let Some((_zone, scoped_list)) = items.next().await.transpose()? {
        for instance in scoped_list.instances {
            if instance
                .labels
                .get("source")
                .is_none_or(|v| !v.starts_with("compute_"))
            {
                println!("Skipping because source label does not match: {instance:?}");
                continue;
            }
            if instance
                .creation_timestamp
                .as_ref()
                .and_then(|v| Timestamp::try_from(v).ok())
                .is_none_or(|t| t > stale_deadline)
            {
                println!("Skipping because creation time is too recent: {instance:?}");
                continue;
            }
            if let (Some(name), Some(zone)) = (instance.name, instance.zone) {
                println!("Deleting VM {name} in zone {zone}");
                let result = client
                    .delete()
                    .set_project(project_id)
                    .set_zone(zone)
                    .set_instance(name)
                    .poller()
                    .until_done()
                    .await;
                match result {
                    Err(e) => println!("operation did not complete, error={e:?}"),
                    Ok(op) => println!("operation completed with {:?}", op.to_result()),
                };
            }
        }
    }

    Ok(())
}

pub async fn create_reports_bucket(
    control: &StorageControl,
    project_id: &str,
    bucket_id: &str,
) -> anyhow::Result<()> {
    use google_cloud_compute_v1::client::Projects;
    use google_cloud_gax::options::RequestOptionsBuilder;
    use google_cloud_iam_v1::model::Binding;
    use google_cloud_storage::model::{
        Bucket, bucket::IamConfig, bucket::iam_config::UniformBucketLevelAccess,
    };
    use google_cloud_wkt::FieldMask;

    let projects = Projects::builder().build().await?;
    let p = projects.get().set_project(project_id).send().await?;
    let Some(account) = p.default_service_account.clone() else {
        return Err(anyhow::Error::msg("missing default service account"));
    };

    let bucket = Bucket::new()
        .set_project(format!("projects/{project_id}"))
        .set_location("us-central1")
        .set_labels([("integration-test", "true")])
        .set_iam_config(
            IamConfig::new()
                .set_uniform_bucket_level_access(UniformBucketLevelAccess::new().set_enabled(true)),
        );
    println!("Creating bucket: {bucket:?}");
    let bucket = control
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(bucket)
        .with_idempotency(true)
        .send()
        .await?;
    println!("Bucket successfully created: {bucket:?}");

    let mut policy = control
        .get_iam_policy()
        .set_resource(&bucket.name)
        .send()
        .await?;
    println!("Successfully obtained IAM = {policy:?}");

    policy.bindings.push(
        Binding::new()
            .set_role("roles/storage.admin")
            .set_members([format!("serviceAccount:{account}")]),
    );
    let policy = control
        .set_iam_policy()
        .set_resource(&bucket.name)
        .set_update_mask(FieldMask::default().set_paths(["bindings"]))
        .set_policy(policy)
        .with_idempotency(true)
        .send()
        .await?;
    println!("Successfully changed IAM policy = {policy:?}");

    Ok(())
}
