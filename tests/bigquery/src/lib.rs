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

use anyhow::Result;
use futures::stream::StreamExt;
use google_cloud_bigquery_v2::client::{DatasetService, JobService};
use google_cloud_bigquery_v2::model::{
    Dataset, DatasetReference, Job, JobConfiguration, JobConfigurationQuery, JobReference,
};
use google_cloud_bigquery_v2::operation::InsertJobBuilderExt;
use google_cloud_gax::{error::rpc::Code, paginator::ItemPaginator};
use google_cloud_lro::Poller;
use google_cloud_test_utils::runtime_config::project_id;
use rand::{RngExt, distr::Alphanumeric};

const INSTANCE_LABEL: &str = "rust-sdk-integration-test";

pub async fn dataset_admin() -> Result<()> {
    let project_id = project_id()?;
    let client = DatasetService::builder().with_tracing().build().await?;
    cleanup_stale_datasets(&client, &project_id).await?;

    let dataset_id = random_dataset_id();

    println!("CREATING DATASET WITH ID: {dataset_id}");

    let create = client
        .insert_dataset()
        .set_project_id(&project_id)
        .set_dataset(
            Dataset::new()
                .set_dataset_reference(DatasetReference::new().set_dataset_id(&dataset_id))
                .set_labels([(INSTANCE_LABEL, "true")]),
        )
        .send()
        .await?;
    println!("CREATE DATASET = {create:?}");

    assert!(create.dataset_reference.is_some(), "{create:?}");

    let list = client
        .list_datasets()
        .set_project_id(&project_id)
        .set_filter(format!("labels.{INSTANCE_LABEL}"))
        .by_item()
        .into_stream();
    let items = list.collect::<Vec<_>>().await;
    println!("LIST DATASET = {} entries", items.len());

    assert!(
        items
            .iter()
            .any(|v| v.as_ref().unwrap().id.contains(&dataset_id))
    );

    client
        .delete_dataset()
        .set_project_id(&project_id)
        .set_dataset_id(&dataset_id)
        .set_delete_contents(true)
        .send()
        .await?;
    println!("DELETE DATASET");

    Ok(())
}

async fn cleanup_stale_datasets(client: &DatasetService, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_millis() as i64;

    let list = client
        .list_datasets()
        .set_project_id(project_id)
        .set_filter(format!("labels.{INSTANCE_LABEL}"))
        .by_item()
        .into_stream();
    let datasets = list.collect::<Vec<_>>().await;

    let pending_all_datasets = datasets
        .iter()
        .filter_map(|v| match v {
            Ok(v) => {
                if let Some(dataset_id) = extract_dataset_id(project_id, &v.id) {
                    return Some(
                        client
                            .get_dataset()
                            .set_project_id(project_id)
                            .set_dataset_id(dataset_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    let stale_datasets = futures::future::join_all(pending_all_datasets)
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok(dataset) => Some(dataset),
            Err(e) if e.status().is_some_and(|s| s.code == Code::NotFound) => None,
            Err(_) => panic!("expected a successful get_dataset()"),
        })
        .filter_map(|dataset| {
            if dataset
                .labels
                .get(INSTANCE_LABEL)
                .is_some_and(|v| v == "true")
                && dataset.creation_time < stale_deadline
            {
                return Some(dataset);
            }
            None
        })
        .collect::<Vec<_>>();

    println!("found {} stale datasets", stale_datasets.len());

    let pending_deletion: Vec<_> = stale_datasets
        .into_iter()
        .filter_map(|ds| {
            if let Some(dataset_id) = extract_dataset_id(project_id, &ds.id) {
                return Some(
                    client
                        .delete_dataset()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .set_delete_contents(true)
                        .send(),
                );
            }
            None
        })
        .collect();

    futures::future::join_all(pending_deletion).await;

    Ok(())
}

fn random_dataset_id() -> String {
    let rand_suffix = random_id_suffix();
    format!("rust_bq_test_dataset_{rand_suffix}")
}

fn random_job_id() -> String {
    let rand_suffix = random_id_suffix();
    format!("rust_bq_test_job_{rand_suffix}")
}

fn random_id_suffix() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

fn extract_dataset_id(project_id: &str, id: &str) -> Option<String> {
    id.strip_prefix(format!("{project_id}:").as_str())
        .map(|v| v.to_string())
}

pub async fn job_service_success() -> Result<()> {
    let project_id = project_id()?;
    let client = JobService::builder().with_tracing().build().await?;
    cleanup_stale_jobs(&client, &project_id).await?;

    let job_id = random_job_id();
    println!("CREATING JOB WITH ID: {job_id}");

    let query = "SELECT 1 as one";
    let req = client.insert_job().set_project_id(&project_id).set_job(
        Job::new()
            .set_job_reference(JobReference::new().set_job_id(&job_id))
            .set_configuration(
                JobConfiguration::new()
                    .set_labels([(INSTANCE_LABEL, "true")])
                    .set_query(JobConfigurationQuery::new().set_query(query)),
            ),
    );
    let poller = req.poller();
    let job = Box::pin(async move { poller.until_done().await }).await?;
    println!("CREATE JOB = {job:?}");

    assert!(job.job_reference.is_some(), "{job:?}");
    let list = client
        .list_jobs()
        .set_project_id(&project_id)
        .by_item()
        .into_stream();
    let items = list.collect::<Vec<_>>().await;
    println!("LIST JOBS = {} entries", items.len());

    assert!(
        items
            .iter()
            .any(|v| v.as_ref().unwrap().id.contains(&job_id))
    );

    Ok(())
}

pub async fn job_service_failing() -> Result<()> {
    let project_id = project_id()?;
    let client = JobService::builder().with_tracing().build().await?;

    let failing_query = "SELECT CAST('abc' AS INT64)";
    let failing_job_id = random_job_id();
    let poller = client
        .insert_job()
        .set_project_id(&project_id)
        .set_job(
            Job::new()
                .set_job_reference(JobReference::new().set_job_id(&failing_job_id))
                .set_configuration(
                    JobConfiguration::new()
                        .set_labels([(INSTANCE_LABEL, "true")])
                        .set_query(JobConfigurationQuery::new().set_query(failing_query)),
                ),
        )
        .poller();

    let failing_job = Box::pin(async move { poller.until_done().await }).await?;
    println!("FAILING JOB (POLLED) = {failing_job:?}");

    assert!(
        failing_job.status.is_some_and(|s| s.error_result.is_some()),
        "Failed job should have error_result populated"
    );

    Ok(())
}

pub async fn job_service_invalid() -> Result<()> {
    let _project_id = project_id()?;
    let client = JobService::builder().with_tracing().build().await?;
    let invalid_job_id = "job_that_definitely_does_not_exist_123456789";
    let invalid_poller = client
        .insert_job()
        .set_project_id("invalid-project-id")
        .set_job(Job::new().set_job_reference(JobReference::new().set_job_id(invalid_job_id)))
        .poller();

    let result = Box::pin(async move { invalid_poller.until_done().await }).await;
    match result {
        Ok(_) => panic!("Expected polling a non-existent job to fail"),
        Err(e) => {
            println!("INVALID JOB ERR = {e:?}");
        }
    }

    Ok(())
}

pub async fn job_service_copy_fail() -> Result<()> {
    let _project_id = project_id()?;
    let client = JobService::builder().with_tracing().build().await?;
    let poller = client
        .insert_job()
        .set_project_id(&_project_id)
        .set_job(
            Job::new().set_configuration(
                JobConfiguration::new().set_copy(
                    google_cloud_bigquery_v2::model::JobConfigurationTableCopy::new()
                        .set_source_table(
                            google_cloud_bigquery_v2::model::TableReference::new()
                                .set_project_id(&_project_id)
                                .set_dataset_id("does_not_exist")
                                .set_table_id("does_not_exist"),
                        )
                        .set_destination_table(
                            google_cloud_bigquery_v2::model::TableReference::new()
                                .set_project_id(&_project_id)
                                .set_dataset_id("does_not_exist")
                                .set_table_id("does_not_exist_either"),
                        ),
                ),
            ),
        )
        .poller();

    let result = Box::pin(async move { poller.until_done().await }).await;
    assert!(result.is_err(), "Expected job to fail");
    Ok(())
}

pub async fn job_service_load_fail() -> Result<()> {
    let _project_id = project_id()?;
    let client = JobService::builder().with_tracing().build().await?;
    let poller = client
        .insert_job()
        .set_project_id(&_project_id)
        .set_job(
            Job::new().set_configuration(
                JobConfiguration::new().set_load(
                    google_cloud_bigquery_v2::model::JobConfigurationLoad::new()
                        .set_source_uris(vec!["gs://bucket_does_not_exist/file.csv".to_string()])
                        .set_destination_table(
                            google_cloud_bigquery_v2::model::TableReference::new()
                                .set_project_id(&_project_id)
                                .set_dataset_id("does_not_exist")
                                .set_table_id("does_not_exist"),
                        ),
                ),
            ),
        )
        .poller();

    let result = Box::pin(async move { poller.until_done().await }).await;
    assert!(result.is_err(), "Expected job to fail");
    Ok(())
}

pub async fn job_service_extract_fail() -> Result<()> {
    let _project_id = project_id()?;
    let client = JobService::builder().with_tracing().build().await?;
    let poller = client
        .insert_job()
        .set_project_id(&_project_id)
        .set_job(
            Job::new().set_configuration(
                JobConfiguration::new().set_extract(
                    google_cloud_bigquery_v2::model::JobConfigurationExtract::new()
                        .set_source_table(
                            google_cloud_bigquery_v2::model::TableReference::new()
                                .set_project_id(&_project_id)
                                .set_dataset_id("does_not_exist")
                                .set_table_id("does_not_exist"),
                        )
                        .set_destination_uris(vec![
                            "gs://bucket_does_not_exist/file.csv".to_string(),
                        ]),
                ),
            ),
        )
        .poller();

    let result = Box::pin(async move { poller.until_done().await }).await;
    assert!(result.is_err(), "Expected job to fail");
    Ok(())
}

async fn cleanup_stale_jobs(client: &JobService, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_millis() as u64;

    let list = client
        .list_jobs()
        .set_project_id(project_id)
        .set_max_creation_time(stale_deadline)
        .by_item()
        .into_stream();
    let items = list.collect::<Vec<_>>().await;
    println!("LIST JOBS = {} entries", items.len());

    let pending_all_stale_jobs = items
        .iter()
        .filter_map(|v| match v {
            Ok(v) => {
                if let Some(job_reference) = &v.job_reference {
                    return Some(
                        client
                            .get_job()
                            .set_project_id(project_id)
                            .set_job_id(&job_reference.job_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    let pending_deletion = futures::future::join_all(pending_all_stale_jobs)
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok(r) => {
                let job_reference = r.job_reference?;
                if r.configuration
                    .is_some_and(|c| c.labels.get(INSTANCE_LABEL).is_some_and(|v| v == "true"))
                    && r.status.is_some_and(|s| s.state == "DONE")
                {
                    return Some(
                        client
                            .delete_job()
                            .set_project_id(project_id)
                            .set_job_id(&job_reference.job_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    println!("found {} stale test jobs", pending_deletion.len());

    futures::future::join_all(pending_deletion).await;
    Ok(())
}
