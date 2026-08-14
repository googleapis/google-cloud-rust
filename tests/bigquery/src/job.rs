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

use super::{INSTANCE_LABEL, random_id_suffix};
use anyhow::Result;
use futures::stream::{StreamExt, TryStreamExt};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{Job, JobConfiguration, JobConfigurationQuery, JobReference};
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_test_utils::runtime_config::project_id;

pub async fn job_service() -> Result<()> {
    let project_id = project_id()?;
    let client = JobService::builder().with_tracing().build().await?;
    cleanup_stale_jobs(&client, &project_id).await?;

    let job_id = random_job_id();
    println!("CREATING JOB WITH ID: {job_id}");

    let query = "SELECT 1 as one";
    let job = client
        .insert_job()
        .set_project_id(&project_id)
        .set_job(
            Job::new()
                .set_job_reference(JobReference::new().set_job_id(&job_id))
                .set_configuration(
                    JobConfiguration::new()
                        .set_labels([(INSTANCE_LABEL, "true")])
                        .set_query(JobConfigurationQuery::new().set_query(query)),
                ),
        )
        .send()
        .await?;
    println!("CREATE JOB = {job:?}");

    assert!(job.job_reference.is_some(), "{job:?}");

    let list = client
        .list_jobs()
        .set_project_id(&project_id)
        .by_item()
        .into_stream();
    let items: Vec<_> = list.try_collect().await?;
    println!("LIST JOBS = {} entries", items.len());

    assert!(items.iter().any(|v| v.id.contains(&job_id)));

    Ok(())
}

fn random_job_id() -> String {
    let rand_suffix = random_id_suffix();
    format!("rust_bq_test_job_{rand_suffix}")
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

pub async fn job_service_poller() -> Result<()> {
    let project_id = project_id()?;
    let client = JobService::builder().build().await?;
    cleanup_stale_jobs(&client, &project_id).await?;

    let job_id = random_job_id();
    println!("CREATING JOB (WITH POLLER) ID: {job_id}");

    let query = "SELECT 1 as one";

    // Use the job poller extension to insert and poll the job until completion.
    let job = client
        .insert_job()
        .set_project_id(&project_id)
        .set_job(
            Job::new()
                .set_job_reference(JobReference::new().set_job_id(&job_id))
                .set_configuration(
                    JobConfiguration::new()
                        .set_labels([(INSTANCE_LABEL, "true")])
                        .set_query(JobConfigurationQuery::new().set_query(query)),
                ),
        )
        .into_job_poller()
        .until_done()
        .await?;

    println!("CREATE JOB (POLLED) = {job:?}");

    assert!(job.job_reference.is_some(), "{job:?}");
    let status = job.status.as_ref().expect("job should have status");
    assert_eq!(status.state.as_str(), "DONE");
    assert!(
        status.error_result.is_none(),
        "job completed with unexpected error_result: {:?}",
        status.error_result
    );

    Ok(())
}

pub async fn job_service_poller_error() -> Result<()> {
    let project_id = project_id()?;
    let client = JobService::builder().build().await?;

    let job_id = random_job_id();
    let query = "SELECT * FROM `non_existent_dataset_12345.non_existent_table_67890`";

    let result = client
        .insert_job()
        .set_project_id(&project_id)
        .set_job(
            Job::new()
                .set_job_reference(JobReference::new().set_job_id(&job_id))
                .set_configuration(
                    JobConfiguration::new()
                        .set_labels([(INSTANCE_LABEL, "true")])
                        .set_query(JobConfigurationQuery::new().set_query(query)),
                ),
        )
        .into_job_poller()
        .until_done()
        .await;

    let err = result.expect_err("expected job polling to return error");
    match err {
        google_cloud_bigquery_v2::operation::JobPollerError::ErrorProto(proto) => {
            assert!(
                !proto.reason.is_empty(),
                "expected non-empty error reason in ErrorProto"
            );
        }
        google_cloud_bigquery_v2::operation::JobPollerError::Rpc(rpc_err) => {
            panic!(
                "expected JobPollerError::ErrorProto from BigQuery job, got RPC error: {rpc_err:?}"
            );
        }
    }

    Ok(())
}

pub async fn job_service_poller_heavy() -> Result<()> {
    let project_id = project_id()?;
    let client = JobService::builder().build().await?;
    cleanup_stale_jobs(&client, &project_id).await?;

    let job_id = random_job_id();
    println!("CREATING JOB (HEAVY POLLER) ID: {job_id}");

    let query = r#"
        DECLARE DELAY_TIME DATETIME;
        DECLARE WAIT STRING;
        SET WAIT = 'TRUE';
        SET DELAY_TIME = DATETIME_ADD(CURRENT_DATETIME, INTERVAL 5 SECOND);

        WHILE WAIT = 'TRUE' DO
          IF (DELAY_TIME < CURRENT_DATETIME) THEN
            SET WAIT = 'FALSE';
          END IF;
        END WHILE;
    "#;

    let job = client
        .insert_job()
        .set_project_id(&project_id)
        .set_job(
            Job::new()
                .set_job_reference(JobReference::new().set_job_id(&job_id))
                .set_configuration(
                    JobConfiguration::new()
                        .set_labels([(INSTANCE_LABEL, "true")])
                        .set_query(
                            JobConfigurationQuery::new()
                                .set_query(query)
                                .set_use_legacy_sql(false),
                        ),
                ),
        )
        .into_job_poller()
        .until_done()
        .await?;

    assert!(job.job_reference.is_some(), "{job:?}");
    let status = job.status.as_ref().expect("job should have status");
    assert_eq!(status.state.as_str(), "DONE");
    assert!(
        status.error_result.is_none(),
        "job completed with unexpected error_result: {:?}",
        status.error_result
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_bigquery_v2::client::JobService;

    #[tokio::test]
    async fn poller_manageable_future_size() -> Result<()> {
        let client = JobService::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let fut = client
            .insert_job()
            .set_project_id("test-project")
            .into_job_poller()
            .until_done();
        let size = std::mem::size_of_val(&fut);
        assert!(size < 1024, "{size}");
        Ok(())
    }
}
