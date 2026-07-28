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

use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{ErrorProto, InsertJobRequest, Job, JobReference, JobStatus};
use google_cloud_bigquery_v2::operation::JobPollerError;
use google_cloud_bigquery_v2::stub::JobService as JobServiceStub;
use google_cloud_gax::Result as GaxResult;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::response::Response;
use mockall::{Sequence, mock};

mock! {
    #[derive(Debug)]
    pub TestJobService {}

    impl JobServiceStub for TestJobService {
        async fn insert_job(
            &self,
            req: InsertJobRequest,
            options: RequestOptions,
        ) -> GaxResult<Response<Job>>;

        async fn get_job(
            &self,
            req: google_cloud_bigquery_v2::model::GetJobRequest,
            options: RequestOptions,
        ) -> GaxResult<Response<Job>>;
    }
}

fn transient_job_failure(job_id: &str) -> Job {
    Job::new()
        .set_job_reference(JobReference::new().set_project_id("p1").set_job_id(job_id))
        .set_status(
            JobStatus::new()
                .set_state("DONE")
                .set_error_result(ErrorProto::new().set_reason("jobBackendError")),
        )
}

fn non_retryable_job_failure(job_id: &str) -> Job {
    Job::new()
        .set_job_reference(JobReference::new().set_project_id("p1").set_job_id(job_id))
        .set_status(
            JobStatus::new()
                .set_state("DONE")
                .set_error_result(ErrorProto::new().set_reason("invalidQuery")),
        )
}

#[tokio::test]
async fn success() -> anyhow::Result<()> {
    let mut mock = MockTestJobService::new();
    mock.expect_insert_job().return_once(|_, _| {
        Ok(Response::from(
            Job::new().set_status(JobStatus::new().set_state("DONE")),
        ))
    });

    let client = JobService::from_stub(mock);
    let poller = client.insert_job().into_job_poller();
    let job = poller.until_done().await?;
    let status = job.status.unwrap();
    assert_eq!(status.state.as_str(), "DONE");
    assert!(status.error_result.is_none());
    Ok(())
}

#[tokio::test]
async fn retry_success() -> anyhow::Result<()> {
    let failed_job = transient_job_failure("job-1");
    let success_job = Job::new()
        .set_job_reference(JobReference::new().set_project_id("p1").set_job_id("job-2"))
        .set_status(JobStatus::new().set_state("DONE"));

    let mut mock = MockTestJobService::new();
    let mut seq = Sequence::new();

    mock.expect_insert_job()
        .times(1)
        .in_sequence(&mut seq)
        .return_once(move |req, _| {
            assert_eq!(req.job.unwrap().job_reference.unwrap().job_id, "job-1");
            Ok(Response::from(failed_job))
        });

    mock.expect_insert_job()
        .times(1)
        .in_sequence(&mut seq)
        .return_once(move |req, _| {
            let retried_job = req.job.unwrap();
            assert!(retried_job.status.is_none());
            assert_ne!(retried_job.job_reference.unwrap().job_id, "job-1");
            Ok(Response::from(success_job))
        });

    let client = JobService::from_stub(mock);
    let initial_job = transient_job_failure("job-1");
    let poller = client.insert_job().set_job(initial_job).into_job_poller();

    let result = poller.until_done().await?;

    let status = result.status.unwrap();
    assert_eq!(status.state.as_str(), "DONE");
    assert!(status.error_result.is_none());
    Ok(())
}

#[tokio::test]
async fn non_retryable_error() -> anyhow::Result<()> {
    let failed_job = non_retryable_job_failure("job-1");

    let mut mock = MockTestJobService::new();

    mock.expect_insert_job()
        .return_once(move |_, _| Ok(Response::from(failed_job)));

    let client = JobService::from_stub(mock);
    let poller = client.insert_job().into_job_poller();
    let result = poller.until_done().await;

    // Should return immediately after 1st attempt
    let JobPollerError::Job(failed_job) = result.unwrap_err() else {
        panic!("expected JobPollerError::Job");
    };

    let status = failed_job.status.unwrap();
    assert_eq!(status.error_result.unwrap().reason.as_str(), "invalidQuery");
    Ok(())
}

#[tokio::test]
async fn retry_exhausted() -> anyhow::Result<()> {
    let mut mock = MockTestJobService::new();

    mock.expect_insert_job()
        .times(3)
        .returning(move |_, _| Ok(Response::from(transient_job_failure("job-unknown"))));

    let client = JobService::from_stub(mock);
    let poller = client.insert_job().into_job_poller().with_attempt_limit(3);

    let result = poller.until_done().await;

    // Should stop retrying after limit of 3
    let JobPollerError::Job(failed_job) = result.unwrap_err() else {
        panic!("expected JobPollerError::Job");
    };

    let status = failed_job.status.unwrap();
    assert_eq!(
        status.error_result.unwrap().reason.as_str(),
        "jobBackendError"
    );
    Ok(())
}

#[tokio::test]
async fn polling_success() -> anyhow::Result<()> {
    let mut mock = MockTestJobService::new();
    let mut seq = Sequence::new();

    mock.expect_insert_job()
        .times(1)
        .in_sequence(&mut seq)
        .return_once(move |_, _| {
            Ok(Response::from(
                Job::new()
                    .set_job_reference(
                        JobReference::new()
                            .set_project_id("p1")
                            .set_job_id("job-running"),
                    )
                    .set_status(JobStatus::new().set_state("RUNNING")),
            ))
        });

    mock.expect_get_job()
        .times(1)
        .in_sequence(&mut seq)
        .return_once(move |req, _| {
            assert_eq!(req.job_id, "job-running");
            Ok(Response::from(
                Job::new()
                    .set_job_reference(
                        JobReference::new()
                            .set_project_id("p1")
                            .set_job_id("job-running"),
                    )
                    .set_status(JobStatus::new().set_state("DONE")),
            ))
        });

    let client = JobService::from_stub(mock);
    let poller = client.insert_job().into_job_poller();

    let result = poller.until_done().await?;

    let status = result.status.unwrap();
    assert_eq!(status.state.as_str(), "DONE");
    Ok(())
}
