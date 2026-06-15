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

use crate::error::QueryError;
use crate::query::{Query, Result};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::InsertJobRequest;
use std::sync::Arc;

pub(crate) struct InsertJobExecutor {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) request: InsertJobRequest,
}

impl InsertJobExecutor {
    pub(crate) fn new(job_service: Arc<JobService>, request: InsertJobRequest) -> Self {
        Self {
            job_service,
            request,
        }
    }

    pub(crate) async fn execute(self) -> Result<Query> {
        let is_query = self
            .request
            .job
            .as_ref()
            .and_then(|job| job.configuration.as_ref())
            .and_then(|c| c.query.as_ref())
            .is_some();
        if !is_query {
            return Err(QueryError::UnsupportedJobType);
        }

        let res = self
            .job_service
            .insert_job()
            .with_request(self.request)
            .send()
            .await
            .map_err(|e| QueryError::Rpc { source: e })?;

        let job_status = res.status.as_ref();
        if job_status.and_then(|s| s.error_result.as_ref()).is_some() {
            let errors = job_status.map(|s| s.errors.clone()).unwrap_or_default();
            return Err(QueryError::JobFailed { errors });
        }

        let completed = job_status.map(|s| s.state == "DONE").unwrap_or(false);
        let job_ref = res
            .job_reference
            .clone()
            .expect("newly inserted job should have job reference");

        Ok(Query {
            job_service: self.job_service.clone(),
            job_ref: Some(job_ref),
            completed,
            initial_job: Some(res),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{
        RunQueryRequest,
        tests::{MockJobService, create_job_service},
    };
    use google_cloud_bigquery_v2::model::{
        ErrorProto, Job, JobConfiguration, JobConfigurationQuery, JobReference, JobStatus,
    };
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    async fn test_unsupported_job_type() -> TestResult {
        let mock = MockJobService::new();
        let job_service = create_job_service(mock);
        let req = InsertJobRequest::new(); // no job config at all
        let executor = InsertJobExecutor::new(job_service, req);
        let res = executor.execute().await;
        assert!(matches!(res, Err(QueryError::UnsupportedJobType)));
        Ok(())
    }

    #[tokio::test]
    async fn test_rpc_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_insert_job().returning(|_, _| {
            let status = Status::default()
                .set_code(Code::InvalidArgument)
                .set_message("simulated bad request");
            Err(google_cloud_gax::error::Error::service(status))
        });
        let job_service = create_job_service(mock);

        let job_config = JobConfiguration::new().set_query(JobConfigurationQuery::new());
        let job = Job::new().set_configuration(job_config);
        let req = InsertJobRequest::new().set_job(job);
        let executor = InsertJobExecutor::new(job_service, req);
        let res = executor.execute().await;
        assert!(matches!(res, Err(QueryError::Rpc { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn test_job_failed() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_insert_job().returning(|_, _| {
            let error_proto = ErrorProto::new()
                .set_reason("invalidQuery")
                .set_message("Syntax error");
            let status = JobStatus::new()
                .set_error_result(error_proto.clone())
                .set_errors(vec![error_proto]);
            let job = Job::new().set_status(status);
            Ok(Response::from(job))
        });
        let job_service = create_job_service(mock);

        let job_config = JobConfiguration::new().set_query(JobConfigurationQuery::new());
        let job = Job::new().set_configuration(job_config);
        let req = InsertJobRequest::new().set_job(job);
        let executor = InsertJobExecutor::new(job_service, req);
        let err = executor.execute().await.unwrap_err();

        assert!(matches!(err, QueryError::JobFailed { .. }));
        Ok(())
    }

    #[test_case("DONE", true; "completed")]
    #[test_case("RUNNING", false; "pending")]
    #[tokio::test]
    async fn test_execute_success(job_state: &'static str, completed: bool) -> TestResult {
        let job_ref = JobReference::new()
            .set_job_id("test-job")
            .set_project_id("my-project");
        let job_ref_clone = job_ref.clone();

        let mut mock = MockJobService::new();
        mock.expect_insert_job().returning(move |_, _| {
            let status = JobStatus::new().set_state(job_state);
            let job = Job::new()
                .set_job_reference(job_ref_clone.clone())
                .set_status(status);
            Ok(Response::from(job))
        });
        let job_service = create_job_service(mock);

        let job_config = JobConfiguration::new().set_query(JobConfigurationQuery::new());
        let job = Job::new().set_configuration(job_config);
        let req = InsertJobRequest::new().set_job(job);
        let executor = InsertJobExecutor::new(job_service, req);
        let query = executor.execute().await?;

        assert_eq!(query.completed, completed);
        assert_eq!(query.job_ref, Some(job_ref));
        Ok(())
    }
}
