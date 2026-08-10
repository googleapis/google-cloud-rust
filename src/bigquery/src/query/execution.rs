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
use crate::query::run_query::{
    QUERY_REQUEST_ID_PREFIX, generate_job_reference, generate_prefixed_id,
};
use crate::query::{Query, Result, RunQuery};
use crate::retry_policy::JobRetryResult;
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    InsertJobRequest, Job, JobConfiguration, PostQueryRequest, QueryRequest, QueryResponse,
};
use google_cloud_gax::options::RequestOptionsBuilder as _;
use google_cloud_gax::retry_state::RetryState;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct PostQueryExecutor {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) request: PostQueryRequest,
}

impl PostQueryExecutor {
    pub(crate) fn new(job_service: Arc<JobService>, request: PostQueryRequest) -> Self {
        Self {
            job_service,
            request,
        }
    }

    pub(crate) async fn execute(self) -> Result<QueryResponse> {
        let res = self
            .job_service
            .query()
            // requests to jobs.query are idempotent because every request
            // carries a generated request_id.
            .with_idempotency(true)
            .with_request(self.request)
            .send()
            .await?;

        if !res.errors.is_empty() {
            return Err(QueryError::JobFailed { errors: res.errors });
        }

        Ok(res)
    }
}

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

    pub(crate) async fn execute(self) -> Result<Job> {
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
            // jobs.insert is idempotent because every request
            // carries a generated job_id.
            .with_idempotency(true)
            .send()
            .await?;

        let job_status = res.status.as_ref();
        if job_status.and_then(|s| s.error_result.as_ref()).is_some() {
            let errors = job_status.map(|s| s.errors.clone()).unwrap_or_default();
            return Err(QueryError::JobFailed { errors });
        }

        Ok(res)
    }
}

/// Context for running queries and handling job-level retries / re-issuances.
#[derive(Clone)]
pub(crate) struct RetryContext {
    pub(crate) template: RunQuery,
    pub(crate) state: RetryState,
}

impl RetryContext {
    pub(crate) fn new(template: RunQuery) -> Self {
        Self {
            template,
            state: RetryState::default(),
        }
    }

    pub(crate) fn on_error(&self, error: QueryError) -> JobRetryResult {
        self.template.job_retry_policy.on_error(&self.state, error)
    }

    pub(crate) async fn reissue(mut self, delay: Duration) -> Result<Query> {
        tokio::time::sleep(delay).await;
        self.state.attempt_count += 1;
        self.execute().await
    }

    pub(crate) async fn execute(mut self) -> Result<Query> {
        let project_id = self
            .template
            .project_id
            .as_ref()
            .ok_or(QueryError::MissingProjectId)?;

        loop {
            match self.execute_once(project_id).await {
                Ok(query) => return Ok(query),
                Err(err) => match self.on_error(err) {
                    JobRetryResult::Continue(delay, _) => {
                        tokio::time::sleep(delay).await;
                        self.state.attempt_count += 1;
                    }
                    JobRetryResult::Permanent(e) | JobRetryResult::Exhausted(e) => {
                        return Err(e);
                    }
                },
            }
        }
    }

    async fn execute_once(&self, project_id: &str) -> Result<Query> {
        let job_service = self.template.job_service.clone();
        let max_results = self.template.request.max_results;

        if self.template.request.force_job_path() {
            // Route to jobs.insert
            let job_config: JobConfiguration = self.template.request.clone().into();
            let job_ref = generate_job_reference(project_id, &self.template.request.location);
            let job = Job::new()
                .set_configuration(job_config)
                .set_job_reference(job_ref);
            let req = InsertJobRequest::new()
                .set_job(job)
                .set_project_id(project_id);

            let job = InsertJobExecutor::new(job_service.clone(), req)
                .execute()
                .await?;

            Ok(Query::from_job(
                job_service,
                job,
                Some(self.clone()),
                max_results,
            ))
        } else {
            let query_request_id = generate_prefixed_id(QUERY_REQUEST_ID_PREFIX);
            // Route to jobs.quer
            let query_request: QueryRequest = self.template.request.clone().into();
            let query_request = query_request
                .set_format_options(
                    google_cloud_bigquery_v2::model::DataFormatOptions::new()
                        .set_use_int64_timestamp(true),
                )
                .set_request_id(query_request_id);
            let req = PostQueryRequest::new()
                .set_project_id(project_id)
                .set_query_request(query_request);

            let res = PostQueryExecutor::new(job_service.clone(), req)
                .execute()
                .await?;

            Ok(Query::from_query_response(
                job_service,
                res,
                Some(self.clone()),
                max_results,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::tests::{MockJobService, create_job_service};
    use google_cloud_bigquery_v2::model::{
        ErrorProto, Job, JobConfiguration, JobConfigurationQuery, JobReference, JobStatus,
        QueryResponse,
    };
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;
    use serde_json::{Map, json};
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    async fn test_jobs_query_execute_success() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(|_, _| {
            let job_ref = JobReference::new().set_job_id("my-job-123");
            let query_res = QueryResponse::new()
                .set_job_complete(true)
                .set_job_reference(job_ref.clone())
                .set_rows([Map::from_iter([("f".to_string(), json!([{"v": "Hello"}]))])]);
            Ok(Response::from(query_res))
        });

        let job_service = create_job_service(mock);

        let request = PostQueryRequest::new();
        let executor = PostQueryExecutor::new(job_service.clone(), request);
        let res = executor.execute().await?;
        let query = Query::from_query_response(job_service, res, None, None);

        assert!(query.completed, "{query:?}");
        let job_ref = query
            .metadata
            .job_reference
            .clone()
            .expect("should have job_ref");
        assert_eq!(job_ref.job_id, "my-job-123", "{job_ref:?}");
        assert!(query.cached_rows.is_some(), "{query:?}");

        Ok(())
    }

    #[tokio::test]
    async fn test_jobs_query_execute_job_failed_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(|_, _| {
            let err_proto = ErrorProto::new()
                .set_reason("invalidQuery")
                .set_message("Syntax error");
            let query_res = QueryResponse::new().set_errors(vec![err_proto.clone()]);
            Ok(Response::from(query_res))
        });
        let job_service = create_job_service(mock);

        let request = PostQueryRequest::new();
        let executor = PostQueryExecutor::new(job_service, request);
        let err = executor.execute().await.unwrap_err();

        let errors = match err {
            QueryError::JobFailed { errors } => errors,
            _ => panic!("expected QueryError::JobFailed, got {err:?}"),
        };
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].reason, "invalidQuery");
        assert_eq!(errors[0].message, "Syntax error");

        Ok(())
    }

    #[tokio::test]
    async fn test_jobs_query_execute_rpc_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(|_, _| {
            let status = Status::default()
                .set_code(Code::InvalidArgument)
                .set_message("simulated bad request");
            Err(GaxError::service(status))
        });
        let job_service = create_job_service(mock);

        let request = PostQueryRequest::new();
        let executor = PostQueryExecutor::new(job_service, request);
        let err = executor.execute().await.unwrap_err();

        let source = match err {
            QueryError::Rpc { source } => source,
            _ => panic!("expected QueryError::Rpc, got {err:?}"),
        };
        assert_eq!(source.status().unwrap().code, Code::InvalidArgument);

        Ok(())
    }

    #[tokio::test]
    async fn test_jobs_insert_unsupported_job_type() -> TestResult {
        let mock = MockJobService::new();
        let job_service = create_job_service(mock);
        let req = InsertJobRequest::new(); // no job config at all
        let executor = InsertJobExecutor::new(job_service, req);
        let res = executor.execute().await;
        assert!(matches!(res, Err(QueryError::UnsupportedJobType)));
        Ok(())
    }

    #[tokio::test]
    async fn test_jobs_insert_rpc_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_insert_job().returning(|_, _| {
            let status = Status::default()
                .set_code(Code::InvalidArgument)
                .set_message("simulated bad request");
            Err(GaxError::service(status))
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
    async fn test_jobs_insert_job_failed() -> TestResult {
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

        let errors = match err {
            QueryError::JobFailed { errors } => errors,
            _ => panic!("expected QueryError::JobFailed, got {err:?}"),
        };
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].reason, "invalidQuery");
        assert_eq!(errors[0].message, "Syntax error");

        Ok(())
    }

    #[test_case("DONE", true; "completed")]
    #[test_case("RUNNING", false; "pending")]
    #[tokio::test]
    async fn test_jobs_insert_execute_success(
        job_state: &'static str,
        completed: bool,
    ) -> TestResult {
        let job_ref = JobReference::new()
            .set_job_id("test-job")
            .set_project_id("my-project");
        let job_ref_clone = job_ref.clone();

        let mut mock = MockJobService::new();
        mock.expect_insert_job().return_once(move |_, _| {
            let status = JobStatus::new().set_state(job_state);
            let job = Job::new()
                .set_job_reference(job_ref_clone)
                .set_status(status);
            Ok(Response::from(job))
        });
        let job_service = create_job_service(mock);

        let job_config = JobConfiguration::new().set_query(JobConfigurationQuery::new());
        let job = Job::new().set_configuration(job_config);
        let req = InsertJobRequest::new().set_job(job);
        let executor = InsertJobExecutor::new(job_service.clone(), req);
        let job = executor.execute().await?;
        let query = Query::from_job(job_service, job, None, None);

        assert_eq!(query.completed, completed);
        assert_eq!(query.metadata.job_reference, Some(job_ref));
        Ok(())
    }
}
