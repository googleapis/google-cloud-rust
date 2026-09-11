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
use crate::query::builder::{
    QUERY_REQUEST_ID_PREFIX, Query, generate_job_reference, generate_prefixed_id,
};
use crate::query::query_handle::build_get_job;
use crate::query::retry_policy::{JobRetryResult, is_duplicate_job_error};
use crate::query::{Query as QueryHandle, Result};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    InsertJobRequest, Job, JobConfiguration, JobReference, PostQueryRequest, QueryRequest,
    QueryResponse,
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
        // Safe to resend: `request_id` merges a duplicate that is still in
        // flight, and a duplicate that arrives after the original completed
        // returns 409, which the caller recovers by adopting the named job.
        let res = self
            .job_service
            .query()
            .with_request(self.request)
            .with_idempotency(true)
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

        check_job_status(res)
    }
}

/// Context for running queries and handling job-level retries / re-issuances.
#[derive(Clone, Debug)]
pub(crate) struct RetryContext {
    pub(crate) template: Arc<Query>,
    pub(crate) state: RetryState,
}

impl RetryContext {
    pub(crate) fn new(template: Query) -> Self {
        Self {
            template: Arc::new(template),
            state: RetryState::default(),
        }
    }

    pub(crate) fn on_error(&self, error: QueryError) -> JobRetryResult {
        self.template.job_retry_policy.on_error(&self.state, error)
    }

    pub(crate) async fn reissue(mut self, delay: Duration) -> Result<QueryHandle> {
        tokio::time::sleep(delay).await;
        self.state.attempt_count += 1;
        // Box heavy RPC call future to avoid large stack frames.
        Box::pin(self.execute()).await
    }

    pub(crate) async fn execute(mut self) -> Result<QueryHandle> {
        let project_id = self.template.project_id.clone().unwrap_or_default();

        loop {
            // Box heavy RPC call future to avoid large stack frames.
            match Box::pin(self.execute_once(&project_id)).await {
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

    /// Returns true if the query should use the jobs.insert API.
    fn force_job_path(&self) -> bool {
        // jobs.query doesn't return full statistics information, so we route to jobs.insert
        let dry_run = self.template.request.dry_run;
        self.template.request.force_job_path() || dry_run
    }

    async fn execute_once(&self, project_id: &str) -> Result<QueryHandle> {
        if self.force_job_path() {
            self.execute_jobs_insert(project_id).await
        } else {
            self.execute_jobs_query(project_id).await
        }
    }

    // Execute the query using the jobs.insert method.
    async fn execute_jobs_insert(&self, project_id: &str) -> Result<QueryHandle> {
        let job_service = self.template.job_service.clone();
        let max_results = self.template.request.max_results;

        let job_config: JobConfiguration = self.template.request.clone().into();
        let job_ref = generate_job_reference(project_id, &self.template.request.location);
        let job = Job::new()
            .set_configuration(job_config)
            .set_job_reference(job_ref.clone());
        let req = InsertJobRequest::new()
            .set_job(job)
            .set_project_id(project_id);

        // Box heavy RPC call future to avoid large stack frames.
        let job = match Box::pin(InsertJobExecutor::new(job_service.clone(), req).execute()).await {
            Ok(job) => job,
            Err(err) if is_duplicate_job_error(&err) => {
                // A fresh job ID is generated per attempt, so a duplicate means
                // an earlier attempt of this request reached the service. Adopt
                // the job it created rather than fail a running, billing query.
                let existing_job = match build_get_job(&job_service, &job_ref) {
                    Some(get) => Box::pin(get.send()).await.ok(),
                    None => None,
                };
                let Some(existing_job) = existing_job else {
                    // The original error names the running job, and unlike a
                    // `jobs.get` failure it never makes the job retry loop
                    // reissue the query.
                    return Err(err);
                };
                check_job_status(existing_job)?
            }
            Err(err) => return Err(err),
        };

        Ok(QueryHandle::from_job(
            job_service,
            job,
            Some(self.clone()),
            max_results,
        ))
    }

    // Execute the query using the jobs.query method.
    async fn execute_jobs_query(&self, project_id: &str) -> Result<QueryHandle> {
        let job_service = self.template.job_service.clone();
        let max_results = self.template.request.max_results;

        let query_request_id = generate_prefixed_id(QUERY_REQUEST_ID_PREFIX);
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

        // Box heavy RPC call future to avoid large stack frames.
        let res = match Box::pin(PostQueryExecutor::new(job_service.clone(), req).execute()).await {
            Ok(res) => res,
            Err(err) if is_duplicate_job_error(&err) => {
                // A resent request collided with the job its earlier attempt
                // created. That job ran and was billed, so adopt it instead of
                // reporting a failure for a query that already succeeded.
                let get = parse_duplicate_job_reference(&err)
                    .and_then(|job_ref| build_get_job(&job_service, &job_ref));
                let existing_job = match get {
                    Some(get) => Box::pin(get.send()).await.ok(),
                    None => None,
                };
                let Some(existing_job) = existing_job else {
                    // The original error names the job, and unlike a `jobs.get`
                    // failure it never makes the job retry loop reissue the
                    // query.
                    return Err(err);
                };
                return Ok(QueryHandle::from_job(
                    job_service,
                    check_job_status(existing_job)?,
                    Some(self.clone()),
                    max_results,
                ));
            }
            Err(err) => return Err(err),
        };

        Ok(QueryHandle::from_query_response(
            job_service,
            res,
            Some(self.clone()),
            max_results,
        ))
    }
}

/// Returns [`QueryError::JobFailed`] if the service reports the job as failed.
fn check_job_status(job: Job) -> Result<Job> {
    if let Some(status) = job.status.as_ref()
        && status.error_result.is_some()
    {
        let errors = status.errors.clone();
        return Err(QueryError::JobFailed { errors });
    }

    Ok(job)
}

/// Extracts the job named by an `Already Exists: Job my-project:US.job_123`
/// error message.
///
/// `jobs.query` does not let the caller name the job it creates, and the 409
/// carries no structured reference, so the message is the only handle on the
/// duplicated job. Best effort by design: when the message does not parse the
/// caller reports the original error, which is what it would have reported
/// anyway.
fn parse_duplicate_job_reference(error: &QueryError) -> Option<JobReference> {
    const PREFIX: &str = "Already Exists: Job ";

    let QueryError::Rpc { source } = error else {
        return None;
    };
    let name = source.status()?.message.strip_prefix(PREFIX)?;
    // Split from the right: domain scoped project IDs contain both separators,
    // as in `example.com:my-project:US.job_123`.
    let (project_and_location, job_id) = name.rsplit_once('.')?;
    let (project_id, location) = project_and_location.rsplit_once(':')?;
    if project_id.is_empty() || location.is_empty() || job_id.is_empty() {
        return None;
    }

    Some(
        JobReference::new()
            .set_project_id(project_id)
            .set_location(location)
            .set_job_id(job_id),
    )
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
    use google_cloud_gax::error::rpc::{Code, Status, StatusDetails};
    use google_cloud_gax::response::Response;
    use google_cloud_rpc::model::ErrorInfo;
    use serde_json::{Map, json};
    use std::sync::Mutex;
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    // The error BigQuery returns when a request tries to create a job that a
    // previous, apparently failed, attempt of the same request already created.
    fn duplicate_job_error(job_id: &str) -> GaxError {
        let message = format!("Already Exists: Job my-project:US.{job_id}");
        let status = Status::default()
            .set_code(Code::AlreadyExists)
            .set_message(message.clone())
            .set_details(vec![StatusDetails::ErrorInfo(
                ErrorInfo::new()
                    .set_reason("duplicate")
                    .set_domain("global")
                    .set_metadata([("message".to_string(), message)]),
            )]);
        GaxError::service(status)
    }

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
        let query = QueryHandle::from_query_response(job_service, res, None, None);

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
        let query = QueryHandle::from_job(job_service, job, None, None);

        assert_eq!(query.completed, completed);
        assert_eq!(query.metadata.job_reference, Some(job_ref));
        Ok(())
    }

    #[tokio::test]
    async fn test_dry_run_routes_to_jobs_insert() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_insert_job().returning(|req, _| {
            let job_config = req.job.as_ref().unwrap().configuration.as_ref().unwrap();
            assert_eq!(job_config.dry_run, Some(wkt::BoolValue::from(true)));
            let job = Job::new().set_job_reference(JobReference::new().set_job_id("insert-job"));
            Ok(Response::from(job))
        });
        mock.expect_query().never();

        let job_service = create_job_service(mock);
        let query = Query::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_dry_run(true);

        let retry_ctx = RetryContext::new(query);
        assert!(retry_ctx.force_job_path(), "Dry run should force job path");

        let handle = retry_ctx.execute_once("my-project").await?;
        assert_eq!(handle.metadata.job_reference.unwrap().job_id, "insert-job");
        Ok(())
    }

    #[tokio::test]
    async fn test_non_dry_run_routes_to_jobs_query() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(|req, _| {
            let query_req = req.query_request.as_ref().unwrap();
            assert!(!query_req.dry_run);
            let res =
                QueryResponse::new().set_job_reference(JobReference::new().set_job_id("query-job"));
            Ok(Response::from(res))
        });
        mock.expect_insert_job().never();

        let job_service = create_job_service(mock);
        let query = Query::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_dry_run(false);

        let retry_ctx = RetryContext::new(query);
        assert!(
            !retry_ctx.force_job_path(),
            "Non-dry run should not force job path"
        );

        let handle = retry_ctx.execute_once("my-project").await?;
        assert_eq!(handle.metadata.job_reference.unwrap().job_id, "query-job");
        Ok(())
    }

    #[tokio::test]
    async fn test_jobs_insert_duplicate_adopts_existing_job() -> TestResult {
        let inserted = Arc::new(Mutex::new(None));

        let mut mock = MockJobService::new();
        let captured = inserted.clone();
        mock.expect_insert_job().return_once(move |req, _| {
            let job_ref = req.job.unwrap().job_reference.unwrap();
            let err = duplicate_job_error(&job_ref.job_id);
            *captured.lock().unwrap() = Some(job_ref);
            Err(err)
        });
        mock.expect_get_job().return_once(move |req, _| {
            let job_ref = JobReference::new()
                .set_project_id(req.project_id)
                .set_job_id(req.job_id)
                .set_location(req.location);
            let job = Job::new()
                .set_configuration(JobConfiguration::new().set_query(JobConfigurationQuery::new()))
                .set_job_reference(job_ref)
                .set_status(JobStatus::new().set_state("RUNNING"));
            Ok(Response::from(job))
        });

        let job_service = create_job_service(mock);
        let query = Query::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_location("us-central1")
            .set_priority("BATCH");

        let retry_ctx = RetryContext::new(query);
        assert!(retry_ctx.force_job_path(), "priority should force job path");

        let handle = retry_ctx.execute_once("my-project").await?;

        // The recovered job must be the one the earlier attempt created, and it
        // must be fetched from the location of the query.
        let inserted = inserted.lock().unwrap().clone().expect("job inserted");
        let job_ref = handle.metadata.job_reference.expect("job reference");
        assert_eq!(job_ref, inserted, "{job_ref:?}");
        assert_eq!(job_ref.location.as_deref(), Some("us-central1"));
        assert!(!handle.completed, "the recovered job is still running");
        Ok(())
    }

    #[tokio::test]
    async fn test_jobs_insert_duplicate_reports_original_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_insert_job()
            .return_once(move |_, _| Err(duplicate_job_error("job_123")));
        // The job cannot be fetched, for example because the caller lacks
        // permissions to read it.
        mock.expect_get_job().return_once(move |_, _| {
            let status = Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("simulated permission denied");
            Err(GaxError::service(status))
        });

        let job_service = create_job_service(mock);
        let query = Query::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_priority("BATCH");

        let err = RetryContext::new(query)
            .execute_once("my-project")
            .await
            .unwrap_err();

        // The duplicate error names the running job, so it is more useful than
        // the error from `jobs.get`.
        let QueryError::Rpc { source } = &err else {
            panic!("expected QueryError::Rpc, got {err:?}");
        };
        let status = source.status().expect("status");
        assert_eq!(status.code, Code::AlreadyExists, "{status:?}");
        assert!(status.message.contains("Already Exists: Job"), "{status:?}");
        Ok(())
    }
    #[tokio::test]
    async fn test_jobs_query_duplicate_adopts_existing_job() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query()
            .return_once(move |_, _| Err(duplicate_job_error("job_123")));
        mock.expect_get_job().return_once(move |req, _| {
            let job_ref = JobReference::new()
                .set_project_id(req.project_id)
                .set_job_id(req.job_id)
                .set_location(req.location);
            let job = Job::new()
                .set_configuration(JobConfiguration::new().set_query(JobConfigurationQuery::new()))
                .set_job_reference(job_ref)
                .set_status(JobStatus::new().set_state("RUNNING"));
            Ok(Response::from(job))
        });

        let job_service = create_job_service(mock);
        let query = Query::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");

        let retry_ctx = RetryContext::new(query);
        assert!(!retry_ctx.force_job_path(), "must use the jobs.query path");

        let handle = retry_ctx.execute_once("my-project").await?;

        // The adopted job is the one the 409 names, fetched in its location.
        let job_ref = handle.metadata.job_reference.expect("job reference");
        assert_eq!(job_ref.project_id, "my-project");
        assert_eq!(job_ref.location.as_deref(), Some("US"));
        assert_eq!(job_ref.job_id, "job_123");
        assert!(!handle.completed, "the adopted job is still running");
        Ok(())
    }

    #[tokio::test]
    async fn test_jobs_query_duplicate_reports_original_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query()
            .return_once(move |_, _| Err(duplicate_job_error("job_123")));
        mock.expect_get_job().return_once(move |_, _| {
            let status = Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("simulated permission denied");
            Err(GaxError::service(status))
        });

        let job_service = create_job_service(mock);
        let query = Query::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");

        let err = RetryContext::new(query)
            .execute_once("my-project")
            .await
            .unwrap_err();

        // The duplicate error names the running job, so it is more useful than
        // the error from `jobs.get`.
        let QueryError::Rpc { source } = &err else {
            panic!("expected QueryError::Rpc, got {err:?}");
        };
        let status = source.status().expect("status");
        assert_eq!(status.code, Code::AlreadyExists, "{status:?}");
        Ok(())
    }

    #[test]
    fn test_parse_duplicate_job_reference() {
        let rpc = |message: &str| {
            QueryError::from(GaxError::service(
                Status::default()
                    .set_code(Code::AlreadyExists)
                    .set_message(message),
            ))
        };

        let err = rpc("Already Exists: Job my-project:US.job_123");
        let job_ref = parse_duplicate_job_reference(&err).expect("job reference");
        assert_eq!(job_ref.project_id, "my-project");
        assert_eq!(job_ref.location.as_deref(), Some("US"));
        assert_eq!(job_ref.job_id, "job_123");

        // Domain scoped project IDs contain both separators.
        let err = rpc("Already Exists: Job example.com:my-project:US.job_123");
        let job_ref = parse_duplicate_job_reference(&err).expect("job reference");
        assert_eq!(job_ref.project_id, "example.com:my-project");
        assert_eq!(job_ref.location.as_deref(), Some("US"));
        assert_eq!(job_ref.job_id, "job_123");

        let unparsable = [
            "Already Exists: Job my-project:US.",
            "Already Exists: Job my-project.job_123",
            "Already Exists: Job my-project:US:job_123",
            "Already Exists: Job ",
            "Some other error",
        ];
        for message in unparsable {
            let err = rpc(message);
            assert!(parse_duplicate_job_reference(&err).is_none(), "{message}");
        }

        // Only RPC failures carry a service message.
        let job_failed = QueryError::JobFailed { errors: vec![] };
        assert!(parse_duplicate_job_reference(&job_failed).is_none());
    }

    // Both RPCs opt into retries: each can recover the job a resend collides
    // with, `jobs.insert` from the ID it generated and `jobs.query` from the
    // ID the 409 names.
    #[tokio::test]
    async fn test_query_rpcs_are_idempotent() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().times(1).returning(|_, options| {
            assert_eq!(options.idempotent(), Some(true), "jobs.query");
            Err(GaxError::service(
                Status::default().set_code(Code::Unavailable),
            ))
        });
        mock.expect_insert_job().times(1).returning(|_, options| {
            assert_eq!(options.idempotent(), Some(true), "jobs.insert");
            Err(GaxError::service(
                Status::default().set_code(Code::Unavailable),
            ))
        });
        let job_service = create_job_service(mock);

        let query = Query::new(job_service.clone(), "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_dry_run(false);
        RetryContext::new(query)
            .execute_once("my-project")
            .await
            .unwrap_err();

        let query = Query::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_dry_run(true);
        RetryContext::new(query)
            .execute_once("my-project")
            .await
            .unwrap_err();
        Ok(())
    }
}
