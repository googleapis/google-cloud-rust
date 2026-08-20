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

use crate::generated::QueryRequest;
use crate::model::JobReference;
use crate::model::query_request::JobCreationMode;
use crate::query::execution::RetryContext;
use crate::query::{CompleteQuery, Query as QueryHandle, Result};
use crate::retry_policy::{JobRetryPolicy, default_job_retry_policy};
use google_cloud_bigquery_v2::client::JobService;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) const JOB_ID_PREFIX: &str = "job_";
pub(crate) const QUERY_REQUEST_ID_PREFIX: &str = "req_";

/// A builder for configuring and executing a SQL query.
///
/// [`BigQuery::query()`](crate::client::BigQuery::query) returns a [`Query`](crate::builder::bigquery::Query) builder.
///
/// Use this builder to configure query parameters, dataset defaults, geographic location,
/// result limits, and caching before executing the query with [`send()`](Query::send)
/// or [`until_done()`](Query::until_done).
///
/// # Example
///
/// ```
/// # use google_cloud_bigquery::client::BigQuery;
/// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
/// let mut rows = client
///     .query("SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` WHERE state = 'TX' LIMIT 100")
///     .set_location("US")
///     .set_max_results(50_u32)
///     .until_done()
///     .await?
///     .read();
///
/// while let Some(row) = rows.next().await.transpose()? {
///     let name: String = row.get("name");
///     println!("Name: {name}");
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Query {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) request: QueryRequest,
    pub(crate) project_id: Option<String>,
    pub(crate) job_retry_policy: Arc<dyn JobRetryPolicy>,
}

impl Query {
    /// Creates a new `Query` builder for the given SQL query.
    pub(crate) fn new(job_service: Arc<JobService>, sql: String) -> Self {
        Self {
            job_service,
            request: QueryRequest::default()
                .set_query(sql)
                .set_use_legacy_sql(wkt::BoolValue::from(false))
                .set_job_creation_mode(JobCreationMode::JobCreationOptional),
            project_id: None,
            job_retry_policy: default_job_retry_policy(),
        }
    }

    /// Sets the project ID to override the default client project ID for query
    /// execution and billing.
    ///
    /// If you configured a default project ID on the
    /// [`BigQuery`][crate::client::BigQuery] client via
    /// [`ClientBuilder::with_project_id`][crate::client_builder::ClientBuilder::with_project_id],
    /// the query inherits it automatically. Calling this method overrides the
    /// project ID for this specific query.
    ///
    /// You must specify a project ID either on the client or on the query
    /// builder before calling [`send()`](Query::send).
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
    /// let query_handle = client
    ///     .query("SELECT 1 AS count")
    ///     .with_project_id("my-project-id")
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_project_id<S: Into<String>>(mut self, project_id: S) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Executes the SQL query.
    ///
    /// Returns a [`Query`](crate::Query) handle representing the query execution.
    /// You can call [`until_done()`](crate::Query::until_done) on the returned handle
    /// to wait for results, or inspect the initial [`metadata()`](crate::Query::metadata).
    ///
    /// You must configure a target project ID on either the
    /// [`BigQuery`][crate::client::BigQuery] client via
    /// [`ClientBuilder::with_project_id`][crate::client_builder::ClientBuilder::with_project_id]
    /// or on this builder via [`with_project_id()`](Query::with_project_id).
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
    /// let query_handle = client
    ///     .query("SELECT CURRENT_TIMESTAMP() AS now")
    ///     .send()
    ///     .await?;
    ///
    /// let completed_query = query_handle
    ///     .until_done()
    ///     .await?;
    ///
    /// let mut rows = completed_query.read();
    /// if let Some(row) = rows.next().await.transpose()? {
    ///     let now: String = row.get("now");
    ///     println!("Current time: {now}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(self) -> Result<QueryHandle> {
        // Box heavy RPC call future to avoid large stack frames.
        Box::pin(RetryContext::new(self).execute()).await
    }

    /// Sends the query execution request and waits until execution completes.
    ///
    /// This is a convenience method equivalent to calling
    /// [`.send().await?.until_done().await`](Query::send).
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
    /// let completed_query = client
    ///     .query("SELECT CURRENT_TIMESTAMP() AS now")
    ///     .until_done()
    ///     .await?;
    ///
    /// let mut rows = completed_query.read();
    /// if let Some(row) = rows.next().await.transpose()? {
    ///     let now: String = row.get("now");
    ///     println!("Current time: {now}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn until_done(self) -> Result<CompleteQuery> {
        // Box heavy RPC call future to avoid large stack frames.
        Box::pin(async move { self.send().await?.until_done().await }).await
    }
}

// Create a job reference with a generated job ID.
//
// BigQuery does not strictly define a format for job IDs, just a limit in size.
// See https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
pub(crate) fn generate_job_reference(project_id: &str, location: &str) -> JobReference {
    let job_id = generate_prefixed_id(JOB_ID_PREFIX);
    let mut job_ref = JobReference::new()
        .set_project_id(project_id.to_string())
        .set_job_id(job_id);

    if !location.is_empty() {
        job_ref = job_ref.set_location(location.to_string());
    }
    job_ref
}

// Create a random ID with the given prefix and a UUID.
//
// BigQuery does not strictly define a format for request and job IDs, just a limit in size.
// However, request IDs are more restrictive and have a limit of 36 characters.
// UUID v4 simple format is used to reduce length.
//
// See https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest for request ID
// and https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/JobReference for job ID.
pub(crate) fn generate_prefixed_id(prefix: &str) -> String {
    format!("{prefix}{}", Uuid::new_v4().simple())
}

include!("../generated/builder.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::BigQuery;
    use crate::error::QueryError;
    use crate::query::tests::{MockJobService, create_job_service};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_bigquery_v2::model::query_request::JobCreationMode;
    use google_cloud_bigquery_v2::model::{
        ErrorProto, Job, JobConfiguration, JobReference, JobStatus,
        QueryRequest as JobsQueryRequest, QueryResponse,
    };
    use google_cloud_gax::response::Response;

    // bigquery limits request id to 36 characters
    const BIGQUERY_REQ_ID_LIMIT: usize = 36;

    type TestResult = anyhow::Result<()>;

    #[test]
    fn test_new() {
        let job_service = create_job_service(MockJobService::new());
        let sql = "SELECT 1".to_string();
        let query_builder = Query::new(job_service, sql.clone());
        assert_eq!(query_builder.request.query, sql);
        assert_eq!(
            query_builder.request.use_legacy_sql,
            Some(wkt::BoolValue::from(false))
        );
        assert_eq!(
            query_builder.request.job_creation_mode,
            JobCreationMode::JobCreationOptional
        );
        assert_eq!(query_builder.project_id, None);
    }

    #[test]
    fn test_with_project_id() {
        let job_service = create_job_service(MockJobService::new());
        let query_builder =
            Query::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");
        assert_eq!(query_builder.project_id.unwrap(), "my-project");
    }

    #[tokio::test]
    async fn test_run_missing_project_id() -> anyhow::Result<()> {
        let client = BigQuery::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let query_builder = client.query("SELECT 1");
        let err = query_builder
            .send()
            .await
            .expect_err("should return an error when project_id is missing");
        assert!(
            matches!(&err, QueryError::Rpc { source } if source.is_binding()),
            "expected Binding error for missing project ID, got {err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_run_missing_project_id_force_job_path() -> anyhow::Result<()> {
        let client = BigQuery::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let query_builder = client.query("SELECT 1").set_allow_large_results(true);
        let err = query_builder
            .send()
            .await
            .expect_err("should return an error when project_id is missing");
        assert!(
            matches!(&err, QueryError::Rpc { source } if source.is_binding()),
            "expected Binding error for missing project ID on job path, got {err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_run_until_done_missing_project_id() -> anyhow::Result<()> {
        let client = BigQuery::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let query_builder = client.query("SELECT 1");
        let err = query_builder
            .until_done()
            .await
            .expect_err("should return an error when project_id is missing");
        assert!(
            matches!(&err, QueryError::Rpc { source } if source.is_binding()),
            "expected Binding error for missing project ID, got {err:?}"
        );
        Ok(())
    }

    #[test]
    fn test_generate_prefixed_id() {
        let job_id = generate_prefixed_id(JOB_ID_PREFIX);
        assert!(job_id.starts_with(JOB_ID_PREFIX), "{job_id:?}");
        assert!(
            Uuid::parse_str(&job_id[JOB_ID_PREFIX.len()..]).is_ok(),
            "{job_id:?}"
        );

        let req_id = generate_prefixed_id(QUERY_REQUEST_ID_PREFIX);
        assert!(req_id.starts_with(QUERY_REQUEST_ID_PREFIX), "{req_id:?}");
        assert!(req_id.len() <= BIGQUERY_REQ_ID_LIMIT, "{req_id:?}");
        assert!(
            Uuid::parse_str(&req_id[QUERY_REQUEST_ID_PREFIX.len()..]).is_ok(),
            "{req_id:?}"
        );
    }

    #[test]
    fn test_generate_job_reference() {
        let job_ref = generate_job_reference("my-project", "us-central1");
        assert_eq!(job_ref.project_id, "my-project");
        assert!(job_ref.job_id.starts_with(JOB_ID_PREFIX), "{job_ref:?}");
        assert_eq!(job_ref.location.as_deref(), Some("us-central1"));
    }

    #[tokio::test]
    async fn test_run_jobs_insert() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_insert_job().returning(|req, _| {
            let job_ref = req.job.as_ref().unwrap().job_reference.as_ref().unwrap();
            assert!(job_ref.job_id.starts_with(JOB_ID_PREFIX), "{job_ref:?}");
            let job_ref = JobReference::new()
                .set_job_id("test-job")
                .set_project_id("my-project");
            let job = Job::new()
                .set_job_reference(job_ref)
                .set_status(JobStatus::new().set_state("DONE"));
            Ok(google_cloud_gax::response::Response::from(job))
        });
        let job_service = create_job_service(mock);

        let query_builder = Query::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_allow_large_results(true);
        let query = query_builder.send().await?;
        assert!(query.completed, "{query:?}");

        Ok(())
    }

    #[tokio::test]
    async fn test_run_jobs_query() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(move |req, _| {
            let req_id = &req.query_request.as_ref().unwrap().request_id;
            assert!(req_id.starts_with(QUERY_REQUEST_ID_PREFIX), "{req_id:?}");
            assert!(req_id.len() <= BIGQUERY_REQ_ID_LIMIT, "{req_id:?}");
            Ok(Response::from(
                QueryResponse::new().set_query_id("some_query_id"),
            ))
        });
        let job_service = create_job_service(mock);
        let query_builder =
            Query::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");
        let query = query_builder.send().await?;
        assert!(!query.completed, "{query:?}");
        assert_eq!(query.metadata.query_id, "some_query_id");

        Ok(())
    }

    #[test]
    fn test_force_job_path() {
        let job_service = create_job_service(MockJobService::new());
        let mut query_builder = Query::new(job_service, "SELECT 1".to_string());
        assert!(!query_builder.request.force_job_path());

        // setting a jobs.insert exclusive field
        query_builder = query_builder.set_allow_large_results(true);
        assert!(query_builder.request.force_job_path());
    }

    #[test]
    fn test_request_conversions() {
        let req = QueryRequest::default()
            .set_query("SELECT 1".to_string())
            .set_dry_run(true)
            .set_use_legacy_sql(true);

        let query_request: JobsQueryRequest = req.clone().into();
        assert_eq!(query_request.query, "SELECT 1");
        assert!(query_request.dry_run);
        assert_eq!(
            query_request.use_legacy_sql,
            Some(wkt::BoolValue::from(true))
        );

        let job_config: JobConfiguration = req.into();
        let job_query = job_config.query.as_ref().unwrap();
        assert_eq!(job_query.query, "SELECT 1");
        assert_eq!(job_query.use_legacy_sql, Some(wkt::BoolValue::from(true)));
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_reissue_on_retryable_job_failed() -> TestResult {
        let mut mock = MockJobService::new();
        let mut seq = mockall::Sequence::new();
        let first_request_id = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let first_request_id_clone = first_request_id.clone();

        mock.expect_query()
            .in_sequence(&mut seq)
            .times(1)
            .returning(move |req, _| {
                let req_id = req.query_request.as_ref().unwrap().request_id.clone();
                assert!(req_id.starts_with(QUERY_REQUEST_ID_PREFIX));
                *first_request_id_clone.lock().unwrap() = req_id;
                let err_proto = ErrorProto::new()
                    .set_reason("backendError")
                    .set_message("temporary server issue");
                Ok(Response::from(
                    QueryResponse::new().set_errors(vec![err_proto]),
                ))
            });

        mock.expect_query()
            .in_sequence(&mut seq)
            .times(1)
            .returning(move |req, _| {
                let req_id = req.query_request.as_ref().unwrap().request_id.clone();
                assert!(req_id.starts_with(QUERY_REQUEST_ID_PREFIX));
                assert_ne!(
                    req_id,
                    *first_request_id.lock().unwrap(),
                    "reissued query must generate a fresh request_id to avoid 409 duplicate ID conflicts"
                );
                Ok(Response::from(
                    QueryResponse::new().set_query_id("q_success"),
                ))
            });

        let job_service = create_job_service(mock);
        let query_builder =
            Query::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");
        let query = query_builder.send().await?;
        assert_eq!(query.metadata.query_id, "q_success");

        Ok(())
    }

    #[tokio::test]
    async fn test_run_jobs_query_with_max_results() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(move |req, _| {
            assert_eq!(
                req.query_request.as_ref().and_then(|r| r.max_results),
                Some(100)
            );
            Ok(Response::from(QueryResponse::new()))
        });
        let job_service = create_job_service(mock);
        let query_builder = Query::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_max_results(100_u32);
        let query = query_builder.send().await?;
        assert_eq!(query.max_results, Some(100));

        Ok(())
    }

    #[tokio::test]
    async fn test_run_jobs_insert_with_max_results() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_insert_job().returning(|_, _| {
            let job_ref = JobReference::new()
                .set_job_id("test-job")
                .set_project_id("my-project");
            let job = Job::new()
                .set_job_reference(job_ref)
                .set_status(JobStatus::new().set_state("DONE"));
            Ok(google_cloud_gax::response::Response::from(job))
        });
        let job_service = create_job_service(mock);

        let query_builder = Query::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_allow_large_results(true)
            .set_max_results(50_u32);
        let query = query_builder.send().await?;
        assert_eq!(query.max_results, Some(50));

        Ok(())
    }

    #[tokio::test]
    async fn test_until_done_jobs_query() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(move |_, _| {
            Ok(Response::from(
                QueryResponse::new()
                    .set_job_complete(true)
                    .set_query_id("some_query_id"),
            ))
        });
        let job_service = create_job_service(mock);
        let query = Query::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");
        let complete = query.until_done().await?;
        assert_eq!(complete.metadata().query_id, "some_query_id");

        Ok(())
    }
}
