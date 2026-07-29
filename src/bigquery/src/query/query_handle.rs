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
use crate::model::QueryMetadata;
use crate::query::{QueryReference, Result, RowIterator, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    GetQueryResultsRequest, GetQueryResultsResponse, Job, JobReference, QueryResponse,
};
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_state::PollingState;
use std::collections::VecDeque;
use std::sync::Arc;

/// A handle representing a running or completed SQL query execution.
///
/// An instance of `Query` is returned by [`RunQuery::run()`](crate::query::RunQuery::run).
/// Depending on how the query was routed and executed, this handle may represent an asynchronous
/// background job currently executing on BigQuery, or a fast-path query that has already completed.
///
/// To obtain the final result set, call [`until_done()`](Query::until_done), which will check the
/// execution status and automatically poll the service if the job is still running in the background.
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// use google_cloud_bigquery::client::BigQuery;
///
/// let client = BigQuery::builder().build().await?;
/// let query_handle = client
///     .query("SELECT 42 AS answer")
///     .with_project_id("my-project-id")
///     .run()
///     .await?;
///
/// // Poll until execution completes.
/// let completed = query_handle.until_done().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Query {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
    pub(crate) completed: bool,
    // TODO(#5592): add QueryCreationMetadata to expose initial job and response data.
    #[allow(dead_code)]
    pub(crate) initial_job: Option<Job>,
    pub(crate) initial_response: Option<QueryResponse>,
    pub(crate) max_results: Option<u32>,
}

impl Query {
    /// Returns the [`QueryReference`](crate::model::QueryReference) identifying this query execution.
    ///
    /// The reference will be [`QueryReference::Job`](crate::model::QueryReference::Job) containing a BigQuery Query [job reference]
    /// if a job was created, or [`QueryReference::Stateless`](crate::model::QueryReference::Stateless) with an opaque
    /// query ID if the execution ran statelessly via [jobs.query].
    ///
    /// [job reference]: https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
    /// [jobs.query]: https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    ///
    /// # Example
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_bigquery::client::BigQuery;
    /// use google_cloud_bigquery::model::QueryReference;
    ///
    /// let client = BigQuery::builder().build().await?;
    /// let query_handle = client
    ///     .query("SELECT 'hello' AS msg")
    ///     .with_project_id("my-project-id")
    ///     .run()
    ///     .await?;
    ///
    /// match query_handle.query_reference() {
    ///     QueryReference::Job(job_ref) => println!("Running job ID: {}", job_ref.job_id),
    ///     QueryReference::Stateless { query_id } => println!("Stateless query ID: {query_id}"),
    ///     _ => println!("Other query reference"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_reference(&self) -> QueryReference {
        let from_query_id = self
            .initial_response
            .as_ref()
            .map(|res| res.query_id.clone())
            .filter(|s| !s.is_empty())
            .map(QueryReference::from_query_id);
        let from_job_ref = self.job_ref.clone().map(QueryReference::from);

        from_job_ref
            .or(from_query_id)
            .expect("query must have either a job reference or query id")
    }

    /// Periodically polls the background job status until query execution finishes.
    ///
    /// If the query was executed via the fast path and already completed during the initial request,
    /// this method immediately returns a [`CompleteQuery`](crate::query::CompleteQuery) without making additional network calls.
    /// Otherwise, it implements a backoff loop querying the job status until it succeeds or fails.
    ///
    /// # Errors
    ///
    /// Returns an error if a remote service or network failure happens during polling, or if the BigQuery job
    /// fails due to runtime execution errors.
    ///
    /// # Example
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_bigquery::client::BigQuery;
    ///
    /// let client = BigQuery::builder().build().await?;
    /// let complete = client
    ///     .query("SELECT 1 + 1 AS result")
    ///     .with_project_id("my-project-id")
    ///     .run()
    ///     .await?
    ///     .until_done()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn until_done(self) -> Result<CompleteQuery> {
        let Query {
            job_service,
            job_ref,
            completed,
            initial_job: _,
            initial_response,
            max_results,
        } = self;

        if let (true, Some(initial_response)) = (completed, initial_response) {
            return Ok(CompleteQuery::from_query_response(
                job_service,
                job_ref,
                initial_response,
                max_results,
            ));
        }

        let job_ref = job_ref
            .as_ref()
            .expect("query job should have job reference at this point");
        let backoff_policy = Arc::new(
            ExponentialBackoffBuilder::default()
                .with_initial_delay(std::time::Duration::from_secs(10))
                .build()
                .expect("valid backoff configuration"),
        );
        let res = poll_query_results(&job_service, job_ref, backoff_policy).await?;
        Ok(CompleteQuery::from_get_query_results_response(
            job_service,
            job_ref,
            res,
            max_results,
        ))
    }
}

/// A handle representing a successfully completed query ready for reading results.
///
/// An instance of `CompleteQuery` is returned by [`Query::until_done()`](crate::query::Query::until_done).
///
/// This handle provides access to cached execution metadata, schema definitions, and a
/// row iterator via [`read()`](CompleteQuery::read).
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// use google_cloud_bigquery::client::BigQuery;
///
/// let client = BigQuery::builder().build().await?;
/// let complete = client
///     .query("SELECT 'done' AS status")
///     .with_project_id("my-project-id")
///     .run()
///     .await?
///     .until_done()
///     .await?;
///
/// println!("Cache hit: {:?}", complete.metadata().cache_hit);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct CompleteQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) schema: Arc<Schema>,
    pub(crate) page_token: Option<String>,
    pub(crate) metadata: QueryMetadata,
    pub(crate) max_results: Option<u32>,
}

impl std::fmt::Debug for CompleteQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteQuery")
            .field("job_ref", &self.job_ref)
            .field("cached_rows", &self.cached_rows)
            .field("schema", &self.schema)
            .field("page_token", &self.page_token)
            .finish_non_exhaustive()
    }
}

impl CompleteQuery {
    pub(crate) fn from_get_query_results_response(
        job_service: Arc<JobService>,
        job_ref: &JobReference,
        mut res: GetQueryResultsResponse,
        max_results: Option<u32>,
    ) -> Self {
        let cached_rows = VecDeque::from(std::mem::take(&mut res.rows));
        let metadata = QueryMetadata::from(res);
        // DDL/DML queries have no schema.
        let schema = metadata.schema.clone().unwrap_or_default();
        let schema = Arc::new(Schema::new(schema));
        let page_token = if metadata.page_token.is_empty() {
            None
        } else {
            Some(metadata.page_token.clone())
        };
        Self {
            job_service,
            job_ref: Some(job_ref.clone()),
            cached_rows,
            page_token,
            schema,
            metadata,
            max_results,
        }
    }

    pub(crate) fn from_query_response(
        job_service: Arc<JobService>,
        job_ref: Option<JobReference>,
        mut res: QueryResponse,
        max_results: Option<u32>,
    ) -> Self {
        let cached_rows = VecDeque::from(std::mem::take(&mut res.rows));
        let metadata = QueryMetadata::from(res);
        // DDL/DML queries have no schema.
        let schema = metadata.schema.clone().unwrap_or_default();
        let schema = Arc::new(Schema::new(schema));
        let page_token = if metadata.page_token.is_empty() {
            None
        } else {
            Some(metadata.page_token.clone())
        };
        Self {
            job_service,
            job_ref,
            cached_rows,
            page_token,
            schema,
            metadata,
            max_results,
        }
    }

    /// Returns a row iterator for reading the query result set.
    ///
    /// Consumes the `CompleteQuery` handle and initializes a [`RowIterator`](crate::query::RowIterator) that
    /// iterates over initial cached rows in memory before automatically fetching subsequent pages from the API.
    ///
    /// # Example
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_bigquery::client::BigQuery;
    ///
    /// let client = BigQuery::builder().build().await?;
    /// let mut rows = client
    ///     .query("SELECT 100 AS score")
    ///     .with_project_id("my-project-id")
    ///     .run()
    ///     .await?
    ///     .until_done()
    ///     .await?
    ///     .read();
    ///
    /// while let Some(row) = rows.next().await.transpose()? {
    ///     let score: i64 = row.get("score");
    ///     println!("Score: {score}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn read(self) -> RowIterator {
        RowIterator::new(self)
    }

    /// Returns a reference to the cached summary metadata for this query.
    ///
    /// The returned [`QueryMetadata`](crate::model::QueryMetadata) contains useful summary statistics such as
    /// total rows, schema details, cache hit indicators, and estimated bytes processed without making additional RPC calls.
    ///
    /// # Example
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_bigquery::client::BigQuery;
    ///
    /// let client = BigQuery::builder().build().await?;
    /// let completed = client
    ///     .query("SELECT 'metadata_check'")
    ///     .with_project_id("my-project-id")
    ///     .run()
    ///     .await?
    ///     .until_done()
    ///     .await?;
    ///
    /// let meta = completed.metadata();
    /// println!("Total rows: {:?}", meta.total_rows);
    /// # Ok(())
    /// # }
    /// ```
    pub fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }

    /// Fetches full [Job] execution metadata from the service for this query.
    ///
    /// Unlike [`metadata()`](CompleteQuery::metadata), this method executes an RPC request (`jobs.get`) to
    /// retrieve complete job details, including user email, execution timelines, billing tier estimates, and detailed error summaries.
    ///
    /// > [!IMPORTANT]
    /// > Calling this method on a stateless query will return [`QueryError::StatelessQuery`](crate::error::QueryError::StatelessQuery).
    ///
    /// [Job]: https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job
    ///
    /// # Example
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_bigquery::client::BigQuery;
    /// use google_cloud_bigquery::model::query_request::JobCreationMode;
    ///
    /// let client = BigQuery::builder().build().await?;
    /// let completed = client
    ///     .query("SELECT 1")
    ///     .with_project_id("my-project-id")
    ///     .set_job_creation_mode(JobCreationMode::JobCreationRequired) // Forces a job to be created
    ///     .run()
    ///     .await?
    ///     .until_done()
    ///     .await?;
    ///
    /// let job_info = completed.job_metadata().await?;
    /// println!("Executed by user: {}", job_info.user_email);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn job_metadata(&self) -> Result<Job> {
        let job_ref = self.job_ref.as_ref().ok_or(QueryError::StatelessQuery)?;

        let req = self
            .job_service
            .get_job()
            .set_job_id(job_ref.job_id.clone())
            .set_project_id(job_ref.project_id.clone());

        let req = job_ref
            .location
            .clone()
            .into_iter()
            .fold(req, |req, location| req.set_location(location));

        let job = req.send().await?;
        Ok(job)
    }
}

/// Helper function to poll getQueryResults until a job finishes.
pub(crate) async fn poll_query_results(
    job_service: &JobService,
    job_ref: &JobReference,
    backoff_policy: Arc<dyn PollingBackoffPolicy>,
) -> Result<GetQueryResultsResponse> {
    let mut state = PollingState::default();

    loop {
        let mut req = GetQueryResultsRequest::new()
            .set_max_results(0u32)
            .set_project_id(job_ref.project_id.clone())
            .set_job_id(job_ref.job_id.clone());
        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let res = job_service
            .get_query_results()
            .with_request(req)
            .send()
            .await?;

        if !res.errors.is_empty() {
            // TODO(#5592): handle jobBackendError and other transient/retryable errors.
            return Err(QueryError::JobFailed { errors: res.errors });
        }

        let completed = res.job_complete.unwrap_or(false);
        if completed {
            return Ok(res);
        }

        let delay = backoff_policy.wait_period(&state);
        tokio::time::sleep(delay).await;
        // TODO(#5592): limit retry attempts or add cancellation mechanism
        state.attempt_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::tests::{MockJobService, create_job_service, create_test_backoff_policy};
    use google_cloud_bigquery_v2::model::{
        ErrorProto, GetQueryResultsResponse, Job, JobReference, QueryResponse, TableFieldSchema,
        TableSchema,
    };
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;
    use std::time::Duration;
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    #[test_case(Some("query_123"), None, QueryReference::Stateless{ query_id: "query_123".to_string()}; "with query id")]
    #[test_case(Some(""), Some(JobReference::new()), QueryReference::Job(JobReference::new()); "empty query id")]
    #[test_case(None, Some(JobReference::new()), QueryReference::Job(JobReference::new()); "with job refearence")]
    #[test_case(Some("query_123"), Some(JobReference::new()), QueryReference::Job(JobReference::new()); "with both job reference and query id")]
    fn test_query_query_reference(
        query_id: Option<&str>,
        job_ref: Option<JobReference>,
        expected: QueryReference,
    ) {
        let job_service = create_job_service(MockJobService::new());
        let initial_response = query_id.map(|id| QueryResponse::new().set_query_id(id));

        let query = Query {
            job_service,
            job_ref,
            completed: false,
            initial_job: None,
            initial_response,
            max_results: None,
        };

        let result = query.query_reference();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_query_until_done_already_completed() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");
        let query_res = QueryResponse::new()
            .set_job_complete(true)
            .set_job_reference(job_ref.clone())
            .set_schema(TableSchema::new())
            .set_page_token("some_page_token")
            .set_rows([wkt::Struct::new()])
            .set_cache_hit(true);

        let query = Query {
            job_service,
            job_ref: Some(job_ref),
            completed: true,
            initial_job: None,
            initial_response: Some(query_res),
            max_results: None,
        };

        let completed = query.until_done().await?;
        assert_eq!(completed.job_ref.as_ref().unwrap().job_id, "some_job_id");
        assert_eq!(completed.page_token, Some("some_page_token".to_string()));
        assert_eq!(completed.cached_rows.len(), 1);

        let metadata = completed.metadata();
        assert_eq!(metadata.cache_hit, Some(true));
        assert_eq!(metadata.job_complete, Some(true));
        assert_eq!(metadata.page_token, "some_page_token".to_string());

        Ok(())
    }

    #[tokio::test]
    async fn test_query_until_done_preserves_max_results() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");
        let query_res = QueryResponse::new()
            .set_job_complete(true)
            .set_job_reference(job_ref.clone())
            .set_schema(TableSchema::new());

        let query = Query {
            job_service,
            job_ref: Some(job_ref),
            completed: true,
            initial_job: None,
            initial_response: Some(query_res),
            max_results: Some(42),
        };

        let completed = query.until_done().await?;
        assert_eq!(completed.max_results, Some(42));

        Ok(())
    }

    #[tokio::test]
    async fn test_query_until_done_polls_success() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results()
            .returning(|req, _| {
                assert_eq!(req.project_id, "some_project");
                assert_eq!(req.job_id, "some_job_id");
                assert_eq!(req.max_results, Some(0));
                assert_eq!(req.location, "us-central1");
                let res = GetQueryResultsResponse::new()
                    .set_job_complete(true)
                    .set_job_reference(JobReference::new().set_job_id(req.job_id))
                    .set_schema(TableSchema::new())
                    .set_page_token("")
                    .set_rows(vec![wkt::Struct::new(), wkt::Struct::new()])
                    .set_cache_hit(false);
                Ok(Response::from(res))
            })
            .times(1);
        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id")
            .set_location("us-central1");

        let query = Query {
            job_service,
            job_ref: Some(job_ref),
            completed: false,
            initial_job: None,
            initial_response: None,
            max_results: None,
        };

        let completed = query.until_done().await?;
        assert_eq!(completed.job_ref.as_ref().unwrap().job_id, "some_job_id");
        assert_eq!(completed.page_token, None);
        assert_eq!(completed.cached_rows.len(), 2);

        let metadata = completed.metadata();
        assert_eq!(metadata.cache_hit, Some(false));
        assert_eq!(metadata.job_complete, Some(true));
        assert_eq!(metadata.page_token, "".to_string());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_poll_query_results_loops_until_complete() -> TestResult {
        let mut mock = MockJobService::new();
        let mut backoff_policy = create_test_backoff_policy();
        backoff_policy
            .expect_wait_period()
            .times(2)
            .return_const(Duration::from_millis(1));

        let mut seq = mockall::Sequence::new();

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(2)
            .returning(|_, _| {
                Ok(Response::from(
                    GetQueryResultsResponse::new().set_job_complete(false),
                ))
            });

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|_, _| {
                Ok(Response::from(
                    GetQueryResultsResponse::new().set_job_complete(true),
                ))
            });

        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");

        let res = poll_query_results(&job_service, &job_ref, Arc::new(backoff_policy)).await?;

        assert!(res.job_complete.unwrap(), "{res:?}");

        Ok(())
    }

    #[tokio::test]
    async fn test_query_until_done_job_failed_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results().returning(|req, _| {
            assert_eq!(req.project_id, "some_project");
            assert_eq!(req.job_id, "some_job_id");
            assert_eq!(req.max_results, Some(0));
            let err_proto = ErrorProto::new()
                .set_reason("invalidQuery")
                .set_message("Syntax error");
            let res = GetQueryResultsResponse::new().set_errors(vec![err_proto]);
            Ok(Response::from(res))
        });
        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");

        let query = Query {
            job_service,
            job_ref: Some(job_ref),
            completed: false,
            initial_job: None,
            initial_response: None,
            max_results: None,
        };

        let err = query.until_done().await.unwrap_err();
        let errors = match err {
            QueryError::JobFailed { errors } => errors,
            _ => panic!("expected QueryError::JobFailed, got {err:?}"),
        };
        assert_eq!(
            errors,
            [ErrorProto::new()
                .set_reason("invalidQuery")
                .set_message("Syntax error")]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_query_until_done_rpc_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results().returning(|req, _| {
            assert_eq!(req.project_id, "some_project");
            assert_eq!(req.job_id, "some_job_id");
            assert_eq!(req.max_results, Some(0));
            let status = Status::default()
                .set_code(Code::InvalidArgument)
                .set_message("simulated bad request");
            Err(GaxError::service(status))
        });
        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");

        let query = Query {
            job_service,
            job_ref: Some(job_ref),
            completed: false,
            initial_job: None,
            initial_response: None,
            max_results: None,
        };

        let err = query.until_done().await.unwrap_err();
        let source = match err {
            QueryError::Rpc { source } => source,
            _ => panic!("expected QueryError::Rpc, got {err:?}"),
        };
        assert_eq!(source.status().unwrap().code, Code::InvalidArgument);

        Ok(())
    }

    #[tokio::test]
    async fn test_complete_query_read() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");
        let schema = TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("name")
            .set_type("STRING")
            .set_mode("NULLABLE")]);
        let row = serde_json::Map::from_iter([(
            "f".to_string(),
            serde_json::json!([{ "v": "test_name" }]),
        )]);
        let query_res = QueryResponse::new()
            .set_job_complete(true)
            .set_job_reference(job_ref.clone())
            .set_schema(schema)
            .set_rows(vec![row]);

        let complete_query =
            CompleteQuery::from_query_response(job_service, Some(job_ref), query_res, None);

        let mut iter = complete_query.read();
        let row = iter.next().await.expect("should return first row")?;
        assert_eq!(row.get::<String, _>("name"), "test_name");
        assert!(iter.next().await.is_none(), "{iter:?}");

        Ok(())
    }

    #[tokio::test]
    async fn test_complete_query_job_metadata_success() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_job().returning(|req, _| {
            assert_eq!(req.project_id, "some_project");
            assert_eq!(req.job_id, "some_job_id");
            assert_eq!(req.location, "us-central1");
            let res = Job::new()
                .set_job_reference(JobReference::new().set_job_id(req.job_id))
                .set_user_email("test@example.com");
            Ok(Response::from(res))
        });
        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id")
            .set_location("us-central1");
        let query_res = QueryResponse::new().set_schema(TableSchema::new());

        let complete_query =
            CompleteQuery::from_query_response(job_service, Some(job_ref), query_res, None);
        let job = complete_query.job_metadata().await?;
        assert_eq!(job.user_email, "test@example.com");
        Ok(())
    }

    #[tokio::test]
    async fn test_complete_query_job_metadata_stateless() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let query_res = QueryResponse::new().set_schema(TableSchema::new());
        let complete_query = CompleteQuery::from_query_response(job_service, None, query_res, None);
        let err = complete_query.job_metadata().await.unwrap_err();
        assert!(matches!(err, QueryError::StatelessQuery));
        Ok(())
    }

    #[tokio::test]
    async fn test_complete_query_job_metadata_rpc_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_job().returning(|req, _| {
            assert_eq!(req.project_id, "some_project");
            assert_eq!(req.job_id, "some_job_id");
            let status = Status::default()
                .set_code(Code::NotFound)
                .set_message("job not found");
            Err(GaxError::service(status))
        });
        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");
        let query_res = QueryResponse::new().set_schema(TableSchema::new());

        let complete_query =
            CompleteQuery::from_query_response(job_service, Some(job_ref), query_res, None);
        let err = complete_query.job_metadata().await.unwrap_err();
        let source = match err {
            QueryError::Rpc { source } => source,
            _ => panic!("expected QueryError::Rpc, got {err:?}"),
        };
        assert_eq!(source.status().unwrap().code, Code::NotFound);
        Ok(())
    }
}
