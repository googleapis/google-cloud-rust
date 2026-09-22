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
use crate::generated::{CompleteQueryMetadata, QueryMetadata};
use crate::query::execution::RetryContext;
use crate::query::retry_policy::JobRetryResult;
use crate::query::{Result, RowIterator};
use google_cloud_bigquery_v2::builder::job_service::GetJob;
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
/// [`Query::send()`](crate::builder::bigquery::Query::send) returns a [`Query`].
///
/// To obtain the final result set, call [`until_done()`](Query::until_done),
/// which waits for the query execution to complete and returns a
/// [`CompleteQuery`].
///
/// # Example
///
/// ```
/// # use google_cloud_bigquery::client::BigQuery;
/// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
/// let query_handle = client
///     .query("SELECT 42 AS answer")
///     .send()
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
    pub(crate) completed: bool,
    pub(crate) metadata: QueryMetadata,
    pub(crate) cached_rows: Option<VecDeque<wkt::Struct>>,
    pub(crate) page_size: Option<u32>,
    pub(crate) retry_context: Option<RetryContext>,
}

impl Query {
    pub(crate) fn from_job(
        job_service: Arc<JobService>,
        initial_job: Job,
        retry_context: Option<&RetryContext>,
        page_size: Option<u32>,
    ) -> Self {
        let completed = initial_job
            .status
            .as_ref()
            .map(|s| s.state == "DONE")
            .unwrap_or(false);
        Self {
            job_service,
            completed,
            cached_rows: None,
            metadata: build_query_metadata_from_job(initial_job),
            retry_context: retry_context.filter(|_| !completed).cloned(),
            page_size,
        }
    }

    pub(crate) fn from_query_response(
        job_service: Arc<JobService>,
        mut query_response: QueryResponse,
        retry_context: Option<&RetryContext>,
        page_size: Option<u32>,
    ) -> Self {
        let completed = query_response.job_complete.unwrap_or(false);
        let cached_rows = VecDeque::from(std::mem::take(&mut query_response.rows));
        let metadata = QueryMetadata::from(query_response);
        Self {
            job_service,
            completed,
            cached_rows: Some(cached_rows),
            metadata,
            retry_context: retry_context.filter(|_| !completed).cloned(),
            page_size,
        }
    }

    /// Returns the initial metadata from query execution.
    ///
    /// Depending on how the query was executed, the metadata contains:
    /// - [`job_reference`][QueryMetadata::job_reference]: The reference to the BigQuery job, if one was created.
    /// - [`query_id`][QueryMetadata::query_id]: The unique ID of the query if executed without creating a job.
    /// - [`job_creation_reason`][QueryMetadata::job_creation_reason]: The reason why a job was created (controlled via `set_job_creation_mode`).
    /// - [`job_complete`][QueryMetadata::job_complete]: Whether the query completed immediately without requiring polling.
    ///
    /// To wait for the query to finish and retrieve full results and final
    /// metadata, call [`until_done`][Self::until_done].
    pub fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }

    /// Build a request to fetch full [Job] execution metadata from the service for this query.
    ///
    /// > Returns `None` if the query was executed without creating a job
    /// > (for example, when using [`JobCreationMode::JobCreationOptional`],
    /// > or when the query was a dry run).
    ///
    /// [Job]: https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job
    /// [`JobCreationMode::JobCreationOptional`]: google_cloud_bigquery_v2::model::query_request::JobCreationMode::JobCreationOptional
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # use google_cloud_bigquery_v2::model::query_request::JobCreationMode;
    /// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
    /// let query = client
    ///     .query("SELECT 1")
    ///     .set_job_creation_mode(JobCreationMode::JobCreationRequired) // Forces a job to be created
    ///     .send()
    ///     .await?;
    ///
    /// match query.get_job() {
    ///     Some(req) => {
    ///         let job_info = req.send().await?;
    ///         println!("Executed by user: {}", job_info.user_email);
    ///     }
    ///     None => {
    ///         println!(
    ///             "Query was run without creating a job. Query ID: {}",
    ///             query.metadata().query_id
    ///         );
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_job(&self) -> Option<GetJob> {
        build_get_job(&self.job_service, self.metadata.job_reference.as_ref()?)
    }

    pub(crate) fn is_dry_run(&self) -> bool {
        self.metadata
            .configuration
            .as_ref()
            .and_then(|c| c.dry_run)
            .unwrap_or(false)
    }

    /// Waits for query execution to complete.
    ///
    /// If the query completed immediately, this method returns a
    /// [`CompleteQuery`] without making additional
    /// network calls. Otherwise, it polls the service until the query finishes.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::DryRun`] if the query was configured as a dry run.
    ///
    /// Returns an error if a remote service or network failure happens during
    /// polling, or if the BigQuery job fails due to runtime execution errors.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
    /// let query_handle = client
    ///     .query("SELECT 1 + 1 AS result")
    ///     .send()
    ///     .await?;
    ///
    /// let complete = query_handle.until_done().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn until_done(mut self) -> Result<CompleteQuery> {
        if self.is_dry_run() {
            return Err(QueryError::DryRun);
        }

        loop {
            let Query {
                job_service,
                completed,
                metadata,
                cached_rows,
                page_size,
                retry_context,
            } = self;

            if completed && let Some(cached_rows) = cached_rows {
                return Ok(CompleteQuery::from_query_metadata(
                    job_service,
                    metadata,
                    cached_rows,
                    page_size,
                ));
            }

            let job_ref = metadata
                .job_reference
                .clone()
                .expect("query job should have job reference at this point");

            let backoff_policy = Arc::new(
                ExponentialBackoffBuilder::default()
                    .with_initial_delay(std::time::Duration::from_secs(1))
                    .build()
                    .expect("valid backoff configuration"),
            );

            match poll_query_results(&job_service, &job_ref, backoff_policy).await {
                Ok(res) => {
                    return Ok(CompleteQuery::from_get_query_results_response(
                        job_service,
                        &job_ref,
                        res,
                        metadata,
                        page_size,
                    ));
                }
                Err(err) => {
                    let Some(retry_ctx) = retry_context else {
                        return Err(err);
                    };
                    match retry_ctx.on_error(err) {
                        JobRetryResult::Continue(delay, _) => {
                            self = retry_ctx.reissue(delay).await?;
                        }
                        JobRetryResult::Permanent(e) | JobRetryResult::Exhausted(e) => {
                            return Err(e);
                        }
                    }
                }
            }
        }
    }
}

/// A handle representing a successfully completed query ready for reading
/// results.
///
/// [`Query::until_done()`] returns a [`CompleteQuery`].
///
/// This handle provides access to cached execution metadata, schema
/// definitions, and a row iterator via [`read()`](CompleteQuery::read).
///
/// # Example
///
/// ```
/// # use google_cloud_bigquery::client::BigQuery;
/// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
/// let complete = client
///     .query("SELECT 'done' AS status")
///     .until_done()
///     .await?;
///
/// println!("Cache hit: {:?}", complete.metadata().cache_hit);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct CompleteQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) page_token: Option<String>,
    pub(crate) metadata: CompleteQueryMetadata,
    pub(crate) page_size: Option<u32>,
}

impl CompleteQuery {
    pub(crate) fn from_get_query_results_response(
        job_service: Arc<JobService>,
        job_ref: &JobReference,
        mut res: GetQueryResultsResponse,
        initial_metadata: QueryMetadata,
        page_size: Option<u32>,
    ) -> Self {
        let cached_rows = VecDeque::from(std::mem::take(&mut res.rows));
        let metadata =
            build_complete_query_metadata_from_get_query_results(initial_metadata, res, job_ref);
        Self::from_complete_metadata(
            job_service,
            Some(job_ref.clone()),
            metadata,
            cached_rows,
            page_size,
        )
    }

    pub(crate) fn from_query_metadata(
        job_service: Arc<JobService>,
        metadata: QueryMetadata,
        cached_rows: VecDeque<wkt::Struct>,
        page_size: Option<u32>,
    ) -> Self {
        let job_ref = metadata.job_reference.clone();
        let metadata = CompleteQueryMetadata::from(metadata);
        Self::from_complete_metadata(job_service, job_ref, metadata, cached_rows, page_size)
    }

    pub(crate) fn from_complete_metadata(
        job_service: Arc<JobService>,
        job_ref: Option<JobReference>,
        metadata: CompleteQueryMetadata,
        cached_rows: VecDeque<wkt::Struct>,
        page_size: Option<u32>,
    ) -> Self {
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
            metadata,
            page_size,
        }
    }

    /// Read the result set of the query.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
    /// let mut rows = client
    ///     .query("SELECT 100 AS score")
    ///     .until_done()
    ///     .await?
    ///     .read();
    ///
    /// while let Some(row) = rows.next().await.transpose()? {
    ///     let score: i64 = row.get("score")?;
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
    /// The returned [`CompleteQueryMetadata`] contains
    /// summary statistics such as total rows, schema details, cache hit
    /// indicators, and estimated bytes processed without making additional
    /// RPCs.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
    /// let completed = client
    ///     .query("SELECT 'metadata_check'")
    ///     .until_done()
    ///     .await?;
    ///
    /// let meta = completed.metadata();
    /// println!("Total rows: {:?}", meta.total_rows);
    /// # Ok(())
    /// # }
    /// ```
    pub fn metadata(&self) -> &CompleteQueryMetadata {
        &self.metadata
    }

    /// Build a request to fetch full [Job] execution metadata from the service for this query.
    ///
    /// > Returns `None` if the query was executed without creating a job
    /// > (for example, when using [`JobCreationMode::JobCreationOptional`],
    /// > or when the query was a dry run).
    ///
    /// [Job]: https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job
    /// [`JobCreationMode::JobCreationOptional`]: google_cloud_bigquery_v2::model::query_request::JobCreationMode::JobCreationOptional
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # use google_cloud_bigquery_v2::model::query_request::JobCreationMode;
    /// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
    /// let completed = client
    ///     .query("SELECT 1")
    ///     .set_job_creation_mode(JobCreationMode::JobCreationRequired) // Forces a job to be created
    ///     .until_done()
    ///     .await?;
    ///
    /// match completed.get_job() {
    ///     Some(req) => {
    ///         let job_info = req.send().await?;
    ///         println!("Executed by user: {}", job_info.user_email);
    ///     }
    ///     None => {
    ///         println!(
    ///             "Query was run without creating a job. Query ID: {}",
    ///             completed.metadata().query_id
    ///         );
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_job(&self) -> Option<GetJob> {
        build_get_job(&self.job_service, self.job_ref.as_ref()?)
    }
}

/// Builds a `jobs.get` request from a job reference, or `None` if the
/// reference cannot identify a job. Dry-run queries return a job reference
/// without a job ID.
pub(crate) fn build_get_job(job_service: &JobService, job_ref: &JobReference) -> Option<GetJob> {
    if job_ref.job_id.is_empty() {
        return None;
    }

    let req = job_service
        .get_job()
        .set_job_id(job_ref.job_id.clone())
        .set_project_id(job_ref.project_id.clone());

    let req = job_ref
        .location
        .clone()
        .into_iter()
        .fold(req, |req, location| req.set_location(location));

    Some(req)
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

// Helper function to build QueryMetadata from a Job.
//
// The generated code handle fields with same name on the root level, but some data
// that is returned on jobs.query response are under JobStats for a Query Job when using
// jobs.insert.
fn build_query_metadata_from_job(resp: Job) -> QueryMetadata {
    let mut metadata = QueryMetadata::from(resp);

    let query_stats = metadata.statistics.as_ref().and_then(|s| s.query.as_ref());

    metadata.job_complete = metadata.status.as_ref().map(|s| s.state == "DONE");
    metadata.errors = metadata
        .status
        .as_ref()
        .map(|s| s.errors.clone())
        .unwrap_or_default();
    metadata.schema = query_stats.and_then(|q| q.schema.clone());
    metadata.total_bytes_processed =
        query_stats
            .and_then(|q| q.total_bytes_processed)
            .or_else(|| {
                metadata
                    .statistics
                    .as_ref()
                    .and_then(|s| s.total_bytes_processed)
            });
    metadata.total_bytes_billed = query_stats.and_then(|q| q.total_bytes_billed);
    metadata.total_slot_ms = query_stats
        .and_then(|q| q.total_slot_ms)
        .or_else(|| metadata.statistics.as_ref().and_then(|s| s.total_slot_ms));
    metadata.cache_hit = query_stats.and_then(|q| q.cache_hit);
    metadata.num_dml_affected_rows = query_stats.and_then(|q| q.num_dml_affected_rows);
    metadata.dml_stats = query_stats.and_then(|q| q.dml_stats.clone());
    metadata.statement_type = query_stats
        .map(|q| q.statement_type.clone())
        .unwrap_or_default();
    metadata.session_info = metadata
        .statistics
        .as_ref()
        .and_then(|s| s.session_info.clone());

    metadata.creation_time = metadata
        .statistics
        .as_ref()
        .and_then(|s| (s.creation_time > 0).then_some(s.creation_time));
    metadata.start_time = metadata
        .statistics
        .as_ref()
        .and_then(|s| (s.start_time > 0).then_some(s.start_time));
    metadata.end_time = metadata
        .statistics
        .as_ref()
        .and_then(|s| (s.end_time > 0).then_some(s.end_time));

    if metadata.location.is_empty() {
        metadata.location = metadata
            .job_reference
            .as_ref()
            .and_then(|r| r.location.clone())
            .unwrap_or_default();
    }

    metadata
}

// Helper function to build CompleteQueryMetadata from GetQueryResultsResponse while
// preserving metadata from the initial query execution.
fn build_complete_query_metadata_from_get_query_results(
    initial_metadata: QueryMetadata,
    res: GetQueryResultsResponse,
    job_ref: &JobReference,
) -> CompleteQueryMetadata {
    let mut metadata = CompleteQueryMetadata::from(initial_metadata);
    if res.schema.is_some() {
        metadata.schema = res.schema;
    }
    if res.total_rows.is_some() {
        metadata.total_rows = res.total_rows;
    }
    if !res.page_token.is_empty() {
        metadata.page_token = res.page_token;
    }
    if res.total_bytes_processed.is_some() {
        metadata.total_bytes_processed = res.total_bytes_processed;
    }
    if res.job_complete.is_some() {
        metadata.job_complete = res.job_complete;
    }
    if !res.errors.is_empty() {
        metadata.errors = res.errors;
    }
    if res.cache_hit.is_some() {
        metadata.cache_hit = res.cache_hit;
    }
    if res.num_dml_affected_rows.is_some() {
        metadata.num_dml_affected_rows = res.num_dml_affected_rows;
    }
    if !res.etag.is_empty() {
        metadata.etag = res.etag;
    }
    if res.job_reference.is_some() {
        metadata.job_reference = res.job_reference;
    }
    if metadata.location.is_empty()
        && let Some(loc) = job_ref.location.clone()
    {
        metadata.location = loc;
    }

    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::builder::{QUERY_REQUEST_ID_PREFIX, Query as QueryBuilder};
    use crate::query::retry_policy::RetryableJobErrors;
    use crate::query::tests::{MockJobService, create_job_service, create_test_backoff_policy};
    use google_cloud_bigquery_v2::model::{
        ErrorProto, GetQueryResultsResponse, Job, JobConfiguration, JobReference, QueryResponse,
        TableFieldSchema, TableSchema,
    };
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;
    use std::time::Duration;

    type TestResult = anyhow::Result<()>;

    impl CompleteQuery {
        pub(crate) fn from_query_response(
            job_service: Arc<JobService>,
            mut query_res: QueryResponse,
            page_size: Option<u32>,
        ) -> Self {
            let cached_rows = std::mem::take(&mut query_res.rows).into();
            let metadata = QueryMetadata::from(query_res);
            Self::from_query_metadata(job_service, metadata, cached_rows, page_size)
        }
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

        let query = Query::from_query_response(job_service, query_res, None, None);

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
    async fn test_query_until_done_preserves_page_size() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");
        let query_res = QueryResponse::new()
            .set_job_complete(true)
            .set_job_reference(job_ref.clone())
            .set_schema(TableSchema::new());

        let query = Query::from_query_response(job_service, query_res, None, Some(42));

        let completed = query.until_done().await?;
        assert_eq!(completed.page_size, Some(42));

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
                    .set_page_token("some_page_token")
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
        let query_res = QueryResponse::new()
            .set_job_complete(false)
            .set_job_reference(job_ref);

        let query = Query::from_query_response(job_service, query_res, None, None);

        let completed = query.until_done().await?;
        assert_eq!(completed.job_ref.as_ref().unwrap().job_id, "some_job_id");
        assert_eq!(completed.page_token, Some("some_page_token".to_string()));
        assert_eq!(completed.cached_rows.len(), 2);

        let metadata = completed.metadata();
        assert_eq!(metadata.cache_hit, Some(false));
        assert_eq!(metadata.job_complete, Some(true));
        assert_eq!(metadata.page_token, "some_page_token");

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
        let query_res = QueryResponse::new()
            .set_job_complete(false)
            .set_job_reference(job_ref);

        let query = Query::from_query_response(job_service, query_res, None, None);

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
        let query_res = QueryResponse::new()
            .set_job_complete(false)
            .set_job_reference(job_ref);

        let query = Query::from_query_response(job_service, query_res, None, None);

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
            .set_job_reference(job_ref)
            .set_schema(schema)
            .set_rows(vec![row]);

        let complete_query = CompleteQuery::from_query_response(job_service, query_res, None);

        let mut iter = complete_query.read();
        let row = iter.next().await.expect("should return first row")?;
        assert_eq!(row.get::<String, _>("name")?, "test_name");
        assert!(iter.next().await.is_none(), "{iter:?}");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_query_until_done_reissue_on_retryable_job_failed() -> TestResult {
        let mut mock = MockJobService::new();
        let mut seq = mockall::Sequence::new();

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.job_id, "initial_job_id");
                let err_proto = ErrorProto::new()
                    .set_reason("backendError")
                    .set_message("temporary server issue");
                let res = GetQueryResultsResponse::new().set_errors(vec![err_proto]);
                Ok(Response::from(res))
            });

        mock.expect_query()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                let req_id = &req.query_request.as_ref().unwrap().request_id;
                assert!(req_id.starts_with(QUERY_REQUEST_ID_PREFIX));
                assert!(uuid::Uuid::parse_str(&req_id[QUERY_REQUEST_ID_PREFIX.len()..]).is_ok());
                let new_job_ref = JobReference::new()
                    .set_project_id("some_project")
                    .set_job_id("reissued_job_id");
                Ok(Response::from(
                    QueryResponse::new()
                        .set_job_complete(false)
                        .set_job_reference(new_job_ref)
                        .set_schema(TableSchema::new()),
                ))
            });

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.job_id, "reissued_job_id");
                let res = GetQueryResultsResponse::new()
                    .set_job_complete(true)
                    .set_schema(TableSchema::new());
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("initial_job_id");
        let query_res = QueryResponse::new()
            .set_job_complete(false)
            .set_job_reference(job_ref);

        let query_builder = QueryBuilder::new(job_service.clone(), "SELECT 1".to_string())
            .with_project_id("some_project");
        let mut retry_context = RetryContext::new(query_builder);
        retry_context.state.attempt_count = 1;

        let query = Query::from_query_response(job_service, query_res, Some(&retry_context), None);

        let completed = query.until_done().await?;
        assert_eq!(
            completed.job_ref.as_ref().unwrap().job_id,
            "reissued_job_id"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_query_until_done_reissue_retry_exhausted() -> TestResult {
        let mut mock = MockJobService::new();
        let mut seq = mockall::Sequence::new();

        // First poll on initial_job_id fails with retryable error (attempt 1)
        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.job_id, "initial_job_id");
                let err_proto = ErrorProto::new()
                    .set_reason("backendError")
                    .set_message("first temporary server issue");
                let res = GetQueryResultsResponse::new().set_errors(vec![err_proto]);
                Ok(Response::from(res))
            });

        // Reissue succeeds and returns reissued_job_id (attempt 2)
        mock.expect_query()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                let req_id = &req.query_request.as_ref().unwrap().request_id;
                assert!(req_id.starts_with(QUERY_REQUEST_ID_PREFIX));
                assert!(uuid::Uuid::parse_str(&req_id[QUERY_REQUEST_ID_PREFIX.len()..]).is_ok());
                let new_job_ref = JobReference::new()
                    .set_project_id("some_project")
                    .set_job_id("reissued_job_id");
                Ok(Response::from(
                    QueryResponse::new()
                        .set_job_complete(false)
                        .set_job_reference(new_job_ref)
                        .set_schema(TableSchema::new()),
                ))
            });

        // Second poll on reissued_job_id fails with retryable error, but attempt limit (2) is exhausted!
        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.job_id, "reissued_job_id");
                let err_proto = ErrorProto::new()
                    .set_reason("backendError")
                    .set_message("second temporary server issue");
                let res = GetQueryResultsResponse::new().set_errors(vec![err_proto]);
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("initial_job_id");

        let query_res = QueryResponse::new()
            .set_job_complete(false)
            .set_job_reference(job_ref)
            .set_schema(TableSchema::new());
        let mut query_builder = QueryBuilder::new(job_service.clone(), "SELECT 1".to_string())
            .with_project_id("some_project");
        query_builder.job_retry_policy =
            Arc::new(RetryableJobErrors::default().with_attempt_limit(2));
        let mut retry_context = RetryContext::new(query_builder);
        retry_context.state.attempt_count = 1;

        let query = Query::from_query_response(job_service, query_res, Some(&retry_context), None);

        let err = query.until_done().await.unwrap_err();
        let errors = match err {
            QueryError::JobFailed { errors } => errors,
            _ => panic!("expected QueryError::JobFailed, got {err:?}"),
        };
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].reason, "backendError");
        assert_eq!(errors[0].message, "second temporary server issue");

        Ok(())
    }

    #[tokio::test]
    async fn test_query_get_job_success() -> TestResult {
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
        let query_res = QueryResponse::new()
            .set_schema(TableSchema::new())
            .set_job_reference(job_ref);

        let query = Query::from_query_response(job_service.clone(), query_res.clone(), None, None);
        let job = query.get_job().unwrap().send().await?;
        assert_eq!(job.user_email, "test@example.com");

        let complete_query = CompleteQuery::from_query_response(job_service, query_res, None);
        let job = complete_query.get_job().unwrap().send().await?;
        assert_eq!(job.user_email, "test@example.com");
        Ok(())
    }

    #[tokio::test]
    async fn test_query_get_job_empty_job_id() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        // Dry-runs return a reference that has a location and project id, but no job id.
        let job_ref = JobReference::new()
            .set_location("us-central1")
            .set_project_id("some-project");
        let query_res = QueryResponse::new()
            .set_schema(TableSchema::new())
            .set_job_reference(job_ref);

        let query = Query::from_query_response(job_service.clone(), query_res.clone(), None, None);
        assert!(query.get_job().is_none(), "{query:?}");

        let complete_query = CompleteQuery::from_query_response(job_service, query_res, None);
        assert!(complete_query.get_job().is_none(), "{complete_query:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_query_get_job_rpc_error() -> TestResult {
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
        let query_res = QueryResponse::new()
            .set_schema(TableSchema::new())
            .set_job_reference(job_ref);

        let query = Query::from_query_response(job_service.clone(), query_res.clone(), None, None);
        let req = query.get_job().unwrap();
        let err = req.send().await.unwrap_err();
        assert_eq!(err.status().unwrap().code, Code::NotFound);

        let complete_query = CompleteQuery::from_query_response(job_service, query_res, None);
        let req = complete_query.get_job().unwrap();
        let err = req.send().await.unwrap_err();
        assert_eq!(err.status().unwrap().code, Code::NotFound);
        Ok(())
    }

    #[tokio::test]
    async fn test_query_until_done_dry_run_job_returns_error() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_location("US");
        let job = Job::new()
            .set_job_reference(job_ref)
            .set_configuration(JobConfiguration::new().set_dry_run(true));

        let query = Query::from_job(job_service, job, None, None);
        let err = query.until_done().await.unwrap_err();
        assert!(
            matches!(err, QueryError::DryRun),
            "expected DryRun error, got {err:?}"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_query_until_done_initial_poll_delay() -> TestResult {
        let mut mock = MockJobService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_get_query_results()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| {
                Ok(Response::from(
                    GetQueryResultsResponse::new().set_job_complete(false),
                ))
            });
        mock.expect_get_query_results()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| {
                Ok(Response::from(
                    GetQueryResultsResponse::new().set_job_complete(true),
                ))
            });

        let job_service = create_job_service(mock);
        let job_ref = JobReference::new()
            .set_project_id("some_project")
            .set_job_id("some_job_id");
        let query_res = QueryResponse::new()
            .set_job_complete(false)
            .set_job_reference(job_ref);

        let start = tokio::time::Instant::now();
        let query = Query::from_query_response(job_service, query_res, None, None);
        let _completed = query.until_done().await?;
        let elapsed = start.elapsed();

        assert!(
            elapsed <= Duration::from_secs(1),
            "expected initial poll delay <= 1s, got {elapsed:?}"
        );
        Ok(())
    }

    #[test]
    fn test_build_query_metadata_from_job() {
        use google_cloud_bigquery_v2::model::{
            JobConfigurationQuery, JobCreationReason, JobStatistics, JobStatistics2, JobStatus,
        };

        let job =
            Job::new()
                .set_id("rust-sdk-testing:US.job_123")
                .set_kind("bigquery#job")
                .set_etag("etag123")
                .set_self_link("https://www.googleapis.com/bigquery/v2/...")
                .set_user_email("user@example.com")
                .set_principal_subject("user:user@example.com")
                .set_job_creation_reason(JobCreationReason::new().set_code(
                    google_cloud_bigquery_v2::model::job_creation_reason::Code::Requested,
                ))
                .set_job_reference(
                    JobReference::new()
                        .set_project_id("rust-sdk-testing")
                        .set_job_id("job_123")
                        .set_location("US"),
                )
                .set_configuration(
                    JobConfiguration::new().set_query(
                        JobConfigurationQuery::new()
                            .set_query("SELECT 1 AS one")
                            .set_use_legacy_sql(false),
                    ),
                )
                .set_status(JobStatus::new().set_state("DONE"))
                .set_statistics(
                    JobStatistics::new()
                        .set_creation_time(1790020718574i64)
                        .set_start_time(1790020718592i64)
                        .set_end_time(1790020718847i64)
                        .set_total_bytes_processed(0i64)
                        .set_query(
                            JobStatistics2::new()
                                .set_total_bytes_processed(0i64)
                                .set_total_bytes_billed(0i64)
                                .set_cache_hit(true)
                                .set_statement_type("SELECT")
                                .set_schema(TableSchema::new().set_fields([
                                    TableFieldSchema::new().set_name("one").set_type("INTEGER"),
                                ])),
                        ),
                );

        let metadata = build_query_metadata_from_job(job);

        assert_eq!(metadata.id, "rust-sdk-testing:US.job_123");
        assert_eq!(metadata.job_complete, Some(true));
        assert_eq!(metadata.creation_time, Some(1790020718574));
        assert_eq!(metadata.start_time, Some(1790020718592));
        assert_eq!(metadata.end_time, Some(1790020718847));
        assert_eq!(metadata.cache_hit, Some(true));
        assert_eq!(metadata.statement_type, "SELECT");
        assert_eq!(metadata.location, "US");
        assert_eq!(metadata.total_bytes_processed, Some(0));
        assert_eq!(metadata.total_bytes_billed, Some(0));
        assert!(metadata.schema.is_some());
    }

    #[tokio::test]
    async fn test_query_until_done_preserves_metadata_fields_from_job() -> TestResult {
        use google_cloud_bigquery_v2::model::{
            JobConfigurationQuery, JobCreationReason, JobStatistics, JobStatistics2, JobStatus,
        };

        let mut mock = MockJobService::new();
        mock.expect_get_query_results()
            .returning(|req, _| {
                let res = GetQueryResultsResponse::new()
                    .set_job_complete(true)
                    .set_job_reference(JobReference::new().set_job_id(req.job_id))
                    .set_schema(TableSchema::new())
                    .set_rows(vec![wkt::Struct::new()])
                    .set_cache_hit(true);
                Ok(Response::from(res))
            })
            .times(1);

        let job_service = create_job_service(mock);
        let job =
            Job::new()
                .set_id("rust-sdk-testing:US.job_123")
                .set_kind("bigquery#job")
                .set_job_creation_reason(JobCreationReason::new().set_code(
                    google_cloud_bigquery_v2::model::job_creation_reason::Code::Requested,
                ))
                .set_job_reference(
                    JobReference::new()
                        .set_project_id("rust-sdk-testing")
                        .set_job_id("job_123")
                        .set_location("US"),
                )
                .set_configuration(
                    JobConfiguration::new().set_query(
                        JobConfigurationQuery::new()
                            .set_query("SELECT 1 AS one")
                            .set_use_legacy_sql(false),
                    ),
                )
                .set_status(JobStatus::new().set_state("DONE"))
                .set_statistics(
                    JobStatistics::new()
                        .set_creation_time(1790020718574i64)
                        .set_start_time(1790020718592i64)
                        .set_end_time(1790020718847i64)
                        .set_total_bytes_processed(0i64)
                        .set_query(
                            JobStatistics2::new()
                                .set_total_bytes_processed(0i64)
                                .set_total_bytes_billed(0i64)
                                .set_cache_hit(true)
                                .set_statement_type("SELECT"),
                        ),
                );

        let query = Query::from_job(job_service, job, None, None);
        let complete = query.until_done().await?;
        let meta = complete.metadata();

        assert_eq!(meta.creation_time, Some(1790020718574));
        assert_eq!(meta.start_time, Some(1790020718592));
        assert_eq!(meta.end_time, Some(1790020718847));
        assert_eq!(meta.statement_type, "SELECT");
        assert_eq!(meta.location, "US");
        assert_eq!(meta.cache_hit, Some(true));

        Ok(())
    }
}
