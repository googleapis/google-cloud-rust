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
use crate::generated::QueryCreationMetadata;
use crate::model::QueryMetadata;
use crate::query::{Result, RowIterator, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    GetQueryResultsRequest, GetQueryResultsResponse, Job, JobReference, QueryResponse,
};
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_state::PollingState;
use std::collections::VecDeque;
use std::sync::Arc;

/// A handle representing a running query.
#[derive(Clone)]
pub struct Query {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) completed: bool,
    pub(crate) metadata: QueryCreationMetadata,
    pub(crate) cached_rows: Option<VecDeque<wkt::Struct>>,
    pub(crate) max_results: Option<u32>,
}

impl std::fmt::Debug for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Query")
            .field("completed", &self.completed)
            .field("job_reference", &self.metadata.job_reference)
            .field("query_id", &self.metadata.query_id)
            .field("max_results", &self.max_results)
            .finish()
    }
}

impl Query {
    pub(crate) fn from_job(
        job_service: Arc<JobService>,
        initial_job: Job,
        max_results: Option<u32>,
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
            metadata: QueryCreationMetadata::from(initial_job),
            max_results,
        }
    }

    pub(crate) fn from_job_reference(
        job_service: Arc<JobService>,
        job_ref: JobReference,
        max_results: Option<u32>,
    ) -> Self {
        let metadata = QueryCreationMetadata::new().set_job_reference(job_ref);
        Self {
            job_service,
            completed: false,
            cached_rows: None,
            metadata,
            max_results,
        }
    }

    pub(crate) fn from_query_response(
        job_service: Arc<JobService>,
        mut query_response: QueryResponse,
        max_results: Option<u32>,
    ) -> Self {
        let completed = query_response.job_complete.unwrap_or(false);
        let cached_rows = VecDeque::from(std::mem::take(&mut query_response.rows));
        let metadata = QueryCreationMetadata::from(query_response);
        Self {
            job_service,
            completed,
            cached_rows: Some(cached_rows),
            metadata,
            max_results,
        }
    }

    /// Returns the initial metadata from query creation.
    ///
    /// This provides access to the [`QueryCreationMetadata`] returned by the initial
    /// query execution request (either [`jobs.query`] or [`jobs.insert`]).
    ///
    /// Depending on how the query was executed, the metadata contains:
    /// - [`job_reference`][QueryCreationMetadata::job_reference]: The reference to the BigQuery job, if one was created.
    /// - [`query_id`][QueryCreationMetadata::query_id]: The unique ID of the query if executed statelessly.
    /// - [`job_creation_reason`][QueryCreationMetadata::job_creation_reason]: Why a job was created or skipped.
    /// - [`job_complete`][QueryCreationMetadata::job_complete]: Whether the query completed immediately without requiring polling.
    ///
    /// To wait for the query to finish and retrieve full results and final metadata, call
    /// [`until_done`][Self::until_done].
    ///
    /// [`jobs.query`]: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    /// [`jobs.insert`]: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
    pub fn metadata(&self) -> &QueryCreationMetadata {
        &self.metadata
    }

    /// Periodically checks the status of the background job until it finishes.
    /// Returns an error if a remote service or connection failure happens during polling.
    pub async fn until_done(self) -> Result<CompleteQuery> {
        let Query {
            job_service,
            completed,
            metadata,
            cached_rows,
            max_results,
        } = self;

        if let (true, Some(cached_rows)) = (completed, cached_rows) {
            return Ok(CompleteQuery::from_query_creation_metadata(
                job_service,
                metadata,
                cached_rows,
                max_results,
            ));
        }

        let job_ref = metadata
            .job_reference
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

/// A handle representing a successfully completed query ready for reading.
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

    pub(crate) fn from_query_creation_metadata(
        job_service: Arc<JobService>,
        metadata: QueryCreationMetadata,
        cached_rows: VecDeque<wkt::Struct>,
        max_results: Option<u32>,
    ) -> Self {
        let job_ref = metadata.job_reference.clone();
        let metadata = QueryMetadata::from(metadata);
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

    /// Returns a row iterator for the query result.
    pub fn read(self) -> RowIterator {
        RowIterator::new(self)
    }

    /// Returns the cached metadata for this query.
    pub fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }

    /// Fetches the full `Job` information for the given query.
    ///
    /// Stateless queries will return `QueryError::StatelessQuery`.
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

    type TestResult = anyhow::Result<()>;

    impl CompleteQuery {
        pub(crate) fn from_query_response(
            job_service: Arc<JobService>,
            mut query_res: QueryResponse,
            max_results: Option<u32>,
        ) -> Self {
            let cached_rows = std::mem::take(&mut query_res.rows).into();
            let metadata = QueryCreationMetadata::from(query_res);
            Self::from_query_creation_metadata(job_service, metadata, cached_rows, max_results)
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

        let query = Query::from_query_response(job_service, query_res, None);

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

        let query = Query::from_query_response(job_service, query_res, Some(42));

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
        let job = Job::new().set_job_reference(job_ref);

        let query = Query::from_job(job_service, job, None);

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
        let job = Job::new().set_job_reference(job_ref);

        let query = Query::from_job(job_service, job, None);

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
        let job = Job::new().set_job_reference(job_ref);

        let query = Query::from_job(job_service, job, None);

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
        let query_res = QueryResponse::new()
            .set_schema(TableSchema::new())
            .set_job_reference(job_ref);

        let complete_query = CompleteQuery::from_query_response(job_service, query_res, None);
        let job = complete_query.job_metadata().await?;
        assert_eq!(job.user_email, "test@example.com");
        Ok(())
    }

    #[tokio::test]
    async fn test_complete_query_job_metadata_stateless() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let query_res = QueryResponse::new().set_schema(TableSchema::new());
        let complete_query = CompleteQuery::from_query_response(job_service, query_res, None);
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
        let query_res = QueryResponse::new()
            .set_schema(TableSchema::new())
            .set_job_reference(job_ref);

        let complete_query = CompleteQuery::from_query_response(job_service, query_res, None);
        let err = complete_query.job_metadata().await.unwrap_err();
        let source = match err {
            QueryError::Rpc { source } => source,
            _ => panic!("expected QueryError::Rpc, got {err:?}"),
        };
        assert_eq!(source.status().unwrap().code, Code::NotFound);
        Ok(())
    }
}
