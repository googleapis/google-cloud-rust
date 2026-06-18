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
use crate::query::{QueryReference, Result, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    GetQueryResultsRequest, GetQueryResultsResponse, Job, JobReference, QueryResponse,
};
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_state::PollingState;
use std::collections::VecDeque;
use std::sync::Arc;

/// A handle representing a running query.
#[derive(Clone, Debug)]
pub struct Query {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
    pub(crate) completed: bool,
    pub(crate) initial_job: Option<Job>,
    pub(crate) initial_response: Option<QueryResponse>,
}

impl Query {
    /// Returns the [`QueryReference`] for this query.
    ///
    /// The reference will be [`QueryReference::Job`] with a query [job reference],
    /// or [`QueryReference::Stateless`] with an opaque query ID if job creation
    /// was skipped.
    ///
    /// [job reference]: https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
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

    /// Periodically checks the status of the background job until it finishes.
    /// Returns an error if a remote service or connection failure happens during polling.
    pub async fn until_done(&self) -> Result<CompleteQuery> {
        if let (true, Some(initial_response)) = (self.completed, &self.initial_response) {
            return Ok(CompleteQuery::from_query_response(self, initial_response));
        }

        let job_ref = self
            .job_ref
            .as_ref()
            .expect("query job should have job reference at this point");
        let backoff_policy = Arc::new(
            ExponentialBackoffBuilder::default()
                .with_initial_delay(std::time::Duration::from_secs(10))
                .build()
                .expect("valid backoff configuration"),
        );
        let res = poll_query_results(&self.job_service, job_ref, backoff_policy).await?;
        Ok(CompleteQuery::from_get_query_results_response(self, res))
    }
}

/// A handle representing a successfully completed query ready for reading.
#[derive(Debug, Clone)]
pub struct CompleteQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) job_ref: Option<JobReference>,
}

impl CompleteQuery {
    pub(crate) fn from_get_query_results_response(
        q: &Query,
        _res: GetQueryResultsResponse,
    ) -> Self {
        // TODO(#5592): hold cached rows, page token, schema and query metadata here.
        Self {
            job_service: q.job_service.clone(),
            job_ref: q.job_ref.clone(),
        }
    }

    pub(crate) fn from_query_response(q: &Query, _res: &QueryResponse) -> Self {
        // TODO(#5592): hold cached rows, page token, schema and query metadata here.
        Self {
            job_service: q.job_service.clone(),
            job_ref: q.job_ref.clone(),
        }
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
    use crate::query::tests::{
        MockBackoffPolicy, MockJobService, create_job_service, create_test_backoff_policy,
    };
    use google_cloud_bigquery_v2::model::{
        ErrorProto, GetQueryResultsResponse, JobReference, QueryResponse,
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
            .set_job_reference(job_ref.clone());

        let query = Query {
            job_service,
            job_ref: Some(job_ref),
            completed: true,
            initial_job: None,
            initial_response: Some(query_res),
        };

        let completed = query.until_done().await?;
        assert_eq!(completed.job_ref.unwrap().job_id, "some_job_id");

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
                    .set_job_reference(JobReference::new().set_job_id(req.job_id));
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
        };

        let completed = query.until_done().await?;
        assert_eq!(completed.job_ref.unwrap().job_id, "some_job_id");

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
        };

        let err = query.until_done().await.unwrap_err();
        let source = match err {
            QueryError::Rpc { source } => source,
            _ => panic!("expected QueryError::Rpc, got {err:?}"),
        };
        assert_eq!(source.status().unwrap().code, Code::InvalidArgument);

        Ok(())
    }
}
