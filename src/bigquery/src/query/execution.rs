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
use google_cloud_bigquery_v2::model::{InsertJobRequest, Job, PostQueryRequest};
use std::sync::Arc;

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

    pub(crate) async fn execute(self) -> Result<Query> {
        let res = self
            .job_service
            .query()
            .with_request(self.request)
            .send()
            .await
            .map_err(|e| QueryError::Rpc { source: e })?;

        if !res.errors.is_empty() {
            return Err(QueryError::JobFailed { errors: res.errors });
        }

        let completed = res.job_complete.unwrap_or(false);
        let job_ref = res.job_reference.clone();

        Ok(Query {
            job_service: self.job_service.clone(),
            job_ref,
            completed,
            initial_response: Some(res),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::tests::{MockJobService, create_job_service};
    use google_cloud_bigquery_v2::model::{ErrorProto, JobReference, QueryResponse};
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    async fn test_execute_success() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(|_, _| {
            let job_ref = JobReference::new().set_job_id("my-job-123");
            let query_res = QueryResponse::new()
                .set_job_complete(true)
                .set_job_reference(job_ref.clone());
            Ok(Response::from(query_res))
        });

        let job_service = create_job_service(mock);

        let request = PostQueryRequest::new();
        let executor = PostQueryExecutor::new(job_service, request);
        let query = executor.execute().await?;

        assert!(query.completed, "{query:?}");
        let job_ref = query.job_ref.clone().expect("should have job_ref");
        assert_eq!(job_ref.job_id, "my-job-123", "{job_ref:?}");
        assert!(query.initial_response.is_some(), "{query:?}");

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_job_failed_error() -> TestResult {
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
    async fn test_execute_rpc_error() -> TestResult {
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
}
