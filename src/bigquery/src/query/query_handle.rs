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

use crate::query::QueryReference;
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{Job, JobReference, QueryResponse};
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
    /// The reference will be [`QueryReference::Job`] if a query job was created,
    /// or [`QueryReference::Stateless`] with an opaque query ID if job creation
    /// was skipped.
    ///
    /// # Panics
    ///
    /// Panics if the query has neither a job reference nor a query ID.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::tests::{MockJobService, create_job_service};
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
}
