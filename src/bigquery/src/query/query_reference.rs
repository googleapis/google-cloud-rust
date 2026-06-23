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

/// A reference to a query in BigQuery.
///
/// BigQuery queries can either run by creating a query job, or run statelessly
/// by optionally skipping job creation. See more info at [JobCreationReason] docs.
///
/// This enum represents the reference to either execution model.
///
/// [JobCreationReason]: https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/JobCreationReason
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum QueryReference {
    /// A reference to a standard, stateful query job.
    Job(google_cloud_bigquery_v2::model::JobReference),
    /// A reference to a stateless query, identified by an opaque query ID.
    Stateless {
        /// The unique, opaque ID of the stateless query.
        query_id: String,
    },
}

impl From<google_cloud_bigquery_v2::model::JobReference> for QueryReference {
    fn from(v: google_cloud_bigquery_v2::model::JobReference) -> QueryReference {
        QueryReference::Job(v)
    }
}

impl QueryReference {
    pub(crate) fn from_query_id(query_id: String) -> Self {
        Self::Stateless { query_id }
    }

    pub(crate) fn to_job_ref(&self) -> Option<google_cloud_bigquery_v2::model::JobReference> {
        match self {
            Self::Job(job_ref) => Some(job_ref.clone()),
            Self::Stateless { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stateful_query_job() {
        // With location
        let proto = google_cloud_bigquery_v2::model::JobReference::new()
            .set_project_id("a-project-id")
            .set_job_id("a-job-id")
            .set_location("US");
        let job_ref = QueryReference::from(proto.clone());
        assert_eq!(job_ref, QueryReference::Job(proto.clone()));
        assert_eq!(job_ref.to_job_ref(), Some(proto));

        // Without location
        let proto = google_cloud_bigquery_v2::model::JobReference::new()
            .set_project_id("a-project-id")
            .set_job_id("a-job-id");
        let job_ref = QueryReference::from(proto.clone());
        assert_eq!(job_ref, QueryReference::Job(proto.clone()));
        assert_eq!(job_ref.to_job_ref(), Some(proto));
    }

    #[test]
    fn stateless_query() {
        let query_ref = QueryReference::from_query_id("a-query-id".to_string());
        assert_eq!(
            query_ref,
            QueryReference::Stateless {
                query_id: "a-query-id".to_string(),
            }
        );
        assert_eq!(query_ref.to_job_ref(), None);
    }
}
