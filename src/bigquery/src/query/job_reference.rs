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

#![allow(dead_code, unused_imports)]

/// Job Reference.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JobReference {
    StatefulJob {
        job_id: String,
        project_id: String,
        location: Option<String>,
    },
    StatelessJob {
        query_id: String,
    },
}

impl From<google_cloud_bigquery_v2::model::JobReference> for JobReference {
    fn from(v: google_cloud_bigquery_v2::model::JobReference) -> JobReference {
        JobReference::StatefulJob {
            job_id: v.job_id,
            project_id: v.project_id,
            location: v.location,
        }
    }
}

impl JobReference {
    pub(crate) fn from_query_id(query_id: String) -> Self {
        Self::StatelessJob { query_id }
    }

    pub(crate) fn as_job_ref(&self) -> Option<google_cloud_bigquery_v2::model::JobReference> {
        match self {
            Self::StatefulJob {
                job_id,
                project_id,
                location,
            } => Some(
                google_cloud_bigquery_v2::model::JobReference::new()
                    .set_project_id(project_id.clone())
                    .set_job_id(job_id.clone())
                    .set_or_clear_location(location.clone()),
            ),
            Self::StatelessJob { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stateful_job() {
        // With location
        let proto = google_cloud_bigquery_v2::model::JobReference::new()
            .set_project_id("a-project-id")
            .set_job_id("a-job-id")
            .set_location("US");
        let job_ref = JobReference::from(proto.clone());
        assert_eq!(
            job_ref,
            JobReference::StatefulJob {
                job_id: "a-job-id".to_string(),
                project_id: "a-project-id".to_string(),
                location: Some("US".to_string()),
            }
        );
        assert_eq!(job_ref.as_job_ref(), Some(proto));

        // Without location
        let proto = google_cloud_bigquery_v2::model::JobReference::new()
            .set_project_id("a-project-id")
            .set_job_id("a-job-id");
        let job_ref = JobReference::from(proto.clone());
        assert_eq!(
            job_ref,
            JobReference::StatefulJob {
                job_id: "a-job-id".to_string(),
                project_id: "a-project-id".to_string(),
                location: None,
            }
        );
        assert_eq!(job_ref.as_job_ref(), Some(proto));
    }

    #[test]
    fn stateless_job() {
        let job_ref = JobReference::from_query_id("a-query-id".to_string());
        assert_eq!(
            job_ref,
            JobReference::StatelessJob {
                query_id: "a-query-id".to_string(),
            }
        );
        assert_eq!(job_ref.as_job_ref(), None);
    }
}
