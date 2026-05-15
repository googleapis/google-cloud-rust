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
#[derive(Debug, Clone)]
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
