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

use google_cloud_bigquery_v2::model::{
    DataFormatOptions, Job, JobConfiguration, JobConfigurationQuery, PostQueryRequest,
};

/// A request to execute a query, which can be either using a fast query path with `PostQueryRequest` or an advanced `Job`.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum QueryRequest {
    /// A stateless query request via the `jobs.query` endpoint.
    PostQueryRequest(PostQueryRequest),
    /// An advanced query job via the `jobs.insert` endpoint.
    Job(Job),
}

impl From<String> for QueryRequest {
    fn from(v: String) -> Self {
        let req = google_cloud_bigquery_v2::model::QueryRequest::new()
            .set_query(v)
            .set_format_options(DataFormatOptions::new().set_use_int64_timestamp(true))
            .set_use_legacy_sql(false);
        let post_req = PostQueryRequest::new().set_query_request(req);
        QueryRequest::PostQueryRequest(post_req)
    }
}

impl From<&str> for QueryRequest {
    fn from(v: &str) -> Self {
        v.to_string().into()
    }
}

impl From<google_cloud_bigquery_v2::model::QueryRequest> for QueryRequest {
    fn from(mut req: google_cloud_bigquery_v2::model::QueryRequest) -> Self {
        let format_options = req
            .format_options
            .take()
            .unwrap_or_default()
            .set_use_int64_timestamp(true);
        let req = req.set_format_options(format_options);
        let post_req = PostQueryRequest::new().set_query_request(req);
        QueryRequest::PostQueryRequest(post_req)
    }
}

impl From<PostQueryRequest> for QueryRequest {
    fn from(req: PostQueryRequest) -> Self {
        QueryRequest::PostQueryRequest(req)
    }
}

impl From<Job> for QueryRequest {
    fn from(job: Job) -> Self {
        QueryRequest::Job(job)
    }
}

impl From<JobConfigurationQuery> for QueryRequest {
    fn from(query_config: JobConfigurationQuery) -> Self {
        let config = JobConfiguration::new().set_query(query_config);
        let job = Job::new().set_configuration(config);
        QueryRequest::Job(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str() {
        let req: QueryRequest = "SELECT * FROM my_table".into();
        match req {
            QueryRequest::PostQueryRequest(post_req) => {
                let inner = post_req.query_request.expect("missing inner req");
                assert_eq!(inner.query, "SELECT * FROM my_table");
                assert_eq!(inner.use_legacy_sql, Some(false));
                let format_opts = inner.format_options.unwrap_or_default();
                assert!(format_opts.use_int64_timestamp, "{format_opts:?}");
            }
            _ => panic!("expected PostQueryRequest variant"),
        }
    }

    #[test]
    fn from_query_request() {
        let qr =
            google_cloud_bigquery_v2::model::QueryRequest::new().set_query("SELECT 1".to_string());
        let req: QueryRequest = qr.into();
        match req {
            QueryRequest::PostQueryRequest(post_req) => {
                let inner = post_req.query_request.expect("missing inner req");
                assert_eq!(inner.query, "SELECT 1");
                let format_opts = inner.format_options.unwrap_or_default();
                assert!(format_opts.use_int64_timestamp, "{format_opts:?}");
            }
            _ => panic!("expected PostQueryRequest variant"),
        }
    }

    #[test]
    fn from_query_request_preserves_format_options() {
        let format_options = DataFormatOptions::new().set_timestamp_output_format(
            google_cloud_bigquery_v2::model::data_format_options::TimestampOutputFormat::Iso8601String,
        );
        let qr = google_cloud_bigquery_v2::model::QueryRequest::new()
            .set_query("SELECT 1".to_string())
            .set_format_options(format_options);
        let req: QueryRequest = qr.into();
        match req {
            QueryRequest::PostQueryRequest(post_req) => {
                let inner = post_req.query_request.expect("missing inner req");
                let format_opts = inner.format_options.expect("missing format options");
                assert!(format_opts.use_int64_timestamp, "{format_opts:?}");
                assert_eq!(
                    format_opts.timestamp_output_format,
                    google_cloud_bigquery_v2::model::data_format_options::TimestampOutputFormat::Iso8601String
                );
            }
            _ => panic!("expected PostQueryRequest variant"),
        }
    }

    #[test]
    fn from_job() {
        let job = google_cloud_bigquery_v2::model::Job::new();
        let req: QueryRequest = job.into();
        match req {
            QueryRequest::Job(_) => {}
            _ => panic!("expected Job variant"),
        }
    }

    #[test]
    fn from_job_configuration_query() {
        let jcq = google_cloud_bigquery_v2::model::JobConfigurationQuery::new()
            .set_query("SELECT 2".to_string());
        let req: QueryRequest = jcq.into();
        match req {
            QueryRequest::Job(job) => {
                let config = job.configuration.expect("missing config");
                let query_config = config.query.expect("missing query config");
                assert_eq!(query_config.query, "SELECT 2");
            }
            _ => panic!("expected Job variant"),
        }
    }
}
