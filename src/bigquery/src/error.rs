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

//! Custom errors for the Cloud BigQuery query client.

use google_cloud_bigquery_v2::model::ErrorProto;
use google_cloud_gax::error::Error;

/// Errors that can occur during query configuration, execution, or polling.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum QueryError {
    /// The project ID was not provided or could not be determined.
    #[error("no project ID was provided")]
    MissingProjectId,

    /// Only query jobs are supported by this client.
    #[error("only query jobs are supported")]
    UnsupportedJobType,

    /// The query job failed on the BigQuery service side.
    /// Includes the list of error protocols returned by the service.
    #[error("query job failed: {reason} - {message}")]
    JobFailed {
        /// The primary error reason code (e.g., "invalidQuery", "backendError").
        reason: String,
        /// The error message.
        message: String,
        /// The list of all errors associated with the job.
        errors: Vec<ErrorProto>,
    },

    /// The underlying RPC failed.
    #[non_exhaustive]
    #[error("the operation failed. RPC error: {source}")]
    Rpc {
        /// The error returned by the service for the request.
        #[source]
        source: Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_failed_display() {
        let err = QueryError::JobFailed {
            reason: "invalidQuery".to_string(),
            message: "Syntax error: Unexpected end of input".to_string(),
            errors: vec![
                ErrorProto::new()
                    .set_reason("invalidQuery")
                    .set_message("Syntax error: Unexpected end of input"),
            ],
        };
        assert_eq!(
            err.to_string(),
            "query job failed: invalidQuery - Syntax error: Unexpected end of input"
        );
    }

    #[test]
    fn test_rpc_display() {
        let status = google_cloud_gax::error::rpc::Status::default()
            .set_code(google_cloud_gax::error::rpc::Code::InvalidArgument)
            .set_message("simulated bad request");
        let err = QueryError::Rpc {
            source: Error::service(status),
        };
        assert_eq!(
            err.to_string(),
            "the operation failed. RPC error: the service reports an error with code INVALID_ARGUMENT described as: simulated bad request"
        );
    }
}
