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
use std::sync::Arc;

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
        source: Arc<Error>,
    },

}