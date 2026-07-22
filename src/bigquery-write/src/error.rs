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

use crate::Error;

/// Represents an error that can occur when appending rows.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AppendError {
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
    use google_cloud_gax::error::rpc::{Code, Status};

    #[test]
    fn append_error_rpc_debug() {
        let e = AppendError::Rpc {
            source: Error::service(
                Status::default()
                    .set_code(Code::FailedPrecondition)
                    .set_message("inner fail"),
            ),
        };
        let fmt = format!("{e}");
        assert!(fmt.contains("operation failed."), "{fmt}");
        assert!(fmt.contains("inner fail"), "{fmt}");
    }
}
