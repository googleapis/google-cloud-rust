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

use std::time::Duration;

/// Configuration for automatically retrying a transaction when it is aborted.
#[derive(Clone, Debug)]
pub struct TransactionRetrySettings {
    /// The maximum number of attempts to make. If 0, this field is ignored.
    pub max_attempts: u32,
    /// The total maximum time to spend retrying. If 0, this field is ignored.
    pub total_timeout: Duration,
}

impl Default for TransactionRetrySettings {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            total_timeout: Duration::from_secs(0),
        }
    }
}

/// Helper method to execute an asynchronous closure, retrying it if the
/// transaction is aborted by the server.
///
/// This is used for operations like Partitioned DML transactions in Cloud Spanner, where
/// the server may abort the transaction due to transient issues, indicating that the client
/// should re-attempt the entire operation.
pub(crate) async fn retry_aborted<T, F, Fut>(
    _settings: &TransactionRetrySettings,
    mut f: F,
) -> crate::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = crate::Result<T>>,
{
    // Will be implemented in a separate pull request.
    f().await
}
