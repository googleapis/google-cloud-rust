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

//! Helpers to await data consistency for a [CheckConsistency] request.

use crate::builder::bigtable_table_admin::CheckConsistency;
use google_cloud_gax::polling_state::PollingState;
use google_cloud_gax::retry_policy::NeverRetry;
use google_cloud_gax::retry_result::RetryResult;

impl CheckConsistency {
    /// Polls [check_consistency][crate::client::BigtableTableAdmin::check_consistency]
    /// until the table is consistent, or until the polling policy is exhausted.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigtable_admin_v2::client::BigtableTableAdmin;
    /// # async fn sample(
    /// #     client: &BigtableTableAdmin,
    /// #     table_name: &str,
    /// #     token: &str,
    /// # ) -> google_cloud_bigtable_admin_v2::Result<()> {
    /// client
    ///     .check_consistency()
    ///     .set_name(table_name)
    ///     .set_consistency_token(token)
    ///     .until_consistent()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn until_consistent(mut self) -> crate::Result<()> {
        let error_policy = self
            .0
            .options
            .polling_error_policy()
            .clone()
            .unwrap_or_else(|| self.0.stub.get_polling_error_policy(&self.0.options));
        let backoff_policy = self
            .0
            .options
            .polling_backoff_policy()
            .clone()
            .unwrap_or_else(|| self.0.stub.get_polling_backoff_policy(&self.0.options));
        self.0.options.set_retry_policy(NeverRetry);

        let mut state = PollingState::default();
        loop {
            state.attempt_count += 1;
            match self.clone().send().await {
                Ok(r) if r.consistent => return Ok(()),
                Ok(_) => error_policy.on_in_progress(&state, &self.0.request.name)?,
                Err(e) => match error_policy.on_error(&state, e) {
                    RetryResult::Continue(_) => {}
                    RetryResult::Exhausted(e) | RetryResult::Permanent(e) => return Err(e),
                },
            }
            tokio::time::sleep(backoff_policy.wait_period(&state)).await;
        }
    }
}
