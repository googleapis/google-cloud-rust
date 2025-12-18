// Copyright 2025 Google LLC
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

use google_cloud_gax::retry_result::RetryResult;
use google_cloud_storage::read_resume_policy::ReadResumePolicy;

/// Instrument a [ReadResumePolicy] to log when the client needs to resume.
#[derive(Debug)]
pub struct Instrumented<T> {
    inner: T,
}

impl<T> Instrumented<T>
where
    T: std::fmt::Debug,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> ReadResumePolicy for Instrumented<T>
where
    T: ReadResumePolicy,
{
    fn on_error(
        &self,
        status: &google_cloud_storage::read_resume_policy::ResumeQuery,
        error: google_cloud_storage::Error,
    ) -> google_cloud_gax::retry_result::RetryResult {
        let result = self.inner.on_error(status, error);
        match &result {
            RetryResult::Continue(e) => tracing::info!("read resume policy continues: {e:?}"),
            RetryResult::Exhausted(e) => tracing::info!("read resume policy exhausted: {e:?}"),
            RetryResult::Permanent(e) => {
                tracing::info!("read resume policy permanent error: {e:?}")
            }
        }
        result
    }
}
