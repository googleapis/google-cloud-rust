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

use super::common_options::CommonOptions;
use crate::{
    read_resume_policy::ReadResumePolicy,
    storage::checksum::details::{Checksum, Crc32c},
};
use gax::{
    backoff_policy::BackoffPolicy,
    retry_policy::RetryPolicy,
    retry_throttler::{AdaptiveThrottler, SharedRetryThrottler},
};
use gaxi::options::ClientConfig;
use std::sync::{Arc, Mutex};

/// The per-request options for a client call.
///
/// This is currently an opaque type, used only in mocking the `Storage` client.
/// It is opaque to avoid breaking changes until its interface stabilizes.
#[derive(Clone, Debug)]
pub struct RequestOptions {
    pub(crate) retry_policy: Arc<dyn RetryPolicy>,
    pub(crate) backoff_policy: Arc<dyn BackoffPolicy>,
    pub(crate) retry_throttler: SharedRetryThrottler,
    pub(crate) idempotency: Option<bool>,
    pub(crate) checksum: Checksum,
    pub(crate) automatic_decompression: bool,
    pub(crate) common_options: CommonOptions,
}

impl RequestOptions {
    pub(crate) fn new() -> Self {
        let retry_policy = Arc::new(crate::retry_policy::storage_default());
        let backoff_policy = Arc::new(crate::backoff_policy::default());
        let retry_throttler = Arc::new(Mutex::new(AdaptiveThrottler::default()));
        Self::new_with_policies(
            retry_policy,
            backoff_policy,
            retry_throttler,
            CommonOptions::new(),
        )
    }

    pub(crate) fn new_with_client_config(
        config: &ClientConfig,
        common_options: CommonOptions,
    ) -> Self {
        let retry_policy = config
            .retry_policy
            .clone()
            .unwrap_or_else(|| Arc::new(crate::retry_policy::storage_default()));
        let backoff_policy = config
            .backoff_policy
            .clone()
            .unwrap_or_else(|| Arc::new(crate::backoff_policy::default()));
        let retry_throttler = config.retry_throttler.clone();
        Self::new_with_policies(
            retry_policy,
            backoff_policy,
            retry_throttler,
            common_options,
        )
    }

    pub fn set_read_resume_policy(&mut self, v: Arc<dyn ReadResumePolicy>) {
        self.common_options.read_resume_policy = v;
    }

    pub fn read_resume_policy(&self) -> Arc<dyn ReadResumePolicy> {
        self.common_options.read_resume_policy.clone()
    }

    pub fn set_resumable_upload_threshold(&mut self, v: usize) {
        self.common_options.resumable_upload_threshold = v;
    }

    pub fn resumable_upload_threshold(&self) -> usize {
        self.common_options.resumable_upload_threshold
    }

    pub fn set_resumable_upload_buffer_size(&mut self, v: usize) {
        self.common_options.resumable_upload_buffer_size = v;
    }

    pub fn resumable_upload_buffer_size(&self) -> usize {
        self.common_options.resumable_upload_buffer_size
    }

    fn new_with_policies(
        retry_policy: Arc<dyn RetryPolicy>,
        backoff_policy: Arc<dyn BackoffPolicy>,
        retry_throttler: SharedRetryThrottler,
        common_options: CommonOptions,
    ) -> Self {
        Self {
            retry_policy,
            backoff_policy,
            retry_throttler,
            common_options,
            idempotency: None,
            checksum: Checksum {
                crc32c: Some(Crc32c::default()),
                md5_hash: None,
            },
            automatic_decompression: false,
        }
    }

    #[cfg(google_cloud_unstable_storage_bidi)]
    pub(crate) fn gax(&self) -> gax::options::RequestOptions {
        let mut options = gax::options::RequestOptions::default();
        options.set_backoff_policy(self.backoff_policy.clone());
        options.set_retry_policy(self.retry_policy.clone());
        options.set_retry_throttler(self.retry_throttler.clone());
        if let Some(ref i) = self.idempotency {
            options.set_idempotency(*i);
        }
        options
    }
}

#[cfg(all(test, google_cloud_unstable_storage_bidi))]
mod tests {
    use super::*;
    use crate::storage::client::tests::{MockBackoffPolicy, MockRetryPolicy, MockRetryThrottler};

    #[test]
    fn gax_policies() {
        let mut options = RequestOptions::new();
        options.retry_policy = Arc::new(MockRetryPolicy::new());
        options.retry_throttler = Arc::new(Mutex::new(MockRetryThrottler::new()));
        options.backoff_policy = Arc::new(MockBackoffPolicy::new());

        let got = options.gax();
        assert!(got.backoff_policy().is_some(), "{got:?}");
        assert!(got.retry_policy().is_some(), "{got:?}");
        assert!(got.retry_throttler().is_some(), "{got:?}");
        assert!(got.idempotent().is_none(), "{got:?}");

        let fmt = format!("{got:?}");
        assert!(fmt.contains("MockBackoffPolicy"), "{fmt}");
        assert!(fmt.contains("MockRetryPolicy"), "{fmt}");
        assert!(fmt.contains("MockRetryThrottler"), "{fmt}");
    }

    #[test]
    fn gax_idempotency() {
        let mut options = RequestOptions::new();
        options.idempotency = Some(true);
        let got = options.gax();
        assert_eq!(got.idempotent(), Some(true));
    }
}
