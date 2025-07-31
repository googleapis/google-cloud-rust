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

use crate::download_resume_policy::{DownloadResumePolicy, Recommended};
use gax::{
    backoff_policy::BackoffPolicy,
    retry_policy::RetryPolicy,
    retry_throttler::{AdaptiveThrottler, SharedRetryThrottler},
};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub(crate) struct RequestOptions {
    pub retry_policy: Arc<dyn RetryPolicy>,
    pub backoff_policy: Arc<dyn BackoffPolicy>,
    pub retry_throttler: SharedRetryThrottler,
    pub download_resume_policy: Arc<dyn DownloadResumePolicy>,
    pub resumable_upload_threshold: usize,
    pub resumable_upload_buffer_size: usize,
    pub idempotency: Option<bool>,
}

const MIB: usize = 1024 * 1024_usize;
// There is some justification for these magic numbers at:
//     https://github.com/googleapis/google-cloud-cpp/issues/2657
const RESUMABLE_UPLOAD_THRESHOLD: usize = 16 * MIB;
const RESUMABLE_UPLOAD_TARGET_CHUNK: usize = 8 * MIB;

impl RequestOptions {
    pub(crate) fn new() -> Self {
        let retry_policy = Arc::new(crate::retry_policy::default());
        let backoff_policy = Arc::new(crate::backoff_policy::default());
        let retry_throttler = Arc::new(Mutex::new(AdaptiveThrottler::default()));
        let download_resume_policy = Arc::new(Recommended);
        Self {
            retry_policy,
            backoff_policy,
            retry_throttler,
            download_resume_policy,
            resumable_upload_threshold: RESUMABLE_UPLOAD_THRESHOLD,
            resumable_upload_buffer_size: RESUMABLE_UPLOAD_TARGET_CHUNK,
            idempotency: None,
        }
    }
}
