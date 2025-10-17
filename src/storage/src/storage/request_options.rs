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

use crate::read_resume_policy::{ReadResumePolicy, Recommended};
use crate::storage::checksum::details::{Checksum, Crc32c};
use gax::{
    backoff_policy::BackoffPolicy,
    retry_policy::RetryPolicy,
    retry_throttler::{AdaptiveThrottler, SharedRetryThrottler},
};
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
    pub(crate) read_resume_policy: Arc<dyn ReadResumePolicy>,
    pub(crate) resumable_upload_threshold: usize,
    pub(crate) resumable_upload_buffer_size: usize,
    pub(crate) idempotency: Option<bool>,
    pub(crate) checksum: Checksum,
    pub(crate) automatic_decompression: bool,
}

const MIB: usize = 1024 * 1024_usize;
// There is some justification for these magic numbers at:
//     https://github.com/googleapis/google-cloud-cpp/issues/2657
const RESUMABLE_UPLOAD_THRESHOLD: usize = 16 * MIB;
const RESUMABLE_UPLOAD_TARGET_CHUNK: usize = 8 * MIB;

impl RequestOptions {
    pub(crate) fn new() -> Self {
        let retry_policy = Arc::new(crate::retry_policy::storage_default());
        let backoff_policy = Arc::new(crate::backoff_policy::default());
        let retry_throttler = Arc::new(Mutex::new(AdaptiveThrottler::default()));
        let read_resume_policy = Arc::new(Recommended);
        Self {
            retry_policy,
            backoff_policy,
            retry_throttler,
            read_resume_policy,
            resumable_upload_threshold: RESUMABLE_UPLOAD_THRESHOLD,
            resumable_upload_buffer_size: RESUMABLE_UPLOAD_TARGET_CHUNK,
            idempotency: None,
            checksum: Checksum {
                crc32c: Some(Crc32c::default()),
                md5_hash: None,
            },
            automatic_decompression: false,
        }
    }
}
