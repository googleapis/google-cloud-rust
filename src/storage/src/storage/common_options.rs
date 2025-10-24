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
use std::sync::Arc;

const MIB: usize = 1024 * 1024_usize;
// There is some justification for these magic numbers at:
//     https://github.com/googleapis/google-cloud-cpp/issues/2657
const RESUMABLE_UPLOAD_THRESHOLD: usize = 16 * MIB;
const RESUMABLE_UPLOAD_TARGET_CHUNK: usize = 8 * MIB;

/// Options shared by the client and requests.
#[derive(Clone, Debug)]
pub struct CommonOptions {
    pub read_resume_policy: Arc<dyn ReadResumePolicy>,
    pub resumable_upload_threshold: usize,
    pub resumable_upload_buffer_size: usize,
}

impl CommonOptions {
    pub fn new() -> Self {
        let read_resume_policy = Arc::new(Recommended);
        Self {
            read_resume_policy,
            resumable_upload_threshold: RESUMABLE_UPLOAD_THRESHOLD,
            resumable_upload_buffer_size: RESUMABLE_UPLOAD_TARGET_CHUNK,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let got = CommonOptions::new();
        assert_ne!(got.resumable_upload_threshold, 0);
        assert_ne!(got.resumable_upload_buffer_size, 0);
        assert_eq!(
            got.resumable_upload_buffer_size % (256 * 1024),
            0,
            "{got:?}"
        );
    }
}
