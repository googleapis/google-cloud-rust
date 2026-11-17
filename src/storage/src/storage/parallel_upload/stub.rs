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

use super::segment_payload::SegmentPayload;

pub(crate) mod dynamic {
    use super::SegmentPayload;
    use crate::model::Object;
    use crate::model_ext::Segment;

    /// An object-safe dynamic trait for [crate::parallel_upload_session::ParallelUploadSession].
    #[async_trait::async_trait]
    pub trait ParallelUploadSession: std::fmt::Debug + Send + Sync {
        fn name(&self) -> &str;
        fn segment_size_bytes(&self) -> u64;
        async fn write_segment(
            &self,
            segment_number: u32,
            segment_payload: SegmentPayload,
        ) -> crate::Result<Segment>;
        async fn finalize(&self) -> crate::Result<Object>;
        async fn abort(&self) -> crate::Result<()>;
    }
}
