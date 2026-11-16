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

pub(crate) mod dynamic {
    use crate::model::Object;
    use crate::model_ext::Segment;
    use crate::parallel_upload_session::SegmentPayload;
    use crate::streaming_source::{Payload, Seek, SizeHint, StreamingSource};

    /// An object-safe dynamic trait for rewindable and cloneable segment payloads.
    #[async_trait::async_trait]
    pub trait SegmentSource: Send + Sync {
        async fn next(&mut self) -> Option<crate::Result<bytes::Bytes>>;
        async fn size_hint(&self) -> crate::Result<SizeHint>;
        async fn seek(&mut self, offset: u64) -> crate::Result<()>;
        fn clone_box(&self) -> Box<dyn SegmentSource>;
    }

    #[async_trait::async_trait]
    impl<P> SegmentSource for Payload<P>
    where
        P: StreamingSource + Seek + Clone + Send + Sync + 'static,
    {
        async fn next(&mut self) -> Option<crate::Result<bytes::Bytes>> {
            StreamingSource::next(self)
                .await
                .map(|r| r.map_err(crate::Error::ser))
        }

        async fn size_hint(&self) -> crate::Result<SizeHint> {
            StreamingSource::size_hint(self)
                .await
                .map_err(crate::Error::ser)
        }

        async fn seek(&mut self, offset: u64) -> crate::Result<()> {
            Seek::seek(self, offset).await.map_err(crate::Error::ser)
        }

        fn clone_box(&self) -> Box<dyn SegmentSource> {
            Box::new(self.clone())
        }
    }

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
