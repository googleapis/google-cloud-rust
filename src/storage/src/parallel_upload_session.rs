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

//! Defines the session handle and segment payload for [Storage::parallel_upload][crate::client::Storage::parallel_upload].

use crate::model::Object;
use crate::model_ext::{AbortSessionResponse, Segment};
use crate::storage::parallel_upload::stub::dynamic::ParallelUploadSession as ParallelUploadSessionStub;
use crate::streaming_source::{Payload, Seek, StreamingSource};
use std::sync::Arc;

pub use crate::storage::parallel_upload::segment_payload::SegmentPayload;

/// An active parallel upload session ready to write segments concurrently.
///
/// Dropping a `ParallelUploadSession` does not abort the session. Call
/// [`abort`](Self::abort) to discard uncommitted data if you abandon the
/// upload before [`finalize`](Self::finalize).
#[cfg_attr(docsrs, doc(cfg(google_cloud_unstable_storage_parallel_upload)))]
#[derive(Clone, Debug)]
pub struct ParallelUploadSession {
    inner: Arc<dyn ParallelUploadSessionStub>,
}

impl ParallelUploadSession {
    /// Returns the resource name of the session.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns the server-determined segment size in bytes.
    pub fn segment_size_bytes(&self) -> u64 {
        self.inner.segment_size_bytes()
    }

    /// Uploads a single segment to the session.
    ///
    /// # Errors
    ///
    /// Returns an error if reading from `segment_payload` fails or the segment upload RPC fails.
    ///
    /// # Panics
    ///
    /// Panics with the default transport until segment uploads are implemented.
    pub async fn write_segment<T, P>(
        &self,
        segment_number: u32,
        segment_payload: T,
    ) -> crate::Result<Segment>
    where
        T: Into<Payload<P>>,
        P: StreamingSource + Seek + Clone + Send + Sync + 'static,
    {
        let payload = SegmentPayload::new(segment_payload.into());
        self.inner.write_segment(segment_number, payload).await
    }

    /// Finalizes the parallel upload session and returns the created object metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if segment validation fails or the finalize RPC fails.
    pub async fn finalize(&self) -> crate::Result<Object> {
        self.inner.finalize().await
    }

    /// Aborts the parallel upload session and discards any uncommitted segment data.
    ///
    /// # Errors
    ///
    /// Returns an error if the abort RPC fails.
    pub async fn abort(&self) -> crate::Result<AbortSessionResponse> {
        self.inner.abort().await
    }

    /// Creates a new instance from a stub implementation.
    ///
    /// Application developers should only need to create a `ParallelUploadSession`
    /// in unit tests.
    pub fn new<T>(inner: T) -> Self
    where
        T: crate::stub::ParallelUploadSession + 'static,
    {
        Self {
            inner: Arc::new(inner),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_parts(self) -> Arc<dyn ParallelUploadSessionStub> {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mockall::mock;
    use static_assertions::assert_impl_all;

    mock! {
        #[derive(Debug)]
        Session {}

        impl crate::stub::ParallelUploadSession for Session {
            fn name(&self) -> &str;
            fn segment_size_bytes(&self) -> u64;
            async fn write_segment(
                &self,
                segment_number: u32,
                segment_payload: SegmentPayload,
            ) -> crate::Result<Segment>;
            async fn finalize(&self) -> crate::Result<Object>;
            async fn abort(&self) -> crate::Result<AbortSessionResponse>;
        }
    }

    #[test]
    fn impls() {
        assert_impl_all!(ParallelUploadSession: Clone, std::fmt::Debug, Send, Sync);
    }

    #[tokio::test]
    async fn futures_are_send() -> anyhow::Result<()> {
        fn assert_send<T: Send>(_: &T) {}
        fn assert_static<T: 'static>(_: &T) {}

        let mut mock = MockSession::new();
        mock.expect_write_segment().returning(|num, _| {
            Ok(Segment::new()
                .set_name(format!("seg-{num}"))
                .set_size_bytes(5)
                .set_segment_crc32c(123))
        });
        let session = ParallelUploadSession::new(mock);
        assert_static(&session);

        let write_fut = session.write_segment(1, "hello");
        assert_send(&write_fut);
        let finalize_fut = session.finalize();
        assert_send(&finalize_fut);
        let abort_fut = session.abort();
        assert_send(&abort_fut);
        drop((write_fut, finalize_fut, abort_fut));

        let s1 = session.clone();
        let t1 =
            tokio::spawn(async move { s1.write_segment(1, Bytes::from_static(b"hello")).await });
        let seg = t1.await??;
        assert_eq!(seg.name, "seg-1");
        Ok(())
    }

    #[tokio::test]
    async fn session_delegates() -> anyhow::Result<()> {
        let mut mock = MockSession::new();
        mock.expect_name()
            .return_const("projects/_/buckets/b/objectWriteSessions/s1".to_string());
        mock.expect_segment_size_bytes()
            .return_const(32 * 1024 * 1024_u64);
        let captured_payload = Arc::new(std::sync::Mutex::new(None));
        let captured = captured_payload.clone();
        mock.expect_write_segment()
            .withf(|num, _| *num == 1)
            .returning(move |_, payload| {
                *captured.lock().expect("never poisoned") = Some(payload);
                Ok(Segment::new()
                    .set_name("projects/_/buckets/b/objectWriteSessions/s1/segments/1")
                    .set_size_bytes(11)
                    .set_segment_crc32c(99))
            });
        mock.expect_finalize()
            .returning(|| Ok(Object::new().set_name("obj").set_size(11)));
        mock.expect_abort()
            .returning(|| Ok(AbortSessionResponse::new()));

        let session = ParallelUploadSession::new(mock);
        assert_eq!(
            session.name(),
            "projects/_/buckets/b/objectWriteSessions/s1"
        );
        assert_eq!(session.segment_size_bytes(), 32 * 1024 * 1024);

        let seg = session.write_segment(1, "hello world").await?;
        assert_eq!(seg.size_bytes, 11);
        assert_eq!(seg.segment_crc32c, 99);

        let mut received = captured_payload
            .lock()
            .expect("never poisoned")
            .take()
            .expect("payload captured");
        assert_eq!(
            received.next().await.transpose()?,
            Some(Bytes::from_static(b"hello world"))
        );
        assert_eq!(received.next().await.transpose()?, None);

        let obj = session.finalize().await?;
        assert_eq!(obj.name, "obj");
        assert_eq!(obj.size, 11);

        let abort_resp = session.abort().await?;
        assert_eq!(abort_resp, AbortSessionResponse::new());
        let _ = session.into_parts();
        Ok(())
    }
}
