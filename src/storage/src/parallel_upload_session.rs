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

//! Defines the session handle for [Storage::parallel_upload][crate::client::Storage::parallel_upload].

use crate::model::Object;
use crate::model_ext::Segment;
use crate::storage::parallel_upload::stub::dynamic::{
    ParallelUploadSession as ParallelUploadSessionStub, SegmentSource,
};
use crate::streaming_source::{Payload, Seek, SizeHint, StreamingSource};
use std::sync::Arc;

/// A type-erased rewindable and cloneable payload for a single segment write.
pub(crate) struct SegmentPayload {
    inner: Box<dyn SegmentSource>,
}

impl SegmentPayload {
    pub(crate) fn new<P>(payload: Payload<P>) -> Self
    where
        P: StreamingSource + Seek + Clone + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(payload),
        }
    }
}

impl Clone for SegmentPayload {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone_box(),
        }
    }
}

impl std::fmt::Debug for SegmentPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentPayload").finish_non_exhaustive()
    }
}

impl StreamingSource for SegmentPayload {
    type Error = crate::Error;

    async fn next(&mut self) -> Option<crate::Result<bytes::Bytes>> {
        self.inner.next().await
    }

    async fn size_hint(&self) -> crate::Result<SizeHint> {
        self.inner.size_hint().await
    }
}

impl Seek for SegmentPayload {
    type Error = crate::Error;

    async fn seek(&mut self, offset: u64) -> crate::Result<()> {
        self.inner.seek(offset).await
    }
}

/// An active parallel upload session ready to write segments concurrently.
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
    pub async fn abort(&self) -> crate::Result<()> {
        self.inner.abort().await
    }

    #[allow(dead_code)]
    pub(crate) fn from_dynamic(inner: Arc<dyn ParallelUploadSessionStub>) -> Self {
        Self { inner }
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
    use std::io::{Error as IoError, ErrorKind};

    mock! {
        #[derive(Debug)]
        Session {}

        #[async_trait::async_trait]
        impl ParallelUploadSessionStub for Session {
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

    #[derive(Clone)]
    struct FailingSource {
        fail_next: bool,
        fail_size_hint: bool,
        fail_seek: bool,
    }

    impl StreamingSource for FailingSource {
        type Error = IoError;

        async fn next(&mut self) -> Option<Result<Bytes, Self::Error>> {
            if self.fail_next {
                return Some(Err(IoError::new(ErrorKind::BrokenPipe, "next failed")));
            }
            None
        }

        async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
            if self.fail_size_hint {
                return Err(IoError::other("size_hint failed"));
            }
            Ok(SizeHint::with_exact(42))
        }
    }

    impl Seek for FailingSource {
        type Error = IoError;

        async fn seek(&mut self, _offset: u64) -> Result<(), Self::Error> {
            if self.fail_seek {
                return Err(IoError::new(ErrorKind::InvalidInput, "seek failed"));
            }
            Ok(())
        }
    }

    #[test]
    fn impls() {
        assert_impl_all!(ParallelUploadSession: Clone, std::fmt::Debug, Send, Sync);
        assert_impl_all!(SegmentPayload: Clone, std::fmt::Debug, Send, Sync);
    }

    #[tokio::test]
    async fn futures_are_send() -> anyhow::Result<()> {
        fn assert_send<T: Send>(_: &T) {}
        fn assert_static<T: 'static>(_: &T) {}

        let mut mock = MockSession::new();
        mock.expect_write_segment().returning(|num, _| {
            Ok(Segment {
                name: format!("seg-{num}"),
                size_bytes: 5,
                segment_crc32c: 123,
            })
        });
        let session = ParallelUploadSession::from_dynamic(Arc::new(mock));
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
                Ok(Segment {
                    name: "projects/_/buckets/b/objectWriteSessions/s1/segments/1".into(),
                    size_bytes: 11,
                    segment_crc32c: 99,
                })
            });
        mock.expect_finalize()
            .returning(|| Ok(Object::new().set_name("obj").set_size(11)));
        mock.expect_abort().returning(|| Ok(()));

        let session = ParallelUploadSession::from_dynamic(Arc::new(mock));
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

        session.abort().await?;
        let _ = session.into_parts();
        Ok(())
    }

    #[tokio::test]
    async fn segment_payload_clone_and_seek() -> anyhow::Result<()> {
        let mut payload = SegmentPayload::new(Payload::from(vec![
            Bytes::from_static(b"hello "),
            Bytes::from_static(b"world"),
        ]));
        assert!(format!("{payload:?}").contains("SegmentPayload"));
        assert_eq!(payload.size_hint().await?.exact(), Some(11));

        let mut cloned = payload.clone();
        assert_eq!(
            payload.next().await.transpose()?,
            Some(Bytes::from_static(b"hello "))
        );

        // The clone maintains an independent cursor starting at offset 0.
        assert_eq!(
            cloned.next().await.transpose()?,
            Some(Bytes::from_static(b"hello "))
        );
        assert_eq!(
            cloned.next().await.transpose()?,
            Some(Bytes::from_static(b"world"))
        );
        assert_eq!(cloned.next().await.transpose()?, None);

        // Rewinding `payload` resets its cursor independently.
        payload.seek(0).await?;
        assert_eq!(
            payload.next().await.transpose()?,
            Some(Bytes::from_static(b"hello "))
        );
        Ok(())
    }

    #[tokio::test]
    async fn segment_payload_maps_errors() {
        let mut payload = SegmentPayload::new(Payload::from_stream(FailingSource {
            fail_next: true,
            fail_size_hint: true,
            fail_seek: true,
        }));

        let next_err = payload.next().await.transpose().unwrap_err();
        assert!(next_err.is_serialization(), "{next_err:?}");

        let hint_err = payload.size_hint().await.unwrap_err();
        assert!(hint_err.is_serialization(), "{hint_err:?}");

        let seek_err = payload.seek(0).await.unwrap_err();
        assert!(seek_err.is_serialization(), "{seek_err:?}");
    }
}
