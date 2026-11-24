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

use crate::streaming_source::{Payload, Seek, SizeHint, StreamingSource};

/// An object-safe dynamic trait for rewindable and cloneable segment payloads.
#[async_trait::async_trait]
trait SegmentSource: Send + Sync {
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

/// A type-erased rewindable and cloneable payload for a single segment write.
///
/// Cloning a `SegmentPayload` preserves the current read offset rather than
/// rewinding to `0`, while giving each clone an independent cursor from that
/// point onward. Call [`Seek::seek`] with `0` to rewind before or after cloning.
#[cfg_attr(docsrs, doc(cfg(google_cloud_unstable_storage_parallel_upload)))]
pub struct SegmentPayload {
    inner: Box<dyn SegmentSource>,
}

impl SegmentPayload {
    /// Creates a new [SegmentPayload] from a rewindable, cloneable [Payload].
    pub fn new<P>(payload: Payload<P>) -> Self
    where
        P: StreamingSource + Seek + Clone + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(payload),
        }
    }
}

impl Clone for SegmentPayload {
    /// Clones the payload at its current read offset with an independent cursor.
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use static_assertions::assert_impl_all;
    use std::io::{Error as IoError, ErrorKind};

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
        assert_impl_all!(SegmentPayload: Clone, std::fmt::Debug, Send, Sync);
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

        // Advancing `payload` does not affect `cloned`, which retains its own cursor at offset 0.
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
    async fn segment_payload_clone_mid_stream() -> anyhow::Result<()> {
        let mut payload = SegmentPayload::new(Payload::from(vec![
            Bytes::from_static(b"hello "),
            Bytes::from_static(b"world"),
        ]));

        // Advance past the first chunk before cloning.
        assert_eq!(
            payload.next().await.transpose()?,
            Some(Bytes::from_static(b"hello "))
        );

        // Cloning mid-stream preserves the current offset rather than rewinding to 0,
        // while giving both handles independent cursors.
        let mut mid_clone = payload.clone();
        assert_eq!(
            mid_clone.next().await.transpose()?,
            Some(Bytes::from_static(b"world"))
        );
        assert_eq!(mid_clone.next().await.transpose()?, None);

        assert_eq!(
            payload.next().await.transpose()?,
            Some(Bytes::from_static(b"world"))
        );
        assert_eq!(payload.next().await.transpose()?, None);

        // Seeking one handle back to 0 does not affect the already-exhausted clone.
        payload.seek(0).await?;
        assert_eq!(
            payload.next().await.transpose()?,
            Some(Bytes::from_static(b"hello "))
        );
        assert_eq!(mid_clone.next().await.transpose()?, None);
        Ok(())
    }

    #[tokio::test]
    async fn segment_payload_maps_errors() {
        use std::error::Error as _;

        let mut payload = SegmentPayload::new(Payload::from_stream(FailingSource {
            fail_next: true,
            fail_size_hint: true,
            fail_seek: true,
        }));

        let next_err = payload.next().await.transpose().unwrap_err();
        assert!(next_err.is_serialization(), "{next_err:?}");
        assert_eq!(
            next_err
                .source()
                .and_then(|e| e.downcast_ref::<IoError>())
                .map(IoError::kind),
            Some(ErrorKind::BrokenPipe)
        );

        let hint_err = payload.size_hint().await.unwrap_err();
        assert!(hint_err.is_serialization(), "{hint_err:?}");
        assert_eq!(
            hint_err
                .source()
                .and_then(|e| e.downcast_ref::<IoError>())
                .map(IoError::kind),
            Some(ErrorKind::Other)
        );

        let seek_err = payload.seek(0).await.unwrap_err();
        assert!(seek_err.is_serialization(), "{seek_err:?}");
        assert_eq!(
            seek_err
                .source()
                .and_then(|e| e.downcast_ref::<IoError>())
                .map(IoError::kind),
            Some(ErrorKind::InvalidInput)
        );
    }
}
