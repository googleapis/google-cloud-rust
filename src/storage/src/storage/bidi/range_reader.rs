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

use super::pending_range::PendingRange;
use crate::Error;
use crate::model::Object;
use crate::read_object::dynamic::ReadObjectResponse;
use crate::{error::ReadError, model_ext::ObjectHighlights};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

/// Read the data from a [ObjectDescriptor][super::ObjectDescriptor] range.
///
/// This type is used to stream an open object descriptor range.
#[derive(Debug)]
pub struct RangeReader {
    inner: Receiver<Result<bytes::Bytes, ReadError>>,
    object: Arc<Object>,
    // Unused, holding to a copy prevents the worker task from terminating
    // early.
    _tx: Sender<PendingRange>,
}

impl RangeReader {
    /// Create a new instance.
    ///
    /// This constructor is useful when mocking `ObjectDescriptor`.
    pub fn new(
        inner: Receiver<Result<bytes::Bytes, ReadError>>,
        object: Arc<Object>,
        tx: Sender<PendingRange>,
    ) -> Self {
        Self {
            inner,
            object,
            _tx: tx,
        }
    }
}

#[async_trait::async_trait]
impl ReadObjectResponse for RangeReader {
    fn object(&self) -> ObjectHighlights {
        ObjectHighlights {
            generation: self.object.generation,
            metageneration: self.object.metageneration,
            size: self.object.size,
            content_encoding: self.object.content_encoding.clone(),
            checksums: self.object.checksums.clone(),
            storage_class: self.object.storage_class.clone(),
            content_language: self.object.content_language.clone(),
            content_type: self.object.content_type.clone(),
            content_disposition: self.object.content_disposition.clone(),
            etag: self.object.etag.clone(),
        }
    }

    async fn next(&mut self) -> Option<crate::Result<bytes::Bytes>> {
        let msg = self.inner.recv().await?;
        Some(msg.map_err(Error::io))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::permanent_error;
    use super::*;
    use crate::model::{Object, ObjectChecksums};
    use std::error::Error as _;

    #[tokio::test]
    async fn object() -> anyhow::Result<()> {
        let object = Object::new()
            .set_generation(123456)
            .set_metageneration(234567)
            .set_size(1024)
            .set_checksums(ObjectChecksums::new().set_crc32c(456789_u32))
            .set_etag("test-etag")
            .set_storage_class("STANDARD")
            .set_content_encoding("content-encoding")
            .set_content_language("content-language")
            .set_content_type("content-type")
            .set_content_disposition("content-disposition");
        let object = Arc::new(object);

        let (tx, inner) = tokio::sync::mpsc::channel(1);
        let (pending_tx, mut pending_rx) = tokio::sync::mpsc::channel(100);

        let mut reader = RangeReader::new(inner, object.clone(), pending_tx);
        let got = reader.object();
        let want = ObjectHighlights {
            generation: 123456,
            metageneration: 234567,
            size: 1024,
            checksums: Some(ObjectChecksums::new().set_crc32c(456789_u32)),
            etag: "test-etag".into(),
            storage_class: "STANDARD".into(),
            content_encoding: "content-encoding".into(),
            content_language: "content-language".into(),
            content_type: "content-type".into(),
            content_disposition: "content-disposition".into(),
        };
        assert_eq!(got, want);

        let data = bytes::Bytes::from_static(b"the quick brown fox jumps over the lazy dog");
        tx.send(Ok(data.clone())).await?;
        let got = reader.next().await;
        assert!(matches!(got, Some(Ok(ref d)) if *d == data), "{got:?}");

        let error = ReadError::UnrecoverableBidiReadInterrupt(Arc::new(permanent_error()));
        tx.send(Err(error)).await?;
        let got = reader.next().await;
        assert!(matches!(got, Some(Err(_))), "{got:?}");
        let got = got.unwrap().unwrap_err();
        assert!(got.is_io(), "{got:?}");
        let source = got.source().and_then(|e| e.downcast_ref::<ReadError>());
        assert!(
            matches!(source, Some(ReadError::UnrecoverableBidiReadInterrupt(_))),
            "{source:?}"
        );

        drop(tx);

        let got = reader.next().await;
        assert!(got.is_none(), "{got:?}");

        drop(reader);
        let done = pending_rx.recv().await;
        assert!(done.is_none(), "{done:?}");

        Ok(())
    }
}
