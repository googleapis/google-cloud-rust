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

use crate::error::{ChecksumMismatch, ReadError};
use crate::google::storage::v2::{ChecksummedData, ReadRange as ProtoRange};
use crate::model_ext::ReadRange;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

type ReadResult<T> = std::result::Result<T, ReadError>;

#[derive(Debug)]
pub(crate) struct PendingRange {
    offset: i64,
    limit: i64,
    sender: Sender<Result<bytes::Bytes, ReadError>>,
}

impl PendingRange {
    pub(super) fn new(
        sender: Sender<Result<bytes::Bytes, ReadError>>,
        range: ReadRange,
        size: i64,
    ) -> Self {
        let (offset, limit) = range.normalize(size);
        Self {
            sender,
            offset,
            limit,
        }
    }

    pub(super) async fn handle_data(
        &mut self,
        range: ProtoRange,
        data: Option<ChecksummedData>,
    ) -> ReadResult<()> {
        let Some(data) = data else {
            if self.limit == 0 {
                return Ok(());
            }
            return Err(ReadError::ShortRead(self.limit as u64));
        };
        if let Some(want) = data.crc32c {
            let got = crc32c::crc32c(&data.content);
            if want != got {
                return Err(ReadError::ChecksumMismatch(ChecksumMismatch::Crc32c {
                    got,
                    want,
                }));
            }
        };
        if self.offset == range.read_offset {
            self.offset += range.read_length;
            self.limit -= range.read_length;
            self.limit = self.limit.clamp(0, i64::MAX);
            // Ignore errors, the application can drop a pending range at any
            // time.
            let _ = self.sender.send(Ok(data.content)).await;
            return Ok(());
        }
        Err(ReadError::OutOfOrderBidiResponse {
            got: range.read_offset,
            expected: self.offset,
        })
    }

    pub(super) async fn handle_error(&mut self, error: ReadError) {
        if let Err(e) = self.sender.send(Err(error)).await {
            tracing::error!("cannot notify sender about read error: {e:?}");
        }
    }

    pub(super) async fn interrupted(&mut self, error: Arc<crate::Error>) {
        if let Err(e) = self
            .sender
            .send(Err(ReadError::UnrecoverableBidiReadInterrupt(error)))
            .await
        {
            tracing::error!("cannot notify sender about unrecoverable error: {e:?}");
        }
    }

    pub(super) fn as_proto(&self, id: i64) -> ProtoRange {
        ProtoRange {
            read_id: id,
            read_offset: self.offset,
            read_length: self.limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::permanent_error;
    use super::*;

    #[tokio::test]
    async fn normal() -> anyhow::Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut range = PendingRange::new(tx, ReadRange::all(), 100);
        assert_eq!((range.offset, range.limit), (0, 100));
        let proto_range = ProtoRange {
            read_offset: 0,
            read_length: 25,
            read_id: 0,
        };
        let content = String::from_iter((0..25).map(|_| '0'));
        let data = ChecksummedData {
            content: bytes::Bytes::from_owner(content.clone()),
            ..ChecksummedData::default()
        };
        range.handle_data(proto_range, Some(data)).await?;
        assert_eq!((range.offset, range.limit), (25, 75));

        let recv = rx.recv().await;
        assert!(matches!(recv, Some(Ok(ref b)) if *b == content), "{recv:?}");

        let got = range.as_proto(123);
        let want = ProtoRange {
            read_offset: 25,
            read_length: 75,
            read_id: 123,
        };
        assert_eq!(got, want, "{range:?}");

        rx.close();
        let proto_range = ProtoRange {
            read_offset: 25,
            read_length: 25,
            read_id: 0,
        };
        let content = String::from_iter((0..25).map(|_| '1'));
        let data = ChecksummedData {
            content: bytes::Bytes::from_owner(content.clone()),
            crc32c: Some(crc32c::crc32c(content.as_bytes())),
        };
        range.handle_data(proto_range, Some(data)).await?;
        assert_eq!((range.offset, range.limit), (50, 50));

        Ok(())
    }

    #[tokio::test]
    async fn unexpected_empty_read() -> anyhow::Result<()> {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut range = PendingRange::new(tx, ReadRange::all(), 100);
        let proto_range = ProtoRange {
            read_offset: 0,
            read_length: 0,
            read_id: 0,
        };
        let err = range.handle_data(proto_range, None).await.unwrap_err();
        assert!(
            matches!(err, ReadError::ShortRead(ref l) if *l == 100),
            "err={err:?} {range:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn harmless_empty_read() -> anyhow::Result<()> {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut range = PendingRange::new(tx, ReadRange::offset(100), 100);
        assert_eq!((range.offset, range.limit), (100, 0));
        let proto_range = ProtoRange {
            read_offset: 100,
            read_length: 0,
            read_id: 0,
        };
        range.handle_data(proto_range, None).await?;
        Ok(())
    }

    #[tokio::test]
    async fn checksum_mismatch() -> anyhow::Result<()> {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut range = PendingRange::new(tx, ReadRange::all(), 100);
        let proto_range = ProtoRange {
            read_offset: 0,
            read_length: 25,
            read_id: 0,
        };
        let content = String::from_iter((0..25).map(|_| '0'));
        let actual = crc32c::crc32c(content.as_bytes());
        let data = ChecksummedData {
            content: bytes::Bytes::from_owner(content.clone()),
            crc32c: Some(actual.wrapping_add(1)),
        };
        let err = range
            .handle_data(proto_range, Some(data))
            .await
            .unwrap_err();
        assert!(
            matches!(err, ReadError::ChecksumMismatch(ChecksumMismatch::Crc32c {ref got, ..}) if *got == actual),
            "err={err:?} range={range:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn offset_mismatch() -> anyhow::Result<()> {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut range = PendingRange::new(tx, ReadRange::all(), 100);
        let proto_range = ProtoRange {
            read_offset: 50,
            read_length: 25,
            read_id: 0,
        };
        let content = String::from_iter((0..25).map(|_| '0'));
        let data = ChecksummedData {
            content: bytes::Bytes::from_owner(content.clone()),
            ..ChecksummedData::default()
        };
        let err = range
            .handle_data(proto_range, Some(data))
            .await
            .unwrap_err();
        assert!(
            matches!(err, ReadError::OutOfOrderBidiResponse{ ref got, ref expected } if *got == 50 && *expected == 0),
            "err={err:?} range={range:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_error() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut range = PendingRange::new(tx, ReadRange::all(), 100);
        range
            .handle_error(ReadError::MissingRangeInBidiResponse)
            .await;
        assert_eq!((range.offset, range.limit), (0, 100));
        rx.close();
        range
            .handle_error(ReadError::MissingRangeInBidiResponse)
            .await;
        assert_eq!((range.offset, range.limit), (0, 100));
    }

    #[tokio::test]
    async fn interrupted() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut range = PendingRange::new(tx, ReadRange::all(), 100);
        assert_eq!((range.offset, range.limit), (0, 100));
        let error = Arc::new(permanent_error());
        range.interrupted(error.clone()).await;
        assert_eq!((range.offset, range.limit), (0, 100));
        rx.close();
        range.interrupted(error.clone()).await;
        assert_eq!((range.offset, range.limit), (0, 100));
    }
}
