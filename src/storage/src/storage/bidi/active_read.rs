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

use super::remaining_range::RemainingRange;
use crate::error::{ChecksumMismatch, ReadError};
use crate::google::storage::v2::{ChecksummedData, ReadRange as ProtoRange};
use crate::model_ext::RequestedRange;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

type ReadResult<T> = std::result::Result<T, ReadError>;

#[derive(Debug)]
pub(crate) struct ActiveRead {
    state: RemainingRange,
    sender: Sender<Result<bytes::Bytes, ReadError>>,
}

impl ActiveRead {
    pub(super) fn new(
        sender: Sender<Result<bytes::Bytes, ReadError>>,
        requested_range: RequestedRange,
    ) -> Self {
        Self {
            sender,
            state: RemainingRange::Requested(requested_range),
        }
    }

    pub(super) async fn handle_data(
        &mut self,
        data: Option<ChecksummedData>,
        received_range: ProtoRange,
        end: bool,
    ) -> ReadResult<()> {
        self.state.update(received_range)?;
        let Some(data) = data else {
            return self.state.handle_empty(end);
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
        // Ignore errors, the application can drop a pending range at any time.
        let _ = self.sender.send(Ok(data.content)).await;
        Ok(())
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
        self.state.as_proto(id)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{permanent_error, proto_range};
    use super::*;
    use crate::model_ext::ReadRange;

    #[tokio::test]
    async fn normal() -> anyhow::Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let requested = ReadRange::all().0;
        let mut range = ActiveRead::new(tx, requested);
        let response = proto_range(0, 25);
        let content = String::from_iter((0..25).map(|_| '0'));
        let data = ChecksummedData {
            content: bytes::Bytes::from_owner(content.clone()),
            ..ChecksummedData::default()
        };
        range.handle_data(Some(data), response, false).await?;
        assert_eq!(range.state.as_proto(0), proto_range(25, 0));

        let recv = rx.recv().await;
        assert!(matches!(recv, Some(Ok(ref b)) if *b == content), "{recv:?}");

        assert_eq!(range.state.as_proto(0), proto_range(25, 0), "{range:?}");

        rx.close();
        let response = proto_range(25, 25);
        let content = String::from_iter((0..25).map(|_| '1'));
        let data = ChecksummedData {
            content: bytes::Bytes::from_owner(content.clone()),
            crc32c: Some(crc32c::crc32c(content.as_bytes())),
        };
        range.handle_data(Some(data), response, false).await?;
        assert_eq!(range.state.as_proto(0), proto_range(50, 0));

        Ok(())
    }

    #[tokio::test]
    async fn unexpected_empty_read() -> anyhow::Result<()> {
        // An empty response, that is also the end of the range, while the
        // range has a known, non-zero number of bytes remaining is a problem.
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let requested = ReadRange::segment(0, 100).0;
        let mut range = ActiveRead::new(tx, requested);
        let response = proto_range(0, 0);
        let result = range.handle_data(None, response, true).await;
        assert!(
            matches!(result, Err(ReadError::ShortRead(ref l)) if *l == 100),
            "result={result:?} {range:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn harmless_empty_read() -> anyhow::Result<()> {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let requested = ReadRange::offset(100).0;
        let mut range = ActiveRead::new(tx, requested);
        let proto_range = ProtoRange {
            read_offset: 100,
            read_length: 0,
            read_id: 0,
        };
        range.handle_data(None, proto_range, false).await?;
        Ok(())
    }

    #[tokio::test]
    async fn checksum_mismatch() -> anyhow::Result<()> {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut range = ActiveRead::new(tx, ReadRange::all().0);
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
            .handle_data(Some(data), proto_range, false)
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
        let mut range = ActiveRead::new(tx, ReadRange::all().0);
        let proto_range = ProtoRange {
            read_offset: 25,
            read_length: 25,
            read_id: 0,
        };
        let content = String::from_iter((0..25).map(|_| '0'));
        let data = ChecksummedData {
            content: bytes::Bytes::from_owner(content.clone()),
            ..ChecksummedData::default()
        };
        let err = range
            .handle_data(Some(data), proto_range, false)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ReadError::OutOfOrderBidiResponse{ ref got, ref expected } if *got == 25 && *expected == 0),
            "err={err:?} range={range:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_error() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let requested = ReadRange::all().0;
        let mut range = ActiveRead::new(tx, requested);
        range
            .handle_error(ReadError::MissingRangeInBidiResponse)
            .await;
        rx.close();
        range
            .handle_error(ReadError::MissingRangeInBidiResponse)
            .await;
    }

    #[tokio::test]
    async fn interrupted() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let requested = ReadRange::all().0;
        let mut range = ActiveRead::new(tx, requested);
        let error = Arc::new(permanent_error());
        range.interrupted(error.clone()).await;
        rx.close();
        range.interrupted(error.clone()).await;
    }
}
