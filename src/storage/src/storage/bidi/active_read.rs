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
    sender: Sender<ReadResult<bytes::Bytes>>,
}

impl ActiveRead {
    pub(super) fn new(
        sender: Sender<ReadResult<bytes::Bytes>>,
        requested_range: RequestedRange,
    ) -> Self {
        Self {
            sender,
            state: RemainingRange::Requested(requested_range),
        }
    }

    pub(super) fn handle_data(
        &mut self,
        data: Option<ChecksummedData>,
        received_range: ProtoRange,
        end: bool,
    ) -> ReadResult<Handler> {
        self.state.update(received_range)?;
        let Some(data) = data else {
            self.state.handle_empty(end)?;
            return Ok(Handler(InnerHandler::NoData));
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
        Ok(Handler(InnerHandler::Send(
            self.sender.clone(),
            data.content,
        )))
    }

    pub(super) async fn handle_error(&mut self, error: ReadError) {
        if let Err(e) = self.sender.send(Err(error)).await {
            tracing::error!("cannot notify reader (dropped?) about: {e:?}");
        }
    }

    pub(super) async fn interrupted(&mut self, error: Arc<crate::Error>) {
        if let Err(e) = self
            .sender
            .send(Err(ReadError::UnrecoverableBidiReadInterrupt(error)))
            .await
        {
            tracing::error!("cannot notify reader (dropped?) about: {e:?}");
        }
    }

    pub(super) fn as_proto(&self, id: i64) -> ProtoRange {
        self.state.as_proto(id)
    }
}

#[derive(Debug)]
pub struct Handler(InnerHandler);
impl Handler {
    pub async fn send(self) {
        match self.0 {
            InnerHandler::NoData => {}
            InnerHandler::Send(tx, data) => {
                // Ignore errors, the application can drop the reader (which
                // holds the other side of the `tx` channel) at any time.
                let _ = tx.send(Ok(data)).await;
            }
        }
    }
}

#[derive(Debug)]
enum InnerHandler {
    NoData,
    Send(Sender<ReadResult<bytes::Bytes>>, bytes::Bytes),
}

#[cfg(test)]
mod tests {
    use super::super::tests::{permanent_error, proto_range};
    use super::*;
    use crate::model_ext::ReadRange;
    use std::sync::Mutex;
    use tracing::Subscriber;

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
        let handler = range.handle_data(Some(data), response, false)?;
        assert_eq!(range.as_proto(0), proto_range(25, 0));
        assert!(
            matches!(handler, Handler(InnerHandler::Send(_, ref data)) if *data == content),
            "{handler:?}"
        );
        handler.send().await;

        let recv = rx.recv().await;
        assert!(matches!(recv, Some(Ok(ref b)) if *b == content), "{recv:?}");

        rx.close();
        let response = proto_range(25, 25);
        let content = String::from_iter((0..25).map(|_| '1'));
        let data = ChecksummedData {
            content: bytes::Bytes::from_owner(content.clone()),
            crc32c: Some(crc32c::crc32c(content.as_bytes())),
        };
        let _ = range.handle_data(Some(data), response, false)?;
        assert_eq!(range.as_proto(0), proto_range(50, 0));

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
        let result = range.handle_data(None, response, true);
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
        let handler = range.handle_data(None, proto_range, false)?;
        assert!(
            matches!(handler, Handler(InnerHandler::NoData)),
            "{handler:?}"
        );
        handler.send().await; // Just for coverage, it is a no-op.
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
            .unwrap_err();
        assert!(
            matches!(err, ReadError::InvalidBidiStreamingReadResponse(_)),
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
            .handle_error(ReadError::InvalidBidiStreamingReadResponse(
                "test-only".into(),
            ))
            .await;
        let got = rx
            .recv()
            .await
            .expect("the active reader propagates error messages")
            .unwrap_err();
        assert!(
            matches!(got, ReadError::InvalidBidiStreamingReadResponse(_)),
            "{got:?}"
        );

        // Sending errors on closed stream does not panic and gets logged.
        let capture = CaptureEvents::new();
        let _guard = tracing::subscriber::set_default(capture.clone());
        rx.close();
        range
            .handle_error(ReadError::InvalidBidiStreamingReadResponse(
                "test-only".into(),
            ))
            .await;
        let events = capture.events();
        let got = events
            .iter()
            .find(|m| m.contains("cannot notify reader (dropped?) about: "));
        assert!(got.is_some(), "{events:?}");
    }

    #[tokio::test]
    async fn interrupted() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let requested = ReadRange::all().0;
        let mut range = ActiveRead::new(tx, requested);
        let error = Arc::new(permanent_error());
        range.interrupted(error.clone()).await;
        let got = rx
            .recv()
            .await
            .expect("the active reader propagates error messages")
            .unwrap_err();
        assert!(
            matches!(got, ReadError::UnrecoverableBidiReadInterrupt(ref e) if e.status() == permanent_error().status()),
            "{got:?}"
        );

        // Sending errors on closed stream does not panic and gets logged.
        let capture = CaptureEvents::new();
        let _guard = tracing::subscriber::set_default(capture.clone());
        rx.close();
        range.interrupted(error.clone()).await;
        let events = capture.events();
        let got = events
            .iter()
            .find(|m| m.contains("cannot notify reader (dropped?) about: "));
        assert!(got.is_some(), "{events:?}");
    }

    #[derive(Clone, Debug)]
    struct CaptureEvents {
        captured: Arc<Mutex<Vec<String>>>,
    }

    impl CaptureEvents {
        pub fn new() -> Self {
            Self {
                captured: Arc::new(Mutex::new(Vec::new())),
            }
        }
        pub fn events(&self) -> Vec<String> {
            self.captured.lock().expect("never poisoned").clone()
        }
    }

    use tracing::span;

    impl Subscriber for CaptureEvents {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.is_event()
        }
        fn event(&self, event: &tracing::Event<'_>) {
            let mut guard = self.captured.lock().expect("never poisoned");
            guard.push(format!("{event:?}"));
        }
        fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
            unimplemented!("not interested in spans")
        }
        fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {
            unimplemented!("not interested in spans")
        }
        fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {
            unimplemented!("not interested in spans")
        }
        fn enter(&self, _span: &span::Id) {
            unimplemented!("not interested in spans")
        }
        fn exit(&self, _span: &span::Id) {
            unimplemented!("not interested in spans")
        }
    }
}
