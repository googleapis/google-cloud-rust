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

use super::active_read::ActiveRead;
use super::range_reader::RangeReader;
use crate::error::ReadError;
use crate::model::Object;
use crate::model_ext::ReadRange;
use crate::read_object::ReadObjectResponse;
use crate::stub::ObjectDescriptor;
use crate::{Error, Result};
use http::HeaderMap;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

type ReadResult<T> = std::result::Result<T, ReadError>;

#[derive(Debug)]
pub struct ObjectDescriptorTransport {
    object: Arc<Object>,
    headers: HeaderMap,
    tx: Sender<ActiveRead>,
}

impl ObjectDescriptorTransport {
    pub async fn new<T>(mut connector: super::connector::Connector<T>) -> Result<Self>
    where
        T: super::Client + Clone + Sync,
        <T as super::Client>::Stream: super::TonicStreaming + Send + Sync,
    {
        use gaxi::prost::FromProto;

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let (initial, headers, connection) = connector.connect(Vec::new()).await?;
        let object = FromProto::cnv(initial.metadata.ok_or_else(|| {
            Error::deser("initial response in bidi read must contain object metadata")
        })?)
        .expect("transforming from proto Object never fails");
        let object = Arc::new(object);
        let worker = super::worker::Worker::new(connector);
        let _handle = tokio::spawn(worker.run(connection, rx));
        Ok(Self {
            object,
            headers,
            tx,
        })
    }
}

impl ObjectDescriptor for ObjectDescriptorTransport {
    fn object(&self) -> Object {
        // self.object is Arc<Object>, calling `.clone()` directly would clone
        // the `Arc<>`. Calling `.as_ref()` returns `&Object` and then we can
        // clone.
        self.object.as_ref().clone()
    }

    async fn read_range(&self, range: ReadRange) -> ReadObjectResponse {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let range = ActiveRead::new(tx, range.0);
        self.tx
            .send(range)
            .await
            .expect("worker never exits while ObjectDescriptor is live");
        ReadObjectResponse::new(Box::new(RangeReader::new(
            rx,
            self.object.clone(),
            self.tx.clone(),
        )))
    }

    fn headers(&self) -> HeaderMap {
        self.headers.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::super::mocks::{MockTestClient, mock_connector};
    use super::*;
    use crate::google::storage::v2::{
        BidiReadHandle, BidiReadObjectResponse, ChecksummedData, Object as ProtoObject,
        ObjectRangeData, ReadRange as ProtoRange,
    };
    use crate::storage::bidi::tests::{permanent_error, proto_range};

    #[tokio::test]
    async fn success() -> anyhow::Result<()> {
        const LEN: i64 = 42;
        let (connect_tx, connect_rx) =
            tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(8);
        let initial = BidiReadObjectResponse {
            metadata: Some(ProtoObject {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                generation: 123456,
                ..ProtoObject::default()
            }),
            read_handle: Some(BidiReadHandle {
                handle: bytes::Bytes::from_static(b"test-read-handle"),
            }),
            ..BidiReadObjectResponse::default()
        };
        connect_tx.send(Ok(initial)).await?;
        let connect_stream = tonic::Response::from(connect_rx);

        // Save the receivers sent to the mock connector.
        let receivers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let save = receivers.clone();
        let mut mock = MockTestClient::new();
        mock.expect_start().return_once(move |_, _, rx, _, _, _| {
            save.lock().expect("never poisoned").push(rx);
            Ok(Ok(connect_stream))
        });
        let connector = mock_connector(mock);

        let transport = ObjectDescriptorTransport::new(connector).await?;
        let want = Object::new()
            .set_bucket("projects/_/buckets/test-bucket")
            .set_name("test-object")
            .set_generation(123456);
        assert_eq!(transport.object(), want, "{transport:?}");

        // At this point the mock has executed and we can fetch the data it
        // captured:
        let mut connect_rx = {
            let mut guard = receivers.lock().expect("never poisoned");
            let rx = guard.pop().expect("at least one receiver");
            assert!(guard.is_empty(), "{receivers:?}");
            rx
        };
        let request = connect_rx.recv().await.expect("the initial request");
        assert!(request.read_object_spec.is_some(), "{request:?}");

        let mut reader = transport.read_range(ReadRange::segment(100, 200)).await;

        let request = connect_rx.recv().await.expect("the read request");
        let range_request = request.read_ranges.first();
        assert_eq!(range_request, Some(&proto_range(100, 200)), "{request:?}");

        let content = bytes::Bytes::from_owner(String::from_iter((0..LEN).map(|_| 'x')));
        let response = BidiReadObjectResponse {
            object_data_ranges: vec![ObjectRangeData {
                checksummed_data: Some(ChecksummedData {
                    content: content.clone(),
                    ..ChecksummedData::default()
                }),
                read_range: Some(ProtoRange {
                    read_offset: 100,
                    read_length: LEN,
                    read_id: 0,
                }),
                range_end: true,
            }],
            ..BidiReadObjectResponse::default()
        };
        connect_tx.send(Ok(response)).await?;

        let got = reader.next().await.transpose()?;
        assert_eq!(got, Some(content));
        // Because `range_end` is true, the reader should be closed.
        let got = reader.next().await.transpose()?;
        assert!(got.is_none(), "{got:?}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_range_error() -> anyhow::Result<()> {
        use std::error::Error as _;

        let (connect_tx, connect_rx) =
            tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(8);
        let initial = BidiReadObjectResponse {
            metadata: Some(ProtoObject {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                generation: 123456,
                ..ProtoObject::default()
            }),
            read_handle: Some(BidiReadHandle {
                handle: bytes::Bytes::from_static(b"test-read-handle"),
            }),
            ..BidiReadObjectResponse::default()
        };
        connect_tx.send(Ok(initial)).await?;
        let connect_stream = tonic::Response::from(connect_rx);

        // Save the receivers sent to the mock connector.
        let mut mock = MockTestClient::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(connect_stream)));
        let connector = mock_connector(mock);

        let transport = ObjectDescriptorTransport::new(connector).await?;
        let want = Object::new()
            .set_bucket("projects/_/buckets/test-bucket")
            .set_name("test-object")
            .set_generation(123456);
        assert_eq!(transport.object(), want, "{transport:?}");

        let mut existing = transport.read_range(ReadRange::segment(100, 200)).await;

        // Close the mock connection with an unrecoverable error.
        // This should terminate the worker task, and the object descriptor
        // should stop accepting requests.
        connect_tx
            .send(Err(tonic::Status::permission_denied("uh-oh")))
            .await?;

        // Wait for the worker to stop the main loop and drop the transport.tx receiver.
        let err = existing.next().await.transpose().unwrap_err();
        let source = err.source().and_then(|e| e.downcast_ref::<ReadError>());
        assert!(
            matches!(
                source,
                Some(ReadError::UnrecoverableBidiReadInterrupt(e)) if e.status().is_some()
            ),
            "{err:?}"
        );
        let got = existing.next().await;
        assert!(got.is_none(), "{got:?}");

        // Close the mock I/O stream. From this point the `transport.read_range()`
        // calls should fail.
        drop(connect_tx);

        // Now we know this call will fail, and we verify we get the correct
        // error.
        let mut reader = transport.read_range(ReadRange::segment(100, 200)).await;
        let err = reader.next().await.transpose().unwrap_err();
        let source = err.source().and_then(|e| e.downcast_ref::<ReadError>());
        assert!(
            matches!(
                source,
                Some(ReadError::UnrecoverableBidiReadInterrupt(e)) if e.status().is_some()
            ),
            "{err:?}"
        );
        let got = reader.next().await;
        assert!(got.is_none(), "{got:?}");

        Ok(())
    }

    #[tokio::test]
    async fn connect_error() -> anyhow::Result<()> {
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Err(permanent_error()));
        let connector = mock_connector(mock);

        let err = ObjectDescriptorTransport::new(connector).await.unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn deser_error() -> anyhow::Result<()> {
        let (connect_tx, connect_rx) =
            tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(8);
        let initial = BidiReadObjectResponse {
            metadata: None,
            read_handle: Some(BidiReadHandle {
                handle: bytes::Bytes::from_static(b"test-read-handle"),
            }),
            ..BidiReadObjectResponse::default()
        };
        connect_tx.send(Ok(initial)).await?;
        let connect_stream = tonic::Response::from(connect_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Ok(Ok(connect_stream)));
        let connector = mock_connector(mock);

        let err = ObjectDescriptorTransport::new(connector).await.unwrap_err();
        assert!(err.is_deserialization(), "{err:?}");
        Ok(())
    }
}
