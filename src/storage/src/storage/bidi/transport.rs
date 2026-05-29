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
use crate::model::Object;
use crate::model_ext::{ReadRange, RequestedRange};
use crate::read_object::ReadObjectResponse;
use crate::stub::ObjectDescriptor;
use crate::{Error, Result};
use http::HeaderMap;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub struct ObjectDescriptorTransport {
    object: Arc<Object>,
    headers: HeaderMap,
    tx: Sender<ActiveRead>,
}

impl ObjectDescriptorTransport {
    pub async fn new<T>(
        mut connector: super::connector::Connector<T>,
        ranges: Vec<ReadRange>,
    ) -> Result<(Self, Vec<ReadObjectResponse>)>
    where
        T: super::Client + Clone + Sync,
        <T as super::Client>::Stream: super::TonicStreaming + Send + Sync,
    {
        use gaxi::prost::FromProto;

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // Convert the requested `ReadRange`s to proto format.
        let requested_ranges = ranges.into_iter().map(|r| r.0).collect::<Vec<_>>();
        let proto_ranges = requested_ranges
            .iter()
            .enumerate()
            .map(|(id, r)| r.as_proto(id as i64))
            .collect::<Vec<_>>();

        // Establish the gRPC connection and extract object metadata.
        let (mut initial, headers, connection) = connector.connect(proto_ranges).await?;
        let object = FromProto::cnv(initial.metadata.take().ok_or_else(|| {
            Error::deser("initial response in bidi read must contain object metadata")
        })?)
        .expect("transforming from proto Object never fails");
        let object = Arc::new(object);

        // Construct the initial `ActiveRead`s and their corresponding `ReadObjectResponse` streams.
        let (actives, responses) = requested_ranges
            .into_iter()
            .map(|r| Self::map_range(r, &tx, &object))
            .unzip();

        // Process any data ranges in the initial response and spawn the worker task.
        let mut worker = super::worker::Worker::new(connector, actives);
        worker
            .handle_response_success(initial)
            .await
            .map_err(Error::io)?;
        let _handle = tokio::spawn(worker.run(connection, rx));

        Ok((
            Self {
                object,
                headers,
                tx,
            },
            responses,
        ))
    }

    /// Builds the `ActiveRead`-`ReadObjectResponse` pair corresponding
    /// to the given `RequestedRange`.
    ///
    /// The returned `ActiveRead` needs to be registered with the Worker
    /// either via `Worker::new` or via `ObjectDescriptorTransport.tx.send` (once
    /// the worker is running); otherwise, the corresponding `ReadObjectResponse` will
    /// never receive any bytes.
    fn map_range(
        range: RequestedRange,
        requests: &Sender<ActiveRead>,
        object: &Arc<Object>,
    ) -> (ActiveRead, ReadObjectResponse) {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let active = ActiveRead::new(tx, range);
        let reader = RangeReader::new(rx, object.clone(), requests.clone());
        (active, ReadObjectResponse::new(Box::new(reader)))
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
        let (active, response) = Self::map_range(range.0, &self.tx, &self.object);
        self.tx
            .send(active)
            .await
            .expect("worker never exits while ObjectDescriptor is live");
        response
    }

    fn headers(&self) -> HeaderMap {
        self.headers.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::super::connector::Connector;
    use super::super::mocks::{MockTestClient, SharedMockClient, mock_connector};
    use super::*;
    use crate::error::ReadError;
    use crate::google::storage::v2::{
        BidiReadHandle, BidiReadObjectRequest, BidiReadObjectResponse, ChecksummedData,
        Object as ProtoObject, ObjectRangeData, ReadRange as ProtoRange,
    };
    use crate::storage::bidi::tests::{permanent_error, proto_range, proto_range_id};
    use gaxi::grpc::tonic::{Response as TonicResponse, Result as TonicResult, Status};
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc::{Receiver, Sender, channel};

    #[tokio::test]
    async fn success() -> anyhow::Result<()> {
        const LEN: i64 = 42;
        let (connector, test_context) = BidiTestContext::new();

        // Send initial metadata/handshake response
        test_context.connect_tx.send(Ok(base_response())).await?;

        // Create the transport client
        let (transport, _) = ObjectDescriptorTransport::new(connector, Vec::new()).await?;
        let want = Object::new()
            .set_bucket("projects/_/buckets/test-bucket")
            .set_name("test-object")
            .set_generation(123456);
        assert_eq!(transport.object(), want, "{transport:?}");

        // Verify initial client connection request
        let mut connect_rx = test_context.take_receiver();
        let request = connect_rx.recv().await.expect("the initial request");
        assert!(request.read_object_spec.is_some(), "{request:?}");

        // Issue a read_range call
        let mut reader = transport.read_range(ReadRange::segment(100, 200)).await;

        // Verify that transport sent the range request downstream
        let request = connect_rx.recv().await.expect("the read request");
        let range_request = request.read_ranges.first();
        assert_eq!(range_request, Some(&proto_range(100, 200)), "{request:?}");

        // Prepare and transmit simulated server data response
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
        test_context.connect_tx.send(Ok(response)).await?;

        // Read simulated content and confirm stream closure
        let got = reader.next().await.transpose()?;
        assert_eq!(got, Some(content));
        // Because `range_end` is true, the reader should be closed.
        let got = reader.next().await.transpose()?;
        assert!(got.is_none(), "{got:?}");

        Ok(())
    }

    #[tokio::test]
    async fn success_with_ranges() -> anyhow::Result<()> {
        const LEN: i64 = 42;
        let (connector, test_context) = BidiTestContext::new();

        // Send initial metadata/handshake response
        test_context.connect_tx.send(Ok(base_response())).await?;

        let ranges = vec![
            ReadRange::segment(100, LEN as u64),
            ReadRange::segment(200, LEN as u64),
            ReadRange::segment(300, LEN as u64),
        ];
        let (_transport, readers) = ObjectDescriptorTransport::new(connector, ranges).await?;
        assert_eq!(readers.len(), 3);

        // Verify initial client connection requests contain range requests
        let mut connect_rx = test_context.take_receiver();
        let request = connect_rx.recv().await.expect("the initial request");
        assert!(request.read_object_spec.is_some(), "{request:?}");
        assert_eq!(
            request.read_ranges,
            vec![
                proto_range_id(100, LEN, 0),
                proto_range_id(200, LEN, 1),
                proto_range_id(300, LEN, 2),
            ],
            "{request:?}"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_range_error() -> anyhow::Result<()> {
        use std::error::Error as _;

        let (connector, test_context) = BidiTestContext::new();

        // Send initial metadata/handshake response
        test_context.connect_tx.send(Ok(base_response())).await?;

        let (transport, _) = ObjectDescriptorTransport::new(connector, Vec::new()).await?;
        let want = Object::new()
            .set_bucket("projects/_/buckets/test-bucket")
            .set_name("test-object")
            .set_generation(123456);
        assert_eq!(transport.object(), want, "{transport:?}");

        let mut existing = transport.read_range(ReadRange::segment(100, 200)).await;

        // Close the mock connection with an unrecoverable error.
        // This should terminate the worker task, and the object descriptor
        // should stop accepting requests.
        test_context
            .connect_tx
            .send(Err(Status::permission_denied("uh-oh")))
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
        drop(test_context.connect_tx);

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

        let err = ObjectDescriptorTransport::new(connector, Vec::new())
            .await
            .unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn deser_error() -> anyhow::Result<()> {
        let (connector, test_context) = BidiTestContext::new();
        let mut initial = base_response();
        initial.metadata = None;
        test_context.connect_tx.send(Ok(initial)).await?;

        let err = ObjectDescriptorTransport::new(connector, Vec::new())
            .await
            .unwrap_err();
        assert!(err.is_deserialization(), "{err:?}");
        Ok(())
    }

    fn base_response() -> BidiReadObjectResponse {
        BidiReadObjectResponse {
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
        }
    }

    struct BidiTestContext {
        connect_tx: Sender<TonicResult<BidiReadObjectResponse>>,
        receivers: Arc<Mutex<Vec<Receiver<BidiReadObjectRequest>>>>,
    }

    impl BidiTestContext {
        fn new() -> (Connector<SharedMockClient>, Self) {
            let (connect_tx, connect_rx) = channel::<TonicResult<BidiReadObjectResponse>>(8);
            let connect_stream = TonicResponse::from(connect_rx);

            // Save the receivers sent to the mock connector.
            let receivers = Arc::new(Mutex::new(Vec::new()));
            let save = receivers.clone();
            let mut mock = MockTestClient::new();
            mock.expect_start().return_once(move |_, _, rx, _, _, _| {
                save.lock().expect("never poisoned").push(rx);
                Ok(Ok(connect_stream))
            });
            let connector = mock_connector(mock);

            (
                connector,
                Self {
                    connect_tx,
                    receivers,
                },
            )
        }

        /// Retrieves the single captured request receiver from the mock setup.
        ///
        /// Call this once the mock connector has executed to fetch the request stream captured by the mock.
        fn take_receiver(&self) -> Receiver<BidiReadObjectRequest> {
            let mut guard = self.receivers.lock().expect("never poisoned");
            let rx = guard.pop().expect("at least one captured receiver");
            assert!(
                guard.is_empty(),
                "expected exactly one active client receiver"
            );
            rx
        }
    }
}
