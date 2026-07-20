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

// TODO(#5716): Lift to shared bidi module

use super::connector::{Connection, Connector};
use super::worker::{UploadIntent, Worker};
use super::{Client, TonicStreaming};
use crate::google::storage::v2::BidiWriteObjectResponse;
use crate::google::storage::v2::ObjectChecksums;
use crate::google::storage::v2::{
    BidiWriteObjectRequest, ChecksummedData, bidi_write_object_request::Data,
    bidi_write_object_response::WriteStatus,
};
use crate::model_ext::{OpenAppendableObjectRequest, ReopenAppendableObjectRequest};
use crate::stub::AppendableObjectWriter;
use crate::{Error, Result};
use bytes::Bytes;
use gaxi::prost::FromProto;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct AppendableObjectWriterTransport {
    tx: Sender<UploadIntent>,
    generation: i64,
    persisted_size: i64,
    write_offset: i64,
    running_crc32c: Option<u32>,
    worker_handle: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl AppendableObjectWriterTransport {
    pub async fn new_open<T>(
        mut connector: Connector<T>,
        req: OpenAppendableObjectRequest,
    ) -> Result<Self>
    where
        T: Client + Clone + Sync + Send + 'static,
        <T as Client>::Stream: TonicStreaming + Send + Sync,
    {
        let (initial, connection) = connector.connect_open(req).await?;
        Self::start_worker(connector, initial, connection, 0)
    }

    pub async fn new_reopen<T>(
        mut connector: Connector<T>,
        req: ReopenAppendableObjectRequest,
    ) -> Result<Self>
    where
        T: Client + Clone + Sync + Send + 'static,
        <T as Client>::Stream: TonicStreaming + Send + Sync,
    {
        let generation = req.generation;
        let (initial, connection) = connector.connect_reopen(req).await?;
        Self::start_worker(connector, initial, connection, generation)
    }

    fn start_worker<T>(
        connector: Connector<T>,
        initial: BidiWriteObjectResponse,
        connection: Connection<<T as Client>::Stream>,
        generation: i64,
    ) -> Result<Self>
    where
        T: Client + Clone + Sync + Send + 'static,
        <T as Client>::Stream: TonicStreaming + Send + Sync,
    {
        let mut persisted_size = 0;
        let mut generation = generation;

        // The GCS backend returns `WriteStatus::Resource` in two scenarios:
        // 1. Immediately upon creating a new appendable stream, where `finalize_time` is absent and the new `generation` is returned.
        // 2. When the stream is fully finalized, where `finalize_time` is present.
        // Otherwise, such as on stream reopens, the backend returns `WriteStatus::PersistedSize`.
        if let Some(WriteStatus::Resource(r)) = initial.write_status.as_ref() {
            if r.finalize_time.is_some() {
                return Err(Error::io("object is already finalized"));
            }
            persisted_size = r.size;
            generation = r.generation;
        } else if let Some(WriteStatus::PersistedSize(s)) = initial.write_status.as_ref() {
            persisted_size = *s;
        }

        // Determine whether we should do a full-object CRC32C checksum
        // calculation. Default is `None`. We will then see if we can establish
        // a valid CRC32C baseline.
        let mut running_crc32c = None;
        if persisted_size == 0 {
            // A brand new object or takeover an existing object with 0 bytes written,
            // so we start with a checksum of 0.
            running_crc32c = Some(0);
        } else if let Some(crc) = initial
            .persisted_data_checksums
            .as_ref()
            .and_then(|c| c.crc32c)
        {
            // Takeover an existing object where the server returns the checksum
            // of the persisted data. The running CRC32C checksum will start with
            // this value.
            running_crc32c = Some(crc);
        }
        // If persisted_size > 0 but the server didn't provide a checksum,
        // we can't reliably continue a running checksum, so it remains `None`.

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let worker = Worker::new(connector);
        let worker_handle = Some(tokio::spawn(worker.run(connection, rx)));

        Ok(Self {
            tx,
            generation,
            persisted_size,
            write_offset: persisted_size,
            running_crc32c,
            worker_handle,
        })
    }

    /// If the background worker task exits early, e.g. due to a gRPC error,
    /// stream disconnection, or panicking, the local `tx` channels close.
    /// Subsequent `tx.send()` calls from the foreground fail with a generic
    /// "mpsc channel closed" error. This helper intercepts the different errors
    /// and returns the most appropriate error to the caller.
    async fn extract_worker_error(&mut self, default_err_message: &str) -> Error {
        if let Some(handle) = self.worker_handle.take() {
            match handle.await {
                Ok(Err(worker_err)) => return worker_err,
                Ok(Ok(())) => {
                    return Error::io("worker terminated successfully but channel was closed");
                }
                Err(join_err) => return Error::io(format!("worker task error: {join_err}")),
            }
        }
        Error::io(default_err_message)
    }

    async fn drop_and_join_worker(mut self) -> Result<()> {
        let handle = self.worker_handle.take();

        // Drop the transport to close the mpsc `tx` channel,
        // triggering EOF on the worker's read queue.
        drop(self);

        if let Some(handle) = handle {
            match handle.await {
                Ok(Err(e)) => return Err(e),
                Ok(Ok(())) => {}
                Err(join_err) => return Err(Error::io(format!("worker task error: {join_err}"))),
            }
        }

        Ok(())
    }
}

impl AppendableObjectWriter for AppendableObjectWriterTransport {
    async fn append(&mut self, chunk: Bytes) -> Result<()> {
        let length = chunk.len() as i64;
        let crc32c = crc32c::crc32c(&chunk);

        let new_running_crc32c = self
            .running_crc32c
            .map(|running| crc32c::crc32c_combine(running, crc32c, chunk.len()));

        let request = BidiWriteObjectRequest {
            write_offset: self.write_offset,
            data: Some(Data::ChecksummedData(ChecksummedData {
                content: chunk,
                crc32c: Some(crc32c),
            })),
            ..BidiWriteObjectRequest::default()
        };

        if let Err(e) = self.tx.send(UploadIntent::Append(request)).await {
            return Err(self.extract_worker_error(&e.to_string()).await);
        }

        self.write_offset += length;
        self.running_crc32c = new_running_crc32c;

        Ok(())
    }

    async fn flush(&mut self) -> Result<i64> {
        let (sender, receiver) = oneshot::channel();
        let request = BidiWriteObjectRequest {
            flush: true,
            state_lookup: true,
            write_offset: self.write_offset,
            ..BidiWriteObjectRequest::default()
        };

        if let Err(e) = self.tx.send(UploadIntent::Flush(request, sender)).await {
            return Err(self.extract_worker_error(&e.to_string()).await);
        }

        let response = match receiver.await {
            Ok(res) => res?,
            Err(e) => return Err(self.extract_worker_error(&e.to_string()).await),
        };
        let size = match response.write_status {
            Some(WriteStatus::PersistedSize(s)) => s,
            Some(WriteStatus::Resource(r)) => r.size,
            None => return Err(Error::io("flush response missing write_status")),
        };
        self.persisted_size = size;

        Ok(size)
    }

    async fn finalize(mut self) -> Result<crate::model::Object> {
        let (sender, receiver) = oneshot::channel();
        let object_checksums = self.running_crc32c.map(|crc| ObjectChecksums {
            crc32c: Some(crc),
            md5_hash: bytes::Bytes::new(),
        });
        let request = BidiWriteObjectRequest {
            finish_write: true,
            flush: true,
            write_offset: self.write_offset,
            object_checksums,
            ..BidiWriteObjectRequest::default()
        };

        if let Err(e) = self.tx.send(UploadIntent::Finalize(request, sender)).await {
            return Err(self.extract_worker_error(&e.to_string()).await);
        }

        let response = match receiver.await {
            Ok(res) => res?,
            Err(e) => return Err(self.extract_worker_error(&e.to_string()).await),
        };
        let resource = match response.write_status {
            Some(WriteStatus::Resource(r)) => r,
            _ => return Err(Error::io("finalize did not return a resource")),
        };
        let object =
            FromProto::cnv(resource).map_err(|_| Error::deser("converting resource to object"))?;

        self.drop_and_join_worker().await?;

        Ok(object)
    }

    async fn close(mut self) -> Result<i64> {
        let size = self.flush().await?;
        self.drop_and_join_worker().await?;

        Ok(size)
    }

    fn generation(&self) -> i64 {
        self.generation
    }

    fn persisted_size(&self) -> i64 {
        self.persisted_size
    }
}

#[cfg(test)]
mod tests {
    use super::super::mocks::{MockTestClient, mock_connector};
    use super::super::tests::permanent_error;
    use super::*;
    use crate::google::storage::v2::{
        BidiWriteObjectResponse, Object, bidi_write_object_response::WriteStatus,
    };
    use crate::model_ext::{OpenAppendableObjectRequest, ReopenAppendableObjectRequest};
    use gaxi::grpc::tonic::{Response as TonicResponse, Result as TonicResult};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn success() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: Some(0),
            generation: 123456,
            persisted_size: 0,
            worker_handle: None,
        };

        let handle = tokio::spawn(async move {
            let mut transport = transport;
            transport.append(bytes::Bytes::from("hello")).await.unwrap();
            transport.flush().await.unwrap();
            transport.finalize().await.unwrap();
        });

        // Assert append.
        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Append(req) = intent {
            assert_eq!(req.write_offset, 0);
            assert!(!req.finish_write);
            if let Some(Data::ChecksummedData(data)) = req.data {
                assert_eq!(data.content.as_ref(), b"hello");
                assert_eq!(data.crc32c, Some(crc32c::crc32c(b"hello")));
            } else {
                panic!("expected ChecksummedData");
            }
        } else {
            panic!("expected Append");
        }

        // Assert flush.
        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Flush(req, sender) = intent {
            assert!(req.flush);
            let resp = BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(5)),
                ..Default::default()
            };
            sender.send(Ok(resp)).unwrap();
        } else {
            panic!("expected Flush");
        }

        // Assert finalize.
        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Finalize(req, sender) = intent {
            assert!(req.finish_write);
            let expected_crc = crc32c::crc32c(b"hello");
            assert_eq!(
                req.object_checksums,
                Some(ObjectChecksums {
                    crc32c: Some(expected_crc),
                    md5_hash: bytes::Bytes::new(),
                })
            );

            let object = Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                size: 5,
                generation: 123456,
                ..Default::default()
            };
            let resp = BidiWriteObjectResponse {
                write_status: Some(WriteStatus::Resource(object)),
                ..Default::default()
            };
            sender.send(Ok(resp)).unwrap();
        } else {
            panic!("expected Finalize");
        }

        handle.await?;
        Ok(())
    }

    #[tokio::test]
    async fn append_error() -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel(1);
        let mut transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: Some(0),
            generation: 123456,
            persisted_size: 0,
            worker_handle: None,
        };

        // Simulate an early stream closure, e.g. worker dying.
        drop(rx);
        let err = transport
            .append(bytes::Bytes::from("hello"))
            .await
            .unwrap_err();
        assert!(err.is_io(), "{err:?}");

        // Assert that state was NOT modified due to the error
        assert_eq!(transport.write_offset, 0);
        assert_eq!(transport.running_crc32c, Some(0));

        Ok(())
    }

    #[tokio::test]
    async fn append_without_running_checksum() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let mut transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: None, // No running crc
            generation: 123456,
            persisted_size: 0,
            worker_handle: None,
        };

        let handle = tokio::spawn(async move {
            transport.append(bytes::Bytes::from("hello")).await.unwrap();
            transport
        });

        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Append(_) = intent {
        } else {
            panic!("expected Append");
        }

        let transport = handle.await?;
        assert_eq!(transport.running_crc32c, None);
        Ok(())
    }

    #[tokio::test]
    async fn flush_resource_response() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let mut transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: Some(0),
            generation: 123456,
            persisted_size: 0,
            worker_handle: None,
        };

        let handle = tokio::spawn(async move { transport.flush().await });

        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Flush(req, sender) = intent {
            assert!(req.flush);
            let object = Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                size: 42,
                generation: 123456,
                ..Default::default()
            };
            let resp = BidiWriteObjectResponse {
                write_status: Some(WriteStatus::Resource(object)),
                ..Default::default()
            };
            sender.send(Ok(resp)).unwrap();
        } else {
            panic!("expected Flush");
        }

        let size = handle.await??;
        assert_eq!(size, 42);

        Ok(())
    }

    #[tokio::test]
    async fn flush_missing_status_error() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let mut transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: Some(0),
            generation: 123456,
            persisted_size: 0,
            worker_handle: None,
        };

        let handle = tokio::spawn(async move { transport.flush().await });

        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Flush(req, sender) = intent {
            assert!(req.flush);
            let resp = BidiWriteObjectResponse {
                write_status: None,
                ..Default::default()
            };
            sender.send(Ok(resp)).unwrap();
        } else {
            panic!("expected Flush");
        }

        let err = handle.await?.unwrap_err();
        assert!(err.is_io(), "{err:?}");
        assert!(
            err.to_string()
                .contains("flush response missing write_status")
        );

        Ok(())
    }

    #[tokio::test]
    async fn close_success() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let worker_handle = tokio::spawn(async { Ok(()) });

        let transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: Some(0),
            generation: 123456,
            persisted_size: 0,
            worker_handle: Some(worker_handle),
        };

        let handle = tokio::spawn(async move { transport.close().await });

        // Assert flush intent triggered by close.
        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Flush(req, sender) = intent {
            assert!(req.flush);
            let resp = BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(17)),
                ..Default::default()
            };
            sender.send(Ok(resp)).unwrap();
        } else {
            panic!("expected Flush");
        }

        let size = handle.await??;
        assert_eq!(size, 17);

        Ok(())
    }

    #[tokio::test]
    async fn close_trailing_error() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let worker_handle =
            tokio::spawn(async { Err(crate::Error::io("trailing metadata EOF error!")) });

        let transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: Some(0),
            generation: 123456,
            persisted_size: 0,
            worker_handle: Some(worker_handle),
        };

        let handle = tokio::spawn(async move { transport.close().await });

        // Assert flush intent triggered by close.
        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Flush(req, sender) = intent {
            assert!(req.flush);
            let resp = BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(17)),
                ..Default::default()
            };
            sender.send(Ok(resp)).unwrap();
        } else {
            panic!("expected Flush");
        }

        // Flush succeeded, but when close awaits the worker handle, it receives
        // the error.
        let err = handle.await?.unwrap_err();
        assert!(err.to_string().contains("trailing metadata EOF error!"));

        Ok(())
    }

    #[tokio::test]
    async fn finalize_without_running_checksum() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: None,
            generation: 123456,
            persisted_size: 0,
            worker_handle: None,
        };

        let handle = tokio::spawn(async move { transport.finalize().await });

        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Finalize(req, sender) = intent {
            assert!(req.object_checksums.is_none());
            let object = Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                size: 5,
                generation: 123456,
                ..Default::default()
            };
            let resp = BidiWriteObjectResponse {
                write_status: Some(WriteStatus::Resource(object)),
                ..Default::default()
            };
            sender.send(Ok(resp)).unwrap();
        } else {
            panic!("expected Finalize");
        }

        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn finalize_error() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: Some(0),
            generation: 123456,
            persisted_size: 0,
            worker_handle: None,
        };

        let handle = tokio::spawn(async move {
            let transport = transport;
            transport.finalize().await
        });

        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Finalize(_, sender) = intent {
            // Respond with an invalid WriteStatus (not Resource)
            let resp = BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(5)),
                ..Default::default()
            };
            sender.send(Ok(resp)).unwrap();
        } else {
            panic!("expected Finalize");
        }

        let err = handle.await?.unwrap_err();
        assert!(err.is_io(), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn finalize_trailing_error() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let worker_handle =
            tokio::spawn(async { Err(crate::Error::io("trailing metadata EOF error!")) });

        let transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: Some(0),
            generation: 123456,
            persisted_size: 0,
            worker_handle: Some(worker_handle),
        };

        let handle = tokio::spawn(async move { transport.finalize().await });

        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Finalize(req, sender) = intent {
            assert!(req.flush);
            let object = Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                size: 17,
                generation: 123456,
                ..Default::default()
            };
            let resp = BidiWriteObjectResponse {
                write_status: Some(WriteStatus::Resource(object)),
                ..Default::default()
            };
            sender.send(Ok(resp)).unwrap();
        } else {
            panic!("expected Finalize");
        }

        // Finalize succeeded in stream, but when finalize awaits the worker handle, it receives
        // the error.
        let err = handle.await?.unwrap_err();
        assert!(err.to_string().contains("trailing metadata EOF error!"));

        Ok(())
    }

    #[tokio::test]
    async fn extract_worker_error() -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel(1);
        drop(rx); // Force tx.send to fail
        let worker_handle = tokio::spawn(async { Err(crate::Error::io("simulated worker crash")) });
        let mut transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            running_crc32c: Some(0),
            generation: 123456,
            persisted_size: 0,
            worker_handle: Some(worker_handle),
        };

        let err = transport
            .append(bytes::Bytes::from("hello"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("simulated worker crash"),
            "{err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn open_initial_state() -> anyhow::Result<()> {
        let (tx1, rx1) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream1 = TonicResponse::from(rx1);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream1)));
        let connector = mock_connector(mock);

        let mut req = OpenAppendableObjectRequest {
            spec: Default::default(),
            params: None,
        };
        req.spec.resource = Some(
            crate::model::Object::default()
                .set_bucket("projects/_/buckets/test-bucket")
                .set_name("test-object"),
        );

        // Creating a new appendable object's initial response returns a Resource with the newly
        // generated object metadata, including generation, and an empty finalize_time.
        let initial_response = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::Resource(Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                size: 0,
                generation: 987654321,
                finalize_time: None,
                ..Default::default()
            })),
            ..Default::default()
        };
        tx1.send(Ok(initial_response)).await?;

        let transport = AppendableObjectWriterTransport::new_open(connector, req).await?;

        assert_eq!(transport.generation(), 987654321);
        assert_eq!(transport.persisted_size(), 0);
        assert_eq!(transport.write_offset, 0);

        // Fresh uploads inherently start from 0 for rolling checksums.
        assert_eq!(transport.running_crc32c, Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn open_connect_error() -> anyhow::Result<()> {
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Err(permanent_error()));
        let connector = mock_connector(mock);
        let mut req = OpenAppendableObjectRequest {
            spec: Default::default(),
            params: None,
        };
        req.spec.resource = Some(
            crate::model::Object::default()
                .set_bucket("projects/_/buckets/test-bucket")
                .set_name("test-object"),
        );

        let err = AppendableObjectWriterTransport::new_open(connector, req)
            .await
            .unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn reopen_initial_state() -> anyhow::Result<()> {
        let (tx1, rx1) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream1 = TonicResponse::from(rx1);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream1)));
        let connector = mock_connector(mock);

        let req = ReopenAppendableObjectRequest {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 123456,
            if_metageneration_match: None,
            if_metageneration_not_match: None,
            routing_token: None,
            write_handle: None,
            params: None,
        };

        let initial_response = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(1024)),
            persisted_data_checksums: Some(ObjectChecksums {
                crc32c: Some(9999),
                md5_hash: bytes::Bytes::new(),
            }),
            ..Default::default()
        };
        tx1.send(Ok(initial_response)).await?;

        let transport = AppendableObjectWriterTransport::new_reopen(connector, req).await?;

        assert_eq!(transport.generation(), 123456);
        assert_eq!(transport.persisted_size(), 1024);
        assert_eq!(transport.write_offset, 1024);
        assert_eq!(transport.running_crc32c, Some(9999));
        Ok(())
    }

    #[tokio::test]
    async fn reopen_server_does_not_return_checksum() -> anyhow::Result<()> {
        let (tx1, rx1) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream1 = TonicResponse::from(rx1);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream1)));
        let connector = mock_connector(mock);

        let req = ReopenAppendableObjectRequest {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 123456,
            if_metageneration_match: None,
            if_metageneration_not_match: None,
            routing_token: None,
            write_handle: None,
            params: None,
        };

        let initial_response = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(1024)),
            // Persisted checksums intentionally omitted by mock server
            ..Default::default()
        };
        tx1.send(Ok(initial_response)).await?;

        let transport = AppendableObjectWriterTransport::new_reopen(connector, req).await?;

        assert_eq!(transport.generation(), 123456);
        assert_eq!(transport.persisted_size(), 1024);
        assert_eq!(transport.running_crc32c, None);
        Ok(())
    }

    #[tokio::test]
    async fn reopen_connect_error() -> anyhow::Result<()> {
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Err(permanent_error()));
        let connector = mock_connector(mock);
        let req = ReopenAppendableObjectRequest {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 123,
            if_metageneration_match: None,
            if_metageneration_not_match: None,
            routing_token: None,
            write_handle: None,
            params: None,
        };

        let err = AppendableObjectWriterTransport::new_reopen(connector, req)
            .await
            .unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn reopen_object_already_finalized_error() -> anyhow::Result<()> {
        let (tx1, rx1) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream1 = TonicResponse::from(rx1);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream1)));
        let connector = mock_connector(mock);

        let req = ReopenAppendableObjectRequest {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 123456,
            if_metageneration_match: None,
            if_metageneration_not_match: None,
            routing_token: None,
            write_handle: None,
            params: None,
        };

        let initial_response = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::Resource(Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                size: 1024,
                generation: 123456,
                finalize_time: Some(prost_types::Timestamp::default()),
                ..Default::default()
            })),
            ..Default::default()
        };
        tx1.send(Ok(initial_response)).await?;

        let err = AppendableObjectWriterTransport::new_reopen(connector, req)
            .await
            .unwrap_err();

        assert!(err.is_io(), "{err:?}");
        assert!(err.to_string().contains("object is already finalized"));
        Ok(())
    }
}
