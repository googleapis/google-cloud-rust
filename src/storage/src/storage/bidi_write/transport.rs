// Copyright 2025 Google LLC
#![allow(dead_code)]
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

use super::worker::UploadIntent;
use crate::google::storage::v2::ObjectChecksums;
use crate::google::storage::v2::{
    BidiWriteObjectRequest, ChecksummedData, bidi_write_object_request::Data,
    bidi_write_object_response::WriteStatus,
};
use crate::stub::AppendableObjectWriter;
use bytes::Bytes;
use gaxi::prost::FromProto;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

/// The high-level transport adapter for bidirectional streaming writes.
/// Translates user calls into gRPC requests and delegates them to the background worker.
#[derive(Debug)]
pub struct AppendableObjectWriterTransport {
    tx: Sender<UploadIntent>,
    generation: i64,
    persisted_size: i64,
    write_offset: i64,
    crc32c_persisted: u32,
}

impl AppendableObjectWriterTransport {
    pub async fn new_open<T>(
        mut connector: super::connector::Connector<T>,
        req: crate::model_ext::OpenAppendableObjectRequest,
    ) -> crate::Result<Self>
    where
        T: super::Client + Clone + Sync + Send + 'static,
        <T as super::Client>::Stream: super::TonicStreaming + Send + Sync,
    {
        let (initial, connection) = connector.connect_open(req).await?;
        Self::start_worker(connector, initial, connection, 0)
    }

    pub async fn new_reopen<T>(
        mut connector: super::connector::Connector<T>,
        req: crate::model_ext::ReopenAppendableObjectRequest,
    ) -> crate::Result<Self>
    where
        T: super::Client + Clone + Sync + Send + 'static,
        <T as super::Client>::Stream: super::TonicStreaming + Send + Sync,
    {
        let generation = req.generation;
        let (initial, connection) = connector.connect_reopen(req).await?;
        Self::start_worker(connector, initial, connection, generation)
    }

    fn start_worker<T>(
        connector: super::connector::Connector<T>,
        initial: crate::google::storage::v2::BidiWriteObjectResponse,
        connection: super::connector::Connection<<T as super::Client>::Stream>,
        mut generation: i64,
    ) -> crate::Result<Self>
    where
        T: super::Client + Clone + Sync + Send + 'static,
        <T as super::Client>::Stream: super::TonicStreaming + Send + Sync,
    {
        let mut persisted_size = 0;
        if let Some(WriteStatus::Resource(r)) = initial.write_status.as_ref() {
            generation = r.generation;
            persisted_size = r.size;
        } else if let Some(WriteStatus::PersistedSize(s)) = initial.write_status.as_ref() {
            persisted_size = *s;
        }

        let mut crc32c_persisted = 0;
        if let Some(crc) = initial
            .persisted_data_checksums
            .as_ref()
            .and_then(|c| c.crc32c)
        {
            crc32c_persisted = crc;
        }

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let worker = super::worker::Worker::new(connector);
        tokio::spawn(worker.run(connection, rx));

        Ok(Self {
            tx,
            generation,
            persisted_size,
            write_offset: persisted_size,
            crc32c_persisted,
        })
    }
}

impl AppendableObjectWriter for AppendableObjectWriterTransport {
    async fn append(&mut self, chunk: Bytes) -> crate::Result<()> {
        let length = chunk.len() as i64;
        let crc32c = crc32c::crc32c(&chunk);

        let new_crc32c = crc32c::crc32c_append(self.crc32c_persisted, &chunk);

        let request = BidiWriteObjectRequest {
            write_offset: self.write_offset,
            data: Some(Data::ChecksummedData(ChecksummedData {
                content: chunk,
                crc32c: Some(crc32c),
            })),
            ..BidiWriteObjectRequest::default()
        };

        self.tx
            .send(UploadIntent::Append(request))
            .await
            .map_err(|e| crate::Error::io(e.to_string()))?;

        self.write_offset += length;
        self.crc32c_persisted = new_crc32c;

        Ok(())
    }

    async fn flush(&mut self) -> crate::Result<i64> {
        let (sender, receiver) = oneshot::channel();
        let request = BidiWriteObjectRequest {
            flush: true,
            write_offset: self.write_offset,
            ..BidiWriteObjectRequest::default()
        };

        self.tx
            .send(UploadIntent::Flush(request, sender))
            .await
            .map_err(|e| crate::Error::io(e.to_string()))?;

        let response = receiver
            .await
            .map_err(|e| crate::Error::io(e.to_string()))??;
        let size = match response.write_status {
            Some(WriteStatus::PersistedSize(s)) => s,
            Some(WriteStatus::Resource(r)) => r.size,
            None => return Err(crate::Error::io("flush response missing write_status")),
        };
        self.persisted_size = size;
        Ok(size)
    }

    async fn finalize(self) -> crate::Result<crate::model::Object> {
        let (sender, receiver) = oneshot::channel();
        let request = BidiWriteObjectRequest {
            finish_write: true,
            flush: true,
            write_offset: self.write_offset,
            object_checksums: Some(ObjectChecksums {
                crc32c: Some(self.crc32c_persisted),
                md5_hash: vec![].into(),
            }),
            ..BidiWriteObjectRequest::default()
        };

        self.tx
            .send(UploadIntent::Finalize(request, sender))
            .await
            .map_err(|e| crate::Error::io(e.to_string()))?;

        let response = receiver
            .await
            .map_err(|e| crate::Error::io(e.to_string()))??;

        let resource = match response.write_status {
            Some(WriteStatus::Resource(r)) => r,
            _ => return Err(crate::Error::io("finalize did not return a resource")),
        };

        let object = FromProto::cnv(resource)
            .map_err(|_| crate::Error::deser("converting resource to object"))?;

        Ok(object)
    }

    async fn close(mut self) -> crate::Result<i64> {
        self.flush().await
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
    use super::*;
    use crate::google::storage::v2::{
        BidiWriteObjectResponse, Object, bidi_write_object_response::WriteStatus,
    };
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn success() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            crc32c_persisted: 0,
            generation: 123456,
            persisted_size: 0,
        };

        let handle = tokio::spawn(async move {
            let mut transport = transport;
            transport.append(bytes::Bytes::from("hello")).await.unwrap();
            transport.flush().await.unwrap();
            transport.finalize().await.unwrap();
        });

        // Verify append
        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Append(req) = intent {
            assert_eq!(req.write_offset, 0);
            assert!(!req.finish_write);
        } else {
            panic!("expected Append");
        }

        // Verify flush
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

        // Verify finalize
        let intent = rx.recv().await.unwrap();
        if let UploadIntent::Finalize(req, sender) = intent {
            assert!(req.finish_write);
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
    async fn connect_error_open() -> anyhow::Result<()> {
        use super::super::mocks::{MockTestClient, mock_connector};
        use super::super::tests::permanent_error;
        use crate::model_ext::OpenAppendableObjectRequest;

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
    async fn connect_error_reopen() -> anyhow::Result<()> {
        use super::super::mocks::{MockTestClient, mock_connector};
        use super::super::tests::permanent_error;
        use crate::model_ext::ReopenAppendableObjectRequest;

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
    async fn write_error() -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel(1);
        let mut transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            crc32c_persisted: 0,
            generation: 123456,
            persisted_size: 0,
        };

        // Simulate an early stream closure / dropped rx (like a worker dying)
        drop(rx);
        let err = transport
            .append(bytes::Bytes::from("hello"))
            .await
            .unwrap_err();
        assert!(err.is_io(), "{err:?}");

        // Assert that state was NOT modified due to the error
        assert_eq!(transport.write_offset, 0);
        assert_eq!(transport.crc32c_persisted, 0);

        Ok(())
    }

    #[tokio::test]
    async fn finalize_error() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let transport = AppendableObjectWriterTransport {
            tx,
            write_offset: 0,
            crc32c_persisted: 0,
            generation: 123456,
            persisted_size: 0,
        };

        let handle = tokio::spawn(async move {
            let transport = transport;
            transport.finalize().await
        });

        // Verify finalize intent
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
    async fn new_reopen_initial_persisted_size() -> anyhow::Result<()> {
        use super::super::mocks::{MockTestClient, mock_connector};
        use crate::model_ext::ReopenAppendableObjectRequest;
        use gaxi::grpc::tonic::Response as TonicResponse;
        use gaxi::grpc::tonic::Result as TonicResult;

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
            persisted_data_checksums: Some(crate::google::storage::v2::ObjectChecksums {
                crc32c: Some(9999),
                md5_hash: vec![].into(),
            }),
            ..Default::default()
        };
        tx1.send(Ok(initial_response)).await?;

        let transport = AppendableObjectWriterTransport::new_reopen(connector, req).await?;

        assert_eq!(transport.generation(), 123456);
        assert_eq!(transport.persisted_size(), 1024);
        assert_eq!(transport.write_offset, 1024);
        assert_eq!(transport.crc32c_persisted, 9999);
        Ok(())
    }
}
