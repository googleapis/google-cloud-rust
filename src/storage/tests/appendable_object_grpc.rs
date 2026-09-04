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

#![cfg(google_cloud_unstable_storage_bidi)]

use anyhow::{Context, Result};
use bytes::Bytes;
use gaxi::grpc::tonic::Response;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_storage::client::Storage;
use pretty_assertions::assert_eq;
use storage_grpc_mock::google::storage::v2::bidi_write_object_request::Data;
use storage_grpc_mock::google::storage::v2::bidi_write_object_response::WriteStatus;
use storage_grpc_mock::google::storage::v2::{BidiWriteObjectResponse, Object};
use storage_grpc_mock::{MockStorage, start};
use tokio::sync::mpsc;

/// Wire-level write events recorded by the mock server.
#[derive(Debug, PartialEq, Eq)]
enum WriteEvent {
    Chunk {
        write_offset: i64,
        size: usize,
        crc32c: Option<u32>,
    },
    Flush {
        write_offset: i64,
    },
    Finalize {
        write_offset: i64,
        crc32c: Option<u32>,
    },
}

#[tokio::test]
async fn append_preserves_chunks_offsets_and_checksums() -> Result<()> {
    // Arrange
    const BIND_ADDRESS: &str = "127.0.0.1:0";
    const BUCKET: &str = "projects/_/buckets/wire-parity-bucket";
    const OBJECT: &str = "wire-parity-object";
    const GENERATION: i64 = 73_001;
    const MIB: usize = 1024 * 1024;

    let payload = Bytes::from(vec![0x42; 5 * MIB]);
    let wanted_crc32c = crc32c::crc32c(&payload);
    let (write_events_tx, write_events_rx) = tokio::sync::oneshot::channel();

    let mut mock = MockStorage::new();
    mock.expect_bidi_write_object().return_once(move |request| {
        let (_, _, mut requests) = request.into_parts();
        let (responses_tx, responses_rx) = mpsc::channel(8);
        tokio::spawn(async move {
            // Initialize the bidi stream.
            let first = requests
                .recv()
                .await
                .expect("opening write request")
                .expect("valid opening write request");
            assert!(first.first_message.is_some());
            assert!(first.data.is_none());
            // Respond to the opening request.
            responses_tx
                .send(Ok(BidiWriteObjectResponse {
                    write_status: Some(WriteStatus::Resource(Object {
                        bucket: BUCKET.to_string(),
                        name: OBJECT.to_string(),
                        generation: GENERATION,
                        ..Object::default()
                    })),
                    ..BidiWriteObjectResponse::default()
                }))
                .await
                .expect("opening response");

            let mut write_events = Vec::new();
            // Process stream messages.
            while let Some(request) = requests.recv().await {
                let request = request.expect("valid streaming write request");
                if let Some(Data::ChecksummedData(data)) = request.data {
                    // Record data chunks and verify the sender included a valid per-chunk CRC32C.
                    let computed = crc32c::crc32c(&data.content);
                    assert_eq!(data.crc32c, Some(computed));
                    write_events.push(WriteEvent::Chunk {
                        write_offset: request.write_offset,
                        size: data.content.len(),
                        crc32c: data.crc32c,
                    });
                } else if request.finish_write {
                    // Record final write offset and composite object checksum,
                    // then reply with the finalized object resource to complete the stream.
                    write_events.push(WriteEvent::Finalize {
                        write_offset: request.write_offset,
                        crc32c: request.object_checksums.and_then(|c| c.crc32c),
                    });
                    responses_tx
                        .send(Ok(BidiWriteObjectResponse {
                            write_status: Some(WriteStatus::Resource(Object {
                                bucket: BUCKET.to_string(),
                                name: OBJECT.to_string(),
                                generation: GENERATION,
                                size: request.write_offset,
                                ..Object::default()
                            })),
                            ..BidiWriteObjectResponse::default()
                        }))
                        .await
                        .expect("finalize response");
                    break;
                } else if request.flush {
                    // Record flush offset and acknowledge persisted bytes.
                    write_events.push(WriteEvent::Flush {
                        write_offset: request.write_offset,
                    });
                    responses_tx
                        .send(Ok(BidiWriteObjectResponse {
                            write_status: Some(WriteStatus::PersistedSize(request.write_offset)),
                            ..BidiWriteObjectResponse::default()
                        }))
                        .await
                        .expect("flush response");
                }
            }
            // Send captured wire events to the test assertion thread.
            write_events_tx
                .send(write_events)
                .expect("write events receiver");
        });
        Ok(Response::new(responses_rx))
    });

    // Start the mock gRPC server.
    let (endpoint, server) = start(BIND_ADDRESS, mock).await?;
    let client = Storage::builder()
        .with_credentials(Anonymous::new().build())
        .with_endpoint(endpoint)
        .build()
        .await?;

    // Act
    let mut writer = client.open_appendable_object(BUCKET, OBJECT).send().await?;
    writer.append(payload.clone()).await?;
    let flushed_size = writer.flush().await?;
    let object = writer.finalize().await?;

    // Assert
    assert_eq!(flushed_size, (5 * MIB) as i64);
    assert_eq!(object.size, (5 * MIB) as i64);

    // Retrieve captured wire interactions.
    let write_events = tokio::time::timeout(std::time::Duration::from_secs(15), write_events_rx)
        .await
        .context("wire events timed out")?
        .context("wire events sender dropped")?;

    // A 5 MiB payload should be segmented into 2 MiB + 2 MiB + 1 MiB chunks, followed by flush and finalize.
    assert_eq!(
        write_events,
        [
            WriteEvent::Chunk {
                write_offset: 0,
                size: 2 * MIB,
                crc32c: Some(crc32c::crc32c(&payload[0..2 * MIB])),
            },
            WriteEvent::Chunk {
                write_offset: (2 * MIB) as i64,
                size: 2 * MIB,
                crc32c: Some(crc32c::crc32c(&payload[0..2 * MIB])),
            },
            WriteEvent::Chunk {
                write_offset: (4 * MIB) as i64,
                size: MIB,
                crc32c: Some(crc32c::crc32c(&payload[0..MIB])),
            },
            WriteEvent::Flush {
                write_offset: (5 * MIB) as i64,
            },
            WriteEvent::Finalize {
                write_offset: (5 * MIB) as i64,
                crc32c: Some(wanted_crc32c),
            },
        ]
    );

    // Shut down
    server.abort();
    let _ = server.await;
    Ok(())
}
