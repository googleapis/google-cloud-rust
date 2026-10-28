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

use gaxi::grpc::tonic::{Response as TonicResponse, Result as TonicResult, Status as TonicStatus};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_policy::{AlwaysRetry, NeverRetry, RetryPolicyExt as _};
use google_cloud_storage::client::Storage;
use prost::Message as _;
use std::collections::HashMap;
use std::error::Error as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use storage_grpc_mock::google::rpc::Status as RpcStatus;
use storage_grpc_mock::google::storage::v2::{
    BidiWriteHandle, BidiWriteObjectRedirectedError, BidiWriteObjectRequest,
    BidiWriteObjectResponse, ChecksummedData, Object as ProtoObject, ObjectChecksums,
    bidi_write_object_request::{Data, FirstMessage},
    bidi_write_object_response::WriteStatus,
};
use storage_grpc_mock::{MockStorage, start};

const BIND_ADDRESS: &str = "127.0.0.1:0";
const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";
const ENCODED_BUCKET_NAME: &str = "projects%2F_%2Fbuckets%2Ftest-bucket";
const OBJECT_NAME: &str = "test-object";
const OBJECT_GENERATION: i64 = 123456;

const ERR_STREAM_CLOSED_PREMATURELY: &str = "gRPC stream closed before the request was received";
const ERR_RECV_ERROR: &str = "error while reading the request";

/// Creates a `Storage` client configured with anonymous credentials for a mock endpoint.
///
/// The client uses a millisecond-level backoff policy so that retries and reconnects
/// against the mock server don't add seconds of real sleep to each test.
async fn make_client(endpoint: impl Into<String>) -> anyhow::Result<Storage> {
    let backoff = ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(1))
        .with_maximum_delay(Duration::from_millis(10))
        .build()?;
    let client = Storage::builder()
        .with_credentials(Anonymous::new().build())
        .with_endpoint(endpoint)
        .with_backoff_policy(backoff)
        .build()
        .await?;
    Ok(client)
}

/// Starts an in-process mock storage gRPC server and returns an initialized client and server guard.
async fn start_test_server(
    mock: MockStorage,
) -> anyhow::Result<(Storage, tokio::task::JoinHandle<()>)> {
    let (endpoint, server) = start(BIND_ADDRESS, mock).await?;
    let client = make_client(endpoint).await?;
    Ok((client, server))
}

/// Receives the next request message from the client's streaming channel.
async fn recv_request(
    requests: &mut tokio::sync::mpsc::Receiver<TonicResult<BidiWriteObjectRequest>>,
) -> BidiWriteObjectRequest {
    requests
        .recv()
        .await
        .expect(ERR_STREAM_CLOSED_PREMATURELY)
        .expect(ERR_RECV_ERROR)
}

/// Extracts the gRPC status `Code` from either a direct `google_cloud_storage::Error`
/// or one wrapped in `Error::ser(Arc<Error>)` by the background bidi write worker.
fn extract_status_code(err: &google_cloud_storage::Error) -> Option<Code> {
    if let Some(status) = err.status() {
        return Some(status.code);
    }
    err.source()
        .and_then(|s| s.downcast_ref::<Arc<google_cloud_storage::Error>>())
        .and_then(|inner| inner.status())
        .map(|s| s.code)
}

/// Constructs an expected `Data::ChecksummedData` payload with its computed CRC32C checksum.
fn checksummed_data(payload: &[u8]) -> Option<Data> {
    Some(Data::ChecksummedData(ChecksummedData {
        content: payload.to_vec(),
        crc32c: Some(crc32c::crc32c(payload)),
    }))
}

/// Constructs an `ObjectChecksums` proto from an optional CRC32C value.
fn object_checksums(crc32c: Option<u32>) -> Option<ObjectChecksums> {
    crc32c.map(|crc| ObjectChecksums {
        crc32c: Some(crc),
        ..ObjectChecksums::default()
    })
}

/// Constructs a tonic `Status` representing a GCS `BidiWriteObjectRedirectedError`.
///
/// Per the API contract, `generation` is set if and only if `write_handle` is set, so
/// a redirect without a handle also omits the generation.
fn redirect_status(routing_token: &str, write_handle: Option<&[u8]>) -> TonicStatus {
    let redirect = BidiWriteObjectRedirectedError {
        routing_token: Some(routing_token.to_string()),
        write_handle: write_handle.map(|handle| BidiWriteHandle {
            handle: handle.to_vec(),
        }),
        generation: write_handle.map(|_| OBJECT_GENERATION),
    };
    let redirect = prost_types::Any::from_msg(&redirect).expect("serialize any");
    let status = RpcStatus {
        code: gaxi::grpc::tonic::Code::Aborted as i32,
        message: "redirect".to_string(),
        details: vec![redirect],
    };
    let details = bytes::Bytes::from_owner(status.encode_to_vec());
    TonicStatus::with_details(gaxi::grpc::tonic::Code::Aborted, "redirect", details)
}

/// Constructs a `BidiWriteObjectResponse` carrying a `WriteStatus::Resource`.
fn resource_response(
    size: i64,
    handle: &[u8],
    finalized: bool,
    crc32c: Option<u32>,
) -> BidiWriteObjectResponse {
    BidiWriteObjectResponse {
        write_status: Some(WriteStatus::Resource(ProtoObject {
            bucket: BUCKET_NAME.to_string(),
            name: OBJECT_NAME.to_string(),
            generation: OBJECT_GENERATION,
            size,
            finalize_time: finalized.then(prost_types::Timestamp::default),
            checksums: object_checksums(crc32c),
            ..ProtoObject::default()
        })),
        write_handle: Some(BidiWriteHandle {
            handle: handle.to_vec(),
        }),
        persisted_data_checksums: object_checksums(crc32c),
    }
}

/// Constructs a `BidiWriteObjectResponse` carrying a `WriteStatus::PersistedSize`.
fn persisted_size_response(
    size: i64,
    handle: &[u8],
    crc32c: Option<u32>,
) -> BidiWriteObjectResponse {
    BidiWriteObjectResponse {
        write_status: Some(WriteStatus::PersistedSize(size)),
        write_handle: Some(BidiWriteHandle {
            handle: handle.to_vec(),
        }),
        persisted_data_checksums: object_checksums(crc32c),
    }
}

mod conformance {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Verifies that the client enforces the 3-redirect limit (4 attempts total)
    /// and, on each reconnect, propagates the routing token (in both `AppendObjectSpec`
    /// and the `x-goog-request-params` header) and the latest write handle before
    /// finally failing with `Aborted`.
    #[tokio::test]
    async fn handle_redirect_error_max_attempts() -> anyhow::Result<()> {
        // Only redirect #2 carries a new write handle (and, per the API contract, the
        // generation). Redirects #1, #3, and #4 omit both, so the client must keep
        // the most recent handle it has.
        const INITIAL_HANDLE: &[u8] = b"handle-initial";
        const REDIRECT_HANDLE: &[u8] = b"handle-redirect-2";
        const HANDLE_REDIRECT_NUMBER: usize = 2;

        // =========================================================================
        // Arrange: Mock Server Logic
        // =========================================================================
        // Tracks how many times the server returns a BidiWriteObjectRedirectedError.
        let redirect_counter = Arc::new(AtomicUsize::new(0));
        let redirect_counter_clone = redirect_counter.clone();

        // Returns the write handle (if any) carried by redirect number `n`.
        let handle_for_redirect =
            |n: usize| (n == HANDLE_REDIRECT_NUMBER).then_some(REDIRECT_HANDLE);

        let mut mock = MockStorage::new();
        mock.expect_bidi_write_object().returning(move |request| {
            let redirect_counter = redirect_counter_clone.clone();
            let (metadata, _, mut requests) = request.into_parts();
            let (tx, rx) = tokio::sync::mpsc::channel(4);

            tokio::spawn(async move {
                let first = recv_request(&mut requests).await;
                match first.first_message {
                    // 1. Initial Stream (WriteObjectSpec):
                    // Acknowledge opening data ("ABC", 3 bytes) with INITIAL_HANDLE, then
                    // return Redirect #1 (no write handle) when the client attempts to
                    // flush "DEF".
                    Some(FirstMessage::WriteObjectSpec(_)) => {
                        assert_eq!(first.data, checksummed_data(b"ABC"));
                        tx.send(Ok(resource_response(3, INITIAL_HANDLE, false, None)))
                            .await
                            .expect("failed to send initial open response");

                        // Receive "DEF" append and flush request
                        let append_def = recv_request(&mut requests).await;
                        assert_eq!(append_def.write_offset, 3);
                        assert_eq!(append_def.data, checksummed_data(b"DEF"));
                        let flush_def = recv_request(&mut requests).await;
                        assert!(flush_def.flush);
                        assert_eq!(flush_def.write_offset, 6);

                        // Trigger Redirect #1 (routingToken1, no write handle)
                        let count = redirect_counter.fetch_add(1, Ordering::SeqCst) + 1;
                        let token = format!("routingToken{count}");
                        let _ = tx
                            .send(Err(redirect_status(&token, handle_for_redirect(count))))
                            .await;
                    }
                    // 2. Reconnect Streams (AppendObjectSpec):
                    // Client reconnects with the latest routing token and write handle.
                    // The mock server verifies both and returns subsequent redirect
                    // errors (Redirect #2, #3, #4).
                    Some(FirstMessage::AppendObjectSpec(spec)) => {
                        let count = redirect_counter.fetch_add(1, Ordering::SeqCst) + 1;
                        let prev = count - 1;
                        let prev_token = format!("routingToken{prev}");

                        // Verify client propagated the previous redirect token in the spec and header
                        assert_eq!(spec.routing_token.as_deref(), Some(prev_token.as_str()));
                        assert_eq!(
                            metadata
                                .get("x-goog-request-params")
                                .and_then(|v| v.to_str().ok()),
                            Some(
                                format!("bucket={ENCODED_BUCKET_NAME}&routing_token={prev_token}")
                                    .as_str()
                            )
                        );
                        assert!(first.state_lookup);

                        // Verify client sent the latest write handle:
                        // - Before Redirect #2: the handle from the open response.
                        // - After Redirect #2: the handle from Redirect #2, kept even
                        //   though Redirect #3 omits a handle.
                        let expected_handle = if prev < HANDLE_REDIRECT_NUMBER {
                            INITIAL_HANDLE
                        } else {
                            REDIRECT_HANDLE
                        };
                        assert_eq!(
                            spec.write_handle.as_ref().map(|h| h.handle.as_slice()),
                            Some(expected_handle),
                            "reconnect after redirect #{prev}"
                        );
                        // The generation stays constant across all redirects.
                        assert_eq!(spec.generation, OBJECT_GENERATION);

                        // Return the next redirect error
                        let next_token = format!("routingToken{count}");
                        let _ = tx
                            .send(Err(redirect_status(
                                &next_token,
                                handle_for_redirect(count),
                            )))
                            .await;
                    }
                    other => panic!("unexpected first_message: {other:?}"),
                }
            });

            Ok(TonicResponse::from(rx))
        });

        let (client, _server) = start_test_server(mock).await?;

        // =========================================================================
        // Act: Client Actions
        // =========================================================================
        // 1. Open appendable object and append initial "ABC" segment.
        //    `NeverRetry` disables retries for ordinary errors while redirects are still
        //    followed (up to the redirect limit). If a mock-server assertion panics, the
        //    stream closes with a non-redirect error and the flush fails immediately,
        //    instead of being retried until the default retry budget runs out.
        let mut writer = client
            .open_appendable_object(BUCKET_NAME, OBJECT_NAME)
            .with_retry_policy(NeverRetry)
            .send_and_append("ABC")
            .await?;
        assert_eq!(writer.persisted_size(), 3);

        // 2. Append "DEF" and invoke flush() to commit data, encountering redirects
        writer.append("DEF").await?;
        let err = writer
            .flush()
            .await
            .expect_err("expected flush to fail after max redirects");

        // =========================================================================
        // Assert: Assertions
        // =========================================================================
        // 1. After reaching the consecutive redirect limit (3 reconnect redirects),
        // the client terminates retries and surfaces Code::Aborted to the caller.
        assert_eq!(extract_status_code(&err), Some(Code::Aborted), "{err:?}");

        // 2. Total redirect errors returned across streams is exactly 4
        // (1 mid-stream redirect + 3 consecutive reconnect redirects).
        assert_eq!(redirect_counter.load(Ordering::SeqCst), 4);

        Ok(())
    }

    /// Verifies replay buffer recovery: when a stream fails after the server persisted
    /// part of the unacknowledged data, the client learns the persisted offset from the
    /// reconnect handshake, drops the already-persisted bytes from its replay buffer,
    /// and replays only the unpersisted remainder.
    #[tokio::test]
    async fn flush_multiple_segments_fails_halfway() -> anyhow::Result<()> {
        // =========================================================================
        // Arrange: Mock Server Logic
        // =========================================================================
        // Tracks each segment / message received by the mock server without locks.
        let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel::<&'static str>();

        let mut mock = MockStorage::new();
        let mut seq = mockall::Sequence::new();

        // Stream 1:
        // - Acknowledges "ABC" (bytes 0..3) with persisted_size = 3.
        // - Receives "DEF" + "GHI" (bytes 3..9) and the flush request at offset 9.
        //   The client coalesces both appends into a single "DEFGHI" chunk.
        // - Terminates with a transient Unavailable error WITHOUT acknowledging any of it,
        //   so the client still holds "DEFGHI" in its replay buffer.
        mock.expect_bidi_write_object()
            .once()
            .in_sequence(&mut seq)
            .return_once({
                let events_tx = events_tx.clone();
                move |request| {
                    let (_, _, mut requests) = request.into_parts();
                    let (tx, rx) = tokio::sync::mpsc::channel(4);

                    tokio::spawn(async move {
                        // 1. Open with "ABC"
                        let open_req = recv_request(&mut requests).await;
                        assert!(matches!(
                            open_req.first_message,
                            Some(FirstMessage::WriteObjectSpec(_))
                        ));
                        assert_eq!(open_req.data, checksummed_data(b"ABC"));
                        events_tx.send("open_abc").expect("record open_abc");
                        tx.send(Ok(resource_response(3, b"handle-1", false, None)))
                            .await
                            .expect("send open_abc response");

                        // 2. Append "DEFGHI" at offset 3 + Flush at offset 9
                        let defghi_append = recv_request(&mut requests).await;
                        assert_eq!(defghi_append.write_offset, 3);
                        assert_eq!(defghi_append.data, checksummed_data(b"DEFGHI"));
                        events_tx.send("defghi").expect("record defghi");
                        let defghi_flush = recv_request(&mut requests).await;
                        assert!(defghi_flush.flush && defghi_flush.state_lookup);
                        assert_eq!(defghi_flush.write_offset, 9);

                        // 3. Fail before acknowledging anything beyond "ABC"
                        tx.send(Err(TonicStatus::unavailable("Backend error on DEFGHI")))
                            .await
                            .expect("send transient error");
                    });

                    Ok(TonicResponse::from(rx))
                }
            });

        // Stream 2:
        // - Reconnect handshake with state_lookup: true answers persisted_size = 6
        //   (the server had persisted "DEF" before the failure, but "GHI" was lost).
        // - Client trims "DEF" from the buffered "DEFGHI" chunk, replays ONLY "GHI" at
        //   offset 6 (with a recomputed CRC32C), and re-sends the flush at offset 9 (ACK 9).
        // - Client then appends "J" at offset 9 and finalizes at offset 10 (ACK 10, finalized Resource).
        mock.expect_bidi_write_object()
            .once()
            .in_sequence(&mut seq)
            .return_once(move |request| {
                let (_, _, mut requests) = request.into_parts();
                let (tx, rx) = tokio::sync::mpsc::channel(4);

                tokio::spawn(async move {
                    // 1. Reconnect handshake with state_lookup: true
                    let reconnect_req = recv_request(&mut requests).await;
                    let Some(FirstMessage::AppendObjectSpec(spec)) = reconnect_req.first_message
                    else {
                        panic!(
                            "expected AppendObjectSpec, got: {:?}",
                            reconnect_req.first_message
                        );
                    };
                    assert_eq!(spec.bucket, BUCKET_NAME);
                    assert_eq!(spec.object, OBJECT_NAME);
                    assert_eq!(spec.generation, OBJECT_GENERATION);
                    assert_eq!(
                        spec.write_handle.as_ref().map(|h| h.handle.as_slice()),
                        Some(b"handle-1".as_slice())
                    );
                    assert!(reconnect_req.state_lookup);
                    events_tx.send("reconnect").expect("record reconnect");
                    tx.send(Ok(persisted_size_response(6, b"handle-1", None)))
                        .await
                        .expect("send reconnect handshake response");

                    // 2. Client trims the persisted "DEF" prefix, replays only "GHI" at offset 6
                    //    (CRC32C recomputed for "GHI" alone), and re-sends the pending flush at offset 9
                    let ghi_replay = recv_request(&mut requests).await;
                    assert_eq!(ghi_replay.write_offset, 6);
                    assert_eq!(ghi_replay.data, checksummed_data(b"GHI"));
                    events_tx.send("ghi").expect("record ghi replay");
                    let ghi_flush_replay = recv_request(&mut requests).await;
                    assert!(ghi_flush_replay.flush && ghi_flush_replay.state_lookup);
                    assert_eq!(ghi_flush_replay.write_offset, 9);
                    tx.send(Ok(persisted_size_response(9, b"handle-1", None)))
                        .await
                        .expect("send ghi replay flush response");

                    // 3. Client appends "J" at offset 9 and finalizes at offset 10
                    let j_append = recv_request(&mut requests).await;
                    assert_eq!(j_append.write_offset, 9);
                    assert_eq!(j_append.data, checksummed_data(b"J"));
                    events_tx.send("j").expect("record j");

                    let finalize_req = recv_request(&mut requests).await;
                    assert!(finalize_req.finish_write && finalize_req.flush);
                    assert_eq!(finalize_req.write_offset, 10);
                    let expected_full_crc = crc32c::crc32c(b"ABCDEFGHIJ");
                    assert_eq!(
                        finalize_req.object_checksums.and_then(|c| c.crc32c),
                        Some(expected_full_crc)
                    );
                    events_tx.send("finalize").expect("record finalize");

                    tx.send(Ok(resource_response(
                        10,
                        b"handle-1",
                        true,
                        Some(expected_full_crc),
                    )))
                    .await
                    .expect("send finalize response");
                });

                Ok(TonicResponse::from(rx))
            });

        let (client, _server) = start_test_server(mock).await?;

        // =========================================================================
        // Act: Client Actions
        // =========================================================================
        // 1. Open with "ABC" (bytes 0..3) -> persisted = 3
        let mut writer = client
            .open_appendable_object(BUCKET_NAME, OBJECT_NAME)
            .send_and_append("ABC")
            .await?;
        assert_eq!(writer.persisted_size(), 3);

        // 2. Append "DEF" (bytes 3..6) and "GHI" (bytes 6..9), then flush -> encounters a
        // transient stream failure before any ACK, reconnects on Stream 2, learns that
        // bytes 0..6 are persisted, skips "DEF", replays only "GHI", and completes the
        // flush -> persisted = 9
        writer.append("DEF").await?;
        writer.append("GHI").await?;
        let persisted = writer.flush().await?;
        assert_eq!(persisted, 9);

        // 3. Append "J" (bytes 9..10) and finalize
        writer.append("J").await?;
        let object = writer.finalize().await?;

        // =========================================================================
        // Assert: Assertions
        // =========================================================================
        // 1. Final object metadata: size is 10 bytes
        assert_eq!(object.size, 10);

        // 2. Verify request counts per segment:
        // - "open_abc" and "defghi" were seen exactly ONCE (never replayed in full)
        // - "ghi" was seen ONCE (only the unpersisted remainder replayed on Stream 2)
        // - "reconnect" handshake probe was seen ONCE
        // - "j" and "finalize" were seen ONCE
        let mut observed_counts = HashMap::new();
        while let Ok(event) = events_rx.try_recv() {
            *observed_counts.entry(event).or_insert(0) += 1;
        }
        let expected_counts = HashMap::from([
            ("open_abc", 1),
            ("defghi", 1),
            ("ghi", 1),
            ("reconnect", 1),
            ("j", 1),
            ("finalize", 1),
        ]);
        assert_eq!(observed_counts, expected_counts);

        Ok(())
    }

    /// Verifies CRC32C checksum integrity: validates that each transmitted chunk carries
    /// its per-chunk checksum and that the final request carries the cumulative object checksum.
    #[tokio::test]
    async fn checksum_per_chunk_and_finalization() -> anyhow::Result<()> {
        const PAYLOAD: &[u8] = b"abcdefghijklmnopqrstabcde";
        assert_eq!(PAYLOAD.len(), 25);

        // =========================================================================
        // Arrange: Mock Server Logic
        // =========================================================================
        // Records each individual (write_offset, payload, per_chunk_crc) received,
        // and captures the cumulative object_checksums.crc32c received on finalize.
        let (observed_tx, observed_rx) =
            tokio::sync::oneshot::channel::<Vec<(i64, Vec<u8>, Option<u32>)>>();
        let (final_crc_tx, final_crc_rx) = tokio::sync::oneshot::channel::<Option<u32>>();

        let mut mock = MockStorage::new();
        mock.expect_bidi_write_object().return_once(move |request| {
            let (_, _, mut requests) = request.into_parts();
            let (tx, rx) = tokio::sync::mpsc::channel(8);

            tokio::spawn(async move {
                let mut observed_chunks = Vec::new();

                // 1. Initial Open: Receive Chunk 1 (bytes 0..5, "abcde") bundled in WriteObjectSpec
                let open_req = recv_request(&mut requests).await;
                assert!(matches!(
                    open_req.first_message,
                    Some(FirstMessage::WriteObjectSpec(_))
                ));
                let Some(Data::ChecksummedData(first_cd)) = open_req.data else {
                    panic!("expected ChecksummedData, got: {:?}", open_req.data);
                };
                observed_chunks.push((open_req.write_offset, first_cd.content, first_cd.crc32c));
                tx.send(Ok(resource_response(5, b"handle-crc", false, None)))
                    .await
                    .expect("send open response");

                // 2. Chunks 2, 3, 4, 5: Each appended and flushed (bytes 5..10, 10..15, 15..20, 20..25)
                for expected_end in [10_i64, 15, 20, 25] {
                    let append_req = recv_request(&mut requests).await;
                    let Some(Data::ChecksummedData(cd)) = append_req.data else {
                        panic!("expected ChecksummedData, got: {:?}", append_req.data);
                    };
                    observed_chunks.push((append_req.write_offset, cd.content, cd.crc32c));

                    let flush_req = recv_request(&mut requests).await;
                    assert!(flush_req.flush && flush_req.state_lookup);
                    assert_eq!(flush_req.write_offset, expected_end);
                    tx.send(Ok(persisted_size_response(
                        expected_end,
                        b"handle-crc",
                        None,
                    )))
                    .await
                    .expect("send flush response");
                }

                // 3. Finalization: Receive finish_write: true at offset 25 with cumulative ObjectChecksums
                let finalize_req = recv_request(&mut requests).await;
                assert!(finalize_req.finish_write && finalize_req.flush);
                assert_eq!(finalize_req.write_offset, 25);
                let final_crc = finalize_req.object_checksums.and_then(|c| c.crc32c);
                let _ = observed_tx.send(observed_chunks);
                let _ = final_crc_tx.send(final_crc);

                tx.send(Ok(resource_response(25, b"handle-crc", true, final_crc)))
                    .await
                    .expect("send finalize response");
            });

            Ok(TonicResponse::from(rx))
        });

        let (client, _server) = start_test_server(mock).await?;

        // =========================================================================
        // Act: Client Actions
        // =========================================================================
        // 1. Open appendable object and write Chunk 1 (bytes 0..5)
        let mut writer = client
            .open_appendable_object(BUCKET_NAME, OBJECT_NAME)
            .send_and_append(&PAYLOAD[0..5])
            .await?;

        // 2. Append and flush remaining 4 chunks (bytes 5..10, 10..15, 15..20, 20..25)
        for chunk in PAYLOAD[5..].chunks(5) {
            writer.append(chunk).await?;
            writer.flush().await?;
        }

        // 3. Finalize upload at offset 25, automatically computing cumulative checksum
        let object = writer.finalize().await?;

        // =========================================================================
        // Assert: Assertions
        // =========================================================================
        // 1. Final object metadata: size is 25 bytes
        assert_eq!(object.size, 25);

        // 2. Verify per-chunk details on the wire:
        // Each of the 5 requests carried the exact write_offset and matching per-chunk CRC32C.
        let observed_chunks = observed_rx.await?;
        assert_eq!(observed_chunks.len(), 5);
        for (i, (offset, content, crc)) in observed_chunks.into_iter().enumerate() {
            let expected_offset = (i * 5) as i64;
            let expected_slice = &PAYLOAD[i * 5..(i + 1) * 5];
            assert_eq!(offset, expected_offset);
            assert_eq!(content.as_slice(), expected_slice);
            assert_eq!(crc, Some(crc32c::crc32c(expected_slice)));
        }

        // 3. Verify cumulative checksum on the wire and on the finalized object:
        // Client transmitted the exact CRC32C of all 25 bytes combined on finish_write.
        let expected_full_crc = crc32c::crc32c(PAYLOAD);
        assert_eq!(final_crc_rx.await?, Some(expected_full_crc));
        assert_eq!(
            object.checksums.and_then(|c| c.crc32c),
            Some(expected_full_crc)
        );

        Ok(())
    }

    /// Verifies that receiving an acknowledgement that advances `persisted_size` resets
    /// the client's retry budget timer, preventing long processing delays or server pauses
    /// from prematurely exhausting the retry budget on subsequent errors.
    #[tokio::test]
    async fn stream_reset_after_pause_resets_retry_budget() -> anyhow::Result<()> {
        // =========================================================================
        // Arrange: Mock Server Logic
        // =========================================================================
        let mut mock = MockStorage::new();
        let mut seq = mockall::Sequence::new();

        // Stream 1:
        // - Client opens object with "ABC" (ACK 3).
        // - Client appends "DEF" and calls flush().
        // - Server fails with Unavailable before acknowledging "DEF".
        //   -> Starts the client's retry budget timer (200ms time limit) at T = 0ms.
        mock.expect_bidi_write_object()
            .once()
            .in_sequence(&mut seq)
            .returning(|request| {
                let (_, _, mut requests) = request.into_parts();
                let (tx, rx) = tokio::sync::mpsc::channel(4);

                tokio::spawn(async move {
                    let open_req = recv_request(&mut requests).await;
                    assert!(matches!(
                        open_req.first_message,
                        Some(FirstMessage::WriteObjectSpec(_))
                    ));
                    assert_eq!(open_req.data, checksummed_data(b"ABC"));
                    tx.send(Ok(resource_response(3, b"handle-pause", false, None)))
                        .await
                        .expect("send open response");

                    // Receive "DEF" append + flush, fail before acknowledging
                    let def_append = recv_request(&mut requests).await;
                    assert_eq!(def_append.write_offset, 3);
                    assert_eq!(def_append.data, checksummed_data(b"DEF"));
                    let def_flush = recv_request(&mut requests).await;
                    assert_eq!(def_flush.write_offset, 6);
                    tx.send(Err(TonicStatus::unavailable("initial transient error")))
                        .await
                        .expect("send initial unavailable");
                });

                Ok(TonicResponse::from(rx))
            });

        // Stream 2:
        // - Client reconnects; handshake reports persisted_size = 3 (no progress yet; retry timer running).
        // - Client replays "DEF" + flush.
        // - Server pauses for 300ms (exceeding the 200ms retry budget!).
        // - Server then sends ACK for "DEF" (persisted_size = 6).
        //   -> Crucial test point: Advancing persisted_size RESETS the client's retry timer!
        // - Server immediately fails with Unavailable when receiving "GHI".
        mock.expect_bidi_write_object()
            .once()
            .in_sequence(&mut seq)
            .returning(|request| {
                let (_, _, mut requests) = request.into_parts();
                let (tx, rx) = tokio::sync::mpsc::channel(4);

                tokio::spawn(async move {
                    let reconnect_req = recv_request(&mut requests).await;
                    assert!(matches!(
                        reconnect_req.first_message,
                        Some(FirstMessage::AppendObjectSpec(_))
                    ));
                    tx.send(Ok(persisted_size_response(3, b"handle-pause", None)))
                        .await
                        .expect("send reconnect handshake");

                    // Receive replayed "DEF" + flush
                    let def_replay = recv_request(&mut requests).await;
                    assert_eq!(def_replay.write_offset, 3);
                    assert_eq!(def_replay.data, checksummed_data(b"DEF"));
                    let def_flush = recv_request(&mut requests).await;
                    assert_eq!(def_flush.write_offset, 6);

                    // Pause for 300ms (1.5x the 200ms retry time limit) before acknowledging "DEF"
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    tx.send(Ok(persisted_size_response(6, b"handle-pause", None)))
                        .await
                        .expect("acknowledge DEF after 300ms pause");

                    // Receive "GHI" + flush, then fail with Unavailable
                    let ghi_append = recv_request(&mut requests).await;
                    assert_eq!(ghi_append.write_offset, 6);
                    assert_eq!(ghi_append.data, checksummed_data(b"GHI"));
                    let ghi_flush = recv_request(&mut requests).await;
                    assert_eq!(ghi_flush.write_offset, 9);
                    tx.send(Err(TonicStatus::unavailable("error after 300ms pause")))
                        .await
                        .expect("send unavailable after pause");
                });

                Ok(TonicResponse::from(rx))
            });

        // Stream 3:
        // - Reconnect succeeds because the 6-byte ACK in Stream 2 reset the retry timer, giving a fresh 200ms!
        // - Client replays "GHI" (ACK 9).
        // - Client appends "J" and finalizes (ACK 10).
        mock.expect_bidi_write_object()
            .once()
            .in_sequence(&mut seq)
            .returning(|request| {
                let (_, _, mut requests) = request.into_parts();
                let (tx, rx) = tokio::sync::mpsc::channel(4);

                tokio::spawn(async move {
                    let reconnect_req = recv_request(&mut requests).await;
                    assert!(matches!(
                        reconnect_req.first_message,
                        Some(FirstMessage::AppendObjectSpec(_))
                    ));
                    tx.send(Ok(persisted_size_response(6, b"handle-pause", None)))
                        .await
                        .expect("send second reconnect handshake");

                    // Receive replayed "GHI" + flush
                    let ghi_replay = recv_request(&mut requests).await;
                    assert_eq!(ghi_replay.write_offset, 6);
                    assert_eq!(ghi_replay.data, checksummed_data(b"GHI"));
                    let ghi_flush = recv_request(&mut requests).await;
                    assert_eq!(ghi_flush.write_offset, 9);
                    tx.send(Ok(persisted_size_response(9, b"handle-pause", None)))
                        .await
                        .expect("acknowledge GHI");

                    // Receive "J" + finalize
                    let j_append = recv_request(&mut requests).await;
                    assert_eq!(j_append.write_offset, 9);
                    assert_eq!(j_append.data, checksummed_data(b"J"));
                    let finalize_req = recv_request(&mut requests).await;
                    assert!(finalize_req.finish_write);
                    assert_eq!(finalize_req.write_offset, 10);

                    tx.send(Ok(resource_response(
                        10,
                        b"handle-pause",
                        true,
                        Some(crc32c::crc32c(b"ABCDEFGHIJ")),
                    )))
                    .await
                    .expect("send finalize response");
                });

                Ok(TonicResponse::from(rx))
            });

        let (client, _server) = start_test_server(mock).await?;

        // =========================================================================
        // Act: Client Actions
        // =========================================================================
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(10))
            .with_maximum_delay(Duration::from_millis(20))
            .build()?;

        // Configure strict 200ms retry budget
        let mut writer = client
            .open_appendable_object(BUCKET_NAME, OBJECT_NAME)
            .with_retry_policy(AlwaysRetry.with_time_limit(Duration::from_millis(200)))
            .with_backoff_policy(backoff)
            .send_and_append("ABC")
            .await?;

        // "DEF" triggers Stream 1 failure, Stream 2 pause & ACK
        writer.append("DEF").await?;
        let flushed_def = writer.flush().await?;

        // "GHI" triggers Stream 2 failure, Stream 3 reconnect & ACK
        writer.append("GHI").await?;
        let flushed_ghi = writer.flush().await?;

        // "J" finishes the upload
        writer.append("J").await?;
        assert_eq!(writer.persisted_size(), 9);
        let object = writer.finalize().await?;

        // =========================================================================
        // Assert: Assertions
        // =========================================================================
        // 1. Both flushes succeeded with correct persisted offsets despite errors & pauses
        assert_eq!(flushed_def, 6);
        assert_eq!(flushed_ghi, 9);

        // 2. Finalized object reflects complete 10-byte payload
        assert_eq!(object.size, 10);

        Ok(())
    }
}
