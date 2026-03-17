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

#[cfg(google_cloud_unstable_tracing)]
use super::tracing::TracingResponse;
use crate::Result;
use crate::model::{Object, ReadObjectRequest};
use crate::model_ext::WriteObjectRequest;
use crate::read_object::ReadObjectResponse;
use crate::storage::client::StorageInner;
#[cfg(google_cloud_unstable_tracing)]
use crate::storage::info::INSTRUMENTATION;
use crate::storage::perform_upload::PerformUpload;
use crate::storage::read_object::Reader;
use crate::storage::request_options::RequestOptions;
use crate::storage::streaming_source::{Seek, StreamingSource};
use crate::{
    model_ext::OpenObjectRequest, object_descriptor::ObjectDescriptor,
    storage::bidi::connector::Connector, storage::bidi::transport::ObjectDescriptorTransport,
};
#[cfg(google_cloud_unstable_tracing)]
use gaxi::observability::ResultExt;
use gaxi::gcs_constants::{DEFAULT_GRPC_WRITE_CHUNK_SIZE, ENV_GRPC_WRITE_CHUNK_SIZE, MAX_GRPC_WRITE_CHUNK_SIZE};
use std::sync::Arc;
#[cfg(google_cloud_unstable_tracing)]
use tracing::Instrument;

/// Returns the effective gRPC write chunk size.
///
/// Priority:
///   1. `S3DLIO_GRPC_WRITE_CHUNK_SIZE` env var (bytes) — silently clamped to
///      [`MAX_GRPC_WRITE_CHUNK_SIZE`] if the provided value exceeds the server limit.
///   2. [`DEFAULT_GRPC_WRITE_CHUNK_SIZE`].
///
/// Both constants are defined in `gcs_constants` — the single source of truth
/// for all GCS/gRPC tuning values.
fn grpc_write_chunk_size() -> usize {
    let requested = std::env::var(ENV_GRPC_WRITE_CHUNK_SIZE)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_GRPC_WRITE_CHUNK_SIZE);
    requested.min(MAX_GRPC_WRITE_CHUNK_SIZE)
}

/// An implementation of [`stub::Storage`][crate::storage::stub::Storage] that
/// interacts with the Cloud Storage service.
///
/// This is the default implementation of a
/// [`client::Storage<T>`][crate::storage::client::Storage].
///
/// ## Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// use google_cloud_storage::client::Storage;
/// use google_cloud_storage::stub::DefaultStorage;
/// let client: Storage<DefaultStorage> = Storage::builder().build().await?;
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct Storage {
    inner: Arc<StorageInner>,
    tracing: bool,
}

impl Storage {
    #[cfg(test)]
    pub(crate) fn new_test(inner: Arc<StorageInner>) -> Arc<Self> {
        Self::new(inner, false)
    }

    pub(crate) fn new(inner: Arc<StorageInner>, tracing: bool) -> Arc<Self> {
        Arc::new(Self { inner, tracing })
    }

    async fn read_object_plain(
        &self,
        request: ReadObjectRequest,
        options: RequestOptions,
    ) -> Result<ReadObjectResponse> {
        let reader = Reader {
            inner: self.inner.clone(),
            request,
            options,
        };
        reader.response().await
    }

    #[tracing::instrument(name = "read_object", level = tracing::Level::DEBUG, ret, err(Debug))]
    async fn read_object_tracing(
        &self,
        request: ReadObjectRequest,
        options: RequestOptions,
    ) -> Result<ReadObjectResponse> {
        #[cfg(google_cloud_unstable_tracing)]
        {
            let span =
                gaxi::client_request_span!("client::Storage", "read_object", &INSTRUMENTATION);
            let response = self
                .read_object_plain(request, options)
                .instrument(span.clone())
                .await
                .record_in_span(&span)?;
            let inner = TracingResponse::new(response.into_parts(), span);
            let response = ReadObjectResponse::new(Box::new(inner));
            Ok(response)
        }
        #[cfg(not(google_cloud_unstable_tracing))]
        self.read_object_plain(request, options).await
    }

    async fn write_object_buffered_plain<P>(
        &self,
        payload: P,
        request: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        PerformUpload::new(
            payload,
            self.inner.clone(),
            request.spec,
            request.params,
            options,
        )
        .send()
        .await
    }

    #[tracing::instrument(name = "write_object_buffered", level = tracing::Level::DEBUG, ret, err(Debug), skip(payload))]
    async fn write_object_buffered_tracing<P>(
        &self,
        payload: P,
        request: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        #[cfg(google_cloud_unstable_tracing)]
        {
            let span =
                gaxi::client_request_span!("client::Storage", "write_object", &INSTRUMENTATION);
            self.write_object_buffered_plain(payload, request, options)
                .instrument(span.clone())
                .await
                .record_in_span(&span)
        }
        #[cfg(not(google_cloud_unstable_tracing))]
        self.write_object_buffered_plain(payload, request, options)
            .await
    }

    async fn write_object_unbuffered_plain<P>(
        &self,
        payload: P,
        request: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Seek + Send + Sync + 'static,
    {
        PerformUpload::new(
            payload,
            self.inner.clone(),
            request.spec,
            request.params,
            options,
        )
        .send_unbuffered()
        .await
    }

    #[tracing::instrument(name = "write_object_unbuffered", level = tracing::Level::DEBUG, ret, err(Debug), skip(payload))]
    async fn write_object_unbuffered_tracing<P>(
        &self,
        payload: P,
        request: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Seek + Send + Sync + 'static,
    {
        #[cfg(google_cloud_unstable_tracing)]
        {
            let span =
                gaxi::client_request_span!("client::Storage", "write_object", &INSTRUMENTATION);
            self.write_object_unbuffered_plain(payload, request, options)
                .instrument(span.clone())
                .await
                .record_in_span(&span)
        }
        #[cfg(not(google_cloud_unstable_tracing))]
        self.write_object_unbuffered_plain(payload, request, options)
            .await
    }

    async fn open_object_plain(
        &self,
        request: OpenObjectRequest,
        options: RequestOptions,
    ) -> Result<(ObjectDescriptor, Vec<ReadObjectResponse>)> {
        let (spec, ranges) = request.into_parts();
        let connector = Connector::new(spec, options, self.inner.grpc.clone());
        let (transport, readers) = ObjectDescriptorTransport::new(connector, ranges).await?;
        Ok((ObjectDescriptor::new(transport), readers))
    }

    #[tracing::instrument(name = "open_object", level = tracing::Level::DEBUG, ret, err(Debug))]
    async fn open_object_tracing(
        &self,
        request: OpenObjectRequest,
        options: RequestOptions,
    ) -> Result<(ObjectDescriptor, Vec<ReadObjectResponse>)> {
        #[cfg(google_cloud_unstable_tracing)]
        {
            let span =
                gaxi::client_request_span!("client::Storage", "open_object", &INSTRUMENTATION);
            // TODO(#3178) - wrap descriptor and responses with tracing decorators.
            self.open_object_plain(request, options)
                .instrument(span.clone())
                .await
                .record_in_span(&span)
        }
        #[cfg(not(google_cloud_unstable_tracing))]
        self.open_object_plain(request, options).await
    }
}

impl super::stub::Storage for Storage {
    /// Implements [crate::client::Storage::read_object].
    async fn read_object(
        &self,
        req: ReadObjectRequest,
        options: RequestOptions,
    ) -> Result<ReadObjectResponse> {
        if self.tracing {
            return self.read_object_tracing(req, options).await;
        }
        self.read_object_plain(req, options).await
    }

    /// Implements [crate::client::Storage::write_object].
    async fn write_object_buffered<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        if self.tracing {
            return self
                .write_object_buffered_tracing(payload, req, options)
                .await;
        }
        self.write_object_buffered_plain(payload, req, options)
            .await
    }

    /// Implements [crate::client::Storage::write_object].
    async fn write_object_unbuffered<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Seek + Send + Sync + 'static,
    {
        if self.tracing {
            return self
                .write_object_unbuffered_tracing(payload, req, options)
                .await;
        }
        self.write_object_unbuffered_plain(payload, req, options)
            .await
    }

    async fn write_object_grpc(
        &self,
        data: bytes::Bytes,
        req: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object> {
        use crate::google::storage::v2::{
            bidi_write_object_request, bidi_write_object_response, BidiWriteObjectRequest,
            BidiWriteObjectResponse, ChecksummedData, ObjectChecksums,
            WriteObjectSpec as ProtoWriteObjectSpec,
        };
        use crate::storage::info::X_GOOG_API_CLIENT_HEADER;
        use crate::Error;
        use gaxi::grpc::tonic::{Extensions, GrpcMethod, Streaming};
        use gaxi::prost::ToProto;

        let resource = req
            .spec
            .resource
            .as_ref()
            .expect("resource field must be set");
        let bucket_name = resource.bucket.clone();
        let proto_resource = req
            .spec
            .resource
            .clone()
            .map(|r| r.to_proto())
            .transpose()
            .map_err(|e| Error::io(format!("failed to convert Object to proto: {e}")))?;

        let proto_spec = ProtoWriteObjectSpec {
            resource: proto_resource,
            predefined_acl: req.spec.predefined_acl.clone(),
            if_generation_match: req.spec.if_generation_match,
            if_generation_not_match: req.spec.if_generation_not_match,
            if_metageneration_match: req.spec.if_metageneration_match,
            if_metageneration_not_match: req.spec.if_metageneration_not_match,
            // Appendable objects (RAPID/zonal) must NOT declare object_size.
            // The proto docs say object_size is the "expected final object size"
            // but appendable objects have no fixed final size.  Setting both
            // causes the server to leave metadata at size=0 after finalization.
            object_size: if req.spec.appendable == Some(true) { None } else { Some(data.len() as i64) },
            appendable: req.spec.appendable,
        };

        let object_crc32c = crc32c::crc32c(&data);
        let chunk_size = grpc_write_chunk_size();
        let total_len = data.len();
        let num_chunks_est = if total_len == 0 { 1 } else { (total_len + chunk_size - 1) / chunk_size };
        tracing::trace!(
            "BidiWriteObject: total_size={} bytes, chunk_size={} bytes ({:.1} MiB), estimated_chunks={}, appendable={:?}, object_crc32c={:#010x}",
            total_len, chunk_size, chunk_size as f64 / (1024.0 * 1024.0), num_chunks_est, req.spec.appendable, object_crc32c
        );

        let x_goog_request_params = format!("bucket={bucket_name}");
        let extensions = {
            let mut e = Extensions::new();
            e.insert(GrpcMethod::new("google.storage.v2.Storage", "BidiWriteObject"));
            e
        };
        let path = http::uri::PathAndQuery::from_static("/google.storage.v2.Storage/BidiWriteObject");

        const PRODUCER_CHANNEL_CAPACITY: usize = 8;
        let (tx, rx) = tokio::sync::mpsc::channel::<BidiWriteObjectRequest>(PRODUCER_CHANNEL_CAPACITY);

        // The C++ client (google-cloud-cpp) sends finish_write=true as a
        // SEPARATE empty message after the last data chunk, not combined.
        // For appendable objects (RAPID/zonal) this two-step Flush→Finalize
        // is required for the metadata index to commit the real size.
        let is_appendable = proto_spec.appendable == Some(true);

        // Watch channel: the reader feeds PersistedSize updates back to the
        // producer.  For appendable (RAPID) writes, the producer MUST wait for
        // the server to confirm all data is persisted BEFORE sending finalize.
        // Without this, the server commits only whatever has been flushed at
        // the moment it receives finish_write=true, causing truncation.
        // Sentinel value i64::MAX signals that the final Resource was received.
        const RESOURCE_RECEIVED: i64 = i64::MAX;
        let (persisted_tx, mut persisted_rx) = tokio::sync::watch::channel::<i64>(-1);

        let producer_task = {
            let data = data.clone();
            let proto_spec = proto_spec.clone();
            tokio::spawn(async move {
                let producer_start = std::time::Instant::now();
                let mut offset: usize = 0;
                let mut msg_index: usize = 0;
                while offset < total_len || (total_len == 0 && msg_index == 0) {
                    let end = std::cmp::min(offset + chunk_size, total_len);
                    let chunk = data.slice(offset..end);
                    let chunk_crc = crc32c::crc32c(&chunk);
                    let is_first = msg_index == 0;
                    let is_last = end >= total_len;

                    // For non-appendable: last data chunk carries flush +
                    // finish + full-object checksum (single-step finalize).
                    // For appendable (RAPID): data chunks never finalize;
                    // a separate message handles that below.
                    let finish_on_chunk = is_last && !is_appendable;

                    let request = BidiWriteObjectRequest {
                        write_offset: offset as i64,
                        object_checksums: if finish_on_chunk {
                            Some(ObjectChecksums {
                                crc32c: Some(object_crc32c),
                                md5_hash: bytes::Bytes::new(),
                            })
                        } else {
                            None
                        },
                        state_lookup: false,
                        flush: false,
                        finish_write: finish_on_chunk,
                        common_object_request_params: None,
                        first_message: if is_first {
                            Some(bidi_write_object_request::FirstMessage::WriteObjectSpec(
                                proto_spec.clone(),
                            ))
                        } else {
                            None
                        },
                        data: Some(bidi_write_object_request::Data::ChecksummedData(
                            ChecksummedData {
                                content: chunk,
                                crc32c: Some(chunk_crc),
                            },
                        )),
                    };

                    if is_first {
                        tracing::debug!(
                            "BidiWriteObject: PRODUCER sending first chunk offset=0 len={} appendable={}",
                            end - offset, is_appendable
                        );
                    }
                    tracing::trace!(
                        "BidiWriteObject: chunk {}/{} offset={} len={} crc32c={:#010x} first={} last={} flush=false finish={}",
                        msg_index + 1, num_chunks_est, offset, end - offset, chunk_crc, is_first, is_last, finish_on_chunk
                    );
                    if tx.send(request).await.is_err() {
                        tracing::debug!(
                            "BidiWriteObject: PRODUCER tx.send failed at chunk {}/{} — receiver dropped",
                            msg_index + 1, num_chunks_est
                        );
                        break;
                    }
                    offset = end;
                    msg_index += 1;
                }
                let data_done_elapsed = producer_start.elapsed();
                tracing::debug!(
                    "BidiWriteObject: PRODUCER all {} data chunk(s) sent, {} bytes total, elapsed={:?}",
                    msg_index, total_len, data_done_elapsed
                );

                // Appendable (RAPID): two-phase finalize.
                // Phase 1: flush probe — ask server to confirm all data is persisted.
                // Phase 2: finalize   — only after PersistedSize >= total_len.
                // Without this, the server commits only partially-flushed
                // data when it receives finish_write, causing truncation.
                if is_appendable {
                    // --- Phase 1: flush probe ---
                    let flush_probe = BidiWriteObjectRequest {
                        write_offset: total_len as i64,
                        finish_write: false,
                        flush: true,
                        state_lookup: true,
                        data: None,
                        object_checksums: None,
                        common_object_request_params: None,
                        first_message: None,
                    };
                    tracing::debug!(
                        "BidiWriteObject: PRODUCER sending flush probe #{} write_offset={} (flush=true, state_lookup=true)",
                        msg_index + 1, total_len
                    );
                    if tx.send(flush_probe).await.is_err() {
                        tracing::warn!("BidiWriteObject: PRODUCER flush probe send FAILED — receiver dropped");
                    } else {
                        msg_index += 1;

                        // Wait for server to confirm PersistedSize >= total_len
                        tracing::debug!(
                            "BidiWriteObject: PRODUCER waiting for PersistedSize >= {}",
                            total_len
                        );
                        let wait_start = std::time::Instant::now();
                        loop {
                            // Check current value before waiting
                            let current = *persisted_rx.borrow();
                            if current >= total_len as i64 {
                                tracing::debug!(
                                    "BidiWriteObject: PRODUCER PersistedSize={} >= total_len={}, proceeding to finalize, waited {:?}",
                                    current, total_len, wait_start.elapsed()
                                );
                                break;
                            }
                            if current == RESOURCE_RECEIVED {
                                tracing::debug!(
                                    "BidiWriteObject: PRODUCER got early Resource before finalize — skipping finalize"
                                );
                                break;
                            }
                            let remaining = std::time::Duration::from_secs(60)
                                .saturating_sub(wait_start.elapsed());
                            if remaining.is_zero() {
                                tracing::warn!(
                                    "BidiWriteObject: PRODUCER TIMEOUT (60s) waiting for PersistedSize (current={}, need={})",
                                    current, total_len
                                );
                                break;
                            }
                            match tokio::time::timeout(remaining, persisted_rx.changed()).await {
                                Ok(Ok(())) => {
                                    let ps = *persisted_rx.borrow();
                                    tracing::debug!(
                                        "BidiWriteObject: PRODUCER PersistedSize update: {} (need {}), elapsed={:?}",
                                        ps, total_len, wait_start.elapsed()
                                    );
                                }
                                Ok(Err(_)) => {
                                    tracing::warn!(
                                        "BidiWriteObject: PRODUCER persisted channel closed — reader may have exited"
                                    );
                                    break;
                                }
                                Err(_) => {
                                    tracing::warn!(
                                        "BidiWriteObject: PRODUCER TIMEOUT (60s) waiting for PersistedSize (current={}, need={})",
                                        *persisted_rx.borrow(), total_len
                                    );
                                    break;
                                }
                            }
                        }
                    }

                    // --- Phase 2: finalize (unless Resource already arrived) ---
                    let current = *persisted_rx.borrow();
                    if current != RESOURCE_RECEIVED {
                        let finalize_request = BidiWriteObjectRequest {
                            write_offset: total_len as i64,
                            finish_write: true,
                            flush: true,
                            data: None,
                            object_checksums: Some(ObjectChecksums {
                                crc32c: Some(object_crc32c),
                                md5_hash: bytes::Bytes::new(),
                            }),
                            state_lookup: false,
                            common_object_request_params: None,
                            first_message: None,
                        };
                        tracing::debug!(
                            "BidiWriteObject: PRODUCER sending finalize #{} write_offset={} crc32c={:#010x}",
                            msg_index + 1, total_len, object_crc32c
                        );
                        match tx.send(finalize_request).await {
                            Ok(()) => tracing::debug!("BidiWriteObject: PRODUCER finalize queued OK"),
                            Err(_) => tracing::warn!("BidiWriteObject: PRODUCER finalize send FAILED — receiver dropped"),
                        }
                        msg_index += 1;
                    }
                }

                tracing::debug!(
                    "BidiWriteObject: PRODUCER done: {} message(s) sent, appendable={}, total_elapsed={:?}",
                    msg_index, is_appendable, producer_start.elapsed()
                );

                // CRITICAL: keep `tx` alive until the reader confirms
                // Resource (for appendable).  Dropping `tx` closes the gRPC
                // send-half which can race with the server's final commit.
                if is_appendable {
                    let current = *persisted_rx.borrow();
                    if current != RESOURCE_RECEIVED {
                        tracing::debug!("BidiWriteObject: PRODUCER entering keep-alive wait (holding tx open for up to 60s)");
                        let keepalive_deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
                        loop {
                            let current = *persisted_rx.borrow();
                            if current == RESOURCE_RECEIVED {
                                tracing::debug!(
                                    "BidiWriteObject: PRODUCER Resource confirmed — closing stream, total_elapsed={:?}",
                                    producer_start.elapsed()
                                );
                                break;
                            }
                            let remaining = keepalive_deadline.saturating_duration_since(std::time::Instant::now());
                            if remaining.is_zero() {
                                tracing::warn!(
                                    "BidiWriteObject: PRODUCER TIMEOUT (60s) waiting for Resource — data may be truncated!"
                                );
                                break;
                            }
                            match tokio::time::timeout(remaining, persisted_rx.changed()).await {
                                Ok(Ok(())) => { /* loop will re-check */ }
                                Ok(Err(_)) => {
                                    tracing::warn!(
                                        "BidiWriteObject: PRODUCER persisted channel closed without Resource — data may be truncated!"
                                    );
                                    break;
                                }
                                Err(_) => {
                                    tracing::warn!(
                                        "BidiWriteObject: PRODUCER TIMEOUT (60s) waiting for Resource — data may be truncated!"
                                    );
                                    break;
                                }
                            }
                        }
                    } else {
                        tracing::debug!(
                            "BidiWriteObject: PRODUCER Resource already confirmed, total_elapsed={:?}",
                            producer_start.elapsed()
                        );
                    }
                }
            })
        };

        let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let response: std::result::Result<
            gaxi::grpc::tonic::Result<gaxi::grpc::tonic::Response<Streaming<BidiWriteObjectResponse>>>,
            crate::Error,
        > = self
            .inner
            .grpc
            .bidi_stream_with_status(
                extensions,
                path,
                request_stream,
                options.gax(),
                &X_GOOG_API_CLIENT_HEADER,
                &x_goog_request_params,
            )
            .await;

        let tonic_result = response?;
        let tonic_response = tonic_result.map_err(gaxi::grpc::from_status::to_gax_error)?;
        let (_, mut stream, _) = tonic_response.into_parts();

        // Drain response stream CONCURRENTLY with the producer.
        // BidiWriteObject servers (especially RAPID/zonal) send PersistedSize
        // ACKs for each chunk.  If we don't read them, the HTTP/2 flow-control
        // window fills up and the producer stalls (observed at chunk 14 of 18).
        let reader_task = tokio::spawn(async move {
            let reader_start = std::time::Instant::now();
            let mut response_count: u32 = 0;
            // For appendable (RAPID) writes, the server sends an initial
            // Resource(size=0) as a "spec ack" before any data is persisted.
            // We must NOT treat this as the final Resource — doing so causes
            // the producer to skip finalization and the stream to close with
            // only partially-committed data.
            let mut initial_resource_skipped = false;
            let mut seen_persisted_size = false;
            tracing::debug!("BidiWriteObject: READER started, waiting for server responses");
            loop {
                match stream.message().await {
                    Ok(Some(msg)) => {
                        response_count += 1;
                        match &msg.write_status {
                            Some(bidi_write_object_response::WriteStatus::Resource(proto_obj)) => {
                                // For appendable writes: the FIRST Resource with
                                // size=0 is the spec ack ("hello").  Skip it and
                                // keep listening for PersistedSize + final Resource.
                                if is_appendable
                                    && !initial_resource_skipped
                                    && !seen_persisted_size
                                {
                                    initial_resource_skipped = true;
                                    tracing::debug!(
                                        "BidiWriteObject: READER skipping initial spec-ack Resource (response #{}) size={} name={:?} elapsed={:?}",
                                        response_count, proto_obj.size,
                                        proto_obj.name, reader_start.elapsed()
                                    );
                                    continue;
                                }
                                tracing::debug!(
                                    "BidiWriteObject: READER got final Resource (response #{}) size={} name={:?} elapsed={:?}",
                                    response_count, proto_obj.size,
                                    proto_obj.name, reader_start.elapsed()
                                );
                                // Signal producer: Resource received, safe to close stream.
                                let _ = persisted_tx.send(RESOURCE_RECEIVED);
                                return Ok(proto_obj.clone());
                            }
                            Some(bidi_write_object_response::WriteStatus::PersistedSize(ps)) => {
                                seen_persisted_size = true;
                                tracing::debug!(
                                    "BidiWriteObject: READER got PersistedSize={} (response #{}) elapsed={:?}",
                                    ps, response_count, reader_start.elapsed()
                                );
                                let _ = persisted_tx.send(*ps);
                            }
                            None => {
                                tracing::debug!(
                                    "BidiWriteObject: READER got response #{} with NO write_status (empty ack?) elapsed={:?}",
                                    response_count, reader_start.elapsed()
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::debug!(
                            "BidiWriteObject: READER stream ended (None) after {} response(s), elapsed={:?} — NO Resource received!",
                            response_count, reader_start.elapsed()
                        );
                        return Err(Error::io(
                            "BidiWriteObject stream ended without returning Object",
                        ));
                    }
                    Err(status) => {
                        tracing::debug!(
                            "BidiWriteObject: READER stream error after {} response(s), elapsed={:?}: {:?}",
                            response_count, reader_start.elapsed(), status
                        );
                        return Err(gaxi::grpc::from_status::to_gax_error(status));
                    }
                }
            }
        });

        // Wait for BOTH producer and reader to complete.
        // The producer must finish sending all chunks + finalize before the
        // server will return the final Resource response.
        let (producer_result, reader_result) = tokio::join!(producer_task, reader_task);

        // Check for panics in either task.
        if let Err(e) = producer_result {
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            }
        }
        let proto_obj = reader_result
            .map_err(|e| Error::io(format!("reader task failed: {e}")))?
            ?;

        use gaxi::prost::FromProto;
        let model_obj: crate::model::Object = proto_obj
            .cnv()
            .map_err(|e| Error::io(format!("failed to convert proto Object to model: {e}")))?;

        Ok(model_obj)
    }

    async fn open_object(
        &self,
        request: OpenObjectRequest,
        options: RequestOptions,
    ) -> Result<(ObjectDescriptor, Vec<ReadObjectResponse>)> {
        if self.tracing {
            return self.open_object_tracing(request, options).await;
        }
        self.open_object_plain(request, options).await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(google_cloud_unstable_tracing)]
    use gaxi::observability::attributes::{
        GCP_CLIENT_LANGUAGE_RUST, OTEL_KIND_INTERNAL, RPC_SYSTEM_HTTP, keys::*,
    };
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    #[cfg(google_cloud_unstable_tracing)]
    use google_cloud_test_utils::test_layer::AttributeValue;
    use google_cloud_test_utils::test_layer::{CapturedSpan, TestLayer};
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn read_object() -> anyhow::Result<()> {
        let guard = TestLayer::initialize();

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(status_code(404)),
        );

        let client = crate::client::Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .build()
            .await?;
        let response = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await;
        assert!(
            matches!(response, Err(ref e) if e.is_transport()),
            "{response:?}"
        );

        let captured = TestLayer::capture(&guard);
        check_debug_log(&captured, "read_object");

        #[cfg(google_cloud_unstable_tracing)]
        client_request_span(&captured, "read_object", "404");

        Ok(())
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn read_object_success() -> anyhow::Result<()> {
        let guard = TestLayer::initialize();

        let body = (0..100_000)
            .map(|i| format!("{i:08} {:1000}", ""))
            .collect::<Vec<_>>()
            .join("\n");
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(
                status_code(200)
                    .body(body.clone())
                    .append_header("x-goog-generation", 123456),
            ),
        );

        let client = crate::client::Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .build()
            .await?;
        let mut got = Vec::new();
        let mut response = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let object = response.object();
        assert_eq!(object.generation, 123456, "{object:?}");
        while let Some(b) = response.next().await.transpose()? {
            got.push(b);
        }

        let captured = TestLayer::capture(&guard);
        let span = captured
            .iter()
            .find(|s| s.name == "client_request")
            .unwrap_or_else(|| panic!("missing `client_request` span in capture: {captured:#?}"));
        // The span counts one more event: the EOF
        assert_eq!(span.events, got.len() + 1, "{span:?}");

        Ok(())
    }

    #[tokio::test]
    async fn write_object_buffered() -> anyhow::Result<()> {
        let guard = TestLayer::initialize();

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
            .respond_with(status_code(404)),
        );

        let client = crate::client::Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .build()
            .await?;
        let response = client
            .write_object("projects/_/buckets/test-bucket", "test-object", "payload")
            .send_buffered()
            .await;
        assert!(
            matches!(response, Err(ref e) if e.is_transport()),
            "{response:?}"
        );

        let captured = TestLayer::capture(&guard);
        check_debug_log(&captured, "write_object_buffered");

        #[cfg(google_cloud_unstable_tracing)]
        client_request_span(&captured, "write_object", "404");

        Ok(())
    }

    #[tokio::test]
    async fn write_object_unbuffered() -> anyhow::Result<()> {
        let guard = TestLayer::initialize();

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
            .respond_with(status_code(404)),
        );

        let client = crate::client::Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .build()
            .await?;
        let response = client
            .write_object("projects/_/buckets/test-bucket", "test-object", "payload")
            .send_unbuffered()
            .await;
        assert!(
            matches!(response, Err(ref e) if e.is_transport()),
            "{response:?}"
        );

        let captured = TestLayer::capture(&guard);
        check_debug_log(&captured, "write_object_unbuffered");

        #[cfg(google_cloud_unstable_tracing)]
        client_request_span(&captured, "write_object", "404");

        Ok(())
    }

    #[tokio::test]
    async fn open_object() -> anyhow::Result<()> {
        use gaxi::grpc::tonic::Status as TonicStatus;
        use google_cloud_gax::error::rpc::Code;
        use storage_grpc_mock::{MockStorage, start};

        let guard = TestLayer::initialize();

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object()
            .return_once(|_| Err(TonicStatus::not_found("not here")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;

        let client = crate::client::Storage::builder()
            .with_credentials(Anonymous::new().build())
            .with_endpoint(endpoint.clone())
            .with_tracing()
            .build()
            .await?;
        let response = client
            .open_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await;
        assert!(
            matches!(response, Err(ref e) if e.status().is_some_and(|s| s.code == Code::NotFound)),
            "{response:?}"
        );

        let captured = TestLayer::capture(&guard);
        check_debug_log(&captured, "open_object");

        #[cfg(google_cloud_unstable_tracing)]
        client_request_span(&captured, "open_object", "NOT_FOUND");
        Ok(())
    }

    #[track_caller]
    fn check_debug_log(captured: &Vec<CapturedSpan>, method: &'static str) {
        let span = captured
            .iter()
            .find(|s| s.name == method)
            .unwrap_or_else(|| panic!("missing `{method}` span in capture: {captured:#?}"));

        let got = BTreeMap::from_iter(span.attributes.clone());
        let want = ["self", "options", "request"];
        let missing = want
            .iter()
            .filter(|k| !got.contains_key(**k))
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "missing = {missing:?}\ngot  = {:?}\nwant = {want:?}\nfull = {got:#?}",
            got.keys().collect::<Vec<_>>(),
        );
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[track_caller]
    fn client_request_span(
        captured: &Vec<CapturedSpan>,
        method: &'static str,
        error_type: &'static str,
    ) {
        const EXPECTED_ATTRIBUTES: [(&str, &str); 8] = [
            (OTEL_KIND, OTEL_KIND_INTERNAL),
            (RPC_SYSTEM, RPC_SYSTEM_HTTP),
            (RPC_SERVICE, "storage"),
            (OTEL_STATUS_CODE, "ERROR"),
            (GCP_CLIENT_SERVICE, "storage"),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust"),
            (GCP_CLIENT_ARTIFACT, "google-cloud-storage"),
            (GCP_CLIENT_LANGUAGE, GCP_CLIENT_LANGUAGE_RUST),
        ];
        let span = captured
            .iter()
            .find(|s| s.name == "client_request")
            .unwrap_or_else(|| panic!("missing `client_request` span in capture: {captured:#?}"));
        let got = BTreeMap::from_iter(span.attributes.clone());
        // This is a subset of the fields, but good enough to catch most
        // mistakes. Recall that we use a macro, which is already tested.
        let want = BTreeMap::<String, AttributeValue>::from_iter(
            EXPECTED_ATTRIBUTES
                .iter()
                .map(|(k, v)| (k.to_string(), AttributeValue::from(*v)))
                .chain(
                    [
                        ("gax.client.span", true.into()),
                        (
                            OTEL_NAME,
                            format!("google_cloud_storage::client::Storage::{method}").into(),
                        ),
                        (RPC_METHOD, method.into()),
                        (ERROR_TYPE, error_type.into()),
                    ]
                    .map(|(k, v)| (k.to_string(), v)),
                ),
        );
        let mismatch = want
            .iter()
            .filter(|(k, v)| !got.get(k.as_str()).is_some_and(|g| g == *v))
            .collect::<Vec<_>>();
        assert!(
            mismatch.is_empty(),
            "mismatch = {mismatch:?}\ngot      = {got:?}\nwant     = {want:?}"
        );
    }
}
