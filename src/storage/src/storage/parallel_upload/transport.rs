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

//! Implements parallel upload sessions over gRPC.

use super::segment_payload::SegmentPayload;
use super::{Client, GrpcClient, TonicStreaming};
use crate::google::storage::v2;
use crate::model::Object;
use crate::model_ext::{AbortSessionResponse, CreateObjectWriteSessionRequest, Segment};
use crate::request_options::RequestOptions;
use crate::storage::info::X_GOOG_API_CLIENT_HEADER;
use crate::streaming_source::StreamingSource;
use crate::stub::ParallelUploadSession;
use crate::{Error, Result};
use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::{Extensions, GrpcMethod};
use gaxi::path_parameter::PathMismatchBuilder;
use gaxi::prost::FromProto;
use gaxi::routing_parameter::{self, Segment as PathSegment};
use google_cloud_gax::error::binding::BindingError;
use http::uri::PathAndQuery;
use std::collections::BTreeMap;
use std::sync::Mutex;
use tokio::sync::mpsc::{self, Receiver, Sender};

/// Maximum payload size per `BidiWriteSegmentRequest` message (2 MiB).
const MAX_WRITE_CHUNK_SIZE: usize = 2 * 1024 * 1024;
/// Bounded request queue depth per segment stream.
const WRITE_CHANNEL_CAPACITY: usize = 2;

/// The result of sending the requests for one segment.
enum SendOutcome {
    /// All the requests were sent. Holds the number of bytes sent.
    Sent(u64),
    /// The stream closed before all the requests were sent. This is not an
    /// error on its own, so that an error in the response takes precedence.
    StreamClosed,
}

/// Implements [crate::stub::ParallelUploadSession] using gRPC.
///
/// # Parameters
/// - `T`: a type implementing the [`Client`] trait (mocked in unit tests).
pub(crate) struct ParallelUploadSessionTransport<T = GrpcClient> {
    client: T,
    options: RequestOptions,
    session_name: String,
    request_params: String,
    segment_size_bytes: u64,
    /// The parameters from the request that created the session. They apply to
    /// the requests that write and finalize the object too.
    params: Option<v2::CommonObjectRequestParams>,
    /// The size in bytes of each completed segment, by segment number.
    completed_segments: Mutex<BTreeMap<u32, u64>>,
}

impl<T> ParallelUploadSessionTransport<T>
where
    T: Client,
{
    /// Creates a new session.
    pub(crate) async fn create(
        client: T,
        request: CreateObjectWriteSessionRequest,
        options: RequestOptions,
    ) -> Result<Self> {
        let bucket = request.resource.as_ref().map(|r| r.bucket.as_str());
        let request_params = bucket_request_params(bucket)?;
        let request = v2::CreateObjectWriteSessionRequest::try_from(request)?;
        let params = request.common_object_request_params.clone();

        // TODO: configure idempotency via `crate::idempotency` in a follow-up.
        let response = client
            .create_object_write_session(
                method("CreateObjectWriteSession"),
                PathAndQuery::from_static("/google.storage.v2.Storage/CreateObjectWriteSession"),
                request,
                options.gax(),
                &X_GOOG_API_CLIENT_HEADER,
                &request_params,
            )
            .await?;
        let session = response.into_inner();
        let request_params = session_request_params(&session.name)?;
        let segment_size_bytes = u64::try_from(session.segment_size_bytes).map_err(Error::deser)?;
        if segment_size_bytes == 0 {
            return Err(Error::deser("the service returned a zero segment size"));
        }

        Ok(Self {
            client,
            options,
            session_name: session.name,
            request_params,
            segment_size_bytes,
            params,
            completed_segments: Mutex::default(),
        })
    }

    /// Validates the completed segments and returns their count.
    ///
    /// A session can only be finalized if segments `1..=N` are complete.
    fn validated_segment_count(&self) -> Result<i32> {
        // TODO: return a typed validation error instead of plain strings.
        let segments = self.completed_segments.lock().expect("never poisoned");
        if segments.is_empty() {
            return Err(Error::binding("cannot finalize a session without segments"));
        }
        for (&number, want) in segments.keys().zip(1_u32..) {
            if number != want {
                return Err(Error::binding(format!(
                    "cannot finalize a session with missing segments: expected segment {want}, found segment {number}"
                )));
            }
        }
        i32::try_from(segments.len()).map_err(Error::binding)
    }

    /// Makes a single attempt to write a segment.
    async fn write_segment_once(
        &self,
        segment_number: u32,
        segment_payload: &mut SegmentPayload,
    ) -> Result<Segment> {
        let (tx, rx) = mpsc::channel(WRITE_CHANNEL_CAPACITY);
        // Send the requests while waiting for the response. The response may
        // not arrive until the service receives `finish_write`.
        let (outcome, segment) = tokio::try_join!(
            self.send_segment_requests(segment_number, segment_payload, tx),
            self.recv_segment_response(rx),
        )?;
        let SendOutcome::Sent(bytes_written) = outcome else {
            return Err(Error::io(
                "segment write stream closed before finish_write was sent",
            ));
        };
        if u64::try_from(segment.size_bytes).ok() != Some(bytes_written) {
            return Err(Error::io(format!(
                "segment {segment_number} size mismatch: sent {bytes_written} bytes, server acknowledged {} bytes",
                segment.size_bytes
            )));
        }

        // Allow overwriting an existing entry for re-uploads and hedged attempts.
        self.completed_segments
            .lock()
            .expect("never poisoned")
            .insert(segment_number, bytes_written);
        Ok(Segment::from(segment))
    }

    async fn send_segment_requests(
        &self,
        segment_number: u32,
        segment_payload: &mut SegmentPayload,
        tx: Sender<v2::BidiWriteSegmentRequest>,
    ) -> Result<SendOutcome> {
        let mut bytes_written: u64 = 0;
        let mut is_first = true;

        while let Some(chunk) = segment_payload.next().await {
            let mut chunk = chunk?;
            while !chunk.is_empty() {
                let split_at = chunk.len().min(MAX_WRITE_CHUNK_SIZE);
                let piece = chunk.split_to(split_at);
                let piece_len = piece.len() as u64;

                let request = self.build_segment_request(
                    segment_number,
                    &mut is_first,
                    bytes_written,
                    Some(v2::ChecksummedData {
                        content: piece,
                        // TODO: compute per-chunk CRC32C.
                        crc32c: None,
                    }),
                    false,
                )?;
                if tx.send(request).await.is_err() {
                    return Ok(SendOutcome::StreamClosed);
                }
                bytes_written += piece_len;
            }
        }

        let final_request =
            self.build_segment_request(segment_number, &mut is_first, bytes_written, None, true)?;
        if tx.send(final_request).await.is_err() {
            return Ok(SendOutcome::StreamClosed);
        }
        Ok(SendOutcome::Sent(bytes_written))
    }

    async fn recv_segment_response(
        &self,
        rx: Receiver<v2::BidiWriteSegmentRequest>,
    ) -> Result<v2::Segment> {
        let response = self
            .client
            .bidi_write_segment(
                method("BidiWriteSegment"),
                PathAndQuery::from_static("/google.storage.v2.Storage/BidiWriteSegment"),
                rx,
                self.options.gax(),
                &X_GOOG_API_CLIENT_HEADER,
                &self.request_params,
            )
            .await?
            .map_err(to_gax_error)?;
        let mut stream = response.into_inner();
        let Some(message) = stream.next_message().await.map_err(to_gax_error)? else {
            return Err(Error::io("segment write stream closed without a response"));
        };
        message
            .segment
            .ok_or_else(|| Error::io("segment write response missing segment metadata"))
    }

    fn build_segment_request(
        &self,
        segment_number: u32,
        is_first: &mut bool,
        bytes_written: u64,
        checksummed_data: Option<v2::ChecksummedData>,
        finish_write: bool,
    ) -> Result<v2::BidiWriteSegmentRequest> {
        let write_offset = i64::try_from(bytes_written).map_err(Error::binding)?;
        let (name, common_object_request_params) = if std::mem::take(is_first) {
            (
                format!("{}/segments/{segment_number}", self.session_name),
                self.params.clone(),
            )
        } else {
            (String::new(), None)
        };
        Ok(v2::BidiWriteSegmentRequest {
            name,
            write_offset,
            checksummed_data,
            finish_write,
            common_object_request_params,
        })
    }
}

impl<T> ParallelUploadSession for ParallelUploadSessionTransport<T>
where
    T: Client,
{
    fn name(&self) -> &str {
        &self.session_name
    }

    fn segment_size_bytes(&self) -> u64 {
        self.segment_size_bytes
    }

    async fn write_segment(
        &self,
        segment_number: u32,
        mut segment_payload: SegmentPayload,
    ) -> Result<Segment> {
        // TODO: add per-segment retries, redirects, and hedging in follow-ups.
        self.write_segment_once(segment_number, &mut segment_payload)
            .await
    }

    async fn finalize(&self) -> Result<Object> {
        let total_segment_count = self.validated_segment_count()?;
        let request = v2::FinalizeObjectWriteSessionRequest {
            name: self.session_name.clone(),
            common_object_request_params: self.params.clone(),
            total_segment_count,
            ..v2::FinalizeObjectWriteSessionRequest::default()
        };

        // TODO: set `request_id` and configure idempotency via `crate::idempotency` in a follow-up.
        let response = self
            .client
            .finalize_object_write_session(
                method("FinalizeObjectWriteSession"),
                PathAndQuery::from_static("/google.storage.v2.Storage/FinalizeObjectWriteSession"),
                request,
                self.options.gax(),
                &X_GOOG_API_CLIENT_HEADER,
                &self.request_params,
            )
            .await?;
        FromProto::<Object>::cnv(response.into_inner()).map_err(Error::deser)
    }

    async fn abort(&self) -> Result<AbortSessionResponse> {
        let request = v2::AbortObjectWriteSessionRequest {
            name: self.session_name.clone(),
        };

        // TODO: configure idempotency via `crate::idempotency` in a follow-up.
        self.client
            .abort_object_write_session(
                method("AbortObjectWriteSession"),
                PathAndQuery::from_static("/google.storage.v2.Storage/AbortObjectWriteSession"),
                request,
                self.options.gax(),
                &X_GOOG_API_CLIENT_HEADER,
                &self.request_params,
            )
            .await?;
        Ok(AbortSessionResponse::new())
    }
}

impl<T> std::fmt::Debug for ParallelUploadSessionTransport<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The parameters may include an encryption key.
        let params = self.params.as_ref().map(|_| "[censored]");
        let mut debug = f.debug_struct("ParallelUploadSessionTransport");
        debug
            .field("client", &self.client)
            .field("options", &self.options)
            .field("session_name", &self.session_name)
            .field("request_params", &self.request_params)
            .field("segment_size_bytes", &self.segment_size_bytes)
            .field("params", &params);
        match self.completed_segments.try_lock() {
            Ok(segments) => debug.field("completed_segments", &segments.len()),
            Err(_) => debug.field("completed_segments", &"<locked>"),
        };
        debug.finish()
    }
}

pub(super) fn method(name: &'static str) -> Extensions {
    let mut extensions = Extensions::new();
    extensions.insert(GrpcMethod::new("google.storage.v2.Storage", name));
    extensions
}

/// Returns the routing parameters for requests routed by bucket name.
///
/// The routing template is `{bucket=**}`.
fn bucket_request_params(bucket: Option<&str>) -> Result<String> {
    use PathSegment::MultiWildcard;

    if let Some(bucket) = routing_parameter::value(bucket, &[], &[MultiWildcard], &[]) {
        return Ok(routing_parameter::format(&[Some(("bucket", bucket))]));
    }
    let mismatch = PathMismatchBuilder::default()
        .maybe_add(bucket, &[MultiWildcard], "resource.bucket", "**")
        .build();
    Err(Error::binding(BindingError {
        paths: vec![mismatch],
    }))
}

/// Returns the routing parameters for requests routed by session name.
///
/// The routing template is `{bucket=projects/*/buckets/*}/**`.
fn session_request_params(name: &str) -> Result<String> {
    use PathSegment::{Literal, SingleWildcard, TrailingMultiWildcard};

    let bucket = routing_parameter::value(
        Some(name),
        &[],
        &[
            Literal("projects/"),
            SingleWildcard,
            Literal("/buckets/"),
            SingleWildcard,
        ],
        &[TrailingMultiWildcard],
    );
    if let Some(bucket) = bucket {
        return Ok(routing_parameter::format(&[Some(("bucket", bucket))]));
    }
    Err(Error::deser(format!("invalid session name: {name}")))
}

#[cfg(test)]
mod tests {
    use super::super::mocks::{MockStream, MockTestClient, SharedMockClient};
    use super::super::tests::{permanent_error, test_options, transient_status};
    use super::*;
    use crate::model::CommonObjectRequestParams;
    use crate::streaming_source::{Payload, Seek};
    use bytes::Bytes;
    use gaxi::grpc::tonic::{
        Response as TonicResponse, Result as TonicResult, Status as TonicStatus,
    };
    use google_cloud_gax::error::binding::{PathMismatch, SubstitutionFail, SubstitutionMismatch};
    use google_cloud_gax::error::rpc::Code;
    use static_assertions::assert_impl_all;
    use std::collections::VecDeque;
    use std::error::Error as _;
    use std::io::{Error as IoError, ErrorKind};
    use test_case::test_case;
    use tokio::sync::oneshot;

    const BUCKET: &str = "projects/_/buckets/test-bucket";
    const OBJECT: &str = "test-object";
    const SESSION: &str = "projects/_/buckets/test-bucket/objectWriteSessions/test-session";
    const SEGMENT_SIZE: u64 = 1024;
    const ROUTING: &str = "bucket=projects%2F_%2Fbuckets%2Ftest-bucket";

    fn create_request() -> CreateObjectWriteSessionRequest {
        CreateObjectWriteSessionRequest::new()
            .set_resource(Object::new().set_bucket(BUCKET).set_name(OBJECT))
    }

    fn proto_session(segment_size_bytes: i64) -> v2::ObjectWriteSession {
        v2::ObjectWriteSession {
            name: SESSION.to_string(),
            segment_size_bytes,
            ..v2::ObjectWriteSession::default()
        }
    }

    fn proto_params() -> v2::CommonObjectRequestParams {
        v2::CommonObjectRequestParams {
            encryption_algorithm: "AES256".to_string(),
            encryption_key_bytes: Bytes::from_static(b"test-only-key"),
            ..v2::CommonObjectRequestParams::default()
        }
    }

    async fn create(
        mock: MockTestClient,
        request: CreateObjectWriteSessionRequest,
    ) -> Result<ParallelUploadSessionTransport<SharedMockClient>> {
        ParallelUploadSessionTransport::create(SharedMockClient::new(mock), request, test_options())
            .await
    }

    fn transport(
        mock: MockTestClient,
        session_name: &str,
        segments: &[(u32, u64)],
    ) -> ParallelUploadSessionTransport<SharedMockClient> {
        ParallelUploadSessionTransport {
            client: SharedMockClient::new(mock),
            options: test_options(),
            session_name: session_name.to_string(),
            request_params: session_request_params(session_name).expect("valid test session name"),
            segment_size_bytes: SEGMENT_SIZE,
            params: None,
            completed_segments: Mutex::new(segments.iter().copied().collect()),
        }
    }

    fn segment_response(
        segment_number: u32,
        size_bytes: i64,
        crc32c: u32,
    ) -> v2::BidiWriteSegmentResponse {
        v2::BidiWriteSegmentResponse {
            segment: Some(v2::Segment {
                name: format!("{SESSION}/segments/{segment_number}"),
                size_bytes,
                segment_crc32c: crc32c,
            }),
        }
    }

    fn completed(
        transport: &ParallelUploadSessionTransport<SharedMockClient>,
    ) -> BTreeMap<u32, u64> {
        transport
            .completed_segments
            .lock()
            .expect("never poisoned")
            .clone()
    }

    /// Simulates the service side of a segment stream.
    ///
    /// Replies with `response`, if any, once it receives `finish_write`. Sends
    /// the requests it received to `captured` once the client closes the stream.
    fn collect_and_respond(
        mut rx: Receiver<v2::BidiWriteSegmentRequest>,
        response: Option<v2::BidiWriteSegmentResponse>,
        captured: oneshot::Sender<Vec<v2::BidiWriteSegmentRequest>>,
    ) -> TonicResponse<MockStream> {
        let (resp_tx, resp_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut requests = Vec::new();
            while let Some(request) = rx.recv().await {
                let finished = request.finish_write;
                requests.push(request);
                if finished {
                    break;
                }
            }
            if let Some(response) = response {
                let _ = resp_tx.send(Ok(response)).await;
            }
            while let Some(request) = rx.recv().await {
                requests.push(request);
            }
            let _ = captured.send(requests);
        });
        TonicResponse::new(resp_rx)
    }

    /// Expects one segment stream served by `collect_and_respond` and returns
    /// the receiver for the requests.
    fn expect_segment_stream(
        mock: &mut MockTestClient,
        response: Option<v2::BidiWriteSegmentResponse>,
    ) -> oneshot::Receiver<Vec<v2::BidiWriteSegmentRequest>> {
        let (captured_tx, captured_rx) = oneshot::channel();
        mock.expect_bidi_write_segment()
            .times(1)
            .return_once(move |_, _, rx, _, _, _| {
                Ok(Ok(collect_and_respond(rx, response, captured_tx)))
            });
        captured_rx
    }

    /// Expects one segment stream where the service stops reading after the
    /// first request and then replies with `reply`.
    fn expect_early_reply(
        mock: &mut MockTestClient,
        reply: TonicResult<v2::BidiWriteSegmentResponse>,
        yield_first: bool,
    ) {
        mock.expect_bidi_write_segment()
            .times(1)
            .return_once(move |_, _, mut rx, _, _, _| {
                let (resp_tx, resp_rx) = mpsc::channel(1);
                tokio::spawn(async move {
                    let _ = rx.recv().await;
                    drop(rx);
                    if yield_first {
                        // Let the sender find the closed stream before the reply.
                        tokio::task::yield_now().await;
                    }
                    let _ = resp_tx.send(reply).await;
                });
                Ok(Ok(TonicResponse::new(resp_rx)))
            });
    }

    /// Returns a payload with more chunks than the request channel can hold, so
    /// the sender is still running when the stream fails.
    fn many_chunks() -> SegmentPayload {
        let chunks = vec![Bytes::from_static(b"x"); WRITE_CHANNEL_CAPACITY + 3];
        SegmentPayload::new(Payload::from(chunks))
    }

    type ScriptedItem = std::result::Result<Bytes, ErrorKind>;

    /// A source of unknown size that yields scripted chunks and errors.
    #[derive(Clone)]
    struct ScriptedSource(VecDeque<ScriptedItem>);

    impl StreamingSource for ScriptedSource {
        type Error = IoError;

        async fn next(&mut self) -> Option<std::io::Result<Bytes>> {
            self.0.pop_front().map(|item| item.map_err(IoError::from))
        }
    }

    impl Seek for ScriptedSource {
        type Error = IoError;

        async fn seek(&mut self, _offset: u64) -> std::io::Result<()> {
            Err(ErrorKind::Unsupported.into())
        }
    }

    fn scripted_payload(items: impl IntoIterator<Item = ScriptedItem>) -> SegmentPayload {
        let source = ScriptedSource(items.into_iter().collect());
        SegmentPayload::new(Payload::from_stream(source))
    }

    #[track_caller]
    fn assert_method(extensions: &Extensions, path: &PathAndQuery, want: &str) {
        let got = extensions.get::<GrpcMethod>();
        assert!(
            matches!(got, Some(m) if m.service() == "google.storage.v2.Storage" && m.method() == want),
            "missing or mismatched method, want={want}"
        );
        assert_eq!(path.path(), format!("/google.storage.v2.Storage/{want}"));
    }

    #[track_caller]
    fn assert_binding_error(err: Error, field_name: &'static str, problem: SubstitutionFail) {
        assert!(err.is_binding(), "{err:?}");
        let got = err
            .source()
            .and_then(|e| e.downcast_ref::<BindingError>())
            .expect("should be a BindingError");
        let want = BindingError {
            paths: vec![PathMismatch {
                subs: vec![SubstitutionMismatch {
                    field_name,
                    problem,
                }],
            }],
        };
        assert_eq!(got, &want);
    }

    #[test]
    fn assertions() {
        assert_impl_all!(ParallelUploadSessionTransport: std::fmt::Debug, Send, Sync);
        assert_impl_all!(ParallelUploadSessionTransport: ParallelUploadSession);
    }

    #[test]
    fn futures_are_send() {
        fn need_send<T: Send>(_val: &T) {}

        let client = SharedMockClient::new(MockTestClient::new());
        let create =
            ParallelUploadSessionTransport::create(client, create_request(), test_options());
        need_send(&create);

        let transport = transport(MockTestClient::new(), SESSION, &[]);
        let write = transport.write_segment(1, SegmentPayload::new(Payload::from("hello")));
        need_send(&write);
        let finalize = transport.finalize();
        need_send(&finalize);
        let abort = transport.abort();
        need_send(&abort);
    }

    #[test]
    fn debug_redacts_params() {
        let mut t = transport(MockTestClient::new(), SESSION, &[]);
        let without_params = format!("{t:?}");
        assert!(without_params.contains("params: None"), "{without_params}");
        assert!(!without_params.contains("[censored]"), "{without_params}");

        t.params = Some(proto_params());
        let with_params = format!("{t:?}");
        assert!(with_params.contains("[censored]"), "{with_params}");
        assert!(!with_params.contains("test-only-key"), "{with_params}");
    }

    #[test]
    fn debug_formats_completed_segments() {
        let t = transport(
            MockTestClient::new(),
            SESSION,
            &[(1, SEGMENT_SIZE), (2, 123)],
        );
        let unlocked = format!("{t:?}");
        assert!(unlocked.contains("completed_segments: 2"), "{unlocked}");
        assert!(!unlocked.contains("123"), "{unlocked}");

        let _guard = t.completed_segments.lock().expect("never poisoned");
        let locked = format!("{t:?}");
        assert!(
            locked.contains("completed_segments: \"<locked>\""),
            "{locked}"
        );
    }

    #[tokio::test]
    async fn create_success() -> anyhow::Result<()> {
        let mut mock = MockTestClient::new();
        mock.expect_create_object_write_session()
            .times(1)
            .return_once(|extensions, path, request, options, header, params| {
                assert_method(&extensions, &path, "CreateObjectWriteSession");
                assert_eq!(header, X_GOOG_API_CLIENT_HEADER.as_str());
                assert_eq!(params, ROUTING);
                assert_eq!(options.user_agent().as_deref(), Some("test-agent"));
                let want = v2::CreateObjectWriteSessionRequest {
                    object_write_session_spec: Some(v2::ObjectWriteSessionSpec {
                        resource: Some(v2::Object {
                            bucket: BUCKET.to_string(),
                            name: OBJECT.to_string(),
                            ..v2::Object::default()
                        }),
                        predefined_acl: "private".to_string(),
                    }),
                    common_object_request_params: None,
                    preferred_segment_size_bytes: 2048,
                };
                assert_eq!(request, want);
                Ok(TonicResponse::new(proto_session(SEGMENT_SIZE as i64)))
            });

        let request = create_request()
            .set_predefined_acl("private")
            .set_preferred_segment_size_bytes(2048);
        let mut options = test_options();
        options.with_user_agent("test-agent");
        let transport =
            ParallelUploadSessionTransport::create(SharedMockClient::new(mock), request, options)
                .await?;
        assert_eq!(transport.name(), SESSION);
        assert_eq!(transport.segment_size_bytes(), SEGMENT_SIZE);
        assert_eq!(transport.options.user_agent.as_deref(), Some("test-agent"));
        Ok(())
    }

    #[test_case(CreateObjectWriteSessionRequest::new(); "without resource")]
    #[test_case(CreateObjectWriteSessionRequest::new().set_resource(Object::new().set_name(OBJECT)); "without bucket")]
    #[tokio::test]
    async fn create_binding_error(request: CreateObjectWriteSessionRequest) {
        let mut mock = MockTestClient::new();
        // Binding errors are detected before a request is sent.
        mock.expect_create_object_write_session().never();

        let err = create(mock, request).await.unwrap_err();
        assert_binding_error(
            err,
            "resource.bucket",
            SubstitutionFail::UnsetExpecting("**"),
        );
    }

    #[tokio::test]
    async fn create_rpc_error() {
        let mut mock = MockTestClient::new();
        mock.expect_create_object_write_session()
            .times(1)
            .return_once(|_, _, _, _, _, _| Err(permanent_error()));

        let err = create(mock, create_request()).await.unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
    }

    #[test_case(-1; "negative")]
    #[test_case(0; "zero")]
    #[tokio::test]
    async fn create_invalid_segment_size(size: i64) {
        let mut mock = MockTestClient::new();
        mock.expect_create_object_write_session()
            .times(1)
            .return_once(move |_, _, _, _, _, _| Ok(TonicResponse::new(proto_session(size))));

        let err = create(mock, create_request()).await.unwrap_err();
        assert!(err.is_deserialization(), "{err:?}");
    }

    #[test_case(""; "empty")]
    #[test_case("bad-session-name"; "malformed")]
    #[tokio::test]
    async fn create_invalid_session_name(name: &str) {
        let mut session = proto_session(SEGMENT_SIZE as i64);
        session.name = name.to_string();
        let mut mock = MockTestClient::new();
        mock.expect_create_object_write_session()
            .times(1)
            .return_once(move |_, _, _, _, _, _| Ok(TonicResponse::new(session)));

        let err = create(mock, create_request()).await.unwrap_err();
        assert!(err.is_deserialization(), "{err:?}");
    }

    #[tokio::test]
    async fn write_segment_success() -> anyhow::Result<()> {
        let params = proto_params();
        let (captured_tx, captured_rx) = oneshot::channel();

        let mut mock = MockTestClient::new();
        mock.expect_bidi_write_segment().times(1).return_once(
            move |extensions, path, rx, options, header, routing| {
                assert_method(&extensions, &path, "BidiWriteSegment");
                assert_eq!(header, X_GOOG_API_CLIENT_HEADER.as_str());
                assert_eq!(routing, ROUTING);
                assert_eq!(options.user_agent().as_deref(), Some("test-agent"));
                let response = segment_response(1, 11, 42);
                Ok(Ok(collect_and_respond(rx, Some(response), captured_tx)))
            },
        );

        let mut transport = transport(mock, SESSION, &[]);
        transport.params = Some(params.clone());
        transport.options.with_user_agent("test-agent");

        // The empty chunk is skipped.
        let payload = SegmentPayload::new(Payload::from(vec![
            Bytes::from_static(b"hello"),
            Bytes::new(),
            Bytes::from_static(b" world"),
        ]));
        let got = transport.write_segment(1, payload).await?;
        let want = Segment::new()
            .set_name(format!("{SESSION}/segments/1"))
            .set_size_bytes(11)
            .set_segment_crc32c(42);
        assert_eq!(got, want);
        assert_eq!(completed(&transport), BTreeMap::from([(1, 11)]));

        // Only the first request includes the name and the parameters.
        let want = vec![
            v2::BidiWriteSegmentRequest {
                name: format!("{SESSION}/segments/1"),
                checksummed_data: Some(v2::ChecksummedData {
                    content: Bytes::from_static(b"hello"),
                    crc32c: None,
                }),
                common_object_request_params: Some(params),
                ..v2::BidiWriteSegmentRequest::default()
            },
            v2::BidiWriteSegmentRequest {
                write_offset: 5,
                checksummed_data: Some(v2::ChecksummedData {
                    content: Bytes::from_static(b" world"),
                    crc32c: None,
                }),
                ..v2::BidiWriteSegmentRequest::default()
            },
            v2::BidiWriteSegmentRequest {
                write_offset: 11,
                finish_write: true,
                ..v2::BidiWriteSegmentRequest::default()
            },
        ];
        assert_eq!(captured_rx.await?, want);
        Ok(())
    }

    #[test_case(&[MAX_WRITE_CHUNK_SIZE]; "exact max chunk boundary")]
    #[test_case(&[MAX_WRITE_CHUNK_SIZE, MAX_WRITE_CHUNK_SIZE, 5]; "multiple chunks with remainder")]
    #[tokio::test]
    async fn write_segment_splits_large_chunks(expected_lens: &[usize]) -> anyhow::Result<()> {
        let total_len: usize = expected_lens.iter().sum();
        let data = (0..total_len).map(|i| (i % 251) as u8).collect::<Bytes>();
        let size = data.len() as i64;

        let mut mock = MockTestClient::new();
        let requests = expect_segment_stream(&mut mock, Some(segment_response(2, size, 99)));

        let transport = transport(mock, SESSION, &[]);
        let payload = SegmentPayload::new(Payload::from(data.clone()));
        let got = transport.write_segment(2, payload).await?;
        assert_eq!(got.size_bytes, size);

        let requests = requests.await?;
        let got = requests
            .iter()
            .map(|r| {
                let len = r.checksummed_data.as_ref().map(|d| d.content.len());
                (r.write_offset, len, r.finish_write)
            })
            .collect::<Vec<_>>();
        let mut offset = 0_i64;
        let mut want = Vec::with_capacity(expected_lens.len() + 1);
        for &len in expected_lens {
            want.push((offset, Some(len), false));
            offset += len as i64;
        }
        want.push((size, None, true));
        assert_eq!(got, want);
        let sent = requests
            .into_iter()
            .filter_map(|r| r.checksummed_data)
            .flat_map(|d| d.content)
            .collect::<Bytes>();
        assert!(sent == data, "the chunks must add up to the payload");
        Ok(())
    }

    #[tokio::test]
    async fn write_segment_empty_payload() -> anyhow::Result<()> {
        let params = proto_params();
        let mut mock = MockTestClient::new();
        let requests = expect_segment_stream(&mut mock, Some(segment_response(1, 0, 0)));

        let mut transport = transport(mock, SESSION, &[]);
        transport.params = Some(params.clone());
        let payload = SegmentPayload::new(Payload::from(Bytes::new()));
        let got = transport.write_segment(1, payload).await?;
        assert_eq!(got.size_bytes, 0);
        assert_eq!(completed(&transport), BTreeMap::from([(1, 0)]));

        // A single request has the name, the parameters, and `finish_write`.
        let want = v2::BidiWriteSegmentRequest {
            name: format!("{SESSION}/segments/1"),
            finish_write: true,
            common_object_request_params: Some(params),
            ..v2::BidiWriteSegmentRequest::default()
        };
        assert_eq!(requests.await?, vec![want]);
        Ok(())
    }

    #[tokio::test]
    async fn write_segment_overwrites_existing_entry() -> anyhow::Result<()> {
        let mut mock = MockTestClient::new();
        let _requests = expect_segment_stream(&mut mock, Some(segment_response(1, 8, 20)));

        let transport = transport(mock, SESSION, &[(1, 5)]);
        let payload = SegmentPayload::new(Payload::from("override"));
        let got = transport.write_segment(1, payload).await?;
        assert_eq!(got.size_bytes, 8);
        assert_eq!(completed(&transport), BTreeMap::from([(1, 8)]));
        Ok(())
    }

    #[tokio::test]
    async fn write_segment_source_read_error() -> anyhow::Result<()> {
        let mut mock = MockTestClient::new();
        let requests = expect_segment_stream(&mut mock, None);

        // The first three chunks fill the request channel, so the stream is
        // open by the time the source fails.
        let payload = scripted_payload([
            Ok(Bytes::from_static(b"chunk-1")),
            Ok(Bytes::from_static(b"chunk-2")),
            Ok(Bytes::from_static(b"chunk-3")),
            Err(ErrorKind::BrokenPipe),
        ]);
        let transport = transport(mock, SESSION, &[]);
        let err = transport.write_segment(1, payload).await.unwrap_err();
        assert!(err.is_serialization(), "{err:?}");
        assert!(completed(&transport).is_empty());
        let requests = requests.await?;
        assert!(!requests.is_empty());
        assert!(requests.iter().all(|r| !r.finish_write), "{requests:?}");
        Ok(())
    }

    #[test_case(true; "client error")]
    #[test_case(false; "initial grpc status")]
    #[tokio::test]
    async fn write_segment_open_error(client_error: bool) {
        let mut mock = MockTestClient::new();
        mock.expect_bidi_write_segment()
            .times(1)
            .return_once(move |_, _, _, _, _, _| {
                if client_error {
                    Err(permanent_error())
                } else {
                    Ok(Err(TonicStatus::permission_denied("uh-oh")))
                }
            });

        let transport = transport(mock, SESSION, &[]);
        let err = transport.write_segment(1, many_chunks()).await.unwrap_err();
        assert_eq!(
            err.status().map(|s| s.code),
            Some(Code::PermissionDenied),
            "{err:?}"
        );
        assert!(completed(&transport).is_empty());
    }

    #[test_case(false; "immediate status")]
    #[test_case(true; "status after sender sees closed stream")]
    #[tokio::test]
    async fn write_segment_mid_stream_server_error(yield_first: bool) {
        let mut mock = MockTestClient::new();
        expect_early_reply(&mut mock, Err(transient_status()), yield_first);

        let transport = transport(mock, SESSION, &[]);
        let err = transport.write_segment(1, many_chunks()).await.unwrap_err();
        assert_eq!(
            err.status().map(|s| s.code),
            Some(Code::Unavailable),
            "{err:?}"
        );
        assert!(completed(&transport).is_empty());
    }

    #[tokio::test]
    async fn write_segment_premature_response() {
        let mut mock = MockTestClient::new();
        // The service acknowledges the segment without waiting for `finish_write`.
        let size = WRITE_CHANNEL_CAPACITY as i64 + 3;
        expect_early_reply(&mut mock, Ok(segment_response(1, size, 0)), false);

        let transport = transport(mock, SESSION, &[]);
        let err = transport.write_segment(1, many_chunks()).await.unwrap_err();
        assert!(err.is_io(), "{err:?}");
        assert!(completed(&transport).is_empty());
    }

    #[test_case(None; "stream closed without response")]
    #[test_case(Some(v2::BidiWriteSegmentResponse::default()); "missing segment metadata")]
    #[test_case(Some(segment_response(1, 4, 0)); "short ack")]
    #[test_case(Some(segment_response(1, 6, 0)); "long ack")]
    #[test_case(Some(segment_response(1, -1, 0)); "negative ack")]
    #[tokio::test]
    async fn write_segment_invalid_response(response: Option<v2::BidiWriteSegmentResponse>) {
        let mut mock = MockTestClient::new();
        let _requests = expect_segment_stream(&mut mock, response);

        let transport = transport(mock, SESSION, &[]);
        let payload = SegmentPayload::new(Payload::from("hello"));
        let err = transport.write_segment(1, payload).await.unwrap_err();
        assert!(err.is_io(), "{err:?}");
        assert!(completed(&transport).is_empty());
    }

    #[tokio::test]
    async fn finalize_success() -> anyhow::Result<()> {
        let mut mock = MockTestClient::new();
        mock.expect_finalize_object_write_session()
            .times(1)
            .return_once(|extensions, path, request, options, header, params| {
                assert_method(&extensions, &path, "FinalizeObjectWriteSession");
                assert_eq!(header, X_GOOG_API_CLIENT_HEADER.as_str());
                assert_eq!(params, ROUTING);
                assert_eq!(options.user_agent().as_deref(), Some("test-agent"));
                let want = v2::FinalizeObjectWriteSessionRequest {
                    name: SESSION.to_string(),
                    total_segment_count: 3,
                    ..v2::FinalizeObjectWriteSessionRequest::default()
                };
                assert_eq!(request, want);
                Ok(TonicResponse::new(v2::Object {
                    bucket: BUCKET.to_string(),
                    name: OBJECT.to_string(),
                    size: 2 * SEGMENT_SIZE as i64 + 5,
                    ..v2::Object::default()
                }))
            });

        let segments = [(1, SEGMENT_SIZE), (2, SEGMENT_SIZE), (3, 5)];
        let mut transport = transport(mock, SESSION, &segments);
        transport.options.with_user_agent("test-agent");
        let got = transport.finalize().await?;
        let want = Object::new()
            .set_bucket(BUCKET)
            .set_name(OBJECT)
            .set_size(2 * SEGMENT_SIZE as i64 + 5);
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(&[(1, 0)], 1; "a single empty segment")]
    #[test_case(&[(1, SEGMENT_SIZE)], 1; "a single full segment")]
    #[test_case(&[(1, SEGMENT_SIZE), (2, SEGMENT_SIZE)], 2; "only full segments")]
    #[test_case(&[(1, 5), (2, 5)], 2; "segment sizes left to the server")]
    #[tokio::test]
    async fn finalize_segment_count(segments: &[(u32, u64)], want: i32) -> anyhow::Result<()> {
        let mut mock = MockTestClient::new();
        mock.expect_finalize_object_write_session()
            .times(1)
            .return_once(move |_, _, request, _, _, _| {
                assert_eq!(request.total_segment_count, want);
                Ok(TonicResponse::new(v2::Object::default()))
            });

        let transport = transport(mock, SESSION, segments);
        let _ = transport.finalize().await?;
        Ok(())
    }

    #[tokio::test]
    async fn finalize_uses_session_name_for_routing() -> anyhow::Result<()> {
        const NAME: &str = "projects/123/buckets/test-bucket/objectWriteSessions/test-session";

        let mut mock = MockTestClient::new();
        mock.expect_create_object_write_session()
            .times(1)
            .return_once(|_, _, _, _, _, params| {
                assert_eq!(params, ROUTING);
                let mut session = proto_session(SEGMENT_SIZE as i64);
                session.name = NAME.to_string();
                Ok(TonicResponse::new(session))
            });
        mock.expect_finalize_object_write_session()
            .times(1)
            .return_once(|_, _, request, _, _, params| {
                assert_eq!(request.name, NAME);
                assert_eq!(params, "bucket=projects%2F123%2Fbuckets%2Ftest-bucket");
                Ok(TonicResponse::new(v2::Object::default()))
            });

        let transport = create(mock, create_request()).await?;
        transport
            .completed_segments
            .lock()
            .expect("never poisoned")
            .insert(1, 5);
        let _ = transport.finalize().await?;
        Ok(())
    }

    #[tokio::test]
    async fn finalize_sends_params_from_create() -> anyhow::Result<()> {
        let want = proto_params();

        let mut mock = MockTestClient::new();
        mock.expect_create_object_write_session()
            .times(1)
            .return_once({
                let want = want.clone();
                move |_, _, request, _, _, _| {
                    assert_eq!(request.common_object_request_params, Some(want));
                    Ok(TonicResponse::new(proto_session(SEGMENT_SIZE as i64)))
                }
            });
        mock.expect_finalize_object_write_session()
            .times(1)
            .return_once(move |_, _, request, _, _, _| {
                assert_eq!(request.common_object_request_params, Some(want));
                Ok(TonicResponse::new(v2::Object::default()))
            });

        let request = create_request().set_params(
            CommonObjectRequestParams::new()
                .set_encryption_algorithm("AES256")
                .set_encryption_key_bytes(Bytes::from_static(b"test-only-key")),
        );
        let transport = create(mock, request).await?;

        transport
            .completed_segments
            .lock()
            .expect("never poisoned")
            .insert(1, 5);
        let _ = transport.finalize().await?;
        Ok(())
    }

    #[test_case(&[]; "no segments")]
    #[test_case(&[(2, 5)]; "missing first segment")]
    #[test_case(&[(1, SEGMENT_SIZE), (3, 5)]; "missing middle segment")]
    #[test_case(&[(0, SEGMENT_SIZE), (1, 5)]; "segment zero")]
    #[tokio::test]
    async fn finalize_invalid_segments(segments: &[(u32, u64)]) {
        let mut mock = MockTestClient::new();
        // The problem is detected before a request is sent.
        mock.expect_finalize_object_write_session().never();

        let transport = transport(mock, SESSION, segments);
        let err = transport.finalize().await.unwrap_err();
        assert!(err.is_binding(), "{err:?}");
    }

    #[tokio::test]
    async fn finalize_rpc_error() {
        let mut mock = MockTestClient::new();
        mock.expect_finalize_object_write_session()
            .times(1)
            .return_once(|_, _, _, _, _, _| Err(permanent_error()));

        let transport = transport(mock, SESSION, &[(1, 5)]);
        let err = transport.finalize().await.unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
    }

    #[tokio::test]
    async fn abort_success() -> anyhow::Result<()> {
        let mut mock = MockTestClient::new();
        mock.expect_abort_object_write_session()
            .times(1)
            .return_once(|extensions, path, request, options, header, params| {
                assert_method(&extensions, &path, "AbortObjectWriteSession");
                assert_eq!(header, X_GOOG_API_CLIENT_HEADER.as_str());
                assert_eq!(params, ROUTING);
                assert_eq!(options.user_agent().as_deref(), Some("test-agent"));
                let want = v2::AbortObjectWriteSessionRequest {
                    name: SESSION.to_string(),
                };
                assert_eq!(request, want);
                Ok(TonicResponse::new(()))
            });

        // A session can be aborted before any segment is complete.
        let mut transport = transport(mock, SESSION, &[]);
        transport.options.with_user_agent("test-agent");
        let got = transport.abort().await?;
        assert_eq!(got, AbortSessionResponse::new());
        Ok(())
    }

    #[tokio::test]
    async fn abort_rpc_error() {
        let mut mock = MockTestClient::new();
        mock.expect_abort_object_write_session()
            .times(1)
            .return_once(|_, _, _, _, _, _| Err(permanent_error()));

        let transport = transport(mock, SESSION, &[]);
        let err = transport.abort().await.unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
    }
}
