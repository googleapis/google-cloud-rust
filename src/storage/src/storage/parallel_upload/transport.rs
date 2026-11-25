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
use super::{Client, GrpcClient};
use crate::google::storage::v2;
use crate::model::Object;
use crate::model_ext::{AbortSessionResponse, CreateObjectWriteSessionRequest, Segment};
use crate::request_options::RequestOptions;
use crate::storage::info::X_GOOG_API_CLIENT_HEADER;
use crate::stub::ParallelUploadSession;
use crate::{Error, Result};
use gaxi::grpc::tonic::{Extensions, GrpcMethod};
use gaxi::path_parameter::PathMismatchBuilder;
use gaxi::prost::FromProto;
use gaxi::routing_parameter::{self, Segment as PathSegment};
use google_cloud_gax::error::binding::BindingError;
use http::uri::PathAndQuery;
use std::collections::BTreeMap;
use std::sync::Mutex;

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
    /// A session can only be finalized if segments `1..=N` are complete, and
    /// only the last one may be smaller than the session's segment size.
    fn validated_segment_count(&self) -> Result<i32> {
        // TODO: return a typed validation error instead of plain strings.
        let segments = self.completed_segments.lock().expect("never poisoned");
        let Some((&last, _)) = segments.last_key_value() else {
            return Err(Error::ser("cannot finalize a session without segments"));
        };
        for ((&number, &size), want) in segments.iter().zip(1_u32..) {
            if number != want {
                return Err(Error::ser(format!(
                    "cannot finalize a session with missing segments: expected segment {want}, found segment {number}"
                )));
            }
            let limit = self.segment_size_bytes;
            let valid = if number == last {
                size <= limit
            } else {
                size == limit
            };
            if !valid {
                return Err(Error::ser(format!(
                    "segment {number} has {size} bytes, the session segment size is {limit} bytes"
                )));
            }
        }
        i32::try_from(segments.len()).map_err(Error::ser)
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
        _segment_number: u32,
        _segment_payload: SegmentPayload,
    ) -> Result<Segment> {
        unimplemented!("writing segments is not implemented yet")
    }

    async fn finalize(&self) -> Result<Object> {
        let total_segment_count = self.validated_segment_count()?;
        let request = v2::FinalizeObjectWriteSessionRequest {
            name: self.session_name.clone(),
            common_object_request_params: self.params.clone(),
            total_segment_count,
            ..v2::FinalizeObjectWriteSessionRequest::default()
        };

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
        f.debug_struct("ParallelUploadSessionTransport")
            .field("client", &self.client)
            .field("options", &self.options)
            .field("session_name", &self.session_name)
            .field("request_params", &self.request_params)
            .field("segment_size_bytes", &self.segment_size_bytes)
            .field("params", &params)
            .field("completed_segments", &self.completed_segments)
            .finish()
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
    use super::super::mocks::{MockTestClient, SharedMockClient};
    use super::super::tests::{permanent_error, test_options};
    use super::*;
    use crate::model::CommonObjectRequestParams;
    use gaxi::grpc::tonic::Response as TonicResponse;
    use google_cloud_gax::error::binding::{PathMismatch, SubstitutionFail, SubstitutionMismatch};
    use static_assertions::assert_impl_all;
    use std::error::Error as _;
    use test_case::test_case;

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

        t.params = Some(v2::CommonObjectRequestParams {
            encryption_algorithm: "AES256".to_string(),
            encryption_key_bytes: bytes::Bytes::from_static(b"test-only-key"),
            ..v2::CommonObjectRequestParams::default()
        });
        let with_params = format!("{t:?}");
        assert!(with_params.contains("[censored]"), "{with_params}");
        assert!(!with_params.contains("test-only-key"), "{with_params}");
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
        let want = v2::CommonObjectRequestParams {
            encryption_algorithm: "AES256".to_string(),
            encryption_key_bytes: bytes::Bytes::from_static(b"test-only-key"),
            ..v2::CommonObjectRequestParams::default()
        };

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
                .set_encryption_key_bytes(bytes::Bytes::from_static(b"test-only-key")),
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
    #[test_case(&[(1, SEGMENT_SIZE - 1), (2, 5)]; "short segment before the last")]
    #[test_case(&[(1, SEGMENT_SIZE + 1), (2, 5)]; "long segment before the last")]
    #[test_case(&[(1, SEGMENT_SIZE), (2, SEGMENT_SIZE + 1)]; "long last segment")]
    #[tokio::test]
    async fn finalize_invalid_segments(segments: &[(u32, u64)]) {
        let mut mock = MockTestClient::new();
        // The problem is detected before a request is sent.
        mock.expect_finalize_object_write_session().never();

        let transport = transport(mock, SESSION, segments);
        let err = transport.finalize().await.unwrap_err();
        assert!(err.is_serialization(), "{err:?}");
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
