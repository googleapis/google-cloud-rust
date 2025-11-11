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

use super::redirect::handle_redirect;
use super::retry_redirect::RetryRedirect;
use crate::google::storage::v2::{
    BidiReadObjectRequest, BidiReadObjectResponse, BidiReadObjectSpec, ReadRange as ProtoRange,
};
use crate::read_resume_policy::{ResumeQuery, ResumeResult};
use crate::request_options::RequestOptions;
use crate::storage::bidi::resume_redirect::ResumeRedirect;
use crate::storage::info::X_GOOG_API_CLIENT_HEADER;
use crate::{Error, Result};
use gaxi::grpc::Client as GrpcClient;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub struct Connection<S = tonic::Streaming<BidiReadObjectResponse>> {
    pub tx: Sender<BidiReadObjectRequest>,
    pub rx: S,
}

impl<S> Connection<S> {
    pub fn new(tx: Sender<BidiReadObjectRequest>, rx: S) -> Self {
        Self { tx, rx }
    }
}

/// Establishes connections to gRPC for bidi streaming reads.
#[derive(Clone, Debug)]
pub struct Connector<T = GrpcClient> {
    spec: Arc<Mutex<BidiReadObjectSpec>>,
    options: RequestOptions,
    // This is used in testing, the client library always uses `GrpcClient`.
    client: T,
    reconnect_attempts: u32,
}

impl<T> Connector<T>
where
    T: Client + Clone + Send + 'static,
    <T as Client>::Stream: TonicStreaming,
{
    pub fn new(spec: BidiReadObjectSpec, options: RequestOptions, client: T) -> Self {
        Self {
            spec: Arc::new(Mutex::new(spec)),
            options,
            client,
            reconnect_attempts: 0_u32,
        }
    }

    pub async fn connect(
        &mut self,
        ranges: Vec<ProtoRange>,
    ) -> Result<(BidiReadObjectResponse, Connection<T::Stream>)> {
        let throttler = self.options.retry_throttler.clone();
        let retry = Arc::new(RetryRedirect::new(self.options.retry_policy.clone()));
        let backoff = self.options.backoff_policy.clone();
        let client = self.client.clone();
        let request = BidiReadObjectRequest {
            read_object_spec: Some((*self.spec.lock().expect("never poisoned")).clone()),
            read_ranges: ranges,
        };
        let options = self.options.clone();
        let spec = self.spec.clone();
        let inner = async move |_| {
            Self::connect_attempt(client.clone(), spec.clone(), &request, &options).await
        };
        let sleep = async |backoff| tokio::time::sleep(backoff).await;
        gax::retry_loop_internal::retry_loop(inner, sleep, true, throttler, retry, backoff).await
    }

    pub async fn reconnect(
        &mut self,
        status: tonic::Status,
        ranges: Vec<ProtoRange>,
    ) -> Result<(BidiReadObjectResponse, Connection<T::Stream>)> {
        use crate::read_resume_policy::ReadResumePolicy;

        let error = handle_redirect(self.spec.clone(), status);
        self.reconnect_attempts += 1;
        let policy = ResumeRedirect::new(self.options.read_resume_policy());
        match policy.on_error(&ResumeQuery::new(self.reconnect_attempts), error) {
            ResumeResult::Continue(_) => self.connect(ranges).await,
            ResumeResult::Exhausted(e) => Err(e),
            ResumeResult::Permanent(e) => Err(e),
        }
    }

    async fn connect_attempt(
        client: T,
        spec: Arc<Mutex<BidiReadObjectSpec>>,
        request: &BidiReadObjectRequest,
        options: &RequestOptions,
    ) -> Result<(BidiReadObjectResponse, Connection<T::Stream>)> {
        let bucket_name = request
            .read_object_spec
            .as_ref()
            .map(|s| s.bucket.as_str())
            .unwrap_or_default();
        if bucket_name
            .strip_prefix("projects/_/buckets/")
            .is_none_or(|x| x.is_empty())
        {
            use gax::error::binding::*;
            let problem = SubstitutionFail::MismatchExpecting(
                bucket_name.to_string(),
                "projects/_/buckets/*",
            );
            let mismatch = SubstitutionMismatch {
                field_name: "bucket",
                problem,
            };
            let mismatch = PathMismatch {
                subs: vec![mismatch],
            };
            let mismatch = BindingError {
                paths: vec![mismatch],
            };

            return Err(crate::Error::binding(mismatch));
        }
        let x_goog_request_params = request
            .read_object_spec
            .iter()
            .flat_map(|s| s.routing_token.iter())
            .fold(format!("bucket={bucket_name}"), |s, token| {
                s + &format!(",routing_token={token}")
            });

        let (tx, rx) = tokio::sync::mpsc::channel::<BidiReadObjectRequest>(100);
        tx.send(request.clone()).await.map_err(Error::io)?;

        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.storage.v2.Storage",
                "BidiReadObject",
            ));
            e
        };
        let path =
            http::uri::PathAndQuery::from_static("/google.storage.v2.Storage/BidiReadObject");

        let response = client
            .start(
                extensions,
                path,
                rx,
                options,
                &X_GOOG_API_CLIENT_HEADER,
                &x_goog_request_params,
            )
            .await?;
        Self::started(spec, tx, response).await
    }

    async fn started(
        spec: Arc<Mutex<BidiReadObjectSpec>>,
        tx: Sender<BidiReadObjectRequest>,
        response: tonic::Result<tonic::Response<T::Stream>>,
    ) -> Result<(BidiReadObjectResponse, Connection<T::Stream>)> {
        let response = match response {
            Ok(r) => r,
            Err(status) => return Err(handle_redirect(spec, status)),
        };
        let (_metadata, mut stream, _) = response.into_parts();
        match stream.next_message().await {
            Ok(Some(m)) => {
                let mut guard = spec.lock().expect("never poisoned");
                if let Some(generation) = m.metadata.as_ref().map(|o| o.generation) {
                    guard.generation = generation;
                }
                if m.read_handle.is_some() {
                    guard.read_handle = m.read_handle.clone();
                }
                Ok((m, Connection::new(tx, stream)))
            }
            Ok(None) => Err(Error::io("bidi_read_object stream closed before start")),
            Err(status) => Err(handle_redirect(spec, status)),
        }
    }
}

/// Dependency injection for [gaxi::grpc::Client].
pub trait Client: std::fmt::Debug + Send + 'static {
    type Stream: Sized;
    fn start(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiReadObjectRequest>,
        options: &RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> impl Future<Output = Result<tonic::Result<tonic::Response<Self::Stream>>>> + Send;
}

impl Client for GrpcClient {
    type Stream = tonic::codec::Streaming<BidiReadObjectResponse>;
    async fn start(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiReadObjectRequest>,
        options: &RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> Result<tonic::Result<tonic::Response<Self::Stream>>> {
        let request = tokio_stream::wrappers::ReceiverStream::new(rx);
        self.bidi_stream_with_status(
            extensions,
            path,
            request,
            options.gax(),
            api_client_header,
            request_params,
        )
        .await
    }
}

pub trait TonicStreaming: std::fmt::Debug + Send + 'static {
    async fn next_message(&mut self) -> tonic::Result<Option<BidiReadObjectResponse>>;
}

impl TonicStreaming for tonic::codec::Streaming<BidiReadObjectResponse> {
    async fn next_message(&mut self) -> tonic::Result<Option<BidiReadObjectResponse>> {
        self.message().await
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{permanent_error, redirect_handle, redirect_status, test_options};
    use super::*;
    use crate::google::storage::v2::{BidiReadHandle, Object, ObjectRangeData};
    use crate::read_resume_policy::{AlwaysResume, ReadResumePolicyExt};
    use anyhow::Result;
    use gax::error::binding::{BindingError, SubstitutionFail};
    use gax::retry_policy::NeverRetry;
    use static_assertions::assert_impl_all;
    use std::error::Error as _;
    use std::sync::Arc;
    use test_case::test_case;

    #[test]
    fn assertions() {
        assert_impl_all!(Connector: Clone, std::fmt::Debug, Send, Sync);
    }

    #[tokio::test]
    async fn bad_endpoint() -> Result<()> {
        fn need_send<T: Send>(_val: &T) {}

        let mut config = gaxi::options::ClientConfig::default();
        config.cred = Some(test_credentials());
        let client = GrpcClient::new(config, "http://127.0.0.1:1").await?;

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-only-bucket".into(),
            object: "test-only-object".into(),
            ..BidiReadObjectSpec::default()
        };

        let mut options = test_options();
        options.retry_policy = Arc::new(NeverRetry);
        let mut connector = Connector::new(spec, options, client);
        let start = connector.connect(Vec::new());
        need_send(&start);

        let err = start.await.unwrap_err();
        assert!(err.is_connect(), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    #[test_case("")]
    #[test_case("my-bucket")]
    async fn binding(bucket_name: &str) -> Result<()> {
        let mut mock = MockTestClient::new();
        mock.expect_start().never();
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: bucket_name.to_string(),
            object: "object".into(),
            ..BidiReadObjectSpec::default()
        };

        let mut connector = Connector::new(spec, test_options(), client);
        let err = connector.connect(Vec::new()).await.unwrap_err();
        assert!(err.is_binding(), "{err:?}");
        let source = err.source().and_then(|e| e.downcast_ref::<BindingError>());
        assert!(matches!(source, Some(BindingError { .. })), "{err:?}");
        // Extract all the field names that did not match, and expect a single name:
        let mismatch = source
            .iter()
            .flat_map(|f| f.paths.iter())
            .flat_map(|f| f.subs.iter())
            .map(|f| f.field_name)
            .collect::<Vec<_>>();
        assert_eq!(mismatch, vec!["bucket"], "{err:?}");

        // Extract all the problems:
        let mismatch = source
            .iter()
            .flat_map(|f| f.paths.iter())
            .flat_map(|f| f.subs.iter())
            .map(|f| &f.problem)
            .collect::<Vec<_>>();
        assert!(
            matches!(
                mismatch.first(),
                Some(SubstitutionFail::MismatchExpecting(n, p)) if n == bucket_name && *p == "projects/_/buckets/*"
            ),
            "{err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn start_error() -> Result<()> {
        let ranges = vec![
            ProtoRange {
                read_id: 123,
                read_offset: 100,
                read_length: 200,
            },
            ProtoRange {
                read_id: 234,
                read_offset: 500,
                read_length: 100,
            },
        ];

        let receivers = Arc::new(Mutex::new(Vec::new()));
        let save = receivers.clone();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |extensions, path, rx, _options, header, params| {
                // Verify all the parameters. We should have a couple of tests
                // that do this, but should avoid doing so in every test.
                assert!(
                    matches!(extensions.get::<tonic::GrpcMethod>(), Some(m) if m.service() == "google.storage.v2.Storage" && m.method() == "BidiReadObject")
                );
                assert_eq!(path.path(), "/google.storage.v2.Storage/BidiReadObject");
                assert_eq!(header, *X_GOOG_API_CLIENT_HEADER);
                assert_eq!(params, "bucket=projects/_/buckets/test-bucket");
                save.lock().expect("never poisoned").push(rx);
                Err(permanent_error())
            });
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            ..BidiReadObjectSpec::default()
        };

        let mut connector = Connector::new(spec, test_options(), client);
        let err = connector.connect(ranges.clone()).await.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");

        let mut rx = {
            let mut guard = receivers.lock().expect("never poisoned");
            let rx = guard.pop().expect("at least one receiver");
            assert!(guard.is_empty(), "{receivers:?}");
            rx
        };

        let first = rx.recv().await.expect("non-empty request");
        assert_eq!(
            first.read_object_spec.as_ref().map(|s| s.bucket.as_str()),
            Some("projects/_/buckets/test-bucket")
        );
        assert_eq!(
            first.read_object_spec.as_ref().map(|s| s.object.as_str()),
            Some("test-object")
        );
        assert_eq!(first.read_ranges, ranges);

        Ok(())
    }

    #[tokio::test]
    async fn start_error_with_routing() -> Result<()> {
        let ranges = vec![
            ProtoRange {
                read_id: 123,
                read_offset: 100,
                read_length: 200,
            },
            ProtoRange {
                read_id: 234,
                read_offset: 500,
                read_length: 100,
            },
        ];

        let receivers = Arc::new(Mutex::new(Vec::new()));
        let save = receivers.clone();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |extensions, path, rx, _options, header, params| {
                // Verify all the parameters. We should have a couple of tests
                // that do this, but should avoid doing so in every test.
                assert!(
                    matches!(
                        extensions.get::<tonic::GrpcMethod>(),
                        Some(m) if m.service() == "google.storage.v2.Storage" && m.method() == "BidiReadObject"
                    )
                );
                assert_eq!(path.path(), "/google.storage.v2.Storage/BidiReadObject");
                assert_eq!(header, *X_GOOG_API_CLIENT_HEADER);
                let mut split = params.split(',').collect::<Vec<_>>();
                split.sort();
                assert_eq!(split, vec!["bucket=projects/_/buckets/test-bucket", "routing_token=test-routing-token"]);
                save.lock().expect("never poisoned").push(rx);

                Err(permanent_error())
            });
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 345678,
            read_handle: Some(BidiReadHandle {
                handle: bytes::Bytes::from_static(b"test-handle"),
            }),
            routing_token: Some("test-routing-token".to_string()),
            ..BidiReadObjectSpec::default()
        };

        let mut connector = Connector::new(spec, test_options(), client);
        let err = connector.connect(ranges.clone()).await.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");

        let mut rx = {
            let mut guard = receivers.lock().expect("never poisoned");
            let rx = guard.pop().expect("at least one receiver");
            assert!(guard.is_empty(), "{guard:?}");
            rx
        };
        let first = rx.recv().await.expect("non-empty request");
        let spec = first.read_object_spec.as_ref();
        assert_eq!(
            spec.map(|s| s.bucket.as_str()),
            Some("projects/_/buckets/test-bucket")
        );
        assert_eq!(spec.map(|s| s.object.as_str()), Some("test-object"));
        assert_eq!(spec.map(|s| s.generation), Some(345678));
        assert_eq!(
            spec.and_then(|s| s.read_handle.as_ref())
                .map(|h| h.handle.clone()),
            Some(bytes::Bytes::from_static(b"test-handle")),
        );
        assert_eq!(first.read_ranges, ranges);
        Ok(())
    }

    #[tokio::test]
    async fn start_redirect_then_error() -> Result<()> {
        let mut seq = mockall::Sequence::new();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _, _, _, _, _| Ok(Err(redirect_status("r1"))));
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _, _, _, _, _| Err(permanent_error()));
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            ..BidiReadObjectSpec::default()
        };

        let mut connector = Connector::new(spec, test_options(), client);
        let err = connector.connect(Vec::new()).await.unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
        let guard = connector.spec.lock().expect("never poisoned");
        assert_eq!(guard.routing_token.as_deref(), Some("r1"));
        assert_eq!(guard.read_handle, Some(redirect_handle()));

        Ok(())
    }

    #[tokio::test]
    async fn start_redirect_open_then_redirect() -> Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(5);
        let stream = tonic::Response::from(rx);

        let mut seq = mockall::Sequence::new();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _, _, _, _, _| Ok(Err(redirect_status("r1"))));
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream)));
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_, _, _, _, _, _| Err(permanent_error()));
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            ..BidiReadObjectSpec::default()
        };
        // Initial response is a redirect error
        tx.send(Err(redirect_status("r2"))).await?;
        drop(tx);

        let mut connector = Connector::new(spec, test_options(), client);
        let err = connector.connect(Vec::new()).await.unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
        let guard = connector.spec.lock().expect("never poisoned");
        assert_eq!(guard.routing_token.as_deref(), Some("r2"));
        assert_eq!(guard.read_handle, Some(redirect_handle()));

        Ok(())
    }

    #[tokio::test]
    async fn start_immediately_closed() -> Result<()> {
        let (tx1, rx1) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(5);
        let stream1 = tonic::Response::from(rx1);
        drop(tx1);
        let (tx2, rx2) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(5);
        let stream2 = tonic::Response::from(rx2);

        let mut seq = mockall::Sequence::new();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream1)));
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream2)));
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            ..BidiReadObjectSpec::default()
        };
        let initial = BidiReadObjectResponse {
            metadata: Some(Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                generation: 123456,
                ..Object::default()
            }),
            read_handle: Some(BidiReadHandle {
                handle: bytes::Bytes::from_static(b"test-handle-open"),
            }),
            ..BidiReadObjectResponse::default()
        };
        tx2.send(Ok(initial.clone())).await?;

        let mut connector = Connector::new(spec, test_options(), client);
        let (response, _connection) = connector.connect(Vec::new()).await?;
        assert_eq!(response, initial);

        let guard = connector.spec.lock().expect("never poisoned");
        assert!(guard.routing_token.is_none(), "{guard:?}");
        assert_eq!(guard.generation, 123456, "{guard:?}");
        assert_eq!(
            guard.read_handle.as_ref().map(|h| h.handle.clone()),
            Some(bytes::Bytes::from_static(b"test-handle-open"))
        );
        drop(tx2);

        Ok(())
    }

    #[tokio::test]
    async fn start_success() -> Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(5);
        let stream = tonic::Response::from(rx);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream)));
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            ..BidiReadObjectSpec::default()
        };
        let initial = BidiReadObjectResponse {
            metadata: Some(Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                generation: 123456,
                ..Object::default()
            }),
            read_handle: Some(BidiReadHandle {
                handle: bytes::Bytes::from_static(b"test-handle-open"),
            }),
            ..BidiReadObjectResponse::default()
        };
        tx.send(Ok(initial.clone())).await?;

        let mut connector = Connector::new(spec, test_options(), client);
        let (response, _connection) = connector.connect(Vec::new()).await?;
        assert_eq!(response, initial);

        let guard = connector.spec.lock().expect("never poisoned");
        assert!(guard.routing_token.is_none(), "{guard:?}");
        assert_eq!(guard.generation, 123456, "{guard:?}");
        assert_eq!(
            guard.read_handle.as_ref().map(|h| h.handle.clone()),
            Some(bytes::Bytes::from_static(b"test-handle-open"))
        );
        drop(tx);

        Ok(())
    }

    #[tokio::test]
    async fn start_success_then_reconnect() -> Result<()> {
        let (tx1, rx1) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(5);
        let stream1 = tonic::Response::from(rx1);
        let (tx2, rx2) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(5);
        let stream2 = tonic::Response::from(rx2);

        let mut seq = mockall::Sequence::new();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream1)));
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream2)));
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            ..BidiReadObjectSpec::default()
        };
        let i1 = BidiReadObjectResponse {
            metadata: Some(Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                generation: 123456,
                ..Object::default()
            }),
            read_handle: Some(BidiReadHandle {
                handle: bytes::Bytes::from_static(b"test-handle-open"),
            }),
            ..BidiReadObjectResponse::default()
        };
        tx1.send(Ok(i1.clone())).await?;

        let mut connector = Connector::new(spec, test_options(), client);
        let (response, _connection) = connector.connect(Vec::new()).await?;
        assert_eq!(response, i1);

        let got = connector.spec.lock().expect("never poisoned").clone();
        assert!(got.routing_token.is_none(), "{got:?}");
        assert_eq!(got.generation, 123456, "{got:?}");
        assert_eq!(
            got.read_handle.map(|h| h.handle.clone()),
            Some(bytes::Bytes::from_static(b"test-handle-open"))
        );
        drop(tx1);

        let ranges = vec![
            ProtoRange {
                read_id: 1,
                ..ProtoRange::default()
            },
            ProtoRange {
                read_id: 2,
                ..ProtoRange::default()
            },
        ];
        let i2 = BidiReadObjectResponse {
            metadata: Some(Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                generation: 123456,
                ..Object::default()
            }),
            object_data_ranges: ranges
                .iter()
                .map(|range| ObjectRangeData {
                    read_range: Some(*range),
                    ..ObjectRangeData::default()
                })
                .collect(),
            ..BidiReadObjectResponse::default()
        };
        tx2.send(Ok(i2.clone())).await?;
        let (response, _connection) = connector
            .reconnect(redirect_status("r2"), ranges.clone())
            .await?;
        assert_eq!(response, i2);

        let got = connector.spec.lock().expect("never poisoned").clone();
        assert_eq!(got.routing_token.as_deref(), Some("r2"), "{got:?}");
        assert_eq!(got.generation, 123456, "{got:?}");
        assert_eq!(got.read_handle, Some(redirect_handle()), "{got:?}");
        drop(tx2);

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_permanent() -> Result<()> {
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(|_, _, _, _, _, _| Err(permanent_error()));
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            ..BidiReadObjectSpec::default()
        };

        let mut connector = Connector::new(spec, test_options(), client);
        let status = tonic::Status::permission_denied("uh-oh");
        let err = connector.reconnect(status, Vec::new()).await.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_exhausted() -> Result<()> {
        let mut mock = MockTestClient::new();
        // The policy is exhausted, this is never called.
        mock.expect_start().never();
        let client = SharedMockClient::new(mock);

        let spec = BidiReadObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            ..BidiReadObjectSpec::default()
        };

        let mut options = test_options();
        options.set_read_resume_policy(Arc::new(AlwaysResume.with_attempt_limit(1)));
        let mut connector = Connector::new(spec, options, client);
        let status = tonic::Status::unavailable("try-again");
        let err = connector.reconnect(status, Vec::new()).await.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");

        Ok(())
    }

    fn test_credentials() -> auth::credentials::Credentials {
        auth::credentials::anonymous::Builder::new().build()
    }

    // mockall mocks are not `Clone` and we need a thing that can be cloned.
    // The solution is to wrap the mock in a think that implements the right
    // trait.
    #[derive(Clone, Debug)]
    struct SharedMockClient(Arc<MockTestClient>);

    impl SharedMockClient {
        fn new(mock: MockTestClient) -> Self {
            Self(Arc::new(mock))
        }
    }

    impl super::Client for SharedMockClient {
        type Stream = MockStream;

        async fn start(
            &self,
            extensions: tonic::Extensions,
            path: http::uri::PathAndQuery,
            rx: Receiver<BidiReadObjectRequest>,
            options: &RequestOptions,
            api_client_header: &'static str,
            request_params: &str,
        ) -> crate::Result<tonic::Result<tonic::Response<Self::Stream>>> {
            self.0.start(
                extensions,
                path,
                rx,
                options,
                api_client_header,
                request_params,
            )
        }
    }

    impl super::TonicStreaming for Receiver<tonic::Result<BidiReadObjectResponse>> {
        async fn next_message(&mut self) -> tonic::Result<Option<BidiReadObjectResponse>> {
            self.recv().await.transpose()
        }
    }

    #[mockall::automock]
    trait TestClient: std::fmt::Debug {
        fn start(
            &self,
            extensions: tonic::Extensions,
            path: http::uri::PathAndQuery,
            rx: Receiver<BidiReadObjectRequest>,
            options: &RequestOptions,
            api_client_header: &'static str,
            request_params: &str,
        ) -> crate::Result<tonic::Result<tonic::Response<MockStream>>>;
    }

    type MockStream = Receiver<tonic::Result<BidiReadObjectResponse>>;
}
