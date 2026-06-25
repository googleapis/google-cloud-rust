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

use super::redirect::handle_redirect;
use super::retry_redirect::RetryRedirect;
use super::{Client, TonicStreaming};
use crate::google::storage::v2::{
    AppendObjectSpec, BidiWriteObjectRequest, BidiWriteObjectResponse, WriteObjectSpec,
    bidi_write_object_request::FirstMessage, bidi_write_object_response::WriteStatus,
};
use crate::request_options::RequestOptions;
use crate::storage::info::X_GOOG_API_CLIENT_HEADER;
use crate::{Error, Result};
use gaxi::grpc::Client as GrpcClient;
use gaxi::grpc::tonic::{Extensions, GrpcMethod, Streaming};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
/// Represents a bidirectional streaming connection.
/// Contains the transmission channel for requests and the receiving stream for responses.
#[derive(Debug)]
pub struct Connection<S = Streaming<BidiWriteObjectResponse>> {
    pub tx: Sender<BidiWriteObjectRequest>,
    pub rx: S,
}

impl<S> Connection<S> {
    pub fn new(tx: Sender<BidiWriteObjectRequest>, rx: S) -> Self {
        Self { tx, rx }
    }
}

/// Represents the state of the initial request in the stream.
#[derive(Clone, Debug)]
pub enum AppendObjectSpecState {
    Write(Box<WriteObjectSpec>),
    Append(AppendObjectSpec),
}

/// Establishes and handles the initial handshake for bidi streaming writes.
///
/// Connecting and reconnecting bidirectional streaming writes requires:
/// - Constructing the initial `WriteObjectSpec` or `AppendObjectSpec`.
/// - Following routing token redirects correctly.
/// - Passing back the established `Connection` to the async worker.
///
/// # Parameters
/// - `T`: a type implementing the [Client] trait, this is used in tests.
#[derive(Clone, Debug)]
pub struct Connector<T = GrpcClient> {
    pub(crate) spec: Arc<Mutex<AppendObjectSpecState>>,
    options: RequestOptions,
    client: T,
}

impl<T> Connector<T>
where
    T: Client + Clone + Send + 'static,
    <T as Client>::Stream: TonicStreaming,
{
    pub fn new(options: RequestOptions, client: T) -> Self {
        Self {
            spec: Arc::new(Mutex::new(AppendObjectSpecState::Write(Box::default()))),
            options,
            client,
        }
    }

    pub async fn connect_open(
        &mut self,
        req: crate::model_ext::OpenAppendableObjectRequest,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        let resource = match req.spec.resource {
            Some(r) => {
                let object: crate::google::storage::v2::Object = gaxi::prost::ToProto::to_proto(r)
                    .map_err(|e: gaxi::prost::ConvertError| Error::io(e.to_string()))?;
                Some(object)
            }
            None => None,
        };
        let spec = crate::google::storage::v2::WriteObjectSpec {
            resource,
            predefined_acl: req.spec.predefined_acl,
            if_generation_match: req.spec.if_generation_match,
            if_generation_not_match: req.spec.if_generation_not_match,
            if_metageneration_match: req.spec.if_metageneration_match,
            if_metageneration_not_match: req.spec.if_metageneration_not_match,
            object_size: req.spec.object_size,
            appendable: req.spec.appendable,
        };
        *self.spec.lock().expect("never poisoned") = AppendObjectSpecState::Write(Box::new(spec));
        self.connect_attempt_loop().await
    }

    pub async fn connect_reopen(
        &mut self,
        req: crate::model_ext::ReopenAppendableObjectRequest,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        let spec = AppendObjectSpec {
            bucket: req.bucket,
            object: req.object,
            generation: req.generation,
            routing_token: req.routing_token,
            if_metageneration_match: req.if_metageneration_match,
            if_metageneration_not_match: req.if_metageneration_not_match,
            ..Default::default()
        };
        *self.spec.lock().expect("never poisoned") = AppendObjectSpecState::Append(spec);
        self.connect_attempt_loop().await
    }

    async fn connect_attempt_loop(
        &mut self,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        let throttler = self.options.retry_throttler.clone();
        let retry = Arc::new(RetryRedirect::new(self.options.retry_policy.clone()));
        let backoff = self.options.backoff_policy.clone();
        let client = self.client.clone();
        let options = self.options.clone();
        let spec = self.spec.clone();
        let sleep = async |backoff| tokio::time::sleep(backoff).await;
        let default_timeout = self.options.bidi_attempt_timeout;

        let inner = async move |d: Option<Duration>| {
            let attempt_timeout = std::cmp::min(default_timeout, d.unwrap_or(default_timeout));
            let attempt = Self::connect_attempt(client.clone(), spec.clone(), &options);
            match tokio::time::timeout(attempt_timeout, attempt).await {
                Ok(r) => r,
                Err(e) => Err(Error::timeout(e)),
            }
        };
        google_cloud_gax::retry_loop_internal::retry_loop(
            inner, sleep, true, throttler, retry, backoff,
        )
        .await
    }

    async fn connect_attempt(
        client: T,
        spec: Arc<Mutex<AppendObjectSpecState>>,
        options: &RequestOptions,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        let first_message = match &*spec.lock().expect("never poisoned") {
            AppendObjectSpecState::Write(spec) => FirstMessage::WriteObjectSpec(*spec.clone()),
            AppendObjectSpecState::Append(spec) => FirstMessage::AppendObjectSpec(spec.clone()),
        };

        let request = BidiWriteObjectRequest {
            first_message: Some(first_message),
            ..BidiWriteObjectRequest::default()
        };

        let bucket_name = request
            .first_message
            .as_ref()
            .and_then(|m| match m {
                FirstMessage::WriteObjectSpec(s) => s.resource.as_ref().map(|r| r.bucket.as_str()),
                FirstMessage::AppendObjectSpec(s) => Some(s.bucket.as_str()),
                _ => None,
            })
            .unwrap_or_default();

        if bucket_name
            .strip_prefix("projects/_/buckets/")
            .is_none_or(|x| x.is_empty())
        {
            use google_cloud_gax::error::binding::*;
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
            .first_message
            .as_ref()
            .and_then(|m| match m {
                FirstMessage::WriteObjectSpec(_) => None, // WriteObjectSpec doesn't have routing_token
                FirstMessage::AppendObjectSpec(s) => s.routing_token.as_deref(),
                _ => None,
            })
            .into_iter()
            .fold(format!("bucket={bucket_name}"), |s, token| {
                s + &format!("&routing_token={token}")
            });

        let (tx, rx) = tokio::sync::mpsc::channel::<BidiWriteObjectRequest>(100);
        tx.send(request.clone()).await.map_err(Error::io)?;

        let extensions = {
            let mut e = Extensions::new();
            e.insert(GrpcMethod::new(
                "google.storage.v2.Storage",
                "BidiWriteObject",
            ));
            e
        };
        let path =
            http::uri::PathAndQuery::from_static("/google.storage.v2.Storage/BidiWriteObject");

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

        let response = match response {
            Ok(r) => r,
            Err(status) => return Err(handle_redirect(spec, status)),
        };

        let (_metadata, mut stream, _) = response.into_parts();
        match stream.next_message().await {
            Ok(Some(m)) => {
                let mut guard = spec.lock().expect("never poisoned");
                let mut new_generation = 0;
                if let Some(WriteStatus::Resource(resource)) = &m.write_status {
                    new_generation = resource.generation;
                }

                let new_state = match &*guard {
                    AppendObjectSpecState::Write(s) => {
                        AppendObjectSpecState::Append(AppendObjectSpec {
                            bucket: s
                                .resource
                                .as_ref()
                                .map(|r| r.bucket.clone())
                                .unwrap_or_default(),
                            object: s
                                .resource
                                .as_ref()
                                .map(|r| r.name.clone())
                                .unwrap_or_default(),
                            generation: new_generation,
                            write_handle: m.write_handle.clone(),
                            ..Default::default()
                        })
                    }
                    AppendObjectSpecState::Append(s) => {
                        AppendObjectSpecState::Append(AppendObjectSpec {
                            generation: new_generation,
                            write_handle: m.write_handle.clone().or_else(|| s.write_handle.clone()),
                            ..s.clone()
                        })
                    }
                };
                *guard = new_state;

                Ok((m, Connection::new(tx, stream)))
            }
            Ok(None) => Err(Error::io("bidi_write_object stream closed before start")),
            Err(status) => Err(handle_redirect(spec, status)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::model_ext::OpenAppendableObjectRequest;
    use crate::storage::request_options::RequestOptions;
    use anyhow::Result;
    use gaxi::grpc::Client as GrpcClient;
    use google_cloud_auth::credentials::{Credentials, anonymous::Builder as Anonymous};
    use google_cloud_gax::retry_policy::NeverRetry;
    use static_assertions::assert_impl_all;
    use std::error::Error as _;
    use std::sync::Arc;

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    fn test_options() -> RequestOptions {
        RequestOptions::new()
    }

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

        let mut options = test_options();
        options.retry_policy = Arc::new(NeverRetry);
        let mut connector = Connector::new(options, client);

        let mut req = OpenAppendableObjectRequest {
            spec: crate::model::WriteObjectSpec::default(),
            params: None,
        };
        req.spec = crate::model::WriteObjectSpec {
            resource: Some(crate::model::Object {
                bucket: "projects/_/buckets/test-only-bucket".into(),
                name: "test-only-object".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let start = connector.connect_open(req);
        need_send(&start);

        let err = start.await.unwrap_err();
        assert!(err.is_connect(), "{err:?}");
        let source = err.source().unwrap().to_string();
        assert!(source.contains("127.0.0.1:1"), "{source}");

        Ok(())
    }

    #[tokio::test]
    #[test_case::test_case("")]
    #[test_case::test_case("my-bucket")]
    async fn binding_error(bucket_name: &str) -> Result<()> {
        use super::super::mocks::{MockTestClient, SharedMockClient};
        let mut mock = MockTestClient::new();
        // Binding errors are detected before a request is sent.
        mock.expect_start().never();
        let client = SharedMockClient::new(mock);

        let mut connector = Connector::new(test_options(), client);

        let mut req = OpenAppendableObjectRequest {
            spec: crate::model::WriteObjectSpec::default(),
            params: None,
        };
        req.spec = crate::model::WriteObjectSpec {
            resource: Some(crate::model::Object {
                bucket: bucket_name.into(),
                name: "object".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let err = connector.connect_open(req).await.unwrap_err();
        assert!(err.is_binding(), "{err:?}");
        use google_cloud_gax::error::binding::{BindingError, SubstitutionFail};
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
        use super::super::mocks::{MockTestClient, SharedMockClient};
        use super::super::tests::permanent_error;
        use gaxi::grpc::tonic::GrpcMethod;
        use std::sync::Mutex;

        let receivers = Arc::new(Mutex::new(Vec::new()));
        let save = receivers.clone();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |extensions, path, rx, _options, header, params| {
                // Verify all the parameters. We should have a couple of tests
                // that do this, but should avoid doing so in every test.
                assert!(
                    matches!(extensions.get::<GrpcMethod>(), Some(m) if m.service() == "google.storage.v2.Storage" && m.method() == "BidiWriteObject")
                );
                assert_eq!(path.path(), "/google.storage.v2.Storage/BidiWriteObject");
                assert_eq!(header, *crate::storage::info::X_GOOG_API_CLIENT_HEADER);
                assert_eq!(params, "bucket=projects/_/buckets/test-bucket");
                save.lock().expect("never poisoned").push(rx);
                Err(permanent_error())
            });
        let client = SharedMockClient::new(mock);

        let mut connector = Connector::new(test_options(), client);

        let mut req = OpenAppendableObjectRequest {
            spec: crate::model::WriteObjectSpec::default(),
            params: None,
        };
        req.spec = crate::model::WriteObjectSpec {
            resource: Some(crate::model::Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let err = connector.connect_open(req).await.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");

        let mut rx = {
            let mut guard = receivers.lock().expect("never poisoned");
            let rx = guard.pop().expect("at least one receiver");
            assert!(guard.is_empty(), "{receivers:?}");
            rx
        };

        let first = rx.recv().await.expect("non-empty request");
        let spec = match first.first_message.as_ref().unwrap() {
            crate::google::storage::v2::bidi_write_object_request::FirstMessage::WriteObjectSpec(s) => s,
            _ => panic!("Expected WriteObjectSpec"),
        };
        assert_eq!(
            spec.resource.as_ref().map(|s| s.bucket.as_str()),
            Some("projects/_/buckets/test-bucket")
        );
        assert_eq!(
            spec.resource.as_ref().map(|s| s.name.as_str()),
            Some("test-object")
        );

        Ok(())
    }

    #[tokio::test]
    async fn start_error_with_routing() -> Result<()> {
        use super::super::mocks::{MockTestClient, SharedMockClient};
        use super::super::tests::permanent_error;
        use gaxi::grpc::tonic::GrpcMethod;
        use std::sync::Mutex;

        let receivers = Arc::new(Mutex::new(Vec::new()));
        let save = receivers.clone();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |extensions, path, rx, _options, header, params| {
                // Verify all the parameters. We should have a couple of tests
                // that do this, but should avoid doing so in every test.
                assert!(
                    matches!(
                        extensions.get::<GrpcMethod>(),
                        Some(m) if m.service() == "google.storage.v2.Storage" && m.method() == "BidiWriteObject"
                    )
                );
                assert_eq!(path.path(), "/google.storage.v2.Storage/BidiWriteObject");
                assert_eq!(header, *crate::storage::info::X_GOOG_API_CLIENT_HEADER);
                let mut split = params.split('&').collect::<Vec<_>>();
                split.sort();
                assert_eq!(split, vec!["bucket=projects/_/buckets/test-bucket", "routing_token=test-routing-token"]);
                save.lock().expect("never poisoned").push(rx);

                Err(permanent_error())
            });
        let client = SharedMockClient::new(mock);

        let req = crate::model_ext::ReopenAppendableObjectRequest {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 345678,
            routing_token: Some("test-routing-token".to_string()),
            if_metageneration_match: None,
            if_metageneration_not_match: None,
            params: None,
            write_handle: None,
        };

        let mut connector = Connector::new(test_options(), client);
        let err = connector.connect_reopen(req).await.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");

        let mut rx = {
            let mut guard = receivers.lock().expect("never poisoned");
            let rx = guard.pop().expect("at least one receiver");
            assert!(guard.is_empty(), "{guard:?}");
            rx
        };
        let first = rx.recv().await.expect("non-empty request");
        let spec = match first.first_message.as_ref().unwrap() {
            crate::google::storage::v2::bidi_write_object_request::FirstMessage::AppendObjectSpec(s) => s,
            _ => panic!("Expected AppendObjectSpec"),
        };
        assert_eq!(spec.bucket.as_str(), "projects/_/buckets/test-bucket");
        assert_eq!(spec.object.as_str(), "test-object");
        assert_eq!(spec.generation, 345678);
        Ok(())
    }

    #[tokio::test]
    async fn start_redirect_then_error() -> Result<()> {
        use super::super::mocks::{MockTestClient, SharedMockClient};
        use super::super::tests::{permanent_error, redirect_status};
        use std::sync::Mutex;

        let mut seq = mockall::Sequence::new();
        let mut mock = MockTestClient::new();
        let receivers = Arc::new(Mutex::new(Vec::new()));
        let save = receivers.clone();
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_, _, rx, _, _, _| {
                save.lock().expect("never poisoned").push(rx);
                Ok(Err(redirect_status("r1")))
            });
        let save = receivers.clone();
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_, _, rx, _, _, _| {
                save.lock().expect("never poisoned").push(rx);
                Err(permanent_error())
            });
        let client = SharedMockClient::new(mock);

        let mut connector = Connector::new(test_options(), client);

        let mut req = OpenAppendableObjectRequest {
            spec: crate::model::WriteObjectSpec::default(),
            params: None,
        };
        req.spec = crate::model::WriteObjectSpec {
            resource: Some(crate::model::Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let err = connector.connect_open(req).await.unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");

        let got = connector.spec.lock().expect("never poisoned").clone();
        match got {
            AppendObjectSpecState::Write(_) => panic!("Should be Append"),
            AppendObjectSpecState::Append(got) => {
                assert_eq!(got.routing_token.as_deref(), Some("r1"));
            }
        }

        let mut rx = receivers
            .lock()
            .expect("never poisoned")
            .pop()
            .expect("at least two receiver");
        // We pop the receivers, so this is the second receiver. This receiver should include an spec with the redirect options.
        let got = rx.recv().await.expect("at least one request sent");
        let want = crate::google::storage::v2::AppendObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            routing_token: Some("r1".to_string()),
            ..crate::google::storage::v2::AppendObjectSpec::default()
        };
        let spec = match got.first_message.unwrap() {
            crate::google::storage::v2::bidi_write_object_request::FirstMessage::AppendObjectSpec(s) => s,
            _ => panic!("Expected AppendObjectSpec"),
        };
        assert_eq!(spec.bucket, want.bucket);
        assert_eq!(spec.object, want.object);
        assert_eq!(spec.routing_token, want.routing_token);

        let mut rx = receivers
            .lock()
            .expect("never poisoned")
            .pop()
            .expect("at least two receiver");
        // We pop the receivers, so this is the second receiver. This receiver should include an spec with the redirect options.
        let got = rx.recv().await.expect("at least one request sent");
        let want = crate::google::storage::v2::WriteObjectSpec {
            resource: Some(crate::google::storage::v2::Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                ..Default::default()
            }),
            ..crate::google::storage::v2::WriteObjectSpec::default()
        };
        let spec = match got.first_message.unwrap() {
            crate::google::storage::v2::bidi_write_object_request::FirstMessage::WriteObjectSpec(s) => s,
            _ => panic!("Expected WriteObjectSpec"),
        };
        assert_eq!(
            spec.resource.as_ref().unwrap().bucket,
            want.resource.as_ref().unwrap().bucket
        );
        assert_eq!(
            spec.resource.as_ref().unwrap().name,
            want.resource.as_ref().unwrap().name
        );

        Ok(())
    }

    #[tokio::test]
    async fn start_immediately_closed() -> Result<()> {
        use super::super::mocks::{MockTestClient, SharedMockClient};
        use gaxi::grpc::tonic::Response as TonicResponse;
        use gaxi::grpc::tonic::Result as TonicResult;

        let (tx1, rx1) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream1 = TonicResponse::from(rx1);
        drop(tx1);
        let (tx2, rx2) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream2 = TonicResponse::from(rx2);

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

        let mut connector = Connector::new(test_options(), client);

        let initial = BidiWriteObjectResponse {
            write_status: Some(
                crate::google::storage::v2::bidi_write_object_response::WriteStatus::Resource(
                    crate::google::storage::v2::Object {
                        bucket: "projects/_/buckets/test-bucket".into(),
                        name: "test-object".into(),
                        generation: 123456,
                        ..crate::google::storage::v2::Object::default()
                    },
                ),
            ),
            write_handle: Some(crate::google::storage::v2::BidiWriteHandle {
                handle: bytes::Bytes::from_static(b"test-handle-open"),
            }),
            ..BidiWriteObjectResponse::default()
        };
        tx2.send(Ok(initial.clone())).await?;

        let mut req = OpenAppendableObjectRequest {
            spec: crate::model::WriteObjectSpec::default(),
            params: None,
        };
        req.spec = crate::model::WriteObjectSpec {
            resource: Some(crate::model::Object {
                bucket: "projects/_/buckets/test-bucket".into(),
                name: "test-object".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let (response, _connection) = connector.connect_open(req).await?;
        assert_eq!(response, initial);

        let guard = connector.spec.lock().expect("never poisoned");
        if let AppendObjectSpecState::Append(s) = &*guard {
            assert!(s.routing_token.is_none(), "{s:?}");
            assert_eq!(s.generation, 123456, "{s:?}");
            assert_eq!(
                s.write_handle.as_ref().map(|h| h.handle.clone()),
                Some(bytes::Bytes::from_static(b"test-handle-open"))
            );
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
        drop(tx2);

        Ok(())
    }
}
