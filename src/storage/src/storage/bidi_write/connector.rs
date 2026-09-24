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

use super::retry_redirect::RetryRedirect;
use super::state::AppendObjectSpecState;
use super::{Client, TonicStreaming, persisted_size};
use crate::google::storage::v2::{
    AppendObjectSpec, BidiWriteObjectRequest, BidiWriteObjectResponse, CommonObjectRequestParams,
    Object, WriteObjectSpec, bidi_write_object_request::FirstMessage,
};
use crate::request_options::RequestOptions;
use crate::storage::info::X_GOOG_API_CLIENT_HEADER;
use crate::{Error, Result};
use gaxi::grpc::Client as GrpcClient;
use gaxi::grpc::tonic::{Extensions, GrpcMethod, Streaming};
use gaxi::prost::ToProto;
use google_cloud_gax::error::binding::{
    BindingError, PathMismatch, SubstitutionFail, SubstitutionMismatch,
};
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;

/// The number of queued messages allowed in the request channel.
const MAX_QUEUED_REQUESTS: usize = 100;

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

/// Establishes and reconnects `BidiWriteObject` streams.
///
/// `Connector` manages:
/// - Building the opening `WriteObjectSpec` or `AppendObjectSpec` handshake message.
/// - Updating the stored `routing_token` and `write_handle` on server redirects.
/// - Tracking byte progress (`last_persisted_size`) so the application's [`RetryPolicy`] bounds
///   consecutive unproductive reconnects (`retry_state`).
///
/// # Parameters
/// - `T`: a type implementing the [`Client`] trait (mocked in unit tests).
#[derive(Clone, Debug)]
pub struct Connector<T = GrpcClient> {
    spec: Arc<Mutex<AppendObjectSpecState>>,
    options: RequestOptions,
    client: T,
    params: Option<CommonObjectRequestParams>,
    /// Retry state shared across consecutive reconnects that make no byte progress.
    retry_state: Option<RetryState>,
    /// Highest `persisted_size` acknowledged by the server; used to detect byte progress.
    last_persisted_size: i64,
}

impl<T> Connector<T>
where
    T: Client + Clone + Send + 'static,
    <T as Client>::Stream: TonicStreaming,
{
    pub fn new(options: RequestOptions, client: T) -> Self {
        Self {
            spec: Arc::new(Mutex::new(AppendObjectSpecState::Write {
                spec: Box::default(),
                routing_token: None,
                initial_chunk: None,
            })),
            options,
            client,
            params: None,
            retry_state: None,
            last_persisted_size: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_spec_state(&mut self, state: AppendObjectSpecState) {
        *self.spec.lock().expect("never poisoned") = state;
    }

    pub async fn connect_open(
        &mut self,
        req: crate::model_ext::OpenAppendableObjectRequest,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        self.connect_open_and_append(req, None).await
    }

    /// Connects to the service to open an appendable object, optionally including
    /// an initial data chunk in the opening request.
    pub async fn connect_open_and_append(
        &mut self,
        req: crate::model_ext::OpenAppendableObjectRequest,
        initial_chunk: Option<bytes::Bytes>,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        let resource = match req.spec.resource {
            Some(r) => {
                let object: Object = r.to_proto().map_err(Error::deser)?;
                Some(object)
            }
            None => None,
        };
        let spec = WriteObjectSpec {
            resource,
            predefined_acl: req.spec.predefined_acl,
            if_generation_match: req.spec.if_generation_match,
            if_generation_not_match: req.spec.if_generation_not_match,
            if_metageneration_match: req.spec.if_metageneration_match,
            if_metageneration_not_match: req.spec.if_metageneration_not_match,
            object_size: req.spec.object_size,
            appendable: req.spec.appendable,
        };
        self.params = req
            .params
            .map(|p| p.to_proto().map_err(Error::deser))
            .transpose()?;
        *self.spec.lock().expect("never poisoned") = AppendObjectSpecState::Write {
            spec: Box::new(spec),
            routing_token: None,
            initial_chunk,
        };
        let (initial, connection) = self.connect_attempt_loop().await?;
        self.last_persisted_size = persisted_size(&initial).unwrap_or(0);
        self.retry_state = None;
        Ok((initial, connection))
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
            write_handle: req
                .write_handle
                .map(|handle| crate::google::storage::v2::BidiWriteHandle { handle }),
        };
        self.params = req
            .params
            .map(|p| p.to_proto().map_err(Error::deser))
            .transpose()?;
        *self.spec.lock().expect("never poisoned") = AppendObjectSpecState::Append {
            spec,
            initial_chunk: None,
        };
        let (initial, connection) = self.connect_attempt_loop().await?;
        self.last_persisted_size = persisted_size(&initial).unwrap_or(0);
        self.retry_state = None;
        Ok((initial, connection))
    }

    /// Reconnects a broken or redirected `BidiWriteObject` stream.
    ///
    /// 1. **Redirects:** If `last_error` carries a `BidiWriteObjectRedirectedError`, updates the
    ///    stored `routing_token` and `write_handle`.
    /// 2. **Retry budget:** Resets `retry_state` whenever the server has persisted more bytes than
    ///    `last_persisted_size` (either via `persisted` or in the new stream's handshake response).
    ///    Otherwise, increments `retry_state.attempt_count` while keeping the original start time so
    ///    the application's [`RetryPolicy`] bounds unproductive reconnect loops.
    /// 3. **Reopen:** Evaluates `last_error` via [`RetryRedirect`]. Permanent or exhausted errors
    ///    fail immediately; retryable errors and redirects (up to `MAX_REDIRECTS_FOLLOWED`) open a
    ///    new stream with `state_lookup: true`.
    pub async fn reconnect(
        &mut self,
        last_error: Error,
        persisted: i64,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        let last_error = match gaxi::as_inner::as_inner::<gaxi::grpc::tonic::Status, _>(&last_error)
        {
            Some(status) => {
                let mut guard = self.spec.lock().expect("never poisoned");
                guard.handle_redirect(status.clone())
            }
            None => last_error,
        };

        if persisted > self.last_persisted_size {
            self.last_persisted_size = persisted;
            self.retry_state = None;
        }
        let state = self
            .retry_state
            .get_or_insert_with(|| RetryState::new(true).set_start(Instant::now()));
        state.attempt_count += 1;

        let policy = Arc::new(RetryRedirect::new(self.options.retry_policy.clone()));
        let last_error = match policy.on_error(state, last_error) {
            RetryResult::Continue(e) => e,
            RetryResult::Permanent(e) | RetryResult::Exhausted(e) => return Err(e),
        };

        tracing::debug!("reconnecting bidi write stream after error: {last_error:?}");

        let (initial, connection) = self.connect_attempt_loop_with_policy(policy).await?;
        if let Some(initial_persisted) = persisted_size(&initial)
            && initial_persisted > self.last_persisted_size
        {
            self.last_persisted_size = initial_persisted;
            self.retry_state = None;
        }
        Ok((initial, connection))
    }

    async fn connect_attempt_loop(
        &mut self,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        let retry = Arc::new(RetryRedirect::new(self.options.retry_policy.clone()));
        self.connect_attempt_loop_with_policy(retry).await
    }

    async fn connect_attempt_loop_with_policy(
        &mut self,
        retry: Arc<RetryRedirect<Arc<dyn RetryPolicy + 'static>>>,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        let throttler = self.options.retry_throttler.clone();
        let backoff = self.options.backoff_policy.clone();
        let client = self.client.clone();
        let options = self.options.clone();
        let spec = self.spec.clone();
        let params = self.params.clone();
        let sleep = async |backoff| tokio::time::sleep(backoff).await;
        let default_timeout = self.options.bidi_attempt_timeout;

        let inner = async move |d: Option<Duration>| {
            let attempt_timeout = std::cmp::min(default_timeout, d.unwrap_or(default_timeout));
            let attempt =
                Self::connect_attempt(client.clone(), spec.clone(), &options, params.clone());
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
        params: Option<CommonObjectRequestParams>,
    ) -> Result<(BidiWriteObjectResponse, Connection<T::Stream>)> {
        let (request, x_goog_request_params) = {
            let guard = spec.lock().expect("never poisoned");
            prepare_request(&guard, params)?
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<BidiWriteObjectRequest>(MAX_QUEUED_REQUESTS);
        tx.send(request).await.map_err(Error::io)?;

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
            Err(status) => {
                let mut guard = spec.lock().expect("never poisoned");
                return Err(guard.handle_redirect(status));
            }
        };

        let (_metadata, mut stream, _) = response.into_parts();
        match stream.next_message().await {
            Ok(Some(m)) => {
                let mut guard = spec.lock().expect("never poisoned");
                guard.handle_response(&m);

                Ok((m, Connection::new(tx, stream)))
            }
            Ok(None) => Err(Error::io("bidi_write_object stream closed before start")),
            Err(status) => {
                let mut guard = spec.lock().expect("never poisoned");
                Err(guard.handle_redirect(status))
            }
        }
    }
}

fn prepare_request(
    state: &AppendObjectSpecState,
    params: Option<CommonObjectRequestParams>,
) -> Result<(BidiWriteObjectRequest, String)> {
    let (first_message, routing_token, initial_chunk) = match state {
        AppendObjectSpecState::Write {
            spec,
            routing_token,
            initial_chunk,
        } => (
            FirstMessage::WriteObjectSpec((**spec).clone()),
            routing_token.clone(),
            initial_chunk.clone(),
        ),
        AppendObjectSpecState::Append {
            spec,
            initial_chunk,
        } => (
            FirstMessage::AppendObjectSpec(spec.clone()),
            spec.routing_token.clone(),
            initial_chunk.clone(),
        ),
    };

    let state_lookup = matches!(first_message, FirstMessage::AppendObjectSpec(_));

    let data = match initial_chunk {
        Some(chunk) if !chunk.is_empty() => {
            let crc = crc32c::crc32c(&chunk);
            Some(
                crate::google::storage::v2::bidi_write_object_request::Data::ChecksummedData(
                    crate::google::storage::v2::ChecksummedData {
                        content: chunk,
                        crc32c: Some(crc),
                    },
                ),
            )
        }
        _ => None,
    };

    let request = BidiWriteObjectRequest {
        first_message: Some(first_message),
        write_offset: 0,
        data,
        common_object_request_params: params,
        state_lookup,
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
        return Err(invalid_bucket_name(bucket_name));
    }

    let mut x_goog_request_params = format!("bucket={}", crate::storage::client::enc(bucket_name));
    if let Some(token) = routing_token {
        x_goog_request_params.push_str("&routing_token=");
        x_goog_request_params.push_str(&crate::storage::client::enc(&token));
    }

    Ok((request, x_goog_request_params))
}

fn invalid_bucket_name(bucket_name: &str) -> crate::Error {
    let problem =
        SubstitutionFail::MismatchExpecting(bucket_name.to_string(), "projects/_/buckets/*");
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
    crate::Error::binding(mismatch)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::model_ext::OpenAppendableObjectRequest;
    use anyhow::Result;
    use gaxi::grpc::Client as GrpcClient;
    use google_cloud_auth::credentials::{Credentials, anonymous::Builder as Anonymous};
    use google_cloud_gax::retry_policy::NeverRetry;
    use static_assertions::assert_impl_all;
    use std::error::Error as _;
    use std::sync::Arc;

    use super::super::mocks::{MockTestClient, SharedMockClient};
    use super::super::retry_redirect::MAX_REDIRECTS_FOLLOWED;
    use super::super::tests::{
        permanent_error, redirect_error, redirect_handle, redirect_status, test_options,
        transient_error,
    };
    use crate::google::storage::v2::bidi_write_object_response::WriteStatus;
    use gaxi::grpc::tonic::GrpcMethod;
    use gaxi::grpc::tonic::Response as TonicResponse;
    use gaxi::grpc::tonic::Result as TonicResult;
    use google_cloud_gax::error::binding::{BindingError, SubstitutionFail};
    use std::sync::Mutex;

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
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
                assert_eq!(params, "bucket=projects%2F_%2Fbuckets%2Ftest-bucket");
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
                assert_eq!(split, vec!["bucket=projects%2F_%2Fbuckets%2Ftest-bucket", "routing_token=test-routing-token"]);
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
            write_handle: Some(bytes::Bytes::from_static(b"test-write-handle")),
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
        assert_eq!(
            spec.write_handle.as_ref().map(|h| h.handle.clone()),
            Some(bytes::Bytes::from_static(b"test-write-handle"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn start_redirect_then_error() -> Result<()> {
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
            AppendObjectSpecState::Write { .. } => panic!("Should be Append"),
            AppendObjectSpecState::Append { spec: got, .. } => {
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
        if let AppendObjectSpecState::Append { spec: s, .. } = &*guard {
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

    fn test_open_request() -> OpenAppendableObjectRequest {
        OpenAppendableObjectRequest {
            spec: crate::model::WriteObjectSpec {
                resource: Some(crate::model::Object {
                    bucket: "projects/_/buckets/test-bucket".into(),
                    name: "test-object".into(),
                    generation: 123456,
                    ..Default::default()
                }),
                ..Default::default()
            },
            params: None,
        }
    }

    async fn setup_mock_open_connector(
        persisted_size: i64,
    ) -> Result<(
        Connector<SharedMockClient>,
        Arc<Mutex<Vec<tokio::sync::mpsc::Receiver<BidiWriteObjectRequest>>>>,
    )> {
        let (tx, rx) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream = TonicResponse::from(rx);

        let receivers = Arc::new(Mutex::new(Vec::new()));
        let save = receivers.clone();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .return_once(move |_, _, rx, _, _, _| {
                save.lock().expect("never poisoned").push(rx);
                Ok(Ok(stream))
            });
        let client = SharedMockClient::new(mock);
        let connector = Connector::new(test_options(), client);

        let initial = BidiWriteObjectResponse {
            write_status: Some(
                crate::google::storage::v2::bidi_write_object_response::WriteStatus::Resource(
                    crate::google::storage::v2::Object {
                        bucket: "projects/_/buckets/test-bucket".into(),
                        name: "test-object".into(),
                        generation: 123456,
                        size: persisted_size,
                        ..crate::google::storage::v2::Object::default()
                    },
                ),
            ),
            ..BidiWriteObjectResponse::default()
        };
        tx.send(Ok(initial)).await?;

        Ok((connector, receivers))
    }

    #[tokio::test]
    async fn connect_open_and_append_sends_payload_in_first_request() -> Result<()> {
        // Arrange: Small payload to verify basic checksummed data packaging.
        let chunk = bytes::Bytes::from_static(b"hello world");
        let expected_crc = crc32c::crc32c(&chunk);
        let (mut connector, receivers) = setup_mock_open_connector(chunk.len() as i64).await?;

        // Act: Open stream with initial payload.
        let (_response, _connection) = connector
            .connect_open_and_append(test_open_request(), Some(chunk.clone()))
            .await?;

        // Assert: Verify opening request attaches ChecksummedData with CRC.
        let mut rx = receivers
            .lock()
            .expect("never poisoned")
            .pop()
            .expect("captured receiver");
        let got = rx.recv().await.expect("at least one request sent");

        assert_eq!(got.write_offset, 0);
        let checksummed_data = match got.data {
            Some(crate::google::storage::v2::bidi_write_object_request::Data::ChecksummedData(
                d,
            )) => d,
            _ => panic!("Expected ChecksummedData"),
        };
        assert_eq!(checksummed_data.content, chunk);
        assert_eq!(checksummed_data.crc32c, Some(expected_crc));

        Ok(())
    }

    #[tokio::test]
    async fn connect_open_and_append_empty_payload_omits_data() -> Result<()> {
        // Arrange: Empty chunk payload.
        let (mut connector, receivers) = setup_mock_open_connector(0).await?;

        // Act: Open with empty payload.
        connector
            .connect_open_and_append(test_open_request(), Some(bytes::Bytes::new()))
            .await?;

        // Assert: Verify no data field is present in the opening request.
        let mut rx = receivers
            .lock()
            .expect("never poisoned")
            .pop()
            .expect("captured receiver");
        let got = rx.recv().await.expect("at least one request sent");

        assert_eq!(got.write_offset, 0);
        assert!(got.data.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn connect_open_and_append_exact_max_chunk_size() -> Result<()> {
        // Arrange: Exact 2 MiB payload (MAX_WRITE_CHUNK_SIZE boundary).
        let chunk = bytes::Bytes::from(vec![0xAAu8; 2 * 1024 * 1024]);
        let expected_crc = crc32c::crc32c(&chunk);
        let (mut connector, receivers) = setup_mock_open_connector(chunk.len() as i64).await?;

        // Act: Open with 2 MiB payload.
        connector
            .connect_open_and_append(test_open_request(), Some(chunk.clone()))
            .await?;

        // Assert: Verify full 2 MiB payload is attached with CRC.
        let mut rx = receivers
            .lock()
            .expect("never poisoned")
            .pop()
            .expect("captured receiver");
        let got = rx.recv().await.expect("at least one request sent");

        assert_eq!(got.write_offset, 0);
        let checksummed_data = match got.data {
            Some(crate::google::storage::v2::bidi_write_object_request::Data::ChecksummedData(
                d,
            )) => d,
            _ => panic!("Expected ChecksummedData"),
        };
        assert_eq!(checksummed_data.content, chunk);
        assert_eq!(checksummed_data.crc32c, Some(expected_crc));

        Ok(())
    }

    #[tokio::test]
    async fn connect_open_and_append_large_chunk() -> Result<()> {
        // Arrange: 3 MiB payload. Connector transmits the chunk provided;
        // partitioning policy is handled at the transport layer.
        let chunk = bytes::Bytes::from(vec![0xBBu8; 3 * 1024 * 1024]);
        let expected_crc = crc32c::crc32c(&chunk);
        let (mut connector, receivers) = setup_mock_open_connector(chunk.len() as i64).await?;

        // Act: Open with 3 MiB payload.
        connector
            .connect_open_and_append(test_open_request(), Some(chunk.clone()))
            .await?;

        // Assert: Verify entire 3 MiB payload is attached by connector.
        let mut rx = receivers
            .lock()
            .expect("never poisoned")
            .pop()
            .expect("captured receiver");
        let got = rx.recv().await.expect("at least one request sent");

        assert_eq!(got.write_offset, 0);
        let checksummed_data = match got.data {
            Some(crate::google::storage::v2::bidi_write_object_request::Data::ChecksummedData(
                d,
            )) => d,
            _ => panic!("Expected ChecksummedData"),
        };
        assert_eq!(checksummed_data.content, chunk);
        assert_eq!(checksummed_data.crc32c, Some(expected_crc));

        Ok(())
    }

    #[tokio::test]
    async fn start_open_with_redirect_then_error() -> Result<()> {
        let mut seq = mockall::Sequence::new();
        let mut mock = MockTestClient::new();
        let receivers = Arc::new(Mutex::new(Vec::new()));

        // Forge an asynchronous stream that immediately yields a redirect error
        // on its very first message instead of closing normally.
        let (tx1, rx1) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream1 = TonicResponse::from(rx1);
        tx1.send(Err(redirect_status("r1"))).await?;
        drop(tx1);

        let save = receivers.clone();
        // The first attempt will successfully "start" the gRPC call and return
        // our forged stream containing the redirect.
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_, _, rx, _, _, _| {
                save.lock().expect("never poisoned").push(rx);
                Ok(Ok(stream1))
            });

        let save = receivers.clone();
        // The second attempt, triggered by the automatic retry loop, will hit a
        // permanent error so we can exit the retry loop and test our results.
        mock.expect_start()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_, _, rx, _, _, _| {
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

        // Running the stream evaluates the retry loop. It should catch the
        // redirect off the stream, retry, hit our permanent error, and return
        // the permanent error.
        let err = connector.connect_open(req).await.unwrap_err();
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");

        // Validate that catching the redirect successfully mutated our
        // spec state to an `Append` state tracking the new routing token.
        let got = connector.spec.lock().expect("never poisoned").clone();
        match got {
            AppendObjectSpecState::Write { .. } => panic!("Should be Append"),
            AppendObjectSpecState::Append { spec: got, .. } => {
                assert_eq!(got.routing_token.as_deref(), Some("r1"));
            }
        }

        // We pushed the outgoing `rx` connection channels into a vector
        // sequentially. Popping the last element gives us the second
        // (retry) attempt's outgoing connection. It must have dynamically
        // pivoted its setup structure to an `AppendObjectSpec` logic.
        let mut rx = receivers
            .lock()
            .expect("never poisoned")
            .pop()
            .expect("at least two receiver");

        // This is the second receiver. This should include an AppendObjectSpec
        // with the redirect options.
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

        // This is the first receiver. This should include a plain WriteObjectSpec.
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
    async fn reconnect_with_redirect_status_updates_spec() -> Result<()> {
        // Arrange.
        let (tx, rx) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream = TonicResponse::from(rx);

        let receivers = Arc::new(Mutex::new(Vec::new()));
        let save = receivers.clone();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .return_once(move |_, _, rx, _, _, params| {
                assert!(params.contains("routing_token=new-token"));
                save.lock().expect("never poisoned").push(rx);
                Ok(Ok(stream))
            });
        let client = SharedMockClient::new(mock);
        let mut connector = Connector::new(test_options(), client);

        let initial_spec = AppendObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 123456,
            routing_token: Some("old-token".into()),
            write_handle: None,
            ..Default::default()
        };
        connector.set_spec_state(AppendObjectSpecState::Append {
            spec: initial_spec,
            initial_chunk: None,
        });

        let initial_response = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(50)),
            ..Default::default()
        };
        tx.send(Ok(initial_response.clone())).await?;

        let redirect_err = redirect_error("new-token");

        // Act.
        let (resp, _conn) = connector.reconnect(redirect_err, 0).await?;

        // Assert.
        assert_eq!(resp, initial_response);

        let guard = connector.spec.lock().expect("never poisoned");
        if let AppendObjectSpecState::Append { spec: s, .. } = &*guard {
            assert_eq!(s.routing_token.as_deref(), Some("new-token"));
            assert_eq!(s.generation, 42); // from test redirect_status
            assert_eq!(s.write_handle, Some(redirect_handle()));
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_with_transient_error() -> Result<()> {
        // Arrange.
        let (tx, rx) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream = TonicResponse::from(rx);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream)));
        let client = SharedMockClient::new(mock);
        let mut connector = Connector::new(test_options(), client);

        let initial_spec = AppendObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 123456,
            routing_token: Some("stable-token".into()),
            write_handle: None,
            ..Default::default()
        };
        connector.set_spec_state(AppendObjectSpecState::Append {
            spec: initial_spec.clone(),
            initial_chunk: None,
        });

        let initial_response = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(100)),
            ..Default::default()
        };
        tx.send(Ok(initial_response.clone())).await?;

        let transient_err = transient_error();

        // Act.
        let (resp, _conn) = connector.reconnect(transient_err, 0).await?;

        // Assert.
        assert_eq!(resp, initial_response);
        let guard = connector.spec.lock().expect("never poisoned");
        if let AppendObjectSpecState::Append { spec: s, .. } = &*guard {
            assert_eq!(s, &initial_spec);
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_permanent_error_fails_fast() -> Result<()> {
        // Arrange.
        let mut mock = MockTestClient::new();
        mock.expect_start().never();
        let client = SharedMockClient::new(mock);
        let mut connector = Connector::new(test_options(), client);

        // Act.
        let err = connector.reconnect(permanent_error(), 0).await.unwrap_err();

        // Assert.
        assert_eq!(err.status(), permanent_error().status(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_io_wrapped_redirect_status_updates_spec() -> Result<()> {
        // Arrange.
        let (tx, rx) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream = TonicResponse::from(rx);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .return_once(move |_, _, _, _, _, params| {
                assert!(params.contains("routing_token=io-wrapped-token"));
                Ok(Ok(stream))
            });
        let client = SharedMockClient::new(mock);
        let mut connector = Connector::new(test_options(), client);

        connector.set_spec_state(AppendObjectSpecState::Append {
            spec: AppendObjectSpec {
                bucket: "projects/_/buckets/test-bucket".into(),
                object: "test-object".into(),
                generation: 1,
                ..Default::default()
            },
            initial_chunk: None,
        });

        let initial_response = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(10)),
            ..Default::default()
        };
        tx.send(Ok(initial_response.clone())).await?;

        // Act.
        let io_redirect = Error::io(redirect_status("io-wrapped-token"));
        let (resp, _conn) = connector.reconnect(io_redirect, 0).await?;

        // Assert.
        assert_eq!(resp, initial_response);
        let guard = connector.spec.lock().expect("never poisoned");
        if let AppendObjectSpecState::Append { spec: s, .. } = &*guard {
            assert_eq!(
                s.routing_token.as_deref(),
                Some("io-wrapped-token"),
                "{s:?}"
            );
            assert_eq!(s.write_handle, Some(redirect_handle()));
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
        Ok(())
    }

    /// Builds a connector in the `Append` state whose mock hands out `count` successive streams,
    /// and returns the senders feeding those streams in the order they are handed out.
    fn connector_with_streams(
        count: usize,
    ) -> (
        Connector<SharedMockClient>,
        Vec<tokio::sync::mpsc::Sender<TonicResult<BidiWriteObjectResponse>>>,
    ) {
        let mut mock = MockTestClient::new();
        let mut senders = Vec::with_capacity(count);
        for _ in 0..count {
            let (tx, rx) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
            let stream = TonicResponse::from(rx);
            mock.expect_start()
                .times(1)
                .return_once(move |_, _, _, _, _, _| Ok(Ok(stream)));
            senders.push(tx);
        }
        let mut connector = Connector::new(test_options(), SharedMockClient::new(mock));
        connector.set_spec_state(AppendObjectSpecState::Append {
            spec: AppendObjectSpec {
                bucket: "projects/_/buckets/test-bucket".into(),
                object: "test-object".into(),
                generation: 1,
                ..Default::default()
            },
            initial_chunk: None,
        });
        (connector, senders)
    }

    /// Queues a first response acknowledging `persisted` bytes on every stream.
    async fn send_persisted_size(
        senders: &[tokio::sync::mpsc::Sender<TonicResult<BidiWriteObjectResponse>>],
        persisted: i64,
    ) -> Result<()> {
        for tx in senders {
            tx.send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(persisted)),
                ..Default::default()
            }))
            .await?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_time_limit_expires_without_progress() -> Result<()> {
        use google_cloud_gax::retry_policy::RetryPolicyExt;
        // Arrange.
        let (mut connector, senders) = connector_with_streams(2);
        send_persisted_size(&senders, 0).await?;
        connector.options.retry_policy =
            Arc::new(crate::retry_policy::RetryableErrors.with_time_limit(Duration::from_secs(30)));

        // Act.
        // First reconnect anchors `retry_state.start` at T+0s.
        connector.reconnect(transient_error(), 0).await?;
        tokio::time::advance(Duration::from_secs(10)).await;
        connector.reconnect(transient_error(), 0).await?;
        // T+35s: past the 30s policy time limit without byte progress.
        tokio::time::advance(Duration::from_secs(25)).await;
        let err = connector.reconnect(transient_error(), 0).await.unwrap_err();

        // Assert.
        assert_eq!(err.status(), transient_error().status(), "{err:?}");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_retry_state_resets_on_acknowledged_bytes() -> Result<()> {
        use google_cloud_gax::retry_policy::RetryPolicyExt;
        // Arrange.
        let (mut connector, senders) = connector_with_streams(3);
        send_persisted_size(&senders, 0).await?;
        connector.options.retry_policy =
            Arc::new(crate::retry_policy::RetryableErrors.with_time_limit(Duration::from_secs(30)));

        // Act.
        // Anchors `retry_state.start` at T+0s.
        connector.reconnect(transient_error(), 0).await?;
        // T+10s: 64 acknowledged bytes reset `retry_state` so `start` becomes T+10s.
        tokio::time::advance(Duration::from_secs(10)).await;
        connector.reconnect(transient_error(), 64).await?;
        // T+35s: past the original T+30s limit, but within the reset T+40s limit.
        tokio::time::advance(Duration::from_secs(25)).await;
        let (resp, _conn) = connector.reconnect(transient_error(), 64).await?;

        // Assert.
        assert_eq!(persisted_size(&resp), Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn connect_open_seeds_persisted_size_and_clears_retry_state() -> Result<()> {
        // Arrange.
        let (tx1, rx1) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream1 = TonicResponse::from(rx1);
        let (tx2, rx2) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream2 = TonicResponse::from(rx2);
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream1)));
        mock.expect_start()
            .times(1)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream2)));
        let mut connector = Connector::new(test_options(), SharedMockClient::new(mock));

        let req = OpenAppendableObjectRequest {
            spec: crate::model::WriteObjectSpec {
                resource: Some(crate::model::Object {
                    bucket: "projects/_/buckets/test-bucket".into(),
                    name: "test-object".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            params: None,
        };
        tx1.send(Ok(BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(100)),
            ..Default::default()
        }))
        .await?;
        tx2.send(Ok(BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(100)),
            ..Default::default()
        }))
        .await?;

        // Act.
        connector.connect_open(req).await?;

        // Assert.
        assert_eq!(connector.last_persisted_size, 100);
        assert!(connector.retry_state.is_none());
        connector.reconnect(transient_error(), 100).await?;
        assert_eq!(
            connector.retry_state.as_ref().map(|s| s.attempt_count),
            Some(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_follows_redirect_even_when_retries_are_disabled() -> Result<()> {
        // Arrange.
        let (tx, rx) = tokio::sync::mpsc::channel::<TonicResult<BidiWriteObjectResponse>>(5);
        let stream = TonicResponse::from(rx);
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .return_once(move |_, _, _, _, _, params| {
                assert!(params.contains("routing_token=new-token"), "{params}");
                Ok(Ok(stream))
            });
        let mut options = test_options();
        options.retry_policy = Arc::new(NeverRetry);
        let mut connector = Connector::new(options, SharedMockClient::new(mock));
        connector.set_spec_state(AppendObjectSpecState::Append {
            spec: AppendObjectSpec {
                bucket: "projects/_/buckets/test-bucket".into(),
                object: "test-object".into(),
                generation: 1,
                ..Default::default()
            },
            initial_chunk: None,
        });
        tx.send(Ok(BidiWriteObjectResponse::default())).await?;

        // Act.
        // `NeverRetry` reports every error as exhausted, but a redirect is a routing change rather
        // than a retry, so it must still be followed.
        connector.reconnect(redirect_error("new-token"), 0).await?;

        // Assert.
        let guard = connector.spec.lock().expect("never poisoned");
        let AppendObjectSpecState::Append { spec, .. } = &*guard else {
            panic!("Expected AppendObjectSpecState::Append");
        };
        assert_eq!(spec.routing_token.as_deref(), Some("new-token"));
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_gives_up_on_transient_error_when_retries_are_disabled() -> Result<()> {
        // Arrange.
        let mut mock = MockTestClient::new();
        mock.expect_start().never();
        let mut options = test_options();
        options.retry_policy = Arc::new(NeverRetry);
        let mut connector = Connector::new(options, SharedMockClient::new(mock));

        // Act.
        let err = connector.reconnect(transient_error(), 0).await.unwrap_err();

        // Assert.
        assert_eq!(err.status(), transient_error().status(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_gives_up_on_an_endless_redirect_loop() -> Result<()> {
        // Arrange.
        let attempts = Arc::new(Mutex::new(0_usize));
        let observed = attempts.clone();
        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(0..)
            .returning(move |_, _, _, _, _, _| {
                *attempts.lock().expect("never poisoned") += 1;
                Ok(Err(redirect_status("loop-token")))
            });
        let mut options = test_options();
        options.retry_policy = Arc::new(NeverRetry);
        let mut connector = Connector::new(options, SharedMockClient::new(mock));
        connector.set_spec_state(AppendObjectSpecState::Append {
            spec: AppendObjectSpec {
                bucket: "projects/_/buckets/test-bucket".into(),
                object: "test-object".into(),
                generation: 1,
                ..Default::default()
            },
            initial_chunk: None,
        });

        // Act.
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            connector.reconnect(redirect_error("seed-token"), 0),
        )
        .await;

        // Assert.
        let err = result
            .expect("the redirect budget must stop the loop")
            .unwrap_err();
        assert!(!err.is_timeout(), "{err:?}");
        let attempts = *observed.lock().expect("never poisoned");
        assert!(
            attempts <= MAX_REDIRECTS_FOLLOWED as usize,
            "followed {attempts} redirects, expected at most {}",
            MAX_REDIRECTS_FOLLOWED
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_respects_attempt_limit_and_resets_on_progress() -> Result<()> {
        use google_cloud_gax::retry_policy::RetryPolicyExt;
        // Arrange: `with_attempt_limit(3)` allows 2 progress-free reconnects (attempts 1 and 2) and
        // stops on the 3rd (`attempt_count == 3`). Once byte progress is reported, `retry_state`
        // resets and allows another reconnect.
        let (mut connector, senders) = connector_with_streams(3);
        send_persisted_size(&senders, 0).await?;
        connector.options.retry_policy =
            Arc::new(crate::retry_policy::RetryableErrors.with_attempt_limit(3));

        // Act & Assert: 2 progress-free reconnects succeed, 3rd stops with `Exhausted`.
        connector.reconnect(transient_error(), 0).await?;
        connector.reconnect(transient_error(), 0).await?;
        let err = connector.reconnect(transient_error(), 0).await.unwrap_err();
        assert_eq!(err.status(), transient_error().status(), "{err:?}");

        // Reporting byte progress resets `retry_state`, so the next reconnect succeeds.
        connector.reconnect(transient_error(), 64).await?;
        assert_eq!(
            connector.retry_state.as_ref().map(|s| s.attempt_count),
            Some(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_handshake_persisted_bytes_resets_retry_state() -> Result<()> {
        use google_cloud_gax::retry_policy::RetryPolicyExt;
        // Arrange: `with_attempt_limit(2)` allows 1 progress-free reconnect (`attempt_count == 1`)
        // and stops on the 2nd (`attempt_count == 2`). If the 1st reconnect's handshake response
        // reports newly persisted bytes (`PersistedSize(64)`), `retry_state` resets to `None` so
        // the next reconnect succeeds (`attempt_count == 1`).
        let (mut connector, senders) = connector_with_streams(2);
        connector.options.retry_policy =
            Arc::new(crate::retry_policy::RetryableErrors.with_attempt_limit(2));
        senders[0]
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(64)),
                ..Default::default()
            }))
            .await?;
        senders[1]
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(64)),
                ..Default::default()
            }))
            .await?;

        // Act.
        connector.reconnect(transient_error(), 0).await?;
        assert!(connector.retry_state.is_none());
        connector.reconnect(transient_error(), 64).await?;

        // Assert.
        assert_eq!(
            connector.retry_state.as_ref().map(|s| s.attempt_count),
            Some(1)
        );
        Ok(())
    }
}
