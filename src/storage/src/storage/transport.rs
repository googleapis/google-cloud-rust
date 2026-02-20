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
use std::sync::Arc;
#[cfg(google_cloud_unstable_tracing)]
use tracing::Instrument;

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
        const EXPECTED_ATTRIBUTES: [(&str, &str); 9] = [
            (OTEL_KIND, OTEL_KIND_INTERNAL),
            (RPC_SYSTEM, RPC_SYSTEM_HTTP),
            (RPC_SERVICE, "storage"),
            (OTEL_STATUS_CODE, "ERROR"),
            (GCP_CLIENT_SERVICE, "storage"),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust"),
            (GCP_CLIENT_ARTIFACT, "google-cloud-storage"),
            (GCP_CLIENT_LANGUAGE, GCP_CLIENT_LANGUAGE_RUST),
            (OTEL_STATUS_CODE, "ERROR"),
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
