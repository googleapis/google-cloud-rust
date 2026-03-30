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

use crate::Result;
use crate::model_ext::{ObjectHighlights, ReadRange, RequestedRange};
use crate::read_object::ReadObjectResponse;
use crate::read_object::dynamic::ReadObjectResponse as DynamicReadObjectResponse;
use crate::storage::bidi::stub::dynamic::ObjectDescriptor as DynamicObjectDescriptorStub;
use crate::storage::info::INSTRUMENTATION;
use crate::storage::stub::ObjectDescriptor as ObjectDescriptorStub;
use gaxi::observability::attributes::keys::{
    GCP_CLIENT_ARTIFACT, GCP_CLIENT_REPO, GCP_CLIENT_SERVICE, GCP_CLIENT_VERSION, GCP_SCHEMA_URL,
    OTEL_KIND, RPC_SERVICE, RPC_SYSTEM_NAME,
};
use gaxi::observability::attributes::{
    GCP_CLIENT_REPO_GOOGLEAPIS, OTEL_KIND_INTERNAL, RPC_SYSTEM_GRPC, SCHEMA_URL_VALUE,
};
use std::sync::Arc;

/// Implements the [ReadObjectResponse][DynamicReadObjectResponse] trait with
/// tracing annotations.
#[derive(Debug)]
pub(crate) struct TracingResponse<T> {
    inner: T,
    span: tracing::Span,
}

impl<T> TracingResponse<T> {
    pub fn new(inner: T, span: tracing::Span) -> Self {
        Self { inner, span }
    }
}

#[async_trait::async_trait]
impl DynamicReadObjectResponse for TracingResponse<Box<dyn DynamicReadObjectResponse + Send>> {
    fn object(&self) -> ObjectHighlights {
        self.inner.object()
    }

    async fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        use ::tracing::Instrument as _;
        let result = self.inner.next().instrument(self.span.clone()).await;
        let r = result.as_ref();
        let eof = r.is_none();
        let err = r.is_some_and(|e| e.is_err());
        let cnt = r
            .and_then(|e| e.as_ref().ok())
            .map(|b| b.len())
            .unwrap_or(0_usize);
        ::tracing::event!(parent: &self.span, tracing::Level::INFO, eof = eof, err = err, cnt = cnt);
        result
    }
}

/// Implements the [ObjectDescriptorStub][DynamicObjectDescriptor] trait with tracing annotations.
#[derive(Clone, Debug)]
pub struct TracingObjectDescriptor<T> {
    inner: T,
}

impl<T> TracingObjectDescriptor<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl ObjectDescriptorStub for TracingObjectDescriptor<Arc<dyn DynamicObjectDescriptorStub>> {
    fn object(&self) -> crate::model::Object {
        // No span annotation as this does not involve any I/O.
        self.inner.object()
    }

    async fn read_range(&self, range: ReadRange) -> ReadObjectResponse {
        let (start, limit) = match &range.0 {
            RequestedRange::Offset(o) => (Some(*o as i64), None),
            RequestedRange::Tail(t) => (Some(-(*t as i64)), None),
            RequestedRange::Segment { offset, limit } => {
                (Some(*offset as i64), Some(*limit as i64))
            }
        };
        let span = tracing::info_span!(
            "read_range",
            { OTEL_KIND } = OTEL_KIND_INTERNAL,
            { RPC_SYSTEM_NAME } = RPC_SYSTEM_GRPC,
            { RPC_SERVICE } = INSTRUMENTATION.service_name,
            { GCP_CLIENT_SERVICE } = INSTRUMENTATION.service_name,
            { GCP_CLIENT_VERSION } = INSTRUMENTATION.client_version,
            { GCP_CLIENT_REPO } = GCP_CLIENT_REPO_GOOGLEAPIS,
            { GCP_CLIENT_ARTIFACT } = INSTRUMENTATION.client_artifact,
            { GCP_SCHEMA_URL } = SCHEMA_URL_VALUE,
            { "read_range.start" } = start,
            { "read_range.limit" } = limit,
        );
        let response = self.inner.read_range(range).await;
        let inner = TracingResponse::new(response.into_parts(), span);
        ReadObjectResponse::new(Box::new(inner))
    }

    fn headers(&self) -> http::HeaderMap {
        // No span annotation as this does not involve any I/O.
        self.inner.headers()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Object;
    use crate::object_descriptor::ObjectDescriptor;
    use crate::object_descriptor::tests::{MockDescriptor, MockResponse};
    use http::{HeaderMap, HeaderName, HeaderValue};

    #[tokio::test]
    async fn descriptor_forwards_calls() {
        let object = Object::new().set_name("test-object").set_generation(123456);
        let headers = HeaderMap::from_iter(
            [
                ("content-type", "application/octet-stream"),
                ("x-guploader-uploadid", "abc-123"),
            ]
            .map(|(k, v)| (HeaderName::from_static(k), HeaderValue::from_static(v))),
        );
        let mut mock = MockDescriptor::new();
        mock.expect_object().times(1).return_const(object.clone());
        mock.expect_headers().times(1).return_const(headers.clone());
        mock.expect_read_range()
            .times(1)
            .withf(|range| range.0 == ReadRange::segment(100, 200).0)
            .returning(|_| ReadObjectResponse::new(Box::new(MockResponse::new())));

        let descriptor = ObjectDescriptor::new(mock);
        let descriptor =
            ObjectDescriptor::new(TracingObjectDescriptor::new(descriptor.into_parts()));
        assert_eq!(descriptor.object(), object);
        assert_eq!(descriptor.headers(), headers);
        let _reader = descriptor.read_range(ReadRange::segment(100, 200)).await;
    }
}
