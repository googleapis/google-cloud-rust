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
use gaxi::observability::{GCP_CLIENT_REPO_GOOGLEAPIS, SCHEMA_URL_VALUE};
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
            "otel.kind" = "Internal",
            "rpc.system.name" = "grpc",
            "gcp.client.service" = INSTRUMENTATION.service_name,
            "gcp.client.version" = INSTRUMENTATION.client_version,
            "gcp.client.repo" = GCP_CLIENT_REPO_GOOGLEAPIS,
            "gcp.client.artifact" = INSTRUMENTATION.client_artifact,
            "gcp.schema.url" = SCHEMA_URL_VALUE,
            "read_range.start" = start,
            "read_range.limit" = limit,
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

#[cfg(google_cloud_unstable_storage_bidi)]
use crate::storage::bidi_write::stub::dynamic::AppendableObjectWriter as DynamicAppendableObjectWriterStub;
#[cfg(google_cloud_unstable_storage_bidi)]
use crate::storage::stub::AppendableObjectWriter as AppendableObjectWriterStub;

#[cfg(google_cloud_unstable_storage_bidi)]
/// Implements the [AppendableObjectWriterStub][DynamicAppendableObjectWriterStub] trait with tracing annotations.
#[derive(Debug)]
#[allow(dead_code)]
pub struct TracingAppendableObjectWriter<T> {
    inner: T,
}

#[cfg(google_cloud_unstable_storage_bidi)]
#[allow(dead_code)]
impl<T> TracingAppendableObjectWriter<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[cfg(google_cloud_unstable_storage_bidi)]
impl AppendableObjectWriterStub
    for TracingAppendableObjectWriter<Box<dyn DynamicAppendableObjectWriterStub>>
{
    #[tracing::instrument(
        name = "append",
        err,
        skip(self, chunk),
        fields(
            otel.kind = "Internal",
            rpc.system.name = "grpc",
            gcp.client.service = INSTRUMENTATION.service_name,
            gcp.client.version = INSTRUMENTATION.client_version,
            gcp.client.repo = GCP_CLIENT_REPO_GOOGLEAPIS,
            gcp.client.artifact = INSTRUMENTATION.client_artifact,
            gcp.schema.url = SCHEMA_URL_VALUE,
            append.chunk_size = chunk.len(),
            append.generation = self.inner.generation(),
        )
    )]
    async fn append(&mut self, chunk: bytes::Bytes) -> crate::Result<()> {
        self.inner.append(chunk).await
    }

    #[tracing::instrument(
        name = "flush",
        err,
        skip(self),
        fields(
            otel.kind = "Internal",
            rpc.system.name = "grpc",
            gcp.client.service = INSTRUMENTATION.service_name,
            gcp.client.version = INSTRUMENTATION.client_version,
            gcp.client.repo = GCP_CLIENT_REPO_GOOGLEAPIS,
            gcp.client.artifact = INSTRUMENTATION.client_artifact,
            gcp.schema.url = SCHEMA_URL_VALUE,
            flush.generation = self.inner.generation(),
            flush.persisted_size = tracing::field::Empty,
        )
    )]
    async fn flush(&mut self) -> crate::Result<i64> {
        self.inner.flush().await.inspect(|size| {
            tracing::Span::current().record("flush.persisted_size", size);
        })
    }

    #[tracing::instrument(
        name = "finalize",
        err,
        skip(self),
        fields(
            otel.kind = "Internal",
            rpc.system.name = "grpc",
            gcp.client.service = INSTRUMENTATION.service_name,
            gcp.client.version = INSTRUMENTATION.client_version,
            gcp.client.repo = GCP_CLIENT_REPO_GOOGLEAPIS,
            gcp.client.artifact = INSTRUMENTATION.client_artifact,
            gcp.schema.url = SCHEMA_URL_VALUE,
            finalize.generation = self.inner.generation(),
            finalize.persisted_size = tracing::field::Empty,
        )
    )]
    async fn finalize(self) -> crate::Result<crate::model::Object> {
        self.inner.finalize().await.inspect(|obj| {
            tracing::Span::current().record("finalize.persisted_size", obj.size);
        })
    }

    #[tracing::instrument(
        name = "close",
        err,
        skip(self),
        fields(
            otel.kind = "Internal",
            rpc.system.name = "grpc",
            gcp.client.service = INSTRUMENTATION.service_name,
            gcp.client.version = INSTRUMENTATION.client_version,
            gcp.client.repo = GCP_CLIENT_REPO_GOOGLEAPIS,
            gcp.client.artifact = INSTRUMENTATION.client_artifact,
            gcp.schema.url = SCHEMA_URL_VALUE,
            close.generation = self.inner.generation(),
            close.persisted_size = tracing::field::Empty,
        )
    )]
    async fn close(self) -> crate::Result<i64> {
        self.inner.close().await.inspect(|size| {
            tracing::Span::current().record("close.persisted_size", size);
        })
    }

    fn generation(&self) -> i64 {
        self.inner.generation()
    }

    fn persisted_size(&self) -> i64 {
        self.inner.persisted_size()
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

    #[cfg(google_cloud_unstable_storage_bidi)]
    mockall::mock! {
        #[derive(Debug)]
        Writer {}
        impl crate::stub::AppendableObjectWriter for Writer {
            async fn append(&mut self, chunk: bytes::Bytes) -> crate::Result<()>;
            async fn flush(&mut self) -> crate::Result<i64>;
            async fn finalize(self) -> crate::Result<crate::model::Object>;
            async fn close(self) -> crate::Result<i64>;
            fn generation(&self) -> i64;
            fn persisted_size(&self) -> i64;
        }
    }

    #[cfg(google_cloud_unstable_storage_bidi)]
    #[tokio::test]
    async fn appendable_writer_forwards_calls()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        use crate::appendable_object_writer::AppendableObjectWriter;
        use bytes::Bytes;

        const GENERATION: i64 = 123;
        const PERSISTED_SIZE: i64 = 456;
        const FLUSH_SIZE: i64 = 789;
        const FINALIZE_SIZE: i64 = 999;
        const CLOSE_SIZE: i64 = 100;

        let mut mock = MockWriter::new();
        mock.expect_generation().returning(|| GENERATION);
        mock.expect_persisted_size().returning(|| PERSISTED_SIZE);
        mock.expect_append().returning(|_| Ok(()));
        mock.expect_flush().returning(|| Ok(FLUSH_SIZE));

        let writer = AppendableObjectWriter::new(mock);
        let mut writer =
            AppendableObjectWriter::new(TracingAppendableObjectWriter::new(writer.into_parts()));

        writer.append(Bytes::from("test")).await?;
        assert_eq!(writer.flush().await?, FLUSH_SIZE);
        assert_eq!(writer.generation(), GENERATION);
        assert_eq!(writer.persisted_size(), PERSISTED_SIZE);

        let mut mock = MockWriter::new();
        mock.expect_generation().returning(|| GENERATION);
        mock.expect_finalize().returning(|| {
            Ok(crate::model::Object {
                size: FINALIZE_SIZE,
                ..Default::default()
            })
        });
        let writer = AppendableObjectWriter::new(mock);
        let writer =
            AppendableObjectWriter::new(TracingAppendableObjectWriter::new(writer.into_parts()));
        assert_eq!(writer.finalize().await?.size, FINALIZE_SIZE);

        let mut mock = MockWriter::new();
        mock.expect_generation().returning(|| GENERATION);
        mock.expect_close().returning(|| Ok(CLOSE_SIZE));
        let writer = AppendableObjectWriter::new(mock);
        let writer =
            AppendableObjectWriter::new(TracingAppendableObjectWriter::new(writer.into_parts()));
        assert_eq!(writer.close().await?, CLOSE_SIZE);

        Ok(())
    }
}
