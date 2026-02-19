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
        req: ReadObjectRequest,
        options: RequestOptions,
    ) -> Result<ReadObjectResponse> {
        let reader = Reader {
            inner: self.inner.clone(),
            request: req,
            options,
        };
        reader.response().await
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tracing::instrument(level = tracing::Level::DEBUG, ret)]
    async fn read_object_tracing(
        &self,
        req: ReadObjectRequest,
        options: RequestOptions,
    ) -> Result<ReadObjectResponse> {
        let span = gaxi::client_request_span!("client::Storage", "read_object", &INSTRUMENTATION);
        let response = self
            .read_object_plain(req, options)
            .instrument(span.clone())
            .await
            .record_in_span(&span)?;
        let inner = TracingResponse::new(response.into_parts(), span);
        let response = ReadObjectResponse::new(Box::new(inner));
        Ok(response)
    }

    async fn write_object_buffered_plain<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        PerformUpload::new(payload, self.inner.clone(), req.spec, req.params, options)
            .send()
            .await
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tracing::instrument(level = tracing::Level::DEBUG, ret, skip(payload))]
    async fn write_object_buffered_tracing<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        let span = gaxi::client_request_span!("client::Storage", "write_object", &INSTRUMENTATION);
        self.write_object_buffered_plain(payload, req, options)
            .instrument(span.clone())
            .await
            .record_in_span(&span)
    }

    async fn write_object_unbuffered_plain<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Seek + Send + Sync + 'static,
    {
        PerformUpload::new(payload, self.inner.clone(), req.spec, req.params, options)
            .send_unbuffered()
            .await
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tracing::instrument(level = tracing::Level::DEBUG, ret, skip(payload))]
    async fn write_object_unbuffered_tracing<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        options: RequestOptions,
    ) -> Result<Object>
    where
        P: StreamingSource + Seek + Send + Sync + 'static,
    {
        let span = gaxi::client_request_span!("client::Storage", "write_object", &INSTRUMENTATION);
        self.write_object_unbuffered_plain(payload, req, options)
            .instrument(span.clone())
            .await
            .record_in_span(&span)
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

    #[cfg(google_cloud_unstable_tracing)]
    #[tracing::instrument(level = tracing::Level::DEBUG, ret)]
    async fn open_object_tracing(
        &self,
        request: OpenObjectRequest,
        options: RequestOptions,
    ) -> Result<(ObjectDescriptor, Vec<ReadObjectResponse>)> {
        let span = gaxi::client_request_span!("client::Storage", "open_object", &INSTRUMENTATION);
        let (descriptor, responses) = self
            .open_object_plain(request, options)
            .instrument(span.clone())
            .await
            .record_in_span(&span)?;
        // TODO(#3178) - wrap descriptor and responses with tracing decorators.
        Ok((descriptor, responses))
    }
}

impl super::stub::Storage for Storage {
    /// Implements [crate::client::Storage::read_object].
    async fn read_object(
        &self,
        req: ReadObjectRequest,
        options: RequestOptions,
    ) -> Result<ReadObjectResponse> {
        #[cfg(google_cloud_unstable_tracing)]
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
        #[cfg(google_cloud_unstable_tracing)]
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
        #[cfg(google_cloud_unstable_tracing)]
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
        #[cfg(google_cloud_unstable_tracing)]
        if self.tracing {
            return self.open_object_tracing(request, options).await;
        }
        self.open_object_plain(request, options).await
    }
}
