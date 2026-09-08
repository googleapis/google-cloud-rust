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

use crate::Error;
use crate::RequestBuilder;
use crate::RequestOptions;
use crate::Result;
use crate::model::BatchWriteRequest;
use crate::model::ExecuteSqlRequest;
use crate::model::FetchCacheUpdateRequest;
use crate::model::ReadRequest;
use crate::server_streaming::stream::BatchWriteStream;
use crate::server_streaming::stream::CacheUpdateStream;
use crate::server_streaming::stream::PartialResultSetStream;
use gaxi::grpc::tonic;
use gaxi::grpc::tonic::Extensions;
use gaxi::grpc::tonic::GrpcMethod;
use gaxi::prost::ToProto;
use prost::Message;
use std::sync::LazyLock;

/// The request builder for [SpannerImpl::execute_streaming_sql][crate::client::SpannerImpl::execute_streaming_sql] calls.
#[derive(Clone, Debug)]
pub(crate) struct ExecuteStreamingSql {
    grpc_client: gaxi::grpc::Client,
    request: ExecuteSqlRequest,
    options: RequestOptions,
}

impl ExecuteStreamingSql {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: ExecuteSqlRequest::default(),
            options: RequestOptions::default(),
        }
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<ExecuteSqlRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<PartialResultSetStream> {
        let session = self.request.session.clone();
        let request = self.request.to_proto().map_err(Error::deser)?;
        let request_params = format!("session={session}");
        let response = make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "ExecuteStreamingSql",
            "/google.spanner.v1.Spanner/ExecuteStreamingSql",
            &request_params,
        )
        .await?;
        let (metadata, stream, _) = response.into_parts();
        let headers = metadata.into_headers();
        Ok(PartialResultSetStream::new(stream, headers))
    }
}

impl RequestBuilder for ExecuteStreamingSql {
    fn request_options(&mut self) -> &mut RequestOptions {
        &mut self.options
    }
}

/// The request builder for [SpannerImpl::streaming_read][crate::client::SpannerImpl::streaming_read] calls.
#[derive(Clone, Debug)]
pub(crate) struct StreamingRead {
    grpc_client: gaxi::grpc::Client,
    request: ReadRequest,
    options: RequestOptions,
}

impl StreamingRead {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: ReadRequest::default(),
            options: RequestOptions::default(),
        }
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<ReadRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<PartialResultSetStream> {
        let session = self.request.session.clone();
        let request = self.request.to_proto().map_err(Error::deser)?;
        let request_params = format!("session={session}");
        let response = make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "StreamingRead",
            "/google.spanner.v1.Spanner/StreamingRead",
            &request_params,
        )
        .await?;
        let (metadata, stream, _) = response.into_parts();
        let headers = metadata.into_headers();
        Ok(PartialResultSetStream::new(stream, headers))
    }
}

impl RequestBuilder for StreamingRead {
    fn request_options(&mut self) -> &mut RequestOptions {
        &mut self.options
    }
}

/// The request builder for [SpannerImpl::batch_write][crate::client::SpannerImpl::batch_write] calls.
#[derive(Clone, Debug)]
pub(crate) struct BatchWrite {
    grpc_client: gaxi::grpc::Client,
    request: BatchWriteRequest,
    options: RequestOptions,
}

impl BatchWrite {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: BatchWriteRequest::default(),
            options: RequestOptions::default(),
        }
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<BatchWriteRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<BatchWriteStream> {
        let session = self.request.session.clone();
        let request = self.request.to_proto().map_err(Error::deser)?;
        let request_params = format!("session={session}");
        let response = make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "BatchWrite",
            "/google.spanner.v1.Spanner/BatchWrite",
            &request_params,
        )
        .await?;
        let (metadata, stream, _) = response.into_parts();
        let headers = metadata.into_headers();
        Ok(BatchWriteStream::new(stream, headers))
    }
}

impl RequestBuilder for BatchWrite {
    fn request_options(&mut self) -> &mut RequestOptions {
        &mut self.options
    }
}

/// The request builder for `FetchCacheUpdate` calls.
#[derive(Clone, Debug)]
pub(crate) struct FetchCacheUpdate {
    grpc_client: gaxi::grpc::Client,
    request: FetchCacheUpdateRequest,
    options: RequestOptions,
}

impl FetchCacheUpdate {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: FetchCacheUpdateRequest::default(),
            options: RequestOptions::default(),
        }
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<FetchCacheUpdateRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<CacheUpdateStream> {
        let database = self.request.database.clone();
        let request = self.request.to_proto().map_err(Error::deser)?;
        let request_params = format!("database={database}");
        let response = make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "FetchCacheUpdate",
            "/google.spanner.v1.Spanner/FetchCacheUpdate",
            &request_params,
        )
        .await?;
        let (metadata, stream, _) = response.into_parts();
        let headers = metadata.into_headers();
        Ok(CacheUpdateStream::new(stream, headers))
    }
}

impl RequestBuilder for FetchCacheUpdate {
    fn request_options(&mut self) -> &mut RequestOptions {
        &mut self.options
    }
}

static X_GOOG_API_CLIENT_HEADER: LazyLock<String> = LazyLock::new(|| {
    let ac = gaxi::api_header::XGoogApiClient {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        library_type: gaxi::api_header::GCCL,
    };
    ac.grpc_header_value()
});

async fn make_server_streaming_request<Req, Res>(
    grpc_client: &gaxi::grpc::Client,
    request: Req,
    options: RequestOptions,
    method_name: &'static str,
    path_str: &'static str,
    x_goog_request_params: &str,
) -> Result<tonic::Response<tonic::Streaming<Res>>>
where
    Req: Message + Default + Clone + 'static,
    Res: Message + Default + 'static,
{
    let options = google_cloud_gax::options::internal::set_default_idempotency(options, false);
    let extensions = {
        let mut e = Extensions::new();
        e.insert(GrpcMethod::new("google.spanner.v1.Spanner", method_name));
        e
    };
    let path = http::uri::PathAndQuery::from_static(path_str);

    grpc_client
        .server_streaming(
            extensions,
            path,
            request,
            options,
            &X_GOOG_API_CLIENT_HEADER,
            x_goog_request_params,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_test_macros::tokio_test_no_panics;

    #[tokio_test_no_panics]
    async fn fetch_cache_update_builder_configuration() {
        let (address, _server) =
            spanner_grpc_mock::start("0.0.0.0:0", spanner_grpc_mock::MockSpanner::new())
                .await
                .expect("mock server should start");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("spanner client should build");

        let grpc_client = spanner.channels[0]
            .grpc_client
            .clone()
            .expect("grpc client should exist");

        let mut builder = FetchCacheUpdate::new(grpc_client)
            .with_request(
                FetchCacheUpdateRequest::default()
                    .set_database("projects/p/instances/i/databases/d"),
            )
            .with_options(RequestOptions::default());

        let _ = builder.request_options();
        assert_eq!(
            builder.request.database,
            "projects/p/instances/i/databases/d"
        );
    }
}
