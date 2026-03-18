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

// TODO(#4969)
#![allow(dead_code)]

use crate::Error;
use crate::Result;
use crate::model::BatchWriteRequest;
use crate::model::ExecuteSqlRequest;
use crate::model::ReadRequest;
use crate::server_streaming::stream::BatchWriteStream;
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
    options: crate::RequestOptions,
}

impl ExecuteStreamingSql {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: ExecuteSqlRequest::default(),
            options: crate::RequestOptions::default(),
        }
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<ExecuteSqlRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<crate::RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<PartialResultSetStream> {
        let session = self.request.session.clone();
        let request = self.request.to_proto().map_err(Error::deser)?;
        let stream = make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "ExecuteStreamingSql",
            "/google.spanner.v1.Spanner/ExecuteStreamingSql",
            &session,
        )
        .await?;
        Ok(PartialResultSetStream::new(stream.into_inner()))
    }
}

impl crate::RequestBuilder for ExecuteStreamingSql {
    fn request_options(&mut self) -> &mut crate::RequestOptions {
        &mut self.options
    }
}

/// The request builder for [SpannerImpl::streaming_read][crate::client::SpannerImpl::streaming_read] calls.
#[derive(Clone, Debug)]
pub(crate) struct StreamingRead {
    grpc_client: gaxi::grpc::Client,
    request: ReadRequest,
    options: crate::RequestOptions,
}

impl StreamingRead {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: ReadRequest::default(),
            options: crate::RequestOptions::default(),
        }
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<ReadRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<crate::RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<PartialResultSetStream> {
        let session = self.request.session.clone();
        let request = self.request.to_proto().map_err(Error::deser)?;
        let stream = make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "StreamingRead",
            "/google.spanner.v1.Spanner/StreamingRead",
            &session,
        )
        .await?;
        Ok(PartialResultSetStream::new(stream.into_inner()))
    }
}

impl crate::RequestBuilder for StreamingRead {
    fn request_options(&mut self) -> &mut crate::RequestOptions {
        &mut self.options
    }
}

/// The request builder for [SpannerImpl::batch_write][crate::client::SpannerImpl::batch_write] calls.
#[derive(Clone, Debug)]
pub(crate) struct BatchWrite {
    grpc_client: gaxi::grpc::Client,
    request: BatchWriteRequest,
    options: crate::RequestOptions,
}

impl BatchWrite {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: BatchWriteRequest::default(),
            options: crate::RequestOptions::default(),
        }
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<BatchWriteRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<crate::RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<BatchWriteStream> {
        let session = self.request.session.clone();
        let request = self.request.to_proto().map_err(Error::deser)?;
        let stream = make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "BatchWrite",
            "/google.spanner.v1.Spanner/BatchWrite",
            &session,
        )
        .await?;
        Ok(BatchWriteStream::new(stream.into_inner()))
    }
}

impl crate::RequestBuilder for BatchWrite {
    fn request_options(&mut self) -> &mut crate::RequestOptions {
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
    options: crate::RequestOptions,
    method_name: &'static str,
    path_str: &'static str,
    session: &str,
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
    let x_goog_request_params = format!("session={session}");

    grpc_client
        .server_streaming(
            extensions,
            path,
            request,
            options,
            &X_GOOG_API_CLIENT_HEADER,
            &x_goog_request_params,
        )
        .await
}
