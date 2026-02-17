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
use crate::Result;
use crate::client::stream::ServerStream;
use crate::client::stream::BatchWriteStream;
use crate::model::ExecuteSqlRequest;
use crate::model::ReadRequest;
use crate::model::BatchWriteRequest;
use gaxi::grpc::tonic::Extensions;
use gaxi::grpc::tonic::GrpcMethod;
use gaxi::prost::ToProto;

/// The request builder for [SpannerImpl::execute_streaming_sql][crate::client::SpannerImpl::execute_streaming_sql] calls.
#[derive(Clone, Debug)]
pub struct ExecuteStreamingSql {
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
    pub fn with_request<V: Into<ExecuteSqlRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub fn with_options<V: Into<crate::RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub async fn send(self) -> Result<ServerStream> {
        let options =
            google_cloud_gax::options::internal::set_default_idempotency(self.options, false);
        let extensions = {
            let mut e = Extensions::new();
            e.insert(GrpcMethod::new(
                "google.spanner.v1.Spanner",
                "ExecuteStreamingSql",
            ));
            e
        };
        let path =
            http::uri::PathAndQuery::from_static("/google.spanner.v1.Spanner/ExecuteStreamingSql");
        let x_goog_request_params = [Some(&self.request)
            .map(|m| &m.session)
            .map(|s| s.as_str())
            .map(|v| format!("session={v}"))]
        .into_iter()
        .flatten()
        .fold(String::new(), |b, p| b + "&" + &p);

        // We use info from the generated client manually or rely on gaxi
        // However, gaxi's Client expects api_client_header. We can copy it from transport info:
        // We'll generate a similar one using gaxi::api_header.

        let ac = gaxi::api_header::XGoogApiClient {
            // we can hardcode the name/version or use env!("CARGO_PKG_NAME") etc
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            library_type: gaxi::api_header::GAPIC,
        };
        let api_client_header = Box::leak(ac.grpc_header_value().into_boxed_str());

        let stream_res = self.grpc_client
            .server_streaming::<crate::google::spanner::v1::ExecuteSqlRequest, crate::google::spanner::v1::PartialResultSet>(
                extensions,
                path,
                self.request.to_proto().map_err(Error::deser)?,
                options,
                api_client_header,
                &x_goog_request_params,
            )
            .await?;

        Ok(ServerStream::new(stream_res.into_inner()))
    }

    // Setters mirroring ExecuteSql request fields

    /// Sets the value of session.
    pub fn set_session<T: Into<std::string::String>>(mut self, v: T) -> Self {
        self.request.session = v.into();
        self
    }

    /// Sets the value of transaction.
    pub fn set_transaction<T: Into<crate::model::TransactionSelector>>(mut self, v: T) -> Self {
        self.request.transaction = Some(v.into());
        self
    }

    /// Sets the value of sql.
    pub fn set_sql<T: Into<std::string::String>>(mut self, v: T) -> Self {
        self.request.sql = v.into();
        self
    }

    /// Sets the value of params.
    pub fn set_params<T: Into<wkt::Struct>>(mut self, v: T) -> Self {
        self.request.params = Some(v.into());
        self
    }

    /// Sets the value of param_types.
    pub fn set_param_types<T, K, V>(mut self, v: T) -> Self
    where
        T: std::iter::IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<crate::model::Type>,
    {
        self.request.param_types = v.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        self
    }

    /// Sets the value of resume_token.
    pub fn set_resume_token<T: Into<::bytes::Bytes>>(mut self, v: T) -> Self {
        self.request.resume_token = v.into();
        self
    }

    /// Sets the value of query_mode.
    pub fn set_query_mode<T: Into<crate::model::execute_sql_request::QueryMode>>(
        mut self,
        v: T,
    ) -> Self {
        self.request.query_mode = v.into();
        self
    }

    /// Sets the value of partition_token.
    pub fn set_partition_token<T: Into<::bytes::Bytes>>(mut self, v: T) -> Self {
        self.request.partition_token = v.into();
        self
    }

    /// Sets the value of seqno.
    pub fn set_seqno<T: Into<i64>>(mut self, v: T) -> Self {
        self.request.seqno = v.into();
        self
    }

    /// Sets the value of query_options.
    pub fn set_query_options<T: Into<crate::model::execute_sql_request::QueryOptions>>(
        mut self,
        v: T,
    ) -> Self {
        self.request.query_options = Some(v.into());
        self
    }

    /// Sets the value of request_options.
    pub fn set_request_options<T: Into<crate::model::RequestOptions>>(mut self, v: T) -> Self {
        self.request.request_options = Some(v.into());
        self
    }

    /// Sets the value of directed_read_options.
    pub fn set_directed_read_options<T: Into<crate::model::DirectedReadOptions>>(
        mut self,
        v: T,
    ) -> Self {
        self.request.directed_read_options = Some(v.into());
        self
    }

    /// Sets the value of data_boost_enabled.
    pub fn set_data_boost_enabled<T: Into<bool>>(mut self, v: T) -> Self {
        self.request.data_boost_enabled = v.into();
        self
    }

    /// Sets the value of last_statement.
    pub fn set_last_statement<T: Into<bool>>(mut self, v: T) -> Self {
        self.request.last_statement = v.into();
        self
    }

    /// Sets the value of routing_hint.
    pub fn set_routing_hint<T: Into<crate::model::RoutingHint>>(mut self, v: T) -> Self {
        self.request.routing_hint = Some(v.into());
        self
    }
}

impl crate::RequestBuilder for ExecuteStreamingSql {
    fn request_options(&mut self) -> &mut crate::RequestOptions {
        &mut self.options
    }
}

/// The request builder for [SpannerImpl::streaming_read][crate::client::SpannerImpl::streaming_read] calls.
#[derive(Clone, Debug)]
pub struct StreamingRead {
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
    pub fn with_request<V: Into<ReadRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub fn with_options<V: Into<crate::RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub async fn send(self) -> Result<ServerStream> {
        let options =
            google_cloud_gax::options::internal::set_default_idempotency(self.options, false);
        let extensions = {
            let mut e = Extensions::new();
            e.insert(GrpcMethod::new(
                "google.spanner.v1.Spanner",
                "StreamingRead",
            ));
            e
        };
        let path = http::uri::PathAndQuery::from_static("/google.spanner.v1.Spanner/StreamingRead");
        let x_goog_request_params = [Some(&self.request)
            .map(|m| &m.session)
            .map(|s| s.as_str())
            .map(|v| format!("session={v}"))]
        .into_iter()
        .flatten()
        .fold(String::new(), |b, p| b + "&" + &p);

        let ac = gaxi::api_header::XGoogApiClient {
            // we can hardcode the name/version or use env!("CARGO_PKG_NAME") etc
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            library_type: gaxi::api_header::GAPIC,
        };
        let api_client_header = Box::leak(ac.grpc_header_value().into_boxed_str());

        let stream_res = self.grpc_client
            .server_streaming::<crate::google::spanner::v1::ReadRequest, crate::google::spanner::v1::PartialResultSet>(
                extensions,
                path,
                self.request.to_proto().map_err(Error::deser)?,
                options,
                api_client_header,
                &x_goog_request_params,
            )
            .await?;

        Ok(ServerStream::new(stream_res.into_inner()))
    }
}

impl crate::RequestBuilder for StreamingRead {
    fn request_options(&mut self) -> &mut crate::RequestOptions {
        &mut self.options
    }
}

/// The request builder for [SpannerImpl::batch_write][crate::client::SpannerImpl::batch_write] calls.
#[derive(Clone, Debug)]
pub struct BatchWrite {
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
    pub fn with_request<V: Into<BatchWriteRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub fn with_options<V: Into<crate::RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub async fn send(self) -> Result<BatchWriteStream> {
        let options =
            google_cloud_gax::options::internal::set_default_idempotency(self.options, false);
        let extensions = {
            let mut e = Extensions::new();
            e.insert(GrpcMethod::new(
                "google.spanner.v1.Spanner",
                "BatchWrite",
            ));
            e
        };
        let path = http::uri::PathAndQuery::from_static("/google.spanner.v1.Spanner/BatchWrite");
        let x_goog_request_params = [Some(&self.request)
            .map(|m| &m.session)
            .map(|s| s.as_str())
            .map(|v| format!("session={v}"))]
        .into_iter()
        .flatten()
        .fold(String::new(), |b, p| b + "&" + &p);

        let ac = gaxi::api_header::XGoogApiClient {
            // we can hardcode the name/version or use env!("CARGO_PKG_NAME") etc
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            library_type: gaxi::api_header::GAPIC,
        };
        let api_client_header = Box::leak(ac.grpc_header_value().into_boxed_str());

        let stream_res = self.grpc_client
            .server_streaming::<crate::google::spanner::v1::BatchWriteRequest, crate::google::spanner::v1::BatchWriteResponse>(
                extensions,
                path,
                self.request.to_proto().map_err(Error::deser)?,
                options,
                api_client_header,
                &x_goog_request_params,
            )
            .await?;

        Ok(BatchWriteStream::new(stream_res.into_inner()))
    }
}

impl crate::RequestBuilder for BatchWrite {
    fn request_options(&mut self) -> &mut crate::RequestOptions {
        &mut self.options
    }
}
