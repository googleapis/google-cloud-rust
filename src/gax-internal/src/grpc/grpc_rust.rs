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

use crate::grpc::tonic::{Extensions, Response as TonicResponse, Result as TonicResult};
use google_cloud_gax::Result as GaxResult;
use google_cloud_gax::client_builder::Result as ClientBuilderResult;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_error_policy::PollingErrorPolicy;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct GrpcRustClient {}

impl GrpcRustClient {
    pub async fn new(
        _config: crate::options::ClientConfig,
        _default_endpoint: &str,
    ) -> ClientBuilderResult<Self> {
        unimplemented!("not implemented yet")
    }

    pub async fn new_with_instrumentation(
        _config: crate::options::ClientConfig,
        _default_endpoint: &str,
        _instrumentation: &'static crate::options::InstrumentationClientInfo,
    ) -> ClientBuilderResult<Self> {
        unimplemented!("not implemented yet")
    }

    pub async fn execute<Request, Response>(
        &self,
        _extensions: Extensions,
        _path: http::uri::PathAndQuery,
        _request: Request,
        _options: RequestOptions,
        _api_client_header: &'static str,
        _request_params: &str,
    ) -> GaxResult<TonicResponse<Response>>
    where
        Request: prost::Message + Clone + 'static,
        Response: prost::Message + Default + 'static,
    {
        unimplemented!("not implemented yet")
    }

    pub async fn bidi_stream<Request, Response>(
        &self,
        _extensions: Extensions,
        _path: http::uri::PathAndQuery,
        _request: impl tokio_stream::Stream<Item = Request> + Send + 'static,
        _options: RequestOptions,
        _api_client_header: &'static str,
        _request_params: &str,
    ) -> GaxResult<TonicResponse<GrpcRustStreaming<Response>>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + Default + 'static,
    {
        unimplemented!("not implemented yet")
    }

    pub async fn bidi_stream_with_status<Request, Response>(
        &self,
        _extensions: Extensions,
        _path: http::uri::PathAndQuery,
        _request: impl tokio_stream::Stream<Item = Request> + Send + 'static,
        _options: RequestOptions,
        _api_client_header: &'static str,
        _request_params: &str,
    ) -> GaxResult<TonicResult<TonicResponse<GrpcRustStreaming<Response>>>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + Default + 'static,
    {
        unimplemented!("not implemented yet")
    }

    pub fn get_polling_error_policy(
        &self,
        _options: &RequestOptions,
    ) -> Arc<dyn PollingErrorPolicy> {
        unimplemented!("not implemented yet")
    }

    pub fn get_polling_backoff_policy(
        &self,
        _options: &RequestOptions,
    ) -> Arc<dyn PollingBackoffPolicy> {
        unimplemented!("not implemented yet")
    }
}

#[derive(Debug)]
pub struct GrpcRustStreaming<Response> {
    // TODO(#5991): not implemented yet
    _phantom: std::marker::PhantomData<Response>,
}

impl<Response> GrpcRustStreaming<Response>
where
    Response: prost::Message + Default,
{
    pub async fn message(&mut self) -> TonicResult<Option<Response>> {
        unimplemented!("not implemented yet")
    }
}
