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

use google::test::v1::{EchoRequest, EchoResponse};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tonic::metadata::MetadataMap;

type EchoResult = tonic::Result<tonic::Response<EchoResponse>>;

pub mod google {
    pub mod test {
        pub mod v1 {
            include!("generated/protos/google.test.v1.rs");
        }
    }
}

pub async fn start_echo_server() -> anyhow::Result<(String, JoinHandle<()>)> {
    start_echo_server_with_address("127.0.0.1:0").await
}

pub async fn start_echo_server_with_address(
    address: &str,
) -> anyhow::Result<(String, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    let addr = listener.local_addr()?;

    let server = tokio::spawn(async {
        let echo = Echo;
        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let _ = tonic::transport::Server::builder()
            .add_service(google::test::v1::echo_service_server::EchoServiceServer::new(echo))
            .serve_with_incoming(stream)
            .await;
    });

    let uri = if addr.is_ipv6() {
        format!("http://[{}]:{}", addr.ip(), addr.port())
    } else {
        format!("http://{}:{}", addr.ip(), addr.port())
    };

    Ok((uri, server))
}

pub async fn start_fixed_responses<I, V>(responses: I) -> anyhow::Result<(String, JoinHandle<()>)>
where
    I: IntoIterator<Item = V>,
    V: Into<EchoResult>,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let echo = FixedResponses::new(responses);
    let server = tokio::spawn(async move {
        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let _ = tonic::transport::Server::builder()
            .add_service(google::test::v1::echo_service_server::EchoServiceServer::new(echo))
            .serve_with_incoming(stream)
            .await;
    });

    Ok((format!("http://{}:{}", addr.ip(), addr.port()), server))
}

pub fn builder(
    default_endpoint: impl Into<String>,
) -> gax::client_builder::ClientBuilder<Factory, auth::credentials::Credentials> {
    gax::client_builder::internal::new_builder(Factory(default_endpoint.into()))
}

pub struct Factory(String);
impl gax::client_builder::internal::ClientFactory for Factory {
    type Client = gaxi::grpc::Client;
    type Credentials = auth::credentials::Credentials;
    async fn build(
        self,
        config: gaxi::options::ClientConfig,
    ) -> gax::client_builder::Result<Self::Client> {
        Self::Client::new(config, &self.0).await
    }
}

#[derive(Debug)]
struct Echo;

#[async_trait::async_trait]
impl google::test::v1::echo_service_server::EchoService for Echo {
    async fn echo(
        &self,
        request: tonic::Request<EchoRequest>,
    ) -> tonic::Result<tonic::Response<EchoResponse>> {
        let (metadata, _, request) = request.into_parts();

        if request.message.is_empty() {
            return Err(tonic::Status::with_metadata(
                tonic::Code::InvalidArgument,
                "empty message",
                metadata,
            ));
        }

        if let Some(delay) = request.delay_ms.map(tokio::time::Duration::from_millis) {
            tokio::time::sleep(delay).await;
        }

        let response = google::test::v1::EchoResponse {
            message: request.message,
            metadata: headers(metadata),
        };

        Ok(tonic::Response::new(response))
    }

    type ChatStream = tokio_stream::wrappers::ReceiverStream<tonic::Result<EchoResponse>>;
    async fn chat(
        &self,
        request: tonic::Request<tonic::Streaming<EchoRequest>>,
    ) -> tonic::Result<tonic::Response<Self::ChatStream>> {
        let (metadata, _, request) = request.into_parts();
        if metadata
            .get("x-goog-request-params")
            .is_some_and(|h| h.as_bytes() == b"resource=error")
        {
            return Err(tonic::Status::aborted("test with initial error"));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let _handler = tokio::spawn(run_chat(tx, metadata, request));

        let stream = Self::ChatStream::new(rx);
        Ok(tonic::Response::from(stream))
    }
}

async fn run_chat(
    tx: tokio::sync::mpsc::Sender<tonic::Result<EchoResponse>>,
    metadata: MetadataMap,
    mut request_stream: tonic::Streaming<EchoRequest>,
) {
    let mut headers = headers(metadata);
    while let Ok(Some(request)) = request_stream.message().await {
        if let Some(delay) = request.delay_ms.map(tokio::time::Duration::from_millis) {
            tokio::time::sleep(delay).await;
        }
        if request.message.is_empty() {
            let status = tonic::Status::invalid_argument("empty message");
            let _ = tx.send(Err(status)).await;
            return;
        }

        let response = EchoResponse {
            message: request.message,
            metadata: std::mem::take(&mut headers),
        };
        let _ = tx.send(Ok(response)).await;
    }
}

fn headers(metadata: tonic::metadata::MetadataMap) -> HashMap<String, String> {
    use http::header::{HeaderName, HeaderValue};

    let h_as_str = |h: Option<HeaderName>| {
        h.as_ref()
            .map(HeaderName::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let v_as_str = |v: HeaderValue| v.to_str().ok().unwrap_or("[error]").to_string();
    metadata
        .into_headers()
        .into_iter()
        .map(|(k, v)| (h_as_str(k), v_as_str(v)))
        .collect()
}

#[derive(Debug, Default)]
struct FixedResponses {
    responses: Arc<Mutex<VecDeque<EchoResult>>>,
}

impl FixedResponses {
    pub fn new<I, V>(r: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<EchoResult>,
    {
        let responses: VecDeque<EchoResult> = r.into_iter().map(|v| v.into()).collect();
        Self {
            responses: Arc::new(Mutex::new(responses)),
        }
    }
}

#[tonic::async_trait]
impl google::test::v1::echo_service_server::EchoService for FixedResponses {
    async fn echo(
        &self,
        _: tonic::Request<google::test::v1::EchoRequest>,
    ) -> tonic::Result<tonic::Response<google::test::v1::EchoResponse>, tonic::Status> {
        let mut responses = self.responses.lock().expect("responses are poisoned");
        if let Some(r) = responses.pop_front() {
            return r;
        }
        Err(tonic::Status::failed_precondition("no available responses"))
    }

    type ChatStream = tokio_stream::wrappers::ReceiverStream<tonic::Result<EchoResponse>>;
    async fn chat(
        &self,
        _request: tonic::Request<tonic::Streaming<EchoRequest>>,
    ) -> tonic::Result<tonic::Response<Self::ChatStream>> {
        Err(tonic::Status::internal("not implemented".to_string()))
    }
}
