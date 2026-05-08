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

use futures::future::BoxFuture;
use http::{HeaderValue, Request, Response, StatusCode};
use spanner_grpc_mock::to_uri;
use std::convert::Infallible;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::task::JoinHandle;
use tonic::Status;
use tonic::body::Body;
use tonic::server::NamedService;
use tonic::transport::Channel;
use tonic::transport::Server;
use tower::{Service, ServiceExt};

/// The result of an interception operation.
///
/// It allows the interceptor to either let the request `Continue` to the emulator,
/// or `Complete` the request immediately with a mock response.
#[allow(dead_code)]
pub(crate) enum InterceptionResult {
    Continue(Request<Body>),
    Complete(Response<Body>),
}

/// A generic pass-through proxy for the Spanner gRPC service.
///
/// It forwards all requests to the emulator channel by default, but allows
/// an interceptor closure to inspect and potentially handle specific requests.
#[derive(Clone)]
pub(crate) struct PassThroughProxy<F> {
    emulator_channel: Channel,
    interceptor: F,
}

impl<F> PassThroughProxy<F> {
    pub(crate) fn new(emulator_channel: Channel, interceptor: F) -> Self {
        Self {
            emulator_channel,
            interceptor,
        }
    }
}

/// Implement the Tower `Service` trait for `PassThroughProxy`.
///
/// This allows the proxy to be used as a gRPC service by Tonic.
impl<F> Service<Request<Body>> for PassThroughProxy<F>
where
    F: Fn(Request<Body>) -> BoxFuture<'static, InterceptionResult> + Clone + Send + Sync + 'static,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    // We always return `Ready` because we don't have any backpressure logic
    // of our own, and we use `oneshot` in `call` which handles waiting for the
    // inner channel to be ready.
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let channel = self.emulator_channel.clone();
        let interceptor = self.interceptor.clone();

        Box::pin(async move {
            let req = match interceptor(req).await {
                InterceptionResult::Continue(r) => r,
                InterceptionResult::Complete(resp) => return Ok(resp),
            };

            let response = match channel.oneshot(req).await {
                Ok(resp) => resp,
                // gRPC errors from the emulator are returned as successful HTTP responses
                // with `grpc-status` headers and are handled by the `Ok` branch above.
                // This `Err` branch only handles transport-level errors. We must convert
                // them to a valid gRPC response because Tonic requires `Error = Infallible`.
                Err(e) => {
                    let status = Status::from_error(Box::new(e));
                    let mut resp = Response::new(Body::empty());
                    *resp.status_mut() = StatusCode::OK;
                    resp.headers_mut().insert(
                        "grpc-status",
                        HeaderValue::from_str(&format!("{}", status.code() as i32)).unwrap(),
                    );
                    resp.headers_mut().insert(
                        "grpc-message",
                        HeaderValue::from_str(status.message()).unwrap(),
                    );
                    resp
                }
            };
            Ok(response)
        })
    }
}

// Tonic's `add_service` requires the service to implement `NamedService`
// so it knows the gRPC service name for routing.
impl<F> NamedService for PassThroughProxy<F>
where
    F: Send + Sync + 'static,
{
    const NAME: &'static str = "google.spanner.v1.Spanner";
}

/// Starts a proxy server and returns the URI and a join handle.
pub(crate) async fn start_proxy_server<F>(
    address: &str,
    proxy: PassThroughProxy<F>,
) -> anyhow::Result<(String, JoinHandle<()>)>
where
    F: Fn(Request<Body>) -> BoxFuture<'static, InterceptionResult> + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind(address).await?;
    let addr = listener.local_addr()?;

    let server = spawn(async {
        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let _ = Server::builder()
            .add_service(proxy)
            .serve_with_incoming(stream)
            .await;
    });

    Ok((to_uri(addr), server))
}
