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

#[cfg(all(test, feature = "_internal-grpc-client"))]
mod tests {
    use gax::options::*;
    use gax::retry_policy::NeverRetry;
    use google_cloud_auth::credentials::{
        Credentials, anonymous::Builder as Anonymous, testing::error_credentials,
    };
    use google_cloud_gax_internal::grpc;
    use grpc_server::google::test::v1::{EchoRequest, EchoResponse};
    use grpc_server::{builder, start_echo_server};

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    #[tokio::test]
    async fn basic_stream() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;
        let response = send_streaming_request(client.clone(), rx, "resource=test").await?;
        let (_metadata, mut stream, _) = response.into_parts();
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg0"), "{r:?}");
        tx.send(simple_request("msg1")).await?;
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg1"), "{r:?}");
        tx.send(simple_request("msg2")).await?;
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg2"), "{r:?}");

        drop(tx);
        let r = stream.message().await;
        assert!(matches!(r, Ok(None)), "{r:?}");

        Ok(())
    }

    #[tokio::test]
    async fn ends_with_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;
        let response = send_streaming_request(client.clone(), rx, "resource=test").await?;
        let (_metadata, mut stream, _) = response.into_parts();
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg0"), "{r:?}");
        tx.send(simple_request("")).await?;
        let r = stream.message().await;
        assert!(
            matches!(r, Err(ref e) if e.code() == tonic::Code::InvalidArgument),
            "{r:?}"
        );

        drop(tx);
        let r = stream.message().await;
        assert!(matches!(r, Ok(None)), "{r:?}");

        Ok(())
    }

    #[tokio::test]
    async fn request_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let (_tx, rx) = tokio::sync::mpsc::channel(100);
        let response = send_streaming_request(client.clone(), rx, "resource=error").await;
        let err = response.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn request_error_status() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let (_tx, rx) = tokio::sync::mpsc::channel(100);
        let response =
            send_streaming_request_with_status(client.clone(), rx, "resource=error").await?;
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Aborted, "{status:?}");

        Ok(())
    }

    #[tokio::test]
    async fn credentials_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let client = builder(endpoint)
            .with_credentials(error_credentials(false))
            .build()
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;
        let response = send_streaming_request(client.clone(), rx, "").await;
        let err = response.unwrap_err();
        assert!(err.is_authentication(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn connection_error() -> anyhow::Result<()> {
        let client = builder("http://127.0.0.1:1")
            .with_credentials(test_credentials())
            .build()
            .await;
        let client = client.expect("clients should use lazy connections");
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;
        let response = send_streaming_request(client.clone(), rx, "").await;
        let err = response.unwrap_err();
        assert!(err.is_connect(), "{err:?}");
        Ok(())
    }

    fn simple_request(msg: &str) -> EchoRequest {
        EchoRequest {
            message: msg.into(),
            ..Default::default()
        }
    }

    async fn send_streaming_request(
        client: grpc::Client,
        rx: tokio::sync::mpsc::Receiver<EchoRequest>,
        request_params: &str,
    ) -> gax::Result<tonic::Response<tonic::codec::Streaming<EchoResponse>>> {
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Chat",
            ));
            e
        };
        let request_options = {
            let mut o = RequestOptions::default();
            o.set_retry_policy(NeverRetry);
            o
        };
        client
            .bidi_stream::<EchoRequest, EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Chat"),
                tokio_stream::wrappers::ReceiverStream::new(rx),
                request_options,
                "test-only-api-client/1.0",
                request_params,
            )
            .await
    }

    async fn send_streaming_request_with_status(
        client: grpc::Client,
        rx: tokio::sync::mpsc::Receiver<EchoRequest>,
        request_params: &str,
    ) -> gax::Result<tonic::Result<tonic::Response<tonic::codec::Streaming<EchoResponse>>>> {
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Chat",
            ));
            e
        };
        let request_options = {
            let mut o = RequestOptions::default();
            o.set_retry_policy(NeverRetry);
            o
        };
        client
            .bidi_stream_with_status::<EchoRequest, EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Chat"),
                tokio_stream::wrappers::ReceiverStream::new(rx),
                request_options,
                "test-only-api-client/1.0",
                request_params,
            )
            .await
    }
}
