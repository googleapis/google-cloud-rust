// Copyright 2024 Google LLC
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
    use google_cloud_auth::credentials::{
        Credentials, anonymous::Builder as Anonymous, testing::error_credentials,
    };
    use google_cloud_gax::error::Error;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_gax_internal::attempt_interceptor::AttemptInterceptor;
    use google_cloud_gax_internal::grpc;
    use google_cloud_gax_internal::options::ClientConfig;
    use grpc_server::transport::{EchoTransport, transport_builder};
    use grpc_server::{
        both_transports, builder, google, start_echo_server, start_echo_server_with_address,
        start_echo_server_with_mtls, start_echo_server_with_tls,
    };
    use http::{HeaderMap, HeaderName, HeaderValue};
    use pretty_assertions::assert_eq;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    const TEST_CA_PEM: &[u8] = include_bytes!("../../../testdata/tls/ca_cert.pem");
    const TEST_SERVER_CERT_PEM: &[u8] = include_bytes!("../../../testdata/tls/server_cert.pem");
    const TEST_SERVER_KEY_PEM: &[u8] = include_bytes!("../../../testdata/tls/server_key.pem");

    fn init_test_crypto_provider() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    both_transports!(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        default_endpoint
    );
    async fn default_endpoint<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Act & Assert
        check_simple_request(client).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_user_agent() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Act
        let response = send_request(client, "test message", "").await?;

        // Assert
        let got_user_agent = response
            .metadata
            .get("user-agent")
            .expect("user-agent header present");
        assert!(got_user_agent.contains("tonic/"), "{got_user_agent:?}");
        Ok(())
    }

    fn ipv6_loopback_available() -> bool {
        let Ok(listener) = std::net::TcpListener::bind("[::1]:0") else {
            return false;
        };
        let Ok(addr) = listener.local_addr() else {
            return false;
        };
        std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(100)).is_ok()
    }

    both_transports!(non_default_endpoint_ipv6);
    async fn non_default_endpoint_ipv6<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        if !ipv6_loopback_available() {
            eprintln!(
                "Skipping non_default_endpoint_ipv6: IPv6 loopback is not available on this host/container"
            );
            return Ok(());
        }
        let (endpoint, _server) = start_echo_server_with_address("[::]:0").await?;
        let client = transport_builder::<C>("https://storage.googleapis.com")
            .with_endpoint(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Act & Assert
        check_simple_request(client).await
    }

    both_transports!(non_default_endpoint_ipv4);
    async fn non_default_endpoint_ipv4<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server_with_address("0.0.0.0:0").await?;
        let client = transport_builder::<C>("https://storage.googleapis.com")
            .with_endpoint(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Act & Assert
        check_simple_request(client).await
    }

    both_transports!(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        no_request_params
    );
    async fn no_request_params<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Act
        let response = send_request(client, "test message", "").await?;

        // Assert
        assert_eq!(&response.message, "test message");
        assert_eq!(
            response
                .metadata
                .get("x-goog-api-client")
                .map(String::as_str),
            Some("test-only-api-client/1.0")
        );
        assert_eq!(response.metadata.get("x-goog-request-params"), None);
        Ok(())
    }

    both_transports!(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        override_endpoint
    );
    async fn override_endpoint<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>("https://test-only.googleapis.com")
            .with_endpoint(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Act & Assert
        check_simple_request(client).await
    }

    #[tokio::test]
    async fn multiple_endpoints() -> anyhow::Result<()> {
        use std::collections::HashSet;
        let (endpoint, _server) = start_echo_server().await?;

        // Use an explicit client config to set the subchannel count field.
        let mut config = ClientConfig::default();
        config.cred = Some(test_credentials());
        config.endpoint = Some(endpoint.clone());
        config.grpc_subchannel_count = Some(16);
        let client = grpc::Client::new(config, "https://test-only.googleapis.com").await?;

        // Make sure we can make at least one request.
        check_simple_request(client.clone()).await?;

        // Make sure not all requests use the same client address.
        let addresses = futures::future::join_all(
            (0..32).map(|_| send_request(client.clone(), "test message", "")),
        )
        .await
        .into_iter()
        .map(|r| r.map(|e| e.client_address))
        .collect::<google_cloud_gax::Result<HashSet<_>>>()?;

        assert!(addresses.len() > 1, "{addresses:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn credentials_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let client = builder(endpoint)
            .with_credentials(error_credentials(false))
            .build()
            .await?;
        let response = send_request(client, "credentials error", "").await;
        let err = response.unwrap_err();
        assert!(err.is_authentication(), "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connection_error() -> anyhow::Result<()> {
        let client = builder("http://127.0.0.1:1")
            .with_credentials(test_credentials())
            .build()
            .await;
        let client = client.expect("clients should use lazy connections");
        let response = send_request(client, "connection error", "").await;
        let err = response.unwrap_err();
        assert!(err.is_connect(), "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn endpoint_error() -> anyhow::Result<()> {
        let client = builder("http:/invalid-invalid")
            .with_credentials(test_credentials())
            .build()
            .await;
        let err = client.unwrap_err();
        assert!(err.is_transport(), "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uds_and_tls() -> anyhow::Result<()> {
        let client = builder("uds://invalid")
            .with_credentials(test_credentials())
            .build()
            .await;
        let client = client.expect("clients should use lazy connections");
        let response = send_request(client, "connection error", "").await;
        let err = response.unwrap_err();
        assert!(err.is_connect(), "{err:?}");
        Ok(())
    }

    both_transports!(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        request_error
    );
    async fn request_error<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Act
        let response = send_request(client, "", "").await;

        // Assert
        let err = response.expect_err("an empty message should be rejected");
        assert_eq!(err.status().map(|s| s.code), Some(Code::InvalidArgument));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn attempt_interceptor() -> anyhow::Result<()> {
        #[derive(Debug)]
        struct TestInterceptor;
        impl AttemptInterceptor for TestInterceptor {
            fn intercept(&self, headers: &mut HeaderMap, attempt: u32) {
                headers.insert(
                    HeaderName::from_static("x-test-attempt"),
                    HeaderValue::from_str(&attempt.to_string()).expect("valid attempt number"),
                );
            }
        }

        let (endpoint, _server) = start_echo_server().await?;

        let mut config = ClientConfig::default();
        config.cred = Some(test_credentials());
        config.endpoint = Some(endpoint);

        let mut client = grpc::Client::new(config, "https://test-only.googleapis.com").await?;
        client.set_attempt_interceptor(Arc::new(TestInterceptor));

        let response = send_request(client, "test message", "").await?;
        assert_eq!(&response.message, "test message");

        assert_eq!(
            response.metadata.get("x-test-attempt").map(String::as_str),
            Some("1")
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interceptor_lifecycle_hooks() -> anyhow::Result<()> {
        #[derive(Debug, Default)]
        struct CompletionRecord {
            method: String,
            attempt: u32,
            measured_start_time: bool,
            has_response_headers: bool,
            is_success: bool,
        }

        #[derive(Debug, Default)]
        struct LifecycleTracker {
            started: Mutex<Option<(String, u32)>>,
            completed: Mutex<Option<CompletionRecord>>,
        }

        impl AttemptInterceptor for LifecycleTracker {
            fn on_attempt_start(
                &self,
                method: &str,
                attempt: u32,
                headers: &mut HeaderMap,
                _options: &RequestOptions,
            ) -> Instant {
                *self.started.lock().expect("lock failed") = Some((method.to_string(), attempt));
                headers.insert(
                    HeaderName::from_static("x-lifecycle-test"),
                    HeaderValue::from_static("present"),
                );
                Instant::now()
            }

            fn on_attempt_complete(
                &self,
                method: &str,
                attempt: u32,
                start_time: Instant,
                response_headers: Option<&HeaderMap>,
                error: Option<&Error>,
                _options: &RequestOptions,
            ) {
                *self.completed.lock().expect("lock failed") = Some(CompletionRecord {
                    method: method.to_string(),
                    attempt,
                    measured_start_time: start_time.elapsed().as_nanos() > 0,
                    has_response_headers: response_headers.is_some(),
                    is_success: error.is_none(),
                });
            }
        }

        let tracker = Arc::new(LifecycleTracker::default());
        let (endpoint, _server) = start_echo_server().await?;

        let mut config = ClientConfig::default();
        config.cred = Some(test_credentials());
        config.endpoint = Some(endpoint);

        let mut client = grpc::Client::new(config, "https://test-only.googleapis.com").await?;
        client.set_attempt_interceptor(tracker.clone());

        let response = send_request(client, "lifecycle test", "").await?;
        assert_eq!(&response.message, "lifecycle test");
        assert_eq!(
            response
                .metadata
                .get("x-lifecycle-test")
                .map(String::as_str),
            Some("present")
        );

        let started = tracker.started.lock().expect("lock failed").take();
        assert_eq!(
            started,
            Some(("/google.test.v1.EchoService/Echo".to_string(), 1))
        );

        let completed = tracker
            .completed
            .lock()
            .expect("lock failed")
            .take()
            .expect("completed record present");
        assert_eq!(completed.method, "/google.test.v1.EchoService/Echo");
        assert_eq!(completed.attempt, 1);
        assert!(completed.measured_start_time);
        assert!(completed.has_response_headers);
        assert!(completed.is_success);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interceptor_lifecycle_hooks_on_error() -> anyhow::Result<()> {
        #[derive(Debug, Default)]
        struct CompletionRecord {
            method: String,
            attempt: u32,
            measured_start_time: bool,
            is_success: bool,
            status_code: Option<Code>,
        }

        #[derive(Debug, Default)]
        struct LifecycleTracker {
            started: Mutex<Option<(String, u32)>>,
            completed: Mutex<Option<CompletionRecord>>,
        }

        impl AttemptInterceptor for LifecycleTracker {
            fn on_attempt_start(
                &self,
                method: &str,
                attempt: u32,
                headers: &mut HeaderMap,
                _options: &RequestOptions,
            ) -> Instant {
                *self.started.lock().expect("lock failed") = Some((method.to_string(), attempt));
                headers.insert(
                    HeaderName::from_static("x-lifecycle-test"),
                    HeaderValue::from_static("present"),
                );
                Instant::now()
            }

            fn on_attempt_complete(
                &self,
                method: &str,
                attempt: u32,
                start_time: Instant,
                _response_headers: Option<&HeaderMap>,
                error: Option<&Error>,
                _options: &RequestOptions,
            ) {
                *self.completed.lock().expect("lock failed") = Some(CompletionRecord {
                    method: method.to_string(),
                    attempt,
                    measured_start_time: start_time.elapsed().as_nanos() > 0,
                    is_success: error.is_none(),
                    status_code: error.and_then(|e| e.status().map(|s| s.code)),
                });
            }
        }

        let tracker = Arc::new(LifecycleTracker::default());
        let (endpoint, _server) = start_echo_server().await?;

        let mut config = ClientConfig::default();
        config.cred = Some(test_credentials());
        config.endpoint = Some(endpoint);

        let mut client = grpc::Client::new(config, "https://test-only.googleapis.com").await?;
        client.set_attempt_interceptor(tracker.clone());

        // An empty message causes the Echo server to return InvalidArgument error
        let response = send_request(client, "", "").await;
        assert!(response.is_err());

        let started = tracker.started.lock().expect("lock failed").take();
        assert_eq!(
            started,
            Some(("/google.test.v1.EchoService/Echo".to_string(), 1))
        );

        let completed = tracker
            .completed
            .lock()
            .expect("lock failed")
            .take()
            .expect("completed record present");
        assert_eq!(completed.method, "/google.test.v1.EchoService/Echo");
        assert_eq!(completed.attempt, 1);
        assert!(completed.measured_start_time);
        assert!(!completed.is_success);
        assert_eq!(completed.status_code, Some(Code::InvalidArgument));

        Ok(())
    }

    async fn send_request<C: EchoTransport>(
        client: C,
        msg: &str,
        request_params: &str,
    ) -> google_cloud_gax::Result<google::test::v1::EchoResponse> {
        let request = google::test::v1::EchoRequest {
            message: msg.into(),
            ..google::test::v1::EchoRequest::default()
        };
        let request_options = {
            let mut o = RequestOptions::default();
            o.set_retry_policy(NeverRetry);
            o
        };
        client
            .echo(request, request_options, request_params)
            .await
            .map(tonic::Response::into_inner)
    }

    async fn check_simple_request<C: EchoTransport>(client: C) -> anyhow::Result<()> {
        let response = send_request(client, "test message", "name=test-only").await?;
        assert_eq!(&response.message, "test message");
        assert_eq!(
            response
                .metadata
                .get("x-goog-api-client")
                .map(String::as_str),
            Some("test-only-api-client/1.0")
        );
        assert_eq!(
            response
                .metadata
                .get("x-goog-request-params")
                .map(String::as_str),
            Some("name=test-only")
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tls_custom_root_certificate_succeeds() -> anyhow::Result<()> {
        init_test_crypto_provider();
        let server_identity = Identity::from_pem(TEST_SERVER_CERT_PEM, TEST_SERVER_KEY_PEM);
        let (endpoint, _server) = start_echo_server_with_tls(server_identity).await?;

        let root_certificate = Certificate::from_pem(TEST_CA_PEM);
        let client_tls = ClientTlsConfig::new()
            .ca_certificate(root_certificate)
            .domain_name("localhost");

        let client = builder(&endpoint)
            .with_credentials(test_credentials())
            .with_extension(client_tls)
            .build()
            .await?;

        check_simple_request(client).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tls_untrusted_server_certificate_fails() -> anyhow::Result<()> {
        init_test_crypto_provider();
        let server_identity = Identity::from_pem(TEST_SERVER_CERT_PEM, TEST_SERVER_KEY_PEM);
        let (endpoint, _server) = start_echo_server_with_tls(server_identity).await?;

        // Client without custom root certificate connecting to self-signed TLS server
        let client = builder(&endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let response = send_request(client, "test message", "").await;
        assert!(
            response.is_err(),
            "expected TLS handshake failure with untrusted self-signed certificate"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mtls_with_client_identity_succeeds() -> anyhow::Result<()> {
        init_test_crypto_provider();
        let server_identity = Identity::from_pem(TEST_SERVER_CERT_PEM, TEST_SERVER_KEY_PEM);
        let client_ca_root = Certificate::from_pem(TEST_CA_PEM);
        let (endpoint, _server) =
            start_echo_server_with_mtls(server_identity, client_ca_root).await?;

        let root_certificate = Certificate::from_pem(TEST_CA_PEM);
        let client_identity = Identity::from_pem(TEST_SERVER_CERT_PEM, TEST_SERVER_KEY_PEM);
        let client_tls = ClientTlsConfig::new()
            .ca_certificate(root_certificate)
            .identity(client_identity)
            .domain_name("localhost");

        let client = builder(&endpoint)
            .with_credentials(test_credentials())
            .with_extension(client_tls)
            .build()
            .await?;

        check_simple_request(client).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mtls_without_client_identity_fails() -> anyhow::Result<()> {
        init_test_crypto_provider();
        let server_identity = Identity::from_pem(TEST_SERVER_CERT_PEM, TEST_SERVER_KEY_PEM);
        let client_ca_root = Certificate::from_pem(TEST_CA_PEM);
        let (endpoint, _server) =
            start_echo_server_with_mtls(server_identity, client_ca_root).await?;

        // Client trusts server CA, but does not provide client identity required by server
        let root_certificate = Certificate::from_pem(TEST_CA_PEM);
        let client_tls = ClientTlsConfig::new()
            .ca_certificate(root_certificate)
            .domain_name("localhost");

        let client = builder(&endpoint)
            .with_credentials(test_credentials())
            .with_extension(client_tls)
            .build()
            .await?;

        let response = send_request(client, "test message", "").await;
        assert!(
            response.is_err(),
            "expected mTLS handshake failure when server requires client certificate"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tls_domain_name_mismatch_fails() -> anyhow::Result<()> {
        init_test_crypto_provider();
        let server_identity = Identity::from_pem(TEST_SERVER_CERT_PEM, TEST_SERVER_KEY_PEM);
        let (endpoint, _server) = start_echo_server_with_tls(server_identity).await?;

        // Server certificate SAN is "localhost", client verifies against "unmatched-host.example.com"
        let root_certificate = Certificate::from_pem(TEST_CA_PEM);
        let client_tls = ClientTlsConfig::new()
            .ca_certificate(root_certificate)
            .domain_name("unmatched-host.example.com");

        let client = builder(&endpoint)
            .with_credentials(test_credentials())
            .with_extension(client_tls)
            .build()
            .await?;

        let response = send_request(client, "test message", "").await;
        assert!(
            response.is_err(),
            "expected TLS handshake failure when server certificate SAN does not match domain_name"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tls_with_multiple_subchannels_succeeds() -> anyhow::Result<()> {
        use std::collections::HashSet;

        init_test_crypto_provider();
        let server_identity = Identity::from_pem(TEST_SERVER_CERT_PEM, TEST_SERVER_KEY_PEM);
        let (endpoint, _server) = start_echo_server_with_tls(server_identity).await?;

        let root_certificate = Certificate::from_pem(TEST_CA_PEM);
        let client_tls = ClientTlsConfig::new()
            .ca_certificate(root_certificate)
            .domain_name("localhost");

        let mut client_config = ClientConfig::default();
        client_config.grpc_subchannel_count = Some(16);
        client_config.extensions.insert(client_tls);
        client_config.cred = Some(test_credentials());

        let client = grpc::Client::new(client_config, &endpoint).await?;

        // Make sure we can make at least one request.
        check_simple_request(client.clone()).await?;

        // Verify that multiple requests use different client connection addresses across the subchannel pool.
        let addresses = futures::future::join_all(
            (0..32).map(|_| send_request(client.clone(), "test message", "")),
        )
        .await
        .into_iter()
        .map(|r| r.map(|e| e.client_address))
        .collect::<google_cloud_gax::Result<HashSet<_>>>()?;

        assert!(addresses.len() > 1, "{addresses:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_client_header_client_extension_wire() -> anyhow::Result<()> {
        check_api_client_header_client_extension_wire::<grpc::Client>("tonic").await
    }

    #[cfg(google_cloud_unstable_grpc_rust)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_client_header_client_extension_wire_grpc_rust() -> anyhow::Result<()> {
        check_api_client_header_client_extension_wire::<grpc_server::transport::GrpcRustClient>(
            "grpc-rust",
        )
        .await
    }

    async fn check_api_client_header_client_extension_wire<C: EchoTransport>(
        expected_transport: &str,
    ) -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .with_extension(google_cloud_gax_internal::api_header::XGoogApiClient {
                name: "storage",
                library_type: google_cloud_gax_internal::api_header::GCCL,
                version: "1.0.0",
            })
            .build()
            .await?;

        // Act
        let response = send_request(client, "test message", "").await?;

        // Assert
        let header = response
            .metadata
            .get("x-goog-api-client")
            .expect("header present");
        let want_grpc = format!("grpc/{}-{expected_transport}", env!("CARGO_PKG_VERSION"));
        assert!(header.contains("gccl/1.0.0"), "{header}");
        assert!(header.contains(&want_grpc), "{header}");
        assert!(!header.contains("gapic/"), "{header}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn large_response() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        // Tonic's default decoding limit is 4 MiB; verify messages > 4 MiB succeed.
        let large_msg = "a".repeat(5 * 1024 * 1024);
        let response = send_request(client, &large_msg, "").await?;
        assert_eq!(response.message.len(), large_msg.len());
        Ok(())
    }
}
