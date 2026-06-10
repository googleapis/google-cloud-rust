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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use google_cloud_auth::credentials::mds::Builder;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn hyper_incomplete_message() -> Result<()> {
        let endpoint = spawn_incomplete_message_server().await;
        let creds = Builder::default()
            .with_endpoint(endpoint)
            .build_access_token_credentials()?;
        let token = creds.access_token().await;
        let err = token.expect_err("token request should fail");
        assert!(err.is_transient());

        let hyper_err = as_inner::<hyper::Error, _>(&err).expect("should contain a hyper error");
        assert!(hyper_err.is_incomplete_message(), "{hyper_err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn io_connection_reset() -> Result<()> {
        let endpoint = spawn_connection_reset_server().await;
        let creds = Builder::default()
            .with_endpoint(endpoint)
            .build_access_token_credentials()?;
        let token = creds.access_token().await;
        let err = token.expect_err("token request should fail");
        assert!(err.is_transient());

        let io_err = as_inner::<std::io::Error, _>(&err).expect("should contain an io error");
        assert!(
            matches!(io_err.kind(), std::io::ErrorKind::ConnectionReset),
            "{io_err:?}"
        );

        Ok(())
    }

    /// Spawns a background TCP server that gracefully hangs up mid-response.
    ///
    /// The client-side error should be transient, and classified as a
    /// `hyper::Error::Kind::IncompleteMessage`.
    ///
    /// Returns the `host:port` string it bound to.
    async fn spawn_incomplete_message_server() -> String {
        // Bind to port 0 to let the OS pick a free port
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind TCP listener");
        let addr = listener.local_addr().expect("Failed to get local address");

        tokio::spawn(async move {
            // Loop to handle potential retries from the client
            loop {
                if let Ok((mut socket, _)) = listener.accept().await {
                    tokio::spawn(async move {
                        let mut buf = [0; 1024];

                        // Read the incoming HTTP GET request
                        let _ = socket.read(&mut buf).await;

                        // Send an incomplete HTTP status line without \r\n\r\n
                        let _ = socket.write_all(b"HTTP/1.1 200 O").await;
                        let _ = socket.flush().await;

                        // The socket goes out of scope and drops here.
                        // This sends a TCP FIN, cleanly closing the connection
                        // while hyper is still waiting for the rest of the headers.
                    });
                }
            }
        });

        format!("http://{}", addr)
    }

    /// Spawns a background TCP server that abruptly sends a `RST` frame.
    ///
    /// The client-side error should be transient, and classified as a
    /// `std::io::ErrorKind::ConnectionReset`.
    ///
    /// Returns the `host:port` string it bound to.
    async fn spawn_connection_reset_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind TCP listener");
        let addr = listener.local_addr().expect("Failed to get local address");

        tokio::spawn(async move {
            loop {
                if let Ok((mut socket, _)) = listener.accept().await {
                    tokio::spawn(async move {
                        let mut buf = [0; 1024];
                        let _ = socket.read(&mut buf).await;

                        // Force a TCP RST (Connection reset by peer)
                        let _ = socket.set_zero_linger();
                    });
                }
            }
        });

        format!("http://{}", addr)
    }

    /// Extract the first source error of type `T`.
    fn as_inner<T, E>(error: &E) -> Option<&T>
    where
        T: std::error::Error + 'static,
        E: std::error::Error,
    {
        let mut e = error.source()?;
        // Prevent infinite loops due to cycles in the `source()` errors. This seems
        // unlikely, and it would require effort to create, but it is easy to
        // prevent.
        for _ in 0..32 {
            if let Some(value) = e.downcast_ref::<T>() {
                return Some(value);
            }
            e = e.source()?;
        }
        None
    }
}
