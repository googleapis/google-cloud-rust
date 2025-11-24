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

use auth::credentials::{CacheableResource, Credentials, EntityTag};
use tokio::sync::watch;
use tokio::time::{Duration, sleep};
use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use tonic::{Request, Status};

const REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const ERROR_RETRY_DELAY: Duration = Duration::from_secs(10);

/// A [tonic] interceptor that injects Google Cloud authentication headers into
/// every gRPC request.
///
/// This is a special-purpose interceptor for the Cloud Telemetry API. We chose
/// to implement this as a `tonic::service::Interceptor` specifically for use
/// with the `opentelemetry-otlp` crate:
///
/// 1.  **Ease of Integration:** The `opentelemetry-otlp` crate's builder
///     hardcodes the underlying `Channel` type, making it difficult to inject
///     generic middleware. It does, however, provide a simple way to add a
///     `tonic` interceptor.
/// 3.  **Performance:** The background task pre-converts the `HeaderMap` into
///     a `tonic::metadata::MetadataMap`. This ensures the critical path in the
///     interceptor's `call` method is fast, as it only needs to clone the
///     pre-converted metadata.
///
/// [tonic]: https://docs.rs/tonic/latest/tonic/service/trait.Interceptor.html
#[derive(Clone)]
pub struct CloudTelemetryAuthInterceptor {
    // Our plan of record is to migrate to Google's gRPC at some point. However,
    // this is for integration with `opentelemetry-otlp` which uses Tonic and may
    // never migrate.
    rx: watch::Receiver<Option<MetadataMap>>,
}

impl CloudTelemetryAuthInterceptor {
    /// Creates a new `CloudTelemetryAuthInterceptor` and starts a background task to keep
    /// credentials refreshed.
    pub async fn new(credentials: Credentials) -> Self {
        let (tx, mut rx) = watch::channel(None);
        tokio::spawn(refresh_task(credentials, tx));

        // Wait for the first refresh to complete.
        // We ignore the result because if the sender is dropped (unlikely),
        // the interceptor will just fail requests, which is the correct behavior.
        let _ = rx.changed().await;

        Self { rx }
    }
}

impl Interceptor for CloudTelemetryAuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        // Read the latest headers from the watch channel.
        let rx_ref = self.rx.borrow();
        let metadata = rx_ref.as_ref().ok_or_else(|| {
            // If the first refresh hasn't completed yet, fail the request.
            // The OTLP exporter is expected to handle this transient failure
            // with its built-in retry mechanism.
            Status::unauthenticated("GCP credentials not yet available")
        })?;

        for entry in metadata.iter() {
            match entry {
                tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                    request.metadata_mut().insert(key.clone(), value.clone());
                }
                tonic::metadata::KeyAndValueRef::Binary(key, value) => {
                    request
                        .metadata_mut()
                        .insert_bin(key.clone(), value.clone());
                }
            }
        }
        Ok(request)
    }
}

/// Background task that periodically refreshes credentials and sends them
/// to the interceptor via a watch channel.
async fn refresh_task(credentials: Credentials, tx: watch::Sender<Option<MetadataMap>>) {
    let mut last_etag: Option<EntityTag> = None;
    loop {
        let mut extensions = http::Extensions::new();
        if let Some(etag) = last_etag.clone() {
            extensions.insert(etag);
        }

        match credentials.headers(extensions).await {
            Ok(CacheableResource::New { entity_tag, data }) => {
                let mut metadata = MetadataMap::new();
                for (name, value) in data.iter() {
                    if let (Ok(key), Ok(val)) = (
                        tonic::metadata::MetadataKey::from_bytes(name.as_str().as_bytes()),
                        tonic::metadata::MetadataValue::try_from(value.as_bytes()),
                    ) {
                        metadata.insert(key, val);
                    } else {
                        // Skip invalid headers. This can happen if the header name or value
                        // contains characters that are not allowed in HTTP/2 metadata
                        // (e.g., non-ASCII characters in values without -bin suffix).
                    }
                }

                if tx.send(Some(metadata)).is_err() {
                    // Receiver dropped (all interceptors are gone), stop task.
                    break;
                }
                last_etag = Some(entity_tag);
                sleep(REFRESH_INTERVAL).await;
            }
            Ok(CacheableResource::NotModified) => {
                sleep(REFRESH_INTERVAL).await;
            }
            Err(e) => {
                tracing::warn!("Failed to refresh GCP credentials: {e:?}");
                sleep(ERROR_RETRY_DELAY).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::credentials::{CredentialsProvider, EntityTag};
    use auth::errors::CredentialsError;
    use http::{Extensions, HeaderMap, HeaderValue};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn test_interceptor_injects_headers() {
        let (tx, rx) = watch::channel(None);
        // Manually construct because new() spawns a task we don't want here,
        // or we could just use new() and let it spawn.
        // But since we want to control the channel, we construct manually as before.
        // Wait, the previous test code manually constructed it:
        // let mut interceptor = CloudTelemetryAuthInterceptor { rx };
        // So we don't need to change this test if it doesn't use new().
        let mut interceptor = CloudTelemetryAuthInterceptor { rx };

        // 1. Initial state (no headers)
        let req = Request::new(());
        let res = interceptor.call(req);
        assert!(matches!(
            res,
            Err(status) if status.code() == tonic::Code::Unauthenticated
        ));

        // 2. Send headers
        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", "Bearer test-token".parse().unwrap());
        metadata.insert("x-goog-user-project", "test-project".parse().unwrap());
        tx.send(Some(metadata)).unwrap();

        // 3. Verify injection
        let req = Request::new(());
        let res = interceptor.call(req).unwrap();
        let metadata = res.metadata();
        assert_eq!(metadata.get("authorization").unwrap(), "Bearer test-token");
        assert_eq!(metadata.get("x-goog-user-project").unwrap(), "test-project");
    }

    #[derive(Debug, Clone)]
    /// A controllable mock `CredentialsProvider` for testing the refresh loop.
    ///
    /// It allows setting the internal state to simulate various scenarios like
    /// successful token refresh, unchanged tokens (NotModified), and transient errors.
    struct MockProvider {
        state: Arc<Mutex<MockState>>,
    }

    #[derive(Debug)]
    enum MockState {
        /// Returns new headers and a new entity tag.
        New(HeaderMap, EntityTag),
        /// Returns `NotModified` if the provided ETag matches, otherwise returns an error.
        NotModified(EntityTag),
        /// Returns an error, with a boolean indicating if it's retryable (transient).
        Error(bool),
    }

    impl MockProvider {
        fn new(initial_state: MockState) -> Self {
            Self {
                state: Arc::new(Mutex::new(initial_state)),
            }
        }

        fn set_state(&self, new_state: MockState) {
            *self.state.lock().unwrap() = new_state;
        }
    }

    impl CredentialsProvider for MockProvider {
        async fn headers(
            &self,
            extensions: Extensions,
        ) -> std::result::Result<CacheableResource<HeaderMap>, CredentialsError> {
            let guard = self.state.lock().unwrap();
            match &*guard {
                MockState::New(headers, etag) => Ok(CacheableResource::New {
                    entity_tag: etag.clone(),
                    data: headers.clone(),
                }),
                MockState::NotModified(expected_etag) => {
                    if let Some(etag) = extensions.get::<EntityTag>() {
                        if etag == expected_etag {
                            return Ok(CacheableResource::NotModified);
                        }
                    }
                    // Fallback if etag doesn't match or is missing
                    Err(CredentialsError::from_msg(false, "etag mismatch"))
                }
                MockState::Error(retryable) => {
                    Err(CredentialsError::from_msg(*retryable, "mock error"))
                }
            }
        }

        async fn universe_domain(&self) -> Option<String> {
            None
        }
    }

    #[tokio::test]
    /// Verifies that the refresh task successfully fetches initial credentials
    /// and pushes them to the watch channel.
    async fn test_refresh_task_basic_flow() {
        tokio::time::pause();
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", HeaderValue::from_static("Bearer token1"));
        let etag = EntityTag::new();
        let mock = MockProvider::new(MockState::New(headers.clone(), etag));
        let credentials = Credentials::from(mock);
        let (tx, mut rx) = watch::channel(None);

        tokio::spawn(refresh_task(credentials, tx));

        // Wait for the first update
        rx.changed().await.unwrap();
        let received = rx.borrow().clone().unwrap();
        assert_eq!(received.get("authorization").unwrap(), "Bearer token1");
    }

    #[tokio::test]
    /// Verifies that the refresh task correctly handles `CacheableResource::NotModified`
    /// by sleeping and not pushing redundant updates to the channel.
    async fn test_refresh_task_handles_not_modified() {
        tokio::time::pause();
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", HeaderValue::from_static("Bearer token1"));
        let etag = EntityTag::new();
        let mock = MockProvider::new(MockState::New(headers.clone(), etag.clone()));
        let credentials = Credentials::from(mock.clone());
        let (tx, mut rx) = watch::channel(None);

        tokio::spawn(refresh_task(credentials, tx));

        // First update
        rx.changed().await.unwrap();
        let received = rx.borrow().clone().unwrap();
        assert_eq!(received.get("authorization").unwrap(), "Bearer token1");

        // Switch to NotModified
        mock.set_state(MockState::NotModified(etag));

        // Advance time to trigger refresh
        tokio::time::advance(REFRESH_INTERVAL).await;

        // Ensure no new change is pushed (rx.changed() would hang if we waited,
        // so we check the value remains the same and no error occurred in task)
        assert!(!rx.has_changed().unwrap_or(false));
        let received = rx.borrow().clone().unwrap();
        assert_eq!(received.get("authorization").unwrap(), "Bearer token1");
    }

    #[tokio::test]
    /// Verifies that the refresh task continues running and retries after encountering
    /// a transient error.
    async fn test_refresh_task_retries_on_error() {
        tokio::time::pause();
        let mock = MockProvider::new(MockState::Error(true));
        let credentials = Credentials::from(mock.clone());
        let (tx, mut rx) = watch::channel(None);

        tokio::spawn(refresh_task(credentials, tx));

        // Should be no value initially
        assert!(rx.borrow().is_none());

        // Advance time past error retry
        tokio::time::advance(ERROR_RETRY_DELAY).await;

        // Still no value
        assert!(rx.borrow().is_none());

        // Switch to success
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", HeaderValue::from_static("Bearer token2"));
        mock.set_state(MockState::New(headers, EntityTag::new()));

        // Advance time again
        tokio::time::advance(ERROR_RETRY_DELAY).await;

        // Should receive update
        rx.changed().await.unwrap();
        let received = rx.borrow().clone().unwrap();
        assert_eq!(received.get("authorization").unwrap(), "Bearer token2");
    }

    #[tokio::test]
    /// Verifies that the refresh task terminates gracefully when the receiver
    /// side of the watch channel is dropped.
    async fn test_refresh_task_exits_when_receiver_dropped() {
        tokio::time::pause();
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", HeaderValue::from_static("Bearer token1"));
        let mock = MockProvider::new(MockState::New(headers, EntityTag::new()));
        let credentials = Credentials::from(mock);
        let (tx, rx) = watch::channel(None);

        let handle = tokio::spawn(refresh_task(credentials, tx));

        // Wait for first update to ensure task is running
        // We need to keep rx alive until here
        {
            let mut rx = rx;
            rx.changed().await.unwrap();
        } // rx dropped here

        // Advance time to trigger next refresh loop iteration
        tokio::time::advance(REFRESH_INTERVAL).await;

        // Task should finish
        assert!(handle.await.is_ok());
    }
}
