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

//! Proactive background cache subscriber for Spanner location-aware routing.
//!
//! Subscribes to the server-streaming `FetchCacheUpdate` gRPC RPC to continuously receive
//! [`CacheUpdate`][crate::model::CacheUpdate] messages in real time. Incoming updates are applied
//! to the in-memory routing table ([`KeyRangeCache`][crate::routing::key_range_cache::KeyRangeCache])
//! and connection pool ([`ConnectionCache`][crate::routing::connection_cache::ConnectionCache]) via
//! [`CacheUpdater`].
//!
//! When the stream disconnects or encounters transient errors, the subscriber automatically
//! reconnects using configured [`RetryPolicy`] and [`BackoffPolicy`] implementations. Permanent or
//! exhausted errors terminate the subscriber task cleanly.

use crate::RequestOptions;
use crate::client::Spanner;
use crate::google::spanner::v1::CacheUpdate as ProtoCacheUpdate;
use crate::model::FetchCacheUpdateRequest;
use crate::retry_policy::SpannerRetryPolicy;
use crate::routing::cache_updater::CacheUpdater;
use crate::server_streaming::stream::CacheUpdateStream;
use gaxi::prost::FromProto;
use google_cloud_gax::backoff_policy::{BackoffPolicy, BackoffPolicyArg};
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_policy::{RetryPolicy, RetryPolicyArg};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use tracing::{debug, error, warn};

/// Minimum duration a stream connection must remain active before it is considered a healthy
/// connection session that resets retry attempts upon routine disconnection / connection cycling.
const MIN_HEALTHY_STREAM_DURATION: Duration = Duration::from_secs(10);

/// Represents a running proactive background cache subscriber task.
#[derive(Debug)]
pub(crate) struct CacheSubscriber {
    shutdown_sender: watch::Sender<bool>,
    task_handle: Option<JoinHandle<()>>,
}

impl CacheSubscriber {
    /// Creates a [`CacheSubscriberBuilder`] to configure and start a background cache subscriber.
    pub(crate) fn builder(
        database: String,
        spanner: Spanner,
        cache_updater: Arc<CacheUpdater>,
    ) -> CacheSubscriberBuilder {
        CacheSubscriberBuilder::new(database, spanner, cache_updater)
    }

    /// Starts a background subscriber task with default retry and backoff settings.
    pub(crate) fn start(
        database: String,
        spanner: Spanner,
        cache_updater: Arc<CacheUpdater>,
    ) -> Self {
        Self::builder(database, spanner, cache_updater).start()
    }

    /// Requests the background subscriber task to shut down.
    #[allow(dead_code)] // Used in tests and lifecycle shutdown
    pub(crate) fn stop(&self) {
        let _ = self.shutdown_sender.send(true);
    }

    /// Returns `true` if the background subscriber task has finished executing.
    #[allow(dead_code)] // Used for lifecycle monitoring and tests
    pub(crate) fn is_finished(&self) -> bool {
        self.task_handle
            .as_ref()
            .map(|handle| handle.is_finished())
            .unwrap_or(true)
    }

    /// Signals the background subscriber task to shut down and waits for it to complete.
    #[allow(dead_code)] // Used for lifecycle shutdown and tests
    pub(crate) async fn wait_for_shutdown(mut self) {
        self.stop();
        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for CacheSubscriber {
    fn drop(&mut self) {
        let _ = self.shutdown_sender.send(true);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}

/// Builder for configuring and starting a [`CacheSubscriber`].
#[derive(Clone, Debug)]
pub(crate) struct CacheSubscriberBuilder {
    database: String,
    spanner: Spanner,
    cache_updater: Arc<CacheUpdater>,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
    max_recipe_count: Option<i32>,
    max_range_count: Option<i32>,
}

impl CacheSubscriberBuilder {
    /// Creates a new `CacheSubscriberBuilder`.
    pub(crate) fn new(
        database: String,
        spanner: Spanner,
        cache_updater: Arc<CacheUpdater>,
    ) -> Self {
        Self {
            database,
            spanner,
            cache_updater,
            retry_policy: Arc::new(CacheSubscriberRetryPolicy::default()),
            backoff_policy: Arc::new(ExponentialBackoff::default()),
            max_recipe_count: None,
            max_range_count: None,
        }
    }

    /// Sets the retry policy applied to determine if a stream disconnect or error is retryable.
    #[allow(dead_code)] // Used for custom retry configuration and tests
    pub(crate) fn with_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.retry_policy = policy.into().into();
        self
    }

    /// Sets the backoff policy applied between stream reconnection attempts.
    #[allow(dead_code)] // Used for custom backoff configuration and tests
    pub(crate) fn with_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.backoff_policy = policy.into().into();
        self
    }

    /// Sets the maximum number of key recipes requested in each cache update.
    #[allow(dead_code)] // Used for tuning update batch limits and tests
    pub(crate) fn with_max_recipe_count(mut self, max_recipe_count: i32) -> Self {
        self.max_recipe_count = Some(max_recipe_count);
        self
    }

    /// Sets the maximum number of key ranges requested in each cache update.
    #[allow(dead_code)] // Used for tuning update batch limits and tests
    pub(crate) fn with_max_range_count(mut self, max_range_count: i32) -> Self {
        self.max_range_count = Some(max_range_count);
        self
    }

    /// Starts the background subscriber task and returns a [`CacheSubscriber`].
    pub(crate) fn start(self) -> CacheSubscriber {
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let task_handle = tokio::spawn(async move {
            run_subscriber_loop(self, shutdown_receiver).await;
        });

        CacheSubscriber {
            shutdown_sender,
            task_handle: Some(task_handle),
        }
    }
}

/// Default retry policy for the `FetchCacheUpdate` background subscriber.
///
/// Extends [`SpannerRetryPolicy`] to retry `UNAVAILABLE`, `RESOURCE_EXHAUSTED` (rate limiting / overload),
/// transient pre-RPC errors, and transport/I/O disconnects.
#[derive(Clone, Debug)]
pub(crate) struct CacheSubscriberRetryPolicy {
    inner: SpannerRetryPolicy,
}

impl CacheSubscriberRetryPolicy {
    /// Creates a new `CacheSubscriberRetryPolicy`.
    pub(crate) fn new() -> Self {
        Self {
            inner: SpannerRetryPolicy::new(),
        }
    }
}

impl Default for CacheSubscriberRetryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryPolicy for CacheSubscriberRetryPolicy {
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        if let Some(status) = error.status()
            && status.code == Code::ResourceExhausted
        {
            return RetryResult::Continue(error);
        }
        self.inner.on_error(state, error)
    }

    fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult {
        self.inner.on_throttle(state, error)
    }

    fn remaining_time(&self, state: &RetryState) -> Option<Duration> {
        self.inner.remaining_time(state)
    }
}

/// The outcome of consuming an active `FetchCacheUpdate` stream.
#[derive(Debug, PartialEq, Eq)]
enum StreamOutcome {
    /// Received at least one message and the stream concluded or encountered a transient error.
    ConcludedWithMessages,
    /// Stream concluded or encountered a transient error without receiving any messages.
    ConcludedWithoutMessages,
    /// Stream encountered a permanent or exhausted error.
    PermanentError,
    /// Stream consumption was interrupted by a shutdown request.
    Shutdown,
}

/// The result of processing a single event from the stream.
#[derive(Debug, PartialEq, Eq)]
enum StreamEventResult {
    /// A valid cache update was received and processed.
    MessageReceived,
    /// The stream concluded naturally (EOF) or with a retryable transient error.
    Reconnect,
    /// The stream encountered a fatal or permanent error.
    PermanentError,
}

/// The outcome of a single subscriber connection and stream execution iteration.
#[derive(Debug, PartialEq, Eq)]
enum IterationOutcome {
    /// Messages were consumed or connection was healthy; reset retry state and reconnect immediately.
    ResetRetryStateAndContinue,
    /// Transient disconnection/error occurred rapidly; continue with exponential backoff.
    ContinueWithBackoff,
    /// Fatal error or shutdown occurred; terminate the subscriber loop.
    Terminate,
}

fn is_shutdown(shutdown_receiver: &watch::Receiver<bool>) -> bool {
    *shutdown_receiver.borrow() || shutdown_receiver.has_changed().is_err()
}

fn build_fetch_request(
    database: &str,
    max_recipe_count: Option<i32>,
    max_range_count: Option<i32>,
) -> FetchCacheUpdateRequest {
    let mut request = FetchCacheUpdateRequest::new().set_database(database);
    if let Some(max_recipes) = max_recipe_count {
        request = request.set_max_recipe_count(max_recipes);
    }
    if let Some(max_ranges) = max_range_count {
        request = request.set_max_range_count(max_ranges);
    }
    request
}

fn handle_stream_message(proto_cache_update: ProtoCacheUpdate, cache_updater: &CacheUpdater) {
    match proto_cache_update.cnv() {
        Ok(cache_update) => {
            cache_updater.process_cache_update(cache_update);
        }
        Err(conversion_error) => {
            warn!(
                error = %conversion_error,
                "Failed to convert received ProtoCacheUpdate to model CacheUpdate"
            );
        }
    }
}

fn process_stream_event(
    message_option: Option<crate::Result<ProtoCacheUpdate>>,
    cache_updater: &CacheUpdater,
    retry_policy: &dyn RetryPolicy,
    retry_state: &RetryState,
    is_stream_healthy: bool,
) -> StreamEventResult {
    match message_option {
        Some(Ok(proto_cache_update)) => {
            handle_stream_message(proto_cache_update, cache_updater);
            StreamEventResult::MessageReceived
        }
        Some(Err(stream_error)) => {
            let evaluation_state = if is_stream_healthy {
                RetryState::new(true)
            } else {
                retry_state.clone()
            };
            match retry_policy.on_error(&evaluation_state, stream_error) {
                RetryResult::Continue(_) => {
                    warn!("FetchCacheUpdate stream encountered transient error; reconnecting");
                    StreamEventResult::Reconnect
                }
                RetryResult::Permanent(error) | RetryResult::Exhausted(error) => {
                    error!(
                        error = %error,
                        "Permanent or exhausted error on FetchCacheUpdate stream; terminating subscriber task"
                    );
                    StreamEventResult::PermanentError
                }
            }
        }
        None => {
            debug!("FetchCacheUpdate stream concluded with EOF; reconnecting");
            StreamEventResult::Reconnect
        }
    }
}

async fn consume_stream(
    mut stream: CacheUpdateStream,
    cache_updater: &CacheUpdater,
    retry_policy: &dyn RetryPolicy,
    retry_state: &RetryState,
    stream_start: Instant,
    shutdown_receiver: &mut watch::Receiver<bool>,
) -> StreamOutcome {
    let mut received_any_message = false;

    while !is_shutdown(shutdown_receiver) {
        tokio::select! {
            change_result = shutdown_receiver.changed() => {
                if is_shutdown(shutdown_receiver) || change_result.is_err() {
                    debug!("Shutdown signal received or channel closed, closing FetchCacheUpdate stream");
                    return StreamOutcome::Shutdown;
                }
            }
            message_option = stream.next_message() => {
                let is_stream_healthy = received_any_message || stream_start.elapsed() >= MIN_HEALTHY_STREAM_DURATION;
                match process_stream_event(
                    message_option,
                    cache_updater,
                    retry_policy,
                    retry_state,
                    is_stream_healthy,
                ) {
                    StreamEventResult::MessageReceived => received_any_message = true,
                    StreamEventResult::Reconnect => break,
                    StreamEventResult::PermanentError => return StreamOutcome::PermanentError,
                }
            }
        }
    }

    if is_shutdown(shutdown_receiver) {
        return StreamOutcome::Shutdown;
    }
    if received_any_message {
        return StreamOutcome::ConcludedWithMessages;
    }
    StreamOutcome::ConcludedWithoutMessages
}

async fn execute_subscriber_iteration(
    config: &CacheSubscriberBuilder,
    retry_state: &RetryState,
    shutdown_receiver: &mut watch::Receiver<bool>,
) -> IterationOutcome {
    let request = build_fetch_request(
        &config.database,
        config.max_recipe_count,
        config.max_range_count,
    );
    let channel = config.spanner.next_channel();

    debug!(database = %config.database, "Connecting to FetchCacheUpdate stream");
    let connect_future = config
        .spanner
        .fetch_cache_update(request, RequestOptions::default(), channel)
        .send();
    tokio::pin!(connect_future);

    let stream_result = tokio::select! {
        change_result = shutdown_receiver.changed() => {
            if is_shutdown(shutdown_receiver) || change_result.is_err() {
                return IterationOutcome::Terminate;
            }
            connect_future.await
        }
        result = &mut connect_future => result,
    };

    let stream = match stream_result {
        Ok(stream) => stream,
        Err(connection_error) => {
            return match config.retry_policy.on_error(retry_state, connection_error) {
                RetryResult::Continue(_) => {
                    warn!("Failed to establish FetchCacheUpdate stream; reconnecting");
                    IterationOutcome::ContinueWithBackoff
                }
                RetryResult::Permanent(error) | RetryResult::Exhausted(error) => {
                    error!(
                        database = %config.database,
                        error = %error,
                        "Permanent or exhausted error establishing FetchCacheUpdate stream; terminating subscriber task"
                    );
                    IterationOutcome::Terminate
                }
            };
        }
    };

    debug!(
        database = %config.database,
        headers = ?stream.headers(),
        "FetchCacheUpdate stream connected successfully"
    );
    let stream_start = Instant::now();
    let outcome = consume_stream(
        stream,
        &config.cache_updater,
        &(*config.retry_policy),
        retry_state,
        stream_start,
        shutdown_receiver,
    )
    .await;

    match outcome {
        StreamOutcome::Shutdown | StreamOutcome::PermanentError => IterationOutcome::Terminate,
        StreamOutcome::ConcludedWithMessages => IterationOutcome::ResetRetryStateAndContinue,
        StreamOutcome::ConcludedWithoutMessages => {
            if stream_start.elapsed() >= MIN_HEALTHY_STREAM_DURATION {
                IterationOutcome::ResetRetryStateAndContinue
            } else {
                IterationOutcome::ContinueWithBackoff
            }
        }
    }
}

async fn wait_for_backoff(
    backoff_duration: Duration,
    shutdown_receiver: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        change_result = shutdown_receiver.changed() => is_shutdown(shutdown_receiver) || change_result.is_err(),
        _ = sleep(backoff_duration) => false,
    }
}

async fn run_subscriber_loop(
    config: CacheSubscriberBuilder,
    mut shutdown_receiver: watch::Receiver<bool>,
) {
    let mut retry_state = RetryState::new(true);

    while !is_shutdown(&shutdown_receiver) {
        match execute_subscriber_iteration(&config, &retry_state, &mut shutdown_receiver).await {
            IterationOutcome::Terminate => return,
            IterationOutcome::ResetRetryStateAndContinue => {
                retry_state = RetryState::new(true);
                // Healthy stream concluded; reconnect immediately without backoff delay.
                continue;
            }
            IterationOutcome::ContinueWithBackoff => {}
        }

        if is_shutdown(&shutdown_receiver) {
            return;
        }

        let backoff_duration = config.backoff_policy.on_failure(&retry_state);
        retry_state.attempt_count += 1;

        debug!(
            backoff_ms = backoff_duration.as_millis(),
            attempt_count = retry_state.attempt_count,
            "Waiting before FetchCacheUpdate reconnection attempt"
        );

        if wait_for_backoff(backoff_duration, &mut shutdown_receiver).await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Channel;
    use crate::model::{CacheUpdate, Range};
    use crate::routing::connection_cache::ConnectionCache;
    use crate::routing::key_range_cache::{KeyRangeCache, RangeMode};
    use crate::routing::key_recipe_cache::KeyRecipeCache;
    use crate::routing::server_connection::ServerConnection;
    use gaxi::grpc::tonic::Response;
    use gaxi::grpc::tonic::Status;
    use gaxi::options::ClientConfig;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::error::rpc::Status as RpcStatus;
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use google_cloud_test_macros::tokio_test_no_panics;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(CacheSubscriber: Send, Sync, std::fmt::Debug);
        static_assertions::assert_impl_all!(CacheSubscriberBuilder: Send, Sync, std::fmt::Debug);
        static_assertions::assert_impl_all!(CacheSubscriberRetryPolicy: Send, Sync, std::fmt::Debug);
    }

    #[test]
    fn retry_policy_classification() {
        let policy = CacheSubscriberRetryPolicy::new();
        let retry_state = RetryState::new(true);

        let resource_exhausted_status = RpcStatus::default()
            .set_code(Code::ResourceExhausted)
            .set_message("quota exhausted");
        let resource_exhausted_error = Error::service(resource_exhausted_status);
        assert!(
            policy
                .on_error(&retry_state, resource_exhausted_error)
                .is_continue(),
            "RESOURCE_EXHAUSTED should be retryable"
        );

        let unavailable_status = RpcStatus::default()
            .set_code(Code::Unavailable)
            .set_message("server unavailable");
        let unavailable_error = Error::service(unavailable_status);
        assert!(
            policy
                .on_error(&retry_state, unavailable_error)
                .is_continue(),
            "UNAVAILABLE should be retryable"
        );

        let permission_denied_status = RpcStatus::default()
            .set_code(Code::PermissionDenied)
            .set_message("denied");
        let permission_denied_error = Error::service(permission_denied_status);
        assert!(
            policy
                .on_error(&retry_state, permission_denied_error)
                .is_permanent(),
            "PERMISSION_DENIED should be permanent"
        );

        let not_found_status = RpcStatus::default()
            .set_code(Code::NotFound)
            .set_message("database not found");
        let not_found_error = Error::service(not_found_status);
        assert!(
            policy
                .on_error(&retry_state, not_found_error)
                .is_permanent(),
            "NOT_FOUND should be permanent"
        );
    }

    #[derive(Debug)]
    struct DummyStub;
    impl crate::generated::gapic_dataplane::stub::Spanner for DummyStub {}

    fn sample_cache_updater() -> Arc<CacheUpdater> {
        let channel = Channel::new_for_test(DummyStub);
        let default_connection =
            ServerConnection::new("default.spanner.googleapis.com:443".to_string(), channel);
        let connection_cache = Arc::new(ConnectionCache::new(default_connection));
        let key_range_cache = Arc::new(KeyRangeCache::new());
        let key_recipe_cache = Arc::new(KeyRecipeCache::new());
        Arc::new(CacheUpdater::new(
            key_range_cache,
            key_recipe_cache,
            connection_cache,
            ClientConfig::default(),
        ))
    }

    fn sample_proto_cache_update(
        start_key: &[u8],
        limit_key: &[u8],
        address: &str,
    ) -> mock_v1::CacheUpdate {
        let range = mock_v1::Range {
            start_key: start_key.to_vec(),
            limit_key: limit_key.to_vec(),
            group_uid: 1,
            ..Default::default()
        };

        let tablet = mock_v1::Tablet {
            server_address: address.to_string(),
            role: mock_v1::tablet::Role::ReadWrite as i32,
            ..Default::default()
        };

        let group = mock_v1::Group {
            group_uid: 1,
            tablets: vec![tablet],
            ..Default::default()
        };

        mock_v1::CacheUpdate {
            range: vec![range],
            group: vec![group],
            ..Default::default()
        }
    }

    async fn setup_spanner(mock: MockSpanner) -> (Spanner, tokio::task::JoinHandle<()>) {
        let (address, server) = spanner_grpc_mock::start("0.0.0.0:0", mock)
            .await
            .expect("mock server should start");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("spanner client should build successfully");
        (spanner, server)
    }

    #[tokio_test_no_panics]
    async fn subscriber_receives_and_processes_updates() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            let (stream_sender, stream_receiver) = mpsc::channel(4);
            let update = sample_proto_cache_update(b"a", b"z", "node-1.spanner.internal:1000");
            let _ = stream_sender.try_send(Ok(update));
            Ok(Response::new(stream_receiver))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let fast_backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(5))
            .with_maximum_delay(Duration::from_millis(20))
            .with_scaling(1.5)
            .build()
            .expect("valid backoff policy");

        let subscriber = CacheSubscriber::builder(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        )
        .with_backoff_policy(fast_backoff)
        .with_max_recipe_count(10)
        .with_max_range_count(100)
        .start();

        // Deterministically wait for the initial connection attempt.
        attempt_receiver
            .recv()
            .await
            .expect("initial connection attempt should arrive");

        // When the initial stream concludes (EOF), the subscriber will reconnect.
        // Awaiting the second connection attempt guarantees that the first update was
        // completely ingested by `consume_stream`.
        attempt_receiver
            .recv()
            .await
            .expect("second connection attempt should arrive after first stream finishes");

        let found_range = cache_updater
            .key_range_cache()
            .find_range(b"m", &[], RangeMode::CoveringSplit)
            .expect("key range cache should contain routed range");
        assert_eq!(found_range.group_uid, 1);

        let cached_group = cache_updater
            .key_range_cache()
            .get_group(found_range.group_uid)
            .expect("cached group should exist");
        assert_eq!(
            cached_group.tablets[0].server_address,
            "node-1.spanner.internal:1000"
        );

        subscriber.wait_for_shutdown().await;
    }

    #[tokio_test_no_panics]
    async fn subscriber_reconnects_on_transient_stream_error() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            let (stream_sender, stream_receiver) = mpsc::channel(1);
            let _ = stream_sender.try_send(Err(Status::unavailable("server unavailable")));
            Ok(Response::new(stream_receiver))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let fast_backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(5))
            .with_maximum_delay(Duration::from_millis(20))
            .with_scaling(1.2)
            .build()
            .expect("valid backoff policy");

        let subscriber = CacheSubscriber::builder(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        )
        .with_backoff_policy(fast_backoff)
        .start();

        // Deterministically wait for the first and subsequent reconnect attempts.
        attempt_receiver
            .recv()
            .await
            .expect("first connection attempt should arrive");
        attempt_receiver
            .recv()
            .await
            .expect("second connection attempt should arrive after transient stream error");

        subscriber.wait_for_shutdown().await;
    }

    #[tokio_test_no_panics]
    async fn subscriber_reconnects_on_resource_exhausted() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            Err(Status::resource_exhausted("resource exhausted/overloaded"))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let fast_backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(5))
            .with_maximum_delay(Duration::from_millis(20))
            .with_scaling(1.2)
            .build()
            .expect("valid backoff policy");

        let subscriber = CacheSubscriber::builder(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        )
        .with_backoff_policy(fast_backoff)
        .start();

        // Deterministically wait for repeated connection attempts on RESOURCE_EXHAUSTED.
        attempt_receiver
            .recv()
            .await
            .expect("first connection attempt should arrive");
        attempt_receiver
            .recv()
            .await
            .expect("second connection attempt should arrive");

        subscriber.wait_for_shutdown().await;
    }

    #[tokio_test_no_panics]
    async fn subscriber_terminates_on_startup_permanent_error() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            Err(Status::not_found("Database not found"))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let subscriber = CacheSubscriber::start(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        );

        attempt_receiver
            .recv()
            .await
            .expect("initial connection attempt should arrive");

        // The subscriber should terminate immediately without attempting reconnection.
        subscriber.wait_for_shutdown().await;
    }

    #[tokio_test_no_panics]
    async fn subscriber_terminates_on_mid_stream_permanent_error() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);
        let (send_error_sender, send_error_receiver) = watch::channel(false);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            let (stream_sender, stream_receiver) = mpsc::channel(4);
            let update = sample_proto_cache_update(b"a", b"z", "node-1.spanner.internal:1000");
            let mut send_error_receiver = send_error_receiver.clone();
            tokio::spawn(async move {
                let _ = stream_sender.send(Ok(update)).await;
                let _ = send_error_receiver.changed().await;
                let _ = stream_sender
                    .send(Err(Status::permission_denied("IAM permission denied")))
                    .await;
            });
            Ok(Response::new(stream_receiver))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let subscriber = CacheSubscriber::start(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        );

        attempt_receiver
            .recv()
            .await
            .expect("initial connection attempt should arrive");

        // Wait until cache contains the update
        loop {
            if cache_updater
                .key_range_cache()
                .find_range(b"m", &[], RangeMode::CoveringSplit)
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }

        // Trigger the permanent mid-stream error
        let _ = send_error_sender.send(true);

        // Wait for subscriber task to finish on its own
        while !subscriber.is_finished() {
            tokio::task::yield_now().await;
        }

        // Verify the update preceding the error was successfully ingested.
        let found_range = cache_updater
            .key_range_cache()
            .find_range(b"m", &[], RangeMode::CoveringSplit)
            .expect("key range cache should contain update ingested prior to error");
        assert_eq!(found_range.group_uid, 1);
    }

    #[tokio_test_no_panics]
    async fn subscriber_with_attempt_limited_retry_policy() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            Err(Status::unavailable("server unavailable"))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let limited_retry = CacheSubscriberRetryPolicy::new().with_attempt_limit(2);
        let fast_backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(5))
            .with_maximum_delay(Duration::from_millis(15))
            .with_scaling(1.2)
            .build()
            .expect("valid backoff policy");

        let subscriber = CacheSubscriber::builder(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        )
        .with_retry_policy(limited_retry)
        .with_backoff_policy(fast_backoff)
        .start();

        attempt_receiver
            .recv()
            .await
            .expect("first connection attempt should arrive");
        attempt_receiver
            .recv()
            .await
            .expect("second connection attempt should arrive");

        // After 2 attempts, retry attempts are exhausted and subscriber shuts down.
        subscriber.wait_for_shutdown().await;
    }

    #[tokio_test_no_panics]
    async fn subscriber_shuts_down_during_backoff() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            Err(Status::unavailable("server unavailable"))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        // Large backoff to verify cancellation interrupts the sleep immediately
        let large_backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(60))
            .with_maximum_delay(Duration::from_secs(60))
            .build()
            .expect("valid backoff policy");

        let subscriber = CacheSubscriber::builder(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        )
        .with_backoff_policy(large_backoff)
        .start();

        attempt_receiver
            .recv()
            .await
            .expect("first connection attempt should arrive");

        // Request shutdown during the 60s backoff sleep
        subscriber.wait_for_shutdown().await;
    }

    #[tokio_test_no_panics]
    async fn subscriber_handles_graceful_shutdown() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            let (_stream_sender, stream_receiver) = mpsc::channel(1);
            Ok(Response::new(stream_receiver))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let subscriber = CacheSubscriber::start(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        );

        attempt_receiver
            .recv()
            .await
            .expect("connection attempt should arrive before shutdown");

        assert!(
            !subscriber.is_finished(),
            "subscriber should be active before shutdown"
        );
        subscriber.wait_for_shutdown().await;
    }

    #[tokio_test_no_panics]
    async fn subscriber_drops_handle_triggers_shutdown() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            let (_stream_sender, stream_receiver) = mpsc::channel(1);
            Ok(Response::new(stream_receiver))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let subscriber = CacheSubscriber::start(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        );

        attempt_receiver
            .recv()
            .await
            .expect("connection attempt should arrive before drop");

        drop(subscriber);
    }

    #[tokio_test_no_panics]
    async fn immediate_empty_stream_cycling_triggers_backoff() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            let (_stream_sender, stream_receiver) = mpsc::channel(1);
            Ok(Response::new(stream_receiver))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let fast_backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(5))
            .with_maximum_delay(Duration::from_millis(20))
            .with_scaling(1.5)
            .build()
            .expect("valid backoff policy");

        let subscriber = CacheSubscriber::builder(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        )
        .with_backoff_policy(fast_backoff)
        .start();

        attempt_receiver
            .recv()
            .await
            .expect("first connection attempt should arrive");
        attempt_receiver
            .recv()
            .await
            .expect("second connection attempt should arrive after rapid EOF backoff");

        subscriber.wait_for_shutdown().await;
    }

    #[tokio_test_no_panics]
    async fn corrupted_proto_cache_update_is_skipped() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            let (stream_sender, stream_receiver) = mpsc::channel(4);
            let mut corrupted_update =
                sample_proto_cache_update(b"a", b"z", "node-1.spanner.internal:1000");
            corrupted_update.group[0].tablets[0].role = -1;
            let valid_update =
                sample_proto_cache_update(b"a", b"z", "node-2.spanner.internal:1000");
            tokio::spawn(async move {
                let _ = stream_sender.send(Ok(corrupted_update)).await;
                let _ = stream_sender.send(Ok(valid_update)).await;
            });
            Ok(Response::new(stream_receiver))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let subscriber = CacheSubscriber::start(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        );

        attempt_receiver
            .recv()
            .await
            .expect("initial connection attempt should arrive");

        loop {
            if let Some(range) =
                cache_updater
                    .key_range_cache()
                    .find_range(b"m", &[], RangeMode::CoveringSplit)
                && let Some(group) = cache_updater.key_range_cache().get_group(range.group_uid)
                && group
                    .tablets
                    .first()
                    .map(|tablet| tablet.server_address.as_str())
                    == Some("node-2.spanner.internal:1000")
            {
                break;
            }
            tokio::task::yield_now().await;
        }

        subscriber.wait_for_shutdown().await;
    }

    #[tokio_test_no_panics]
    async fn subscriber_updates_database_id_and_invalidates_caches_on_database_switch() {
        let cache_updater = sample_cache_updater();
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);

        // Pre-populate with database ID 1
        let pre_update = CacheUpdate::new().set_database_id(1u64).set_range(vec![
            Range::new()
                .set_group_uid(1u64)
                .set_start_key(vec![b'a'])
                .set_limit_key(vec![b'm']),
        ]);
        cache_updater.process_cache_update(pre_update);
        assert_eq!(cache_updater.database_id(), 1);
        assert_eq!(cache_updater.key_range_cache().len(), 1);

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(move |_req| {
            let _ = attempt_sender.try_send(());
            let (stream_sender, stream_receiver) = mpsc::channel(4);
            let mut new_db_update =
                sample_proto_cache_update(b"m", b"z", "new-node.spanner.internal:1000");
            new_db_update.database_id = 999;
            tokio::spawn(async move {
                let _ = stream_sender.send(Ok(new_db_update)).await;
            });
            Ok(Response::new(stream_receiver))
        });

        let (spanner, _server) = setup_spanner(mock).await;
        let subscriber = CacheSubscriber::start(
            "projects/p/instances/i/databases/d".to_string(),
            spanner,
            Arc::clone(&cache_updater),
        );

        attempt_receiver
            .recv()
            .await
            .expect("initial connection attempt should arrive");

        loop {
            if cache_updater.database_id() == 999 {
                break;
            }
            tokio::task::yield_now().await;
        }

        // Cache must have been invalidated on DB switch from 1 to 999, so only the new range (m..z) exists
        assert_eq!(cache_updater.database_id(), 999);
        assert!(
            cache_updater
                .key_range_cache()
                .find_range(b"b", &[], RangeMode::CoveringSplit)
                .is_none(),
            "old database range must be cleared on database ID switch"
        );
        assert!(
            cache_updater
                .key_range_cache()
                .find_range(b"p", &[], RangeMode::CoveringSplit)
                .is_some(),
            "new database range must be present"
        );

        subscriber.wait_for_shutdown().await;
    }
}
