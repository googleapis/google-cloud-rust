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

use crate::{Poller, PollingResult, Result, sealed};
use google_cloud_gax::polling_state::PollingState;
use tracing::{Instrument, Span, info_span};

tokio::task_local! {
    static LRO_RECORDER: LroRecorder;
}

/// A recorder that manages LRO spans and propagates active telemetry context.
///
/// To prevent concurrent mutation race conditions under multi-threaded tokio executors,
/// `LroRecorder` is completely immutable. Context updates (like setting the transient `attempt_count`
/// during a polling cycle) are performed using copy-on-write builders (`with_attempt_count`)
/// to establish new immutable task-local scopes.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LroRecorder {
    span: Span,
    attempt_count: Option<u32>,
}

impl LroRecorder {
    /// Creates a new `LroRecorder` wrapping the given tracing `Span`.
    pub fn new(span: Span) -> Self {
        Self {
            span,
            attempt_count: None,
        }
    }

    /// Returns the recorder in the current task scope.
    pub fn current() -> Option<Self> {
        LRO_RECORDER.try_get().ok()
    }

    /// Runs a future within the scope of this recorder.
    pub async fn scope<F, T>(&self, future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        LRO_RECORDER.scope(self.clone(), future).await
    }

    /// Returns the active LRO tracing `Span` wrapped by this recorder.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Returns the current LRO polling attempt count, if active.
    ///
    /// This returns `Some(u32)` when queried during an active polling attempt,
    /// and `None` otherwise (e.g., when executing outside the scope of an active polling cycle).
    pub fn attempt_count(&self) -> Option<u32> {
        self.attempt_count
    }

    /// Creates a new clone of `LroRecorder` carrying the specified LRO polling attempt count.
    ///
    /// Since `LroRecorder` is immutable to guarantee thread-safety, this updates the context
    /// via copy-on-write, returning a new value to be bound to a new task-local scope.
    pub fn with_attempt_count(&self, count: u32) -> Self {
        Self {
            span: self.span.clone(),
            attempt_count: Some(count),
        }
    }

    pub fn record_destination_id(&self, name: &str) {
        self.span.record("gcp.resource.destination.id", name);
    }

    pub fn record_error(&self, err: &crate::Error) {
        self.span.record("otel.status_code", "ERROR");
        self.span.record("otel.status_description", err.to_string());
    }

    pub async fn record_action<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce(Span) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let span = self.span.clone();
        self.scope(async move { f(span).await }).await
    }
}

/// Injects LRO-specific telemetry attributes into the active span.
#[macro_export]
#[doc(hidden)]
macro_rules! record_polling_attributes {
    ($span:expr) => {
        if let Some(recorder) = $crate::LroRecorder::current() {
            if let Some(attempt) = recorder.attempt_count() {
                let span = &$span;
                span.record("gcp.longrunning.poll_attempt_count", attempt);
                span.record("gcp.longrunning.done", false);
            }
        }
    };
}

/// Decorate a poller with tracing information.
#[derive(Clone, Debug)]
pub struct Tracing<P> {
    inner: P,
    recorder: LroRecorder,
    /// Stateful count of poll attempts managed directly on the decorator.
    poll_attempt_count: u32,
    started: bool,
}

impl<P> Tracing<P> {
    pub(crate) fn new(inner: P, span: Span) -> Self {
        Self {
            inner,
            recorder: LroRecorder::new(span),
            poll_attempt_count: 0,
            started: false,
        }
    }
}

impl<P> sealed::Poller for Tracing<P>
where
    P: sealed::Poller + Send,
{
    async fn backoff(&mut self, state: &PollingState) {
        let span = info_span!("LRO Sleep");
        let inner = &mut self.inner;
        self.recorder
            .record_action(|_| async move { inner.backoff(state).instrument(span).await })
            .await
    }
}

impl<P, ResponseType, MetadataType> Poller<ResponseType, MetadataType> for Tracing<P>
where
    P: Poller<ResponseType, MetadataType>,
    ResponseType: Send,
    MetadataType: Send,
{
    async fn poll(&mut self) -> Option<PollingResult<ResponseType, MetadataType>> {
        // Stateful count of poll attempts is managed directly on the decorator instance,
        // which is called via `&mut self` and is safe from divergent mutations.
        let attempt = if self.started {
            self.poll_attempt_count += 1;
            self.poll_attempt_count
        } else {
            self.started = true;
            0 // Initial triggers record nothing
        };

        let inner = &mut self.inner;
        let span = self.recorder.span().clone();

        // We map the consolidated LroRecorder (holding the active LRO span and stateful attempt count)
        // for the duration of the active poll future.
        let recorder = self.recorder.with_attempt_count(attempt);
        recorder
            .scope(async move { inner.poll().instrument(span).await })
            .await
    }

    async fn until_done(self) -> Result<ResponseType> {
        let this = self;
        let recorder = this.recorder.clone();
        let result = recorder
            .record_action(|wait_span| async move {
                crate::until_done(this).instrument(wait_span).await
            })
            .await;
        if let Err(ref e) = result {
            recorder.record_error(e);
        }
        result
    }
    #[cfg(feature = "unstable-stream")]
    fn into_stream(
        self,
    ) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>> + Unpin {
        crate::into_stream(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use gaxi::client_request_signals;
    use gaxi::options::InstrumentationClientInfo;
    use google_cloud_test_utils::test_layer::TestLayer;
    use google_cloud_wkt::{Duration, Timestamp};

    struct FailingPoller;
    impl sealed::Poller for FailingPoller {
        async fn backoff(&mut self, _state: &PollingState) {}
    }
    impl Poller<Duration, Timestamp> for FailingPoller {
        async fn poll(&mut self) -> Option<PollingResult<Duration, Timestamp>> {
            Some(PollingResult::Completed(Err(Error::io(
                "logical-test-failure",
            ))))
        }
        async fn until_done(self) -> Result<Duration> {
            Err(Error::io("logical-test-failure"))
        }
        #[cfg(feature = "unstable-stream")]
        fn into_stream(
            self,
        ) -> impl futures::Stream<Item = PollingResult<Duration, Timestamp>> + Unpin {
            crate::into_stream(self)
        }
    }

    #[tokio::test]
    async fn test_tracing_decorator_error_reporting() {
        let guard = TestLayer::initialize();

        let span = tracing::info_span!(
            "test_span",
            "otel.status_code" = tracing::field::Empty,
            "otel.status_description" = tracing::field::Empty,
        );

        let poller = Tracing::new(FailingPoller, span);

        let got = poller.until_done().await;
        assert!(got.is_err());

        {
            let captured = TestLayer::capture(&guard);
            let got = captured
                .iter()
                .find(|s| s.name == "test_span")
                .unwrap_or_else(|| panic!("missing `test_span` in captured spans: {captured:?}"));
            assert_eq!(
                got.attributes
                    .get("otel.status_code")
                    .and_then(|v| v.as_string()),
                Some("ERROR".to_string())
            );
            assert!(
                got.attributes
                    .get("otel.status_description")
                    .and_then(|v| v.as_string())
                    .unwrap()
                    .contains("logical-test-failure")
            );
        }
    }

    struct CountingPoller {
        attempts: Vec<u32>,
    }
    impl sealed::Poller for CountingPoller {
        async fn backoff(&mut self, _state: &PollingState) {}
    }
    impl Poller<Duration, Timestamp> for CountingPoller {
        async fn poll(&mut self) -> Option<PollingResult<Duration, Timestamp>> {
            // Safe to unwrap because this mock poller is only called under the `Tracing::poll`
            // decorator, which guarantees that an active `LroRecorder` is in scope with a
            // populated attempt count.
            let attempt = LroRecorder::current()
                .and_then(|r| r.attempt_count())
                .unwrap();
            self.attempts.push(attempt);
            Some(PollingResult::InProgress(None))
        }
        async fn until_done(self) -> Result<Duration> {
            Ok(Duration::clamp(0, 0))
        }
        #[cfg(feature = "unstable-stream")]
        fn into_stream(
            self,
        ) -> impl futures::Stream<Item = PollingResult<Duration, Timestamp>> + Unpin {
            crate::into_stream(self)
        }
    }

    #[tokio::test]
    async fn test_tracing_decorator_attempt_counting() {
        let span = tracing::info_span!("test_lro_span");
        let poller = CountingPoller { attempts: vec![] };
        let mut traced = Tracing::new(poller, span);

        // First poll should record attempt 0
        let _ = traced.poll().await;

        // Second poll should record attempt 1
        let _ = traced.poll().await;

        // Third poll should record attempt 2
        let _ = traced.poll().await;

        assert_eq!(traced.inner.attempts, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_lro_recorder_span_nesting() {
        let span = tracing::info_span!("test_lro_span");
        let recorder = LroRecorder::new(span.clone());

        // Verify span is active in record_action
        let span_clone = span.clone();
        recorder
            .record_action(|_| async move {
                let active_recorder = LroRecorder::current().unwrap();
                assert_eq!(
                    active_recorder.span.metadata().unwrap().name(),
                    "test_lro_span"
                );
                assert_eq!(active_recorder.span, span_clone);
            })
            .await;
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn record_polling_attributes_macro() {
        let guard = TestLayer::initialize();

        let span =
            client_request_signals!(info: &InstrumentationClientInfo::default(), method: "test");

        let recorder = LroRecorder::new(span.clone()).with_attempt_count(42);

        recorder
            .scope(async move {
                crate::record_polling_attributes!(&span);
            })
            .await;

        drop(recorder);

        let captured = TestLayer::capture(&guard);
        let got = captured
            .iter()
            .find(|s| s.name == "client_request")
            .unwrap();

        assert_eq!(
            got.attributes.get("gcp.longrunning.poll_attempt_count"),
            Some(&google_cloud_test_utils::test_layer::AttributeValue::UInt64(42))
        );
        assert_eq!(
            got.attributes.get("gcp.longrunning.done"),
            Some(&google_cloud_test_utils::test_layer::AttributeValue::Boolean(false))
        );
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn record_polling_attributes_macro_no_recorder() {
        let guard = TestLayer::initialize();

        let span =
            client_request_signals!(info: &InstrumentationClientInfo::default(), method: "test");

        crate::record_polling_attributes!(&span);

        drop(span); // capture it

        let captured = TestLayer::capture(&guard);
        let got = captured
            .iter()
            .find(|s| s.name == "client_request")
            .unwrap();

        assert!(
            got.attributes
                .get("gcp.longrunning.poll_attempt_count")
                .is_none()
        );
        assert!(got.attributes.get("gcp.longrunning.done").is_none());
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn record_polling_attributes_macro_no_attempt_count() {
        let guard = TestLayer::initialize();

        let span =
            client_request_signals!(info: &InstrumentationClientInfo::default(), method: "test");

        let recorder = LroRecorder::new(span.clone());

        recorder
            .scope(async move {
                crate::record_polling_attributes!(&span);
            })
            .await;

        drop(recorder);

        let captured = TestLayer::capture(&guard);
        let got = captured
            .iter()
            .find(|s| s.name == "client_request")
            .unwrap();

        assert!(
            got.attributes
                .get("gcp.longrunning.poll_attempt_count")
                .is_none()
        );
        assert!(got.attributes.get("gcp.longrunning.done").is_none());
    }
}
