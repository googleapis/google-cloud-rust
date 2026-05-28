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

#[cfg(google_cloud_unstable_tracing)]
use crate::POLL_ATTEMPT_COUNT;

#[cfg(google_cloud_unstable_tracing)]
tokio::task_local! {
    static LRO_SPAN: Span;
}

#[cfg(google_cloud_unstable_tracing)]
tokio::task_local! {
    static LRO_RECORDER: LroRecorder;
}

#[cfg(google_cloud_unstable_tracing)]
#[derive(Clone, Debug)]
pub(crate) struct LroRecorder {
    span: Span,
    poll_attempt_count: u32,
    started: bool,
}

#[cfg(google_cloud_unstable_tracing)]
impl LroRecorder {
    pub fn new(span: Span) -> Self {
        Self {
            span,
            poll_attempt_count: 0,
            started: false,
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

    #[cfg(test)]
    pub fn span(&self) -> &Span {
        &self.span
    }

    pub fn record_destination_id(&self, name: &str) {
        self.span.record("gcp.resource.destination.id", name);
    }

    pub fn record_error(&self, err: &crate::Error) {
        self.span.record("otel.status_code", "ERROR");
        self.span.record("otel.status_description", err.to_string());
    }

    pub async fn record_poll<F, Fut, T>(&mut self, f: F) -> T
    where
        F: FnOnce(Span) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let attempt = if self.started {
            self.poll_attempt_count += 1;
            self.poll_attempt_count
        } else {
            self.started = true;
            0 // Initial triggers record nothing
        };
        let span = self.span.clone();
        self.scope(async move {
            POLL_ATTEMPT_COUNT
                .scope(attempt, async move { f(span).await })
                .await
        })
        .await
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

/// Decorate a poller with tracing information.
#[derive(Clone, Debug)]
pub struct Tracing<P> {
    inner: P,
    #[cfg(google_cloud_unstable_tracing)]
    recorder: LroRecorder,
    #[cfg(not(google_cloud_unstable_tracing))]
    span: Span,
}

impl<P> Tracing<P> {
    pub(crate) fn new(inner: P, span: Span) -> Self {
        Self {
            inner,
            #[cfg(google_cloud_unstable_tracing)]
            recorder: LroRecorder::new(span),
            #[cfg(not(google_cloud_unstable_tracing))]
            span,
        }
    }
}

impl<P> sealed::Poller for Tracing<P>
where
    P: sealed::Poller + Send,
{
    async fn backoff(&mut self, state: &PollingState) {
        let span = info_span!("LRO Sleep");
        #[cfg(google_cloud_unstable_tracing)]
        {
            let inner = &mut self.inner;
            return self
                .recorder
                .record_action(|_| async move { inner.backoff(state).instrument(span).await })
                .await;
        }
        #[cfg(not(google_cloud_unstable_tracing))]
        {
            self.inner.backoff(state).await
        }
    }
}

impl<P, ResponseType, MetadataType> Poller<ResponseType, MetadataType> for Tracing<P>
where
    P: Poller<ResponseType, MetadataType>,
    ResponseType: Send,
    MetadataType: Send,
{
    async fn poll(&mut self) -> Option<PollingResult<ResponseType, MetadataType>> {
        #[cfg(google_cloud_unstable_tracing)]
        {
            let inner = &mut self.inner;
            self.recorder
                .record_poll(|span| async move { inner.poll().instrument(span).await })
                .await
        }
        #[cfg(not(google_cloud_unstable_tracing))]
        {
            self.inner.poll().await
        }
    }

    async fn until_done(self) -> Result<ResponseType> {
        #[cfg(google_cloud_unstable_tracing)]
        {
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
        #[cfg(not(google_cloud_unstable_tracing))]
        {
            crate::until_done(self).await
        }
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

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn test_tracing_decorator_error_reporting() {
        let guard = google_cloud_test_utils::test_layer::TestLayer::initialize();

        let span = tracing::info_span!(
            "test_span",
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
        );

        let poller = Tracing::new(FailingPoller, span);

        let got = poller.until_done().await;
        assert!(got.is_err());

        {
            let captured = google_cloud_test_utils::test_layer::TestLayer::capture(&guard);
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

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn test_lro_recorder_attempt_counting() {
        let span = tracing::info_span!("test_lro_span");
        let mut recorder = LroRecorder::new(span);

        // First poll should record attempt 0
        recorder
            .record_poll(|active_span| async move {
                assert_eq!(active_span.metadata().unwrap().name(), "test_lro_span");
                let attempt = POLL_ATTEMPT_COUNT.try_with(|c| *c).unwrap();
                assert_eq!(attempt, 0);
            })
            .await;

        // Second poll should record attempt 1
        recorder
            .record_poll(|active_span| async move {
                assert_eq!(active_span.metadata().unwrap().name(), "test_lro_span");
                let attempt = POLL_ATTEMPT_COUNT.try_with(|c| *c).unwrap();
                assert_eq!(attempt, 1);
            })
            .await;

        // Third poll should record attempt 2
        recorder
            .record_poll(|active_span| async move {
                assert_eq!(active_span.metadata().unwrap().name(), "test_lro_span");
                let attempt = POLL_ATTEMPT_COUNT.try_with(|c| *c).unwrap();
                assert_eq!(attempt, 2);
            })
            .await;
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn test_lro_recorder_span_nesting() {
        let span = tracing::info_span!("test_lro_span");
        let recorder = LroRecorder::new(span.clone());

        // Verify span is active in record_poll
        let mut recorder_mut = recorder.clone();
        let span_clone = span.clone();
        recorder_mut
            .record_poll(|_| async move {
                let active_recorder = LroRecorder::current().unwrap();
                assert_eq!(
                    active_recorder.span().metadata().unwrap().name(),
                    "test_lro_span"
                );
                assert_eq!(active_recorder.span(), &span_clone);
            })
            .await;

        // Verify span is active in record_action
        let span_clone2 = span.clone();
        recorder
            .record_action(|_| async move {
                let active_recorder = LroRecorder::current().unwrap();
                assert_eq!(
                    active_recorder.span().metadata().unwrap().name(),
                    "test_lro_span"
                );
                assert_eq!(active_recorder.span(), &span_clone2);
            })
            .await;
    }
}
