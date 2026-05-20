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

/// Decorate a poller with tracing information.
#[derive(Clone, Debug)]
pub struct Tracing<P> {
    inner: P,
    span: Span,
}

impl<P> Tracing<P> {
    pub(crate) fn new(inner: P, span: Span) -> Self {
        Self { inner, span }
    }
}

#[cfg(google_cloud_unstable_tracing)]
tokio::task_local! {
    pub(crate) static LRO_SPAN: Span;
}

impl<P> sealed::Poller for Tracing<P>
where
    P: sealed::Poller + Send,
{
    async fn backoff(&mut self, state: &PollingState) {
        let span = info_span!("LRO Sleep");
        #[cfg(google_cloud_unstable_tracing)]
        {
            LRO_SPAN
                .scope(span.clone(), async move {
                    self.inner.backoff(state).instrument(span).await
                })
                .await
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
        let span = self.span.clone();
        #[cfg(google_cloud_unstable_tracing)]
        {
            LRO_SPAN
                .scope(span.clone(), async move {
                    self.inner.poll().instrument(span).await
                })
                .await
        }
        #[cfg(not(google_cloud_unstable_tracing))]
        {
            self.inner.poll().await
        }
    }

    async fn until_done(self) -> Result<ResponseType> {
        let span = self.span.clone();
        #[cfg(google_cloud_unstable_tracing)]
        {
            let wait_span = span.clone();
            let result = LRO_SPAN
                .scope(span.clone(), async move {
                    crate::until_done(self).instrument(wait_span).await
                })
                .await;
            if let Err(ref e) = result {
                span.record("otel.status_code", "ERROR");
                span.record("otel.status_description", e.to_string());
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
    use std::sync::Arc;

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
    struct TestLayer {
        recorded: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    }

    #[cfg(google_cloud_unstable_tracing)]
    impl<S> tracing_subscriber::layer::Layer<S> for TestLayer
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        fn on_record(
            &self,
            _id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Visitor {
                recorded: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
            }
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.recorded
                        .lock()
                        .unwrap()
                        .insert(field.name().to_string(), value.to_string());
                }
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.recorded
                        .lock()
                        .unwrap()
                        .insert(field.name().to_string(), format!("{:?}", value));
                }
            }
            let mut visitor = Visitor {
                recorded: self.recorded.clone(),
            };
            values.record(&mut visitor);
        }
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[tokio::test]
    async fn test_tracing_decorator_error_reporting() {
        use tracing_subscriber::layer::SubscriberExt;

        let recorded = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let layer = TestLayer {
            recorded: recorded.clone(),
        };
        let subscriber = tracing_subscriber::registry::Registry::default().with(layer);

        let _guard = tracing::subscriber::set_default(subscriber);

        let span = tracing::info_span!(
            "test_span",
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
        );

        let poller = Tracing::new(FailingPoller, span);

        let got = poller.until_done().await;
        assert!(got.is_err());

        {
            let map = recorded.lock().unwrap();
            assert_eq!(
                map.get("otel.status_code").map(|s| s.as_str()),
                Some("ERROR")
            );
            assert!(
                map.get("otel.status_description")
                    .unwrap()
                    .contains("logical-test-failure")
            );
        }
    }
}
