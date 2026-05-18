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

use crate::{Poller, Result, sealed};
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

impl<P> sealed::Poller for Tracing<P> {}

impl<P, ResponseType, MetadataType> Poller<ResponseType, MetadataType> for Tracing<P>
where
    P: Poller<ResponseType, MetadataType>,
    ResponseType: Send,
    MetadataType: Send,
{
    async fn poll(&mut self) -> Option<crate::PollingResult<ResponseType, MetadataType>> {
        let span = info_span!("LRO Poll");
        self.inner.poll().instrument(span).await
    }
    async fn backoff(&mut self, state: &PollingState) {
        let span = info_span!("LRO Sleep");
        self.inner.backoff(state).instrument(span).await
    }
    async fn until_done(self) -> Result<ResponseType> {
        let span = self.span.clone();
        crate::until_done(self).instrument(span).await
    }
    #[cfg(feature = "unstable-stream")]
    fn into_stream(
        self,
    ) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>> + Unpin {
        let span = self.span.clone();
        crate::into_stream(self).instrument(span)
    }
}
