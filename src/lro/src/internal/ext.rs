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

use super::either::Either;
use super::tracing::Tracing;
use crate::Poller;

/// Details for tracing a poller.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct TracingDetails {
    pub method_name: &'static str,
}

/// Options for creating a new poller.
#[derive(Default)]
#[non_exhaustive]
pub struct PollerOptions {
    pub tracing: Option<TracingDetails>,
}

pub trait PollerExt<ResponseType, MetadataType> {
    fn with_options(self, options: PollerOptions) -> impl Poller<ResponseType, MetadataType>;
}

impl<ResponseType, MetadataType, T> PollerExt<ResponseType, MetadataType> for T
where
    T: Poller<ResponseType, MetadataType>,
    ResponseType: Send,
    MetadataType: Send,
{
    fn with_options(self, options: PollerOptions) -> impl Poller<ResponseType, MetadataType> {
        if let Some(t) = options.tracing {
            let method_name = if t.method_name.is_empty() {
                "google_longrunning::Operations/Wait"
            } else {
                t.method_name
            };
            let span = tracing::info_span!(
                "LRO Wait",
                "otel.name" = method_name,
                "gcp.rpc.method" = method_name,
                "gcp.resource.destination.id" = tracing::field::Empty,
                "otel.status_code" = tracing::field::Empty,
                "otel.status_description" = tracing::field::Empty
            );
            let traced = Tracing::new(self, span);
            return Either::Right(traced);
        }
        Either::Left(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PollingResult, sealed};
    use google_cloud_gax::polling_state::PollingState;
    use google_cloud_wkt::{Duration, Timestamp};
    use mockall::mock;

    type ResponseType = Duration;
    type MetadataType = Timestamp;

    mock! {
        PollerA {}
        impl sealed::Poller for PollerA {
            async fn backoff(&mut self, state: &PollingState);
        }
        impl Poller<ResponseType, MetadataType> for PollerA {
            async fn poll(&mut self) -> Option<PollingResult<ResponseType, MetadataType>>;
            async fn until_done(self) -> google_cloud_gax::Result<ResponseType>;
            #[cfg(feature = "unstable-stream")]
            fn into_stream(
                self,
            ) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>> + Unpin;
        }
    }

    #[cfg(google_cloud_unstable_tracing)]
    #[test]
    fn test_poller_initialization_with_tracing() {
        let mock = MockPollerA::new();
        let _poller = mock.with_options(PollerOptions {
            tracing: Some(TracingDetails {
                method_name: "test_method",
            }),
        });
    }
}
