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

use super::TransportMetric;
use google_cloud_gax::error::Error;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Implements a decorator for [Future] adding duration metrics for transport attempts.
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct WithTransportMetric<F> {
    #[pin]
    inner: F,
    metric: TransportMetric,
    attempt_count: u32,
}

impl<F, R> WithTransportMetric<F>
where
    F: Future<Output = Result<R, Error>>,
{
    pub fn new(metric: TransportMetric, inner: F, attempt_count: u32) -> Self {
        Self {
            metric,
            inner,
            attempt_count,
        }
    }
}

impl<F, R> Future for WithTransportMetric<F>
where
    F: Future<Output = Result<R, Error>>,
{
    type Output = <F as Future>::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let attempt_count = *this.attempt_count;
        let output = futures::ready!(this.inner.poll(cx));
        match &output {
            Ok(_) => this.metric.with_recorder_ok(attempt_count),
            Err(error) => this.metric.with_recorder_error(error, attempt_count),
        }
        Poll::Ready(output)
    }
}
