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

use pin_project::pin_project;

#[pin_project]
#[derive(Debug, Clone)]
pub(crate) struct Instrumented<F> {
    #[pin]
    inner: F,
    details: Vec<String>,
    start: std::time::Instant,
}

impl<F> Instrumented<F> {
    pub fn new(inner: F) -> Self {
        Self {
            inner,
            details: Vec::new(),
            start: std::time::Instant::now(),
        }
    }
}

impl<F, O, E> std::future::Future for Instrumented<F>
where
    F: std::future::Future<Output = std::result::Result<O, E>>,
    E: std::fmt::Debug,
{
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll;

        let this = self.project();
        let d = this.start.elapsed();
        this.details.push(format!("poll / {d:?}"));
        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(v)) => Poll::Ready(Ok(v)),
            Poll::Ready(Err(e)) => {
                tracing::error!("instrumented future got {e:?}: {:?}", this.details);
                Poll::Ready(Err(e))
            }
        }
    }
}
