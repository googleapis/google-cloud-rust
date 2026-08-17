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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;

use crate::error::{AppendError, AppendResult};
use crate::model::AppendResponse;

/// A future that resolves to the result of an async append operation.
///
/// This future represents a write request that has already been dispatched to the
/// background network task. Awaiting this future yields the server's acknowledgment
/// or an error if the stream fails.
#[derive(Debug)]
pub struct AppendFuture {
    handle: JoinHandle<AppendResult<AppendResponse>>,
}

impl AppendFuture {
    pub(crate) fn new(handle: JoinHandle<AppendResult<AppendResponse>>) -> Self {
        Self { handle }
    }
}

impl Future for AppendFuture {
    type Output = AppendResult<AppendResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = std::task::ready!(Pin::new(&mut self.handle).poll(cx));
        match result {
            Ok(res) => Poll::Ready(res),
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    // Task was cancelled or dropped.
                    Poll::Ready(Err(AppendError::UnexpectedEndOfStream))
                }
            }
        }
    }
}
