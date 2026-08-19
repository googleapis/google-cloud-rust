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

use crate::error::{AppendError, AppendResult};
use crate::model::AppendResponse;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// A future that resolves to the result of an async append operation.
///
/// This future represents a write request that has already been queued by the
/// client library to send over the network. Awaiting this future yields the server's acknowledgment
/// or an error if the write fails.
#[derive(Debug)]
pub struct AppendFuture {
    rx: oneshot::Receiver<AppendResult<AppendResponse>>,
}

impl AppendFuture {
    #[allow(dead_code)]
    pub(crate) fn new(rx: oneshot::Receiver<AppendResult<AppendResponse>>) -> Self {
        Self { rx }
    }
}

impl Future for AppendFuture {
    type Output = AppendResult<AppendResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = std::task::ready!(Pin::new(&mut self.rx).poll(cx));
        match result {
            Ok(res) => Poll::Ready(res),
            Err(_) => Poll::Ready(Err(AppendError::UnexpectedEndOfStream)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TableSchema;

    #[tokio::test]
    async fn happy_path() {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(AppendResponse {
            offset: None,
            updated_schema: Some(TableSchema::default()),
        }));
        let future = AppendFuture::new(rx);
        let resp = future.await.expect("should succeed");
        assert_eq!(resp.offset, None);
        assert_eq!(resp.updated_schema, Some(TableSchema::default()));
    }

    #[tokio::test]
    async fn dropped_sender() {
        let (tx, rx) = oneshot::channel::<AppendResult<AppendResponse>>();
        // Drop the sender immediately
        drop(tx);

        let future = AppendFuture::new(rx);
        let err = future
            .await
            .expect_err("should return unexpected end of stream");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));
    }

    #[tokio::test]
    async fn channel_returns_error() {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(AppendError::UnexpectedEndOfStream));
        let future = AppendFuture::new(rx);
        let err = future.await.expect_err("should return error from task");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));
    }
}
