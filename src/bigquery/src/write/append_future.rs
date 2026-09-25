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

use super::append_response::AppendResponse;
use super::error::{AppendError, AppendResult};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// A future that resolves to the result of an async append operation.
///
/// This future represents a write request that has already been queued by the
/// client library to send over the network. Awaiting this future yields the server's acknowledgment
/// or an error if the write fails.
///
/// The underlying operation begins immediately and runs independently in the
/// background, even if this future is dropped or never awaited.
#[derive(Debug)]
pub struct AppendFuture {
    inner: Inner,
}

enum Inner {
    Rx(oneshot::Receiver<AppendResult<AppendResponse>>),
    Boxed(Pin<Box<dyn Future<Output = AppendResult<AppendResponse>> + Send>>),
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rx(rx) => f.debug_tuple("Rx").field(rx).finish(),
            Self::Boxed(_) => f.debug_tuple("Boxed").finish(),
        }
    }
}

impl AppendFuture {
    pub(crate) fn new(rx: oneshot::Receiver<AppendResult<AppendResponse>>) -> Self {
        Self {
            inner: Inner::Rx(rx),
        }
    }

    pub(crate) fn from_future<F>(fut: F) -> Self
    where
        F: Future<Output = AppendResult<AppendResponse>> + Send + 'static,
    {
        Self {
            inner: Inner::Boxed(Box::pin(fut)),
        }
    }
}

impl Future for AppendFuture {
    type Output = AppendResult<AppendResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.inner {
            Inner::Rx(rx) => {
                let result = std::task::ready!(Pin::new(rx).poll(cx));
                match result {
                    Ok(res) => Poll::Ready(res),
                    Err(_) => Poll::Ready(Err(AppendError::UnexpectedEndOfStream)),
                }
            }
            Inner::Boxed(fut) => fut.as_mut().poll(cx),
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

    #[tokio::test]
    async fn from_future_success() {
        let future = AppendFuture::from_future(async {
            Ok(AppendResponse {
                offset: Some(7),
                updated_schema: None,
            })
        });
        let resp = future.await.expect("should succeed");
        assert_eq!(resp.offset, Some(7));
    }

    #[tokio::test]
    async fn from_future_error() {
        let future = AppendFuture::from_future(async { Err(AppendError::UnexpectedEndOfStream) });
        let err = future.await.expect_err("should return error");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));
    }

    #[test]
    fn debug_format() {
        let (_, rx) = oneshot::channel();
        let future_rx = AppendFuture::new(rx);
        assert!(format!("{future_rx:?}").contains("Rx"));

        let future_boxed = AppendFuture::from_future(async { Ok(AppendResponse::default()) });
        assert!(format!("{future_boxed:?}").contains("Boxed"));
    }
}
