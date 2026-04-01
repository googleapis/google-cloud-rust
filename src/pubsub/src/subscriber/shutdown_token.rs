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

use futures::future::{BoxFuture, Shared};
use tokio_util::sync::CancellationToken;

/// A token to signal and await shutdown of a stream.
///
/// # Example
/// ```no_rust
/// use google_cloud_pubsub::subscriber::MessageStream;
/// async fn sample(stream: MessageStream) {
///   // Get a shutdown token for the stream.
///   let token = stream.shutdown_token();
///
///   // Signal and await a shutdown of the stream.
///   token.shutdown().await;
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShutdownToken {
    pub(super) inner: CancellationToken,
    pub(super) fut: Shared<BoxFuture<'static, ()>>,
}

impl ShutdownToken {
    /// Signal and await a stream shutdown.
    ///
    /// Applications should call this to ensure all pending ack/nack RPCs have
    /// time to complete before a process exits.
    ///
    /// See [`Subscribe::set_shutdown_behavior`][setter] to configure the exact
    /// behavior on shutdown.
    ///
    /// [setter]: crate::builder::subscriber::Subscribe::set_shutdown_behavior
    pub async fn shutdown(&self) {
        self.inner.cancel();
        self.fut.clone().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use tokio::sync::oneshot::channel;

    #[tokio::test(start_paused = true)]
    async fn shutdown() -> anyhow::Result<()> {
        let (tx, rx) = channel();
        let fut = rx.map(|_| ()).boxed().shared();

        let token = ShutdownToken {
            inner: CancellationToken::new(),
            fut,
        };
        assert!(!token.inner.is_cancelled(), "{token:?}");
        assert!(token.fut.peek().is_none(), "future should be pending");

        let token_clone = token.clone();
        assert!(!token_clone.inner.is_cancelled(), "{token_clone:?}");
        assert!(token_clone.fut.peek().is_none(), "future should be pending");

        let handle = tokio::spawn(async move {
            token_clone.shutdown().await;
            assert!(token_clone.inner.is_cancelled(), "{token_clone:?}");
            assert!(
                token_clone.fut.peek().is_some(),
                "future should be satisfied"
            );
        });
        tokio::task::yield_now().await;

        assert!(token.inner.is_cancelled(), "{token:?}");
        assert!(token.fut.peek().is_none(), "future should be pending");

        // Satisfy the future
        let _ = tx.send(());
        handle.await?;
        assert!(token.inner.is_cancelled(), "{token:?}");
        assert!(token.fut.peek().is_some(), "future should be satisfied");

        // A second shutdown is a no-op.
        token.shutdown().await;
        assert!(token.inner.is_cancelled(), "{token:?}");
        assert!(token.fut.peek().is_some(), "future should be satisfied");

        Ok(())
    }
}
