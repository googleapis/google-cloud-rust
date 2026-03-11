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

use tokio_util::sync::CancellationToken;

/// A token to signal shutdown of a stream.
///
/// # Example
/// ```
/// use google_cloud_pubsub::client::Subscriber;
/// # use google_cloud_pubsub::subscriber::ShutdownToken;
/// # async fn sample() -> anyhow::Result<()> {
/// let client = Subscriber::builder().build().await?;
///
/// // Create a stream that can be shutdown gracefully.
/// let token = ShutdownToken::new();
/// let stream = client.subscribe("projects/my-project/subscriptions/my-subscription")
///     .set_shutdown_token(token.clone())
///     .build();
///
/// // Signal a shutdown of the stream.
/// token.shutdown();
/// # Ok(()) }
/// ```
///
/// Conceptually, this is a [`CancellationToken`]. In fact, it is implemented by
/// one.
///
/// Provide a clone of this token to [`Subscribe::set_shutdown_token`] when
/// creating a message stream. Signal a shutdown by calling
/// [`ShutdownToken::shutdown()`].
#[derive(Clone, Debug)]
pub struct ShutdownToken {
    pub(super) inner: CancellationToken,
}

impl ShutdownToken {
    /// Create a shutdown token.
    pub fn new() -> Self {
        Self {
            inner: CancellationToken::new(),
        }
    }

    /// Signal a shutdown of a stream.
    pub fn shutdown(&self) {
        self.inner.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown() {
        let token = ShutdownToken::new();
        assert!(!token.inner.is_cancelled(), "{token:?}");

        let token_clone = token.clone();
        assert!(!token_clone.inner.is_cancelled(), "{token_clone:?}");

        token.shutdown();
        assert!(token.inner.is_cancelled(), "{token:?}");
        assert!(token_clone.inner.is_cancelled(), "{token_clone:?}");

        token_clone.shutdown();
        assert!(token.inner.is_cancelled(), "{token:?}");
        assert!(token_clone.inner.is_cancelled(), "{token_clone:?}");
    }
}
