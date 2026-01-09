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

use crate::google::pubsub::v1::StreamingPullRequest;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, interval_at};
use tokio_util::sync::CancellationToken;

pub(super) const KEEPALIVE_PERIOD: Duration = Duration::from_secs(30);

/// Spawns a task to keepalive a stream
///
/// This task periodically writes requests into a channel. The receiver of this
/// channel is the request stream for a StreamingPull bidi RPC.
///
/// Callers may signal a graceful shutdown of this task by cancelling the
/// `CancellationToken` and `await`ing the returned handle.
///
/// Callers can also just drop the returned handle to shutdown.
pub(super) fn spawn(
    request_tx: Sender<StreamingPullRequest>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut keepalive = interval_at(Instant::now() + KEEPALIVE_PERIOD, KEEPALIVE_PERIOD);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = keepalive.tick() => {
                    let _ = request_tx.send(StreamingPullRequest::default()).await;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;

    #[tokio::test(start_paused = true)]
    async fn keepalive_interval() {
        let start = Instant::now();
        let (request_tx, mut request_rx) = channel(1);
        let shutdown = CancellationToken::new();
        let _handle = spawn(request_tx, shutdown);

        // Wait for the first keepalive
        let r = request_rx.recv().await.unwrap();
        assert_eq!(r, StreamingPullRequest::default());
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD);

        // Wait for the second keepalive
        let r = request_rx.recv().await.unwrap();
        assert_eq!(r, StreamingPullRequest::default());
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD * 2);

        // Wait for the third keepalive
        let r = request_rx.recv().await.unwrap();
        assert_eq!(r, StreamingPullRequest::default());
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD * 3);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_immediately() -> anyhow::Result<()> {
        let start = Instant::now();
        let (request_tx, mut request_rx) = channel(1);
        let shutdown = CancellationToken::new();
        let handle = spawn(request_tx, shutdown.clone());

        // Wait for the first keepalive
        let _ = request_rx.recv().await.unwrap();
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD);

        // Simulate the loop running for a bit.
        const DELTA: Duration = Duration::from_secs(10);
        tokio::time::advance(DELTA).await;

        // Shutdown the task
        shutdown.cancel();
        handle.await?;

        // Verify that we did not wait for the full keepalive interval.
        assert_eq!(start.elapsed(), KEEPALIVE_PERIOD + DELTA);
        Ok(())
    }
}
