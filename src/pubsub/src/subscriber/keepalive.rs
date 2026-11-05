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
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, interval_at};
use tokio_util::sync::CancellationToken;

pub(super) const KEEPALIVE_PERIOD: Duration = Duration::from_secs(30);
pub(super) const PONG_TIMEOUT: Duration = Duration::from_secs(15);

/// Spawns a task to keepalive a stream
///
/// This task periodically writes requests into a channel. The receiver of this
/// channel is the request stream for a StreamingPull bidi RPC.
///
/// When a keepalive ping is sent and no unanswered ping deadline is currently
/// pending, this task sets `pong_deadline_tx` to
/// `Some(Instant::now() + PONG_TIMEOUT)`.
///
/// Callers may signal a graceful shutdown of this task by cancelling the
/// `CancellationToken` and `await`ing the returned handle.
///
/// Callers can also just drop the returned handle to shutdown.
pub(super) fn spawn(
    request_tx: Sender<StreamingPullRequest>,
    pong_deadline_tx: watch::Sender<Option<Instant>>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let mut keepalive = interval_at(Instant::now() + KEEPALIVE_PERIOD, KEEPALIVE_PERIOD);
    tokio::spawn(async move {
        let keepalive_loop = async {
            loop {
                keepalive.tick().await;
                // Set the deadline before sending the ping so we don't race with
                // an immediate response clearing the deadline, and so we also
                // time out if the stream stops draining requests.
                pong_deadline_tx.send_if_modified(|deadline| {
                    if deadline.is_none() {
                        *deadline = Some(Instant::now() + PONG_TIMEOUT);
                        true
                    } else {
                        false
                    }
                });
                let _ = request_tx.send(StreamingPullRequest::default()).await;
            }
        };
        tokio::select! {
            _ = shutdown.cancelled() => (),
            _ = keepalive_loop => (),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_test_macros::tokio_test_no_panics;
    use tokio::sync::mpsc::channel;

    #[tokio_test_no_panics(start_paused = true)]
    async fn keepalive_interval() {
        let start = Instant::now();
        let (request_tx, mut request_rx) = channel(1);
        let (pong_deadline_tx, _pong_deadline_rx) = watch::channel(None);
        let shutdown = CancellationToken::new();
        let _handle = spawn(request_tx, pong_deadline_tx, shutdown);

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

    #[tokio_test_no_panics(start_paused = true)]
    async fn sets_and_preserves_pong_deadline() {
        let start = Instant::now();
        let (request_tx, mut request_rx) = channel(1);
        let (pong_deadline_tx, pong_deadline_rx) = watch::channel(None);
        let shutdown = CancellationToken::new();
        let _handle = spawn(request_tx, pong_deadline_tx.clone(), shutdown);

        assert_eq!(*pong_deadline_rx.borrow(), None);

        // First ping at 30s sets the deadline to 30s + 15s = 45s.
        let _ = request_rx.recv().await.unwrap();
        assert_eq!(
            *pong_deadline_rx.borrow(),
            Some(start + KEEPALIVE_PERIOD + PONG_TIMEOUT)
        );

        // Second ping at 60s does not overwrite an unanswered deadline.
        let _ = request_rx.recv().await.unwrap();
        assert_eq!(
            *pong_deadline_rx.borrow(),
            Some(start + KEEPALIVE_PERIOD + PONG_TIMEOUT)
        );

        // Clearing the deadline allows the third ping at 90s to set a new deadline.
        pong_deadline_tx.send_replace(None);
        let _ = request_rx.recv().await.unwrap();
        assert_eq!(
            *pong_deadline_rx.borrow(),
            Some(start + KEEPALIVE_PERIOD * 3 + PONG_TIMEOUT)
        );
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn shutdown_immediately() -> anyhow::Result<()> {
        let start = Instant::now();
        let (request_tx, mut request_rx) = channel(1);
        let (pong_deadline_tx, _pong_deadline_rx) = watch::channel(None);
        let shutdown = CancellationToken::new();
        let handle = spawn(request_tx, pong_deadline_tx, shutdown.clone());

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
