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

//! Token bucket implementation for request hedging.

use std::sync::atomic::{AtomicU32, Ordering};

pub(crate) const SCALE: u32 = 1_000;

/// A lock-free token bucket used for request hedging.
///
/// Tokens are represented with a fixed-point scale factor of `SCALE = 1000`
/// to allow fractional token refills without floating-point operations in the fast path.
#[derive(Debug)]
pub(crate) struct TokenBucket {
    tokens: AtomicU32,
    max_scaled_tokens: u32,
    refill_amount: u32,
}

impl TokenBucket {
    /// Creates a new `TokenBucket` with maximum tokens and refill ratio.
    ///
    /// The refill amount is rounded.
    ///
    /// Bucket starts empty (0 tokens).
    fn new(max_tokens: u32, refill_ratio: f32) -> Self {
        let max_scaled_tokens = max_tokens * SCALE;
        let refill_amount = (refill_ratio * SCALE as f32).round() as u32;
        Self {
            tokens: AtomicU32::new(0),
            max_scaled_tokens,
            refill_amount,
        }
    }

    /// Attempts to acquire 1 full token (1000 scaled units).
    ///
    /// Returns `true` if a token was acquired, `false` otherwise.
    fn try_acquire(&self) -> bool {
        self.tokens
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if current >= SCALE {
                    Some(current - SCALE)
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Refills the bucket by the configured refill amount.
    ///
    /// Tokens are capped at `max_tokens * SCALE`.
    fn refill(&self) {
        let _ = self
            .tokens
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if current >= self.max_scaled_tokens {
                    None
                } else {
                    Some(
                        current
                            .saturating_add(self.refill_amount)
                            .min(self.max_scaled_tokens),
                    )
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use google_cloud_test_macros::tokio_test_no_panics;
    use tokio::task::{JoinSet, yield_now};

    impl TokenBucket {
        fn available_scaled_tokens(&self) -> u32 {
            self.tokens.load(Ordering::Acquire)
        }
    }

    #[test]
    fn starts_empty_and_cannot_acquire() {
        let bucket = TokenBucket::new(10, 0.1);
        assert_eq!(bucket.available_scaled_tokens(), 0);
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn refill_and_acquire() {
        let bucket = TokenBucket::new(10, 0.1);
        // 0.1 ratio * 1000 = 100 scaled tokens per refill
        bucket.refill();
        assert_eq!(bucket.available_scaled_tokens(), 100);
        assert!(!bucket.try_acquire());

        // 9 more refills = 900 more scaled tokens, total 1000 (1 token)
        for _ in 0..9 {
            bucket.refill();
        }
        assert_eq!(bucket.available_scaled_tokens(), 1000);

        // Acquire 1 token
        assert!(bucket.try_acquire());
        assert_eq!(bucket.available_scaled_tokens(), 0);
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn capped_at_max_tokens() {
        let bucket = TokenBucket::new(2, 0.5); // max 2 tokens = 2000 scaled tokens; 500 per refill
        for _ in 0..5 {
            bucket.refill();
        }
        assert_eq!(bucket.available_scaled_tokens(), 2000);
        for _ in 0..5 {
            bucket.refill();
        }
        assert_eq!(bucket.available_scaled_tokens(), 2000);

        assert!(bucket.try_acquire());
        assert_eq!(bucket.available_scaled_tokens(), 1000);

        assert!(bucket.try_acquire());
        assert_eq!(bucket.available_scaled_tokens(), 0);

        assert!(!bucket.try_acquire());
    }

    #[tokio_test_no_panics(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_refill_and_acquire() {
        use std::sync::Arc;

        let bucket = Arc::new(TokenBucket::new(1000, 1.0));

        let num_refillers = 4;
        let refills_per_task = 250;
        let num_acquirers = 4;

        // Spawn refiller tasks
        let mut refillers = JoinSet::new();
        for _ in 0..num_refillers {
            let bucket = Arc::clone(&bucket);
            refillers.spawn(async move {
                for _ in 0..refills_per_task {
                    bucket.refill();
                }
                yield_now().await;
            });
        }

        // Spawn acquirer tasks
        let mut acquirers = JoinSet::new();
        for _ in 0..num_acquirers {
            let bucket = Arc::clone(&bucket);
            acquirers.spawn(async move {
                let mut total_acquired = 0;
                loop {
                    if total_acquired == refills_per_task {
                        break;
                    }
                    if bucket.try_acquire() {
                        total_acquired += 1;
                    }
                    yield_now().await;
                }
            });
        }

        // Wait for refillers first
        refillers.join_all().await;

        // Wait for acquirers
        acquirers.join_all().await;

        // Drain any remaining tokens
        let mut remaining = 0;
        while bucket.try_acquire() {
            remaining += 1;
        }

        assert_eq!(remaining, 0, "Acquirers should have acquired all tokens");
        assert_eq!(bucket.available_scaled_tokens(), 0);
    }
}
