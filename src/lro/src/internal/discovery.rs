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

//! This module implements LROs for discovery-based client libraries.
//!
//! The discovery-based services use a different (older) form of LROs, where the
//! "Operation" type does not include the final result, and the errors, if any,
//! are not represented using the `google.rpc.Status` proto.

use super::{Poller, PollingBackoffPolicy, PollingErrorPolicy, PollingResult, Result};
use gax::polling_state::PollingState;
use gax::retry_result::RetryResult;
use std::sync::Arc;

pub trait DiscoveryOperation {
    fn name(&self) -> Option<&String>;
    fn done(&self) -> bool;
    fn error(&self) -> Option<gax::error::Error>;
}

pub fn new_poller<S, SF, Q, QF, O, E>(
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: S,
    query: Q,
) -> impl Poller<O, O>
where
    O: DiscoveryOperation + Send,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<O>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<O>> + Send + 'static,
{
    DiscoveryPoller::new(polling_error_policy, polling_backoff_policy, start, query)
}

struct DiscoveryPoller<S, Q> {
    error_policy: Arc<dyn PollingErrorPolicy>,
    backoff_policy: Arc<dyn PollingBackoffPolicy>,
    start: Option<S>,
    query: Q,
    operation: Option<String>,
    state: PollingState,
}

impl<S, Q> DiscoveryPoller<S, Q> {
    pub fn new(
        error_policy: Arc<dyn PollingErrorPolicy>,
        backoff_policy: Arc<dyn PollingBackoffPolicy>,
        start: S,
        query: Q,
    ) -> Self {
        Self {
            error_policy,
            backoff_policy,
            start: Some(start),
            query,
            operation: None,
            state: PollingState::default(),
        }
    }
}

impl<S, Q> crate::sealed::Poller for DiscoveryPoller<S, Q> {}

impl<O, S, SF, Q, QF> super::Poller<O, O> for DiscoveryPoller<S, Q>
where
    O: DiscoveryOperation + Send,
    S: FnOnce() -> SF + Send + Sync,
    SF: std::future::Future<Output = Result<O>> + Send + 'static,
    Q: Fn(String) -> QF + Send + Sync + Clone,
    QF: std::future::Future<Output = Result<O>> + Send + 'static,
{
    async fn poll(&mut self) -> Option<PollingResult<O, O>> {
        if let Some(start) = self.start.take() {
            let result = start().await;
            let (op, poll) = self::handle_start(result);
            self.operation = op;
            return Some(poll);
        }
        if let Some(name) = self.operation.take() {
            self.state.attempt_count += 1;
            let result = (self.query)(name.clone()).await;
            let (op, poll) =
                self::handle_poll(self.error_policy.clone(), &self.state, name, result);
            self.operation = op;
            return Some(poll);
        }
        None
    }

    async fn until_done(mut self) -> Result<O> {
        let mut state = PollingState::default();
        while let Some(p) = self.poll().await {
            match p {
                // Return, the operation completed or the polling policy is
                // exhausted.
                PollingResult::Completed(r) => return r,
                // Continue, the operation was successfully polled and the
                // polling policy was queried.
                PollingResult::InProgress(_) => (),
                // Continue, the polling policy was queried and decided the
                // error is recoverable.
                PollingResult::PollingError(_) => (),
            }
            state.attempt_count += 1;
            tokio::time::sleep(self.backoff_policy.wait_period(&state)).await;
        }
        // We can only get here if `poll()` returns `None`, but it only returns
        // `None` after it returned `Polling::Completed` and therefore this is
        // never reached.
        unreachable!("loop should exit via the `Completed` branch vs. this line");
    }

    #[cfg(feature = "unstable-stream")]
    fn into_stream(self) -> impl futures::Stream<Item = PollingResult<O, O>> + Unpin {
        use futures::stream::unfold;
        Box::pin(unfold(Some(self), move |state| async move {
            if let Some(mut poller) = state {
                if let Some(pr) = poller.poll().await {
                    return Some((pr, Some(poller)));
                }
            };
            None
        }))
    }
}

fn handle_start<O>(result: Result<O>) -> (Option<String>, PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    match result {
        Err(ref _e) => (None, PollingResult::Completed(result)),
        Ok(ref o) if o.done() => (None, PollingResult::Completed(result)),
        Ok(ref o) if o.error().is_some() => {
            (None, PollingResult::Completed(Err(o.error().unwrap())))
        }
        Ok(o) => (o.name().cloned(), PollingResult::InProgress(Some(o))),
    }
}

fn handle_poll<O>(
    error_policy: Arc<dyn PollingErrorPolicy>,
    state: &PollingState,
    operation_name: String,
    result: Result<O>,
) -> (Option<String>, PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    match result {
        Err(e) => {
            let state = error_policy.on_error(state, e);
            self::handle_polling_error(state, operation_name)
        }
        Ok(ref o) if o.done() => (None, PollingResult::Completed(result)),
        Ok(ref o) if o.error().is_some() => {
            (None, PollingResult::Completed(Err(o.error().unwrap())))
        }
        Ok(o) => (o.name().cloned(), PollingResult::InProgress(Some(o))),
    }
}

fn handle_polling_error<O>(
    state: gax::retry_result::RetryResult,
    operation_name: String,
) -> (Option<String>, PollingResult<O, O>)
where
    O: DiscoveryOperation,
{
    match state {
        RetryResult::Continue(e) => (Some(operation_name), PollingResult::PollingError(e)),
        RetryResult::Exhausted(e) | RetryResult::Permanent(e) => {
            (None, PollingResult::Completed(Err(e)))
        }
    }
}
