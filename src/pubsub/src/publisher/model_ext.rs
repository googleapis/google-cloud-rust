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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::sync::oneshot;

/// A handle that represents an in-flight publish operation.
///
/// This struct is a `Future`. You can `.await` it to get the final
/// result of the publish call: either a server-assigned message ID `String`
/// or an `Error` if the publish failed.
pub struct PublishHandle {
    pub(crate) rx: oneshot::Receiver<Result<String, crate::Error>>,
}

impl Future for PublishHandle {
    type Output = crate::Result<String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(Pin::new(&mut self.rx).poll(cx));
        Poll::Ready(match result {
            Ok(res) => res,
            // This error will only occur if the sender of the self.rx was dropped,
            // which would be a bug.
            Err(_) => Err(crate::Error::deser("unable to get message id")),
        })
    }
}
