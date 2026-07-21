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

//! Types for gRPC streaming requests and responses.

use tokio::sync::mpsc;

/// A handle for sending outbound request items over a gRPC stream.
#[derive(Clone, Debug)]
pub struct RequestSender<Req> {
    req_tx: mpsc::Sender<Req>,
}

impl<Req> RequestSender<Req> {
    /// Creates a new [`RequestSender`].
    pub fn new(req_tx: mpsc::Sender<Req>) -> Self {
        Self { req_tx }
    }

    /// Sends a request item over the stream.
    pub async fn send(&self, item: Req) -> Result<(), crate::error::Error>
    where
        Req: Send + Sync + 'static,
    {
        self.req_tx
            .send(item)
            .await
            .map_err(crate::error::Error::io)
    }
}

/// A handle for receiving inbound response items from a gRPC stream.
#[derive(Debug)]
pub struct ResponseReceiver<Resp> {
    rx: mpsc::Receiver<crate::Result<Resp>>,
}

impl<Resp> ResponseReceiver<Resp> {
    /// Creates a new [`ResponseReceiver`].
    pub fn new(rx: mpsc::Receiver<crate::Result<Resp>>) -> Self {
        Self { rx }
    }

    /// Receives the next response item from the stream.
    pub async fn recv(&mut self) -> Option<crate::Result<Resp>> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request_sender_and_response_receiver() {
        let (req_tx, mut req_rx) = mpsc::channel::<String>(16);
        let (resp_tx, resp_rx) = mpsc::channel::<crate::Result<String>>(16);

        let sender = RequestSender::new(req_tx);
        let mut receiver = ResponseReceiver::new(resp_rx);

        sender.send("hello".to_string()).await.unwrap();
        assert_eq!(req_rx.recv().await.unwrap(), "hello");

        resp_tx.send(Ok("world".to_string())).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap().unwrap(), "world");

        drop(resp_tx);
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_request_sender_send_error() {
        use std::error::Error as _;

        let (req_tx, req_rx) = mpsc::channel::<String>(16);
        let sender = RequestSender::new(req_tx);

        drop(req_rx);
        let err = sender.send("hello".to_string()).await.unwrap_err();
        assert!(err.is_io());
        assert!(err.source().is_some());
    }
}
