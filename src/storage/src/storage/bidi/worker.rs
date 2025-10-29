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

use super::connector::{Connection, Connector};
use super::pending_range::PendingRange;
use crate::error::ReadError;
use crate::google::storage::v2::{BidiReadObjectRequest, BidiReadObjectResponse, ObjectRangeData};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Receiver;

type ReadResult<T> = std::result::Result<T, ReadError>;

#[derive(Debug)]
pub struct Worker {
    next_range_id: i64,
    ranges: Arc<Mutex<HashMap<i64, PendingRange>>>,
    connection: Connection,
}

impl Worker {
    pub fn new(connection: Connection) -> Self {
        let ranges = Arc::new(Mutex::new(HashMap::new()));
        Self {
            next_range_id: 0_i64,
            ranges,
            connection,
        }
    }

    pub async fn run<T>(mut self, mut connector: Connector<T>, mut rx: Receiver<PendingRange>)
    where
        T: super::connector::Client<Stream = tonic::Streaming<BidiReadObjectResponse>>
            + Clone
            + Sync,
    {
        println!("DEBUG DEBUG - run_background() {self:?}");
        loop {
            tokio::select! {
                m = self.next_message(&mut connector) => {
                    let Some(message) = m else {
                        break;
                    };
                    if let Err(e) = self.handle_response(message).await {
                        // An error in the response. These are not recoverable.
                        self.close_readers(Arc::new(e)).await;
                        return;
                    }
                },
                r = rx.recv() => {
                    let Some(range) = r else {
                        println!("DEBUG DEBUG - run_background() {self:?} shutdown");
                        return;
                    };
                    self.insert_range(range).await;
                },
            }
        }
        println!("DEBUG DEBUG - run_background() END");
    }

    async fn next_message<T>(
        &mut self,
        connector: &mut Connector<T>,
    ) -> Option<BidiReadObjectResponse>
    where
        T: super::connector::Client<Stream = tonic::Streaming<BidiReadObjectResponse>>
            + Clone
            + Sync,
    {
        println!("DEBUG DEBUG - State::next_message()");
        let message = self.connection.rx.message().await;
        println!("DEBUG DEBUG - State::next_message() = {message:?}");
        let status = match message {
            Ok(m) => return m,
            Err(status) => {
                println!("error reading from bi-di stream: {status:?}");
                status
            }
        };
        let ranges: Vec<_> = self
            .ranges
            .lock()
            .await
            .iter()
            .map(|(id, r)| r.as_proto(*id))
            .collect();
        match connector.reconnect(status, ranges).await {
            Err(e) => {
                self.close_readers(Arc::new(e)).await;
                None
            }
            Ok((m, connection)) => {
                self.connection = connection;
                Some(m)
            }
        }
    }

    async fn close_readers(&mut self, error: Arc<crate::Error>) {
        let mut guard = self.ranges.lock().await;
        let closing: Vec<_> = guard
            .iter_mut()
            .map(|(_, pending)| pending.interrupted(error.clone()))
            .collect();
        let _ = futures::future::join_all(closing).await;
    }

    async fn insert_range(&mut self, range: PendingRange) {
        println!("DEBUG DEBUG - State::next_message() - {range:?}");
        let id = self.next_range_id;
        self.next_range_id += 1;

        let request = range.as_proto(id);
        self.ranges.lock().await.insert(id, range);
        let request = BidiReadObjectRequest {
            read_ranges: vec![request],
            ..BidiReadObjectRequest::default()
        };
        // Any errors here are recovered by the main background loop.
        if let Err(e) = self.connection.tx.send(request).await {
            tracing::error!("error sending read range request: {e:?}");
        }
    }

    async fn handle_response(&mut self, message: BidiReadObjectResponse) -> crate::Result<()> {
        println!("DEBUG DEBUG - handle_response() {self:?} message = {message:?}");
        let ranges = self.ranges.clone();
        let pending = message
            .object_data_ranges
            .into_iter()
            .map(|r| Self::handle_range_data(ranges.clone(), r))
            .collect::<Vec<_>>();
        let _ = futures::future::join_all(pending)
            .await
            .into_iter()
            .collect::<ReadResult<Vec<_>>>()
            .map_err(crate::Error::io)?; // TODO: think about the error type
        Ok(())
    }

    async fn handle_range_data(
        ranges: Arc<Mutex<HashMap<i64, PendingRange>>>,
        response: ObjectRangeData,
    ) -> ReadResult<()> {
        let range = response
            .read_range
            .ok_or(ReadError::MissingRangeInBidiResponse)?;
        if response.range_end {
            let mut pending = ranges
                .lock()
                .await
                .remove(&range.read_id)
                .ok_or(ReadError::UnknownRange(range.read_id))?;
            pending.handle_data(range, response.checksummed_data).await
        } else {
            let mut guard = ranges.lock().await;
            let pending = guard
                .get_mut(&range.read_id)
                .ok_or(ReadError::UnknownRange(range.read_id))?;
            pending.handle_data(range, response.checksummed_data).await
        }
    }
}
