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

use crate::Result;
use crate::google::spanner::v1::BatchWriteResponse;
use crate::google::spanner::v1::CacheUpdate as ProtoCacheUpdate;
use crate::google::spanner::v1::PartialResultSet;
use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::Streaming;
use http::HeaderMap;
use std::any::Any;
use std::fmt::{Debug, Formatter, Result as FmtResult};

/// Type alias for opaque stream lifetime drop guards.
pub(crate) type StreamLifetimeGuard = Box<dyn Any + Send + Sync>;

/// Generic wrapper around gRPC server-streaming responses with lifetime management.
#[derive(Debug)]
pub(crate) struct SpannerServerStream<T> {
    pub(crate) inner: Streaming<T>,
    pub(crate) headers: HeaderMap,
    pub(crate) lifetime_guard: Option<StreamLifetimeGuard>,
    pub(crate) on_first_transaction_id: Option<TransactionIdCallback>,
}

impl<T> SpannerServerStream<T> {
    pub(crate) fn new(inner: Streaming<T>, headers: HeaderMap) -> Self {
        Self {
            inner,
            headers,
            lifetime_guard: None,
            on_first_transaction_id: None,
        }
    }

    /// Attaches an opaque RAII lifetime guard that remains alive for the duration of the stream.
    #[allow(dead_code)]
    pub(crate) fn with_lifetime_guard(mut self, guard: StreamLifetimeGuard) -> Self {
        self.lifetime_guard = Some(guard);
        self
    }

    /// Attaches a callback to invoke when the first non-empty transaction ID arrives in the stream.
    pub(crate) fn with_transaction_id_callback<C: Into<Option<TransactionIdCallback>>>(
        mut self,
        callback: C,
    ) -> Self {
        self.on_first_transaction_id = callback.into();
        self
    }

    /// Returns the initial response headers for the stream.
    pub(crate) fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

impl<T: StreamTransactionIdExtractor> SpannerServerStream<T> {
    /// Fetches the next message from the stream.
    ///
    /// Returns `Some(Ok(message))` when a message is successfully received,
    /// `None` when the stream concludes naturally, or `Some(Err(_))` on RPC errors.
    /// Drops the attached lifetime guard on EOF or stream errors to release active in-flight accounting early.
    pub(crate) async fn next_message(&mut self) -> Option<Result<T>> {
        let message = match self.inner.message().await.map_err(to_gax_error).transpose() {
            Some(Ok(message)) => message,
            other => {
                self.lifetime_guard = None;
                self.on_first_transaction_id = None;
                return other;
            }
        };

        if self.on_first_transaction_id.is_some()
            && let Some(transaction_id) = message.extract_transaction_id()
            && let Some(callback) = self.on_first_transaction_id.take()
        {
            callback.call(transaction_id);
        }

        Some(Ok(message))
    }
}

/// Trait implemented by stream response message types capable of carrying a transaction ID.
pub(crate) trait StreamTransactionIdExtractor {
    fn extract_transaction_id(&self) -> Option<&[u8]> {
        None
    }
}

impl StreamTransactionIdExtractor for PartialResultSet {
    fn extract_transaction_id(&self) -> Option<&[u8]> {
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.transaction.as_ref())
            .map(|transaction| transaction.id.as_ref())
            .filter(|id| !id.is_empty())
    }
}

impl StreamTransactionIdExtractor for BatchWriteResponse {}

impl StreamTransactionIdExtractor for ProtoCacheUpdate {}

type TransactionIdCallbackFn = dyn FnOnce(&[u8]) + Send + Sync;

/// Callback invoked when a stream receives its first non-empty transaction ID.
pub(crate) struct TransactionIdCallback(Box<TransactionIdCallbackFn>);

impl TransactionIdCallback {
    pub(crate) fn new<F>(callback: F) -> Self
    where
        F: FnOnce(&[u8]) + Send + Sync + 'static,
    {
        Self(Box::new(callback))
    }

    pub(crate) fn call(self, transaction_id: &[u8]) {
        (self.0)(transaction_id);
    }
}

impl Debug for TransactionIdCallback {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("TransactionIdCallback")
    }
}

/// Representation for the `ExecuteStreamingSql` RPC stream.
pub(crate) type PartialResultSetStream = SpannerServerStream<PartialResultSet>;

pub(crate) type BatchWriteStream = SpannerServerStream<BatchWriteResponse>;

/// Representation for the `FetchCacheUpdate` RPC stream.
pub(crate) type CacheUpdateStream = SpannerServerStream<ProtoCacheUpdate>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ExecuteSqlRequest;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use gaxi::grpc::tonic::{Response, Status};
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_test_macros::tokio_test_no_panics;
    use spanner_grpc_mock::google::spanner::v1::{
        PartialResultSet as MockPartialResultSet, ResultSetMetadata as MockResultSetMetadata,
        Transaction as MockTransaction,
    };
    use std::fmt::Debug;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(PartialResultSetStream: Send, Sync, Debug);
        static_assertions::assert_impl_all!(BatchWriteStream: Send, Sync, Debug);
        static_assertions::assert_impl_all!(CacheUpdateStream: Send, Sync, Debug);
        static_assertions::assert_impl_all!(TransactionIdCallback: Send, Sync, Debug);
    }

    #[test]
    fn transaction_id_callback_debug_format() {
        let callback = TransactionIdCallback::new(|_| {});
        assert_eq!(
            format!("{callback:?}"),
            "TransactionIdCallback",
            "TransactionIdCallback debug representation must match expected name"
        );
    }

    #[test]
    fn default_extract_transaction_id_is_none() {
        assert!(
            BatchWriteResponse::default()
                .extract_transaction_id()
                .is_none(),
            "BatchWriteResponse must return None from default extract_transaction_id"
        );
        assert!(
            ProtoCacheUpdate::default()
                .extract_transaction_id()
                .is_none(),
            "ProtoCacheUpdate must return None from default extract_transaction_id"
        );
    }

    struct TestDropGuard {
        dropped: Arc<AtomicBool>,
    }

    impl Drop for TestDropGuard {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    #[tokio_test_no_panics]
    async fn stream_drop_releases_lifetime_guard() -> anyhow::Result<()> {
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Box::new(TestDropGuard {
            dropped: Arc::clone(&dropped),
        });

        let mut mock = create_session_mock();
        let (_sender, receiver) = tokio::sync::mpsc::channel(1);
        mock.expect_execute_streaming_sql()
            .return_once(move |_| Ok(Response::from(receiver)));

        let (db_client, _server) = setup_db_client(mock).await;

        {
            let request = ExecuteSqlRequest::default()
                .set_session(db_client.session_name())
                .set_sql("SELECT 1");
            let stream = db_client
                .execute_streaming_sql(request, RequestOptions::default(), 0)
                .send()
                .await?
                .with_lifetime_guard(guard);

            assert!(
                !dropped.load(Ordering::Relaxed),
                "Guard must be held while stream is active"
            );
            drop(stream);
        }

        assert!(
            dropped.load(Ordering::Relaxed),
            "Guard must be dropped when stream is dropped"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn stream_eof_releases_lifetime_guard() -> anyhow::Result<()> {
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Box::new(TestDropGuard {
            dropped: Arc::clone(&dropped),
        });

        let mut mock = create_session_mock();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        mock.expect_execute_streaming_sql()
            .return_once(move |_| Ok(Response::from(receiver)));

        let (db_client, _server) = setup_db_client(mock).await;

        let request = ExecuteSqlRequest::default()
            .set_session(db_client.session_name())
            .set_sql("SELECT 1");
        let mut stream = db_client
            .execute_streaming_sql(request, RequestOptions::default(), 0)
            .send()
            .await?
            .with_lifetime_guard(guard);

        // Close channel to simulate EOF
        drop(sender);

        assert!(
            !dropped.load(Ordering::Relaxed),
            "Guard must be held before EOF is consumed"
        );

        let next = stream.next_message().await;
        assert!(next.is_none(), "Stream should yield None on EOF");
        assert!(
            dropped.load(Ordering::Relaxed),
            "Guard must be dropped immediately on EOF"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn stream_error_releases_lifetime_guard() -> anyhow::Result<()> {
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Box::new(TestDropGuard {
            dropped: Arc::clone(&dropped),
        });

        let mut mock = create_session_mock();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        mock.expect_execute_streaming_sql()
            .return_once(move |_| Ok(Response::from(receiver)));

        let (db_client, _server) = setup_db_client(mock).await;

        let request = ExecuteSqlRequest::default()
            .set_session(db_client.session_name())
            .set_sql("SELECT 1");
        let mut stream = db_client
            .execute_streaming_sql(request, RequestOptions::default(), 0)
            .send()
            .await?
            .with_lifetime_guard(guard);

        sender
            .send(Err(Status::unavailable("server unavailable")))
            .await
            .expect("send error");

        assert!(
            !dropped.load(Ordering::Relaxed),
            "Guard must be held before error is consumed"
        );

        let next = stream.next_message().await;
        assert!(next.is_some(), "Stream should yield Some on error");
        assert!(
            next.expect("error message").is_err(),
            "Stream message should be an error"
        );
        assert!(
            dropped.load(Ordering::Relaxed),
            "Guard must be dropped immediately on stream error"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_id_callback_invoked_on_first_message() -> anyhow::Result<()> {
        let invoked_transaction_id = Arc::new(Mutex::new(None));
        let invoked_transaction_id_clone = Arc::clone(&invoked_transaction_id);
        let callback = TransactionIdCallback::new(move |transaction_id| {
            *invoked_transaction_id_clone
                .lock()
                .expect("lock callback mutex") = Some(transaction_id.to_vec());
        });

        let mut mock = create_session_mock();
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        mock.expect_execute_streaming_sql()
            .return_once(move |_| Ok(Response::from(receiver)));

        let (db_client, _server) = setup_db_client(mock).await;

        let request = ExecuteSqlRequest::default()
            .set_session(db_client.session_name())
            .set_sql("SELECT 1");
        let mut stream = db_client
            .execute_streaming_sql(request, RequestOptions::default(), 0)
            .send()
            .await?
            .with_transaction_id_callback(callback);

        // Send first chunk with transaction ID
        let first_chunk = MockPartialResultSet {
            metadata: Some(MockResultSetMetadata {
                transaction: Some(MockTransaction {
                    id: b"stream-unit-tx".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        sender
            .send(Ok(first_chunk))
            .await
            .expect("send first chunk");

        // Send second chunk without transaction ID
        let second_chunk = MockPartialResultSet::default();
        sender
            .send(Ok(second_chunk))
            .await
            .expect("send second chunk");

        // First message consumes chunk 1 and triggers callback
        let first_message = stream.next_message().await;
        assert!(first_message.is_some(), "first message must be received");
        assert!(
            first_message.expect("first message").is_ok(),
            "first message must be ok"
        );
        assert_eq!(
            invoked_transaction_id
                .lock()
                .expect("lock invoked transaction ID")
                .as_deref(),
            Some(&b"stream-unit-tx"[..]),
            "callback must be invoked with transaction ID on first message"
        );

        // Second message consumes chunk 2 and callback is not called again
        let second_message = stream.next_message().await;
        assert!(second_message.is_some(), "second message must be received");
        assert!(
            second_message.expect("second message").is_ok(),
            "second message must be ok"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_id_callback_dropped_on_eof_and_error() -> anyhow::Result<()> {
        struct DropTracker(Arc<AtomicBool>);
        impl Drop for DropTracker {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        // EOF case
        {
            let dropped = Arc::new(AtomicBool::new(false));
            let tracker = DropTracker(Arc::clone(&dropped));
            let callback = TransactionIdCallback::new(move |_| {
                let _ = &tracker;
            });

            let mut mock = create_session_mock();
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            mock.expect_execute_streaming_sql()
                .return_once(move |_| Ok(Response::from(receiver)));

            let (db_client, _server) = setup_db_client(mock).await;
            let request = ExecuteSqlRequest::default()
                .set_session(db_client.session_name())
                .set_sql("SELECT 1");
            let mut stream = db_client
                .execute_streaming_sql(request, RequestOptions::default(), 0)
                .send()
                .await?
                .with_transaction_id_callback(callback);

            drop(sender); // EOF
            let message = stream.next_message().await;
            assert!(message.is_none(), "stream should return None on EOF");
            assert!(
                dropped.load(Ordering::Relaxed),
                "callback must be dropped on stream EOF"
            );
        }

        // Error case
        {
            let dropped = Arc::new(AtomicBool::new(false));
            let tracker = DropTracker(Arc::clone(&dropped));
            let callback = TransactionIdCallback::new(move |_| {
                let _ = &tracker;
            });

            let mut mock = create_session_mock();
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            mock.expect_execute_streaming_sql()
                .return_once(move |_| Ok(Response::from(receiver)));

            let (db_client, _server) = setup_db_client(mock).await;
            let request = ExecuteSqlRequest::default()
                .set_session(db_client.session_name())
                .set_sql("SELECT 1");
            let mut stream = db_client
                .execute_streaming_sql(request, RequestOptions::default(), 0)
                .send()
                .await?
                .with_transaction_id_callback(callback);

            sender
                .send(Err(Status::internal("rpc failed")))
                .await
                .expect("send error");
            let message = stream.next_message().await;
            assert!(message.is_some(), "stream should return Some on error");
            assert!(
                dropped.load(Ordering::Relaxed),
                "callback must be dropped on stream error"
            );
        }

        Ok(())
    }
}
