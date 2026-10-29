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

use crate::google::spanner::v1::BatchWriteResponse;
use crate::google::spanner::v1::CacheUpdate as ProtoCacheUpdate;
use crate::google::spanner::v1::PartialResultSet;
use crate::retry_delay::extract_status_code_from_error;
use crate::{Error, Result};
use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::Streaming;
use gaxi::prost::ToProto;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::streaming::ResponseStream;
use http::HeaderMap;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Generic wrapper around gRPC server-streaming responses with lifetime management.
#[derive(Debug)]
pub(crate) struct SpannerServerStream<T> {
    pub(crate) inner: StreamSource<T>,
    pub(crate) headers: HeaderMap,
    pub(crate) lifetime_guard: Option<StreamLifetimeGuard>,
    pub(crate) on_first_transaction_id: Option<TransactionIdCallback>,
}

impl<T> SpannerServerStream<T> {
    pub(crate) fn new(
        inner: Streaming<T>,
        headers: HeaderMap,
        lifetime_guard: Option<StreamLifetimeGuard>,
    ) -> Self {
        Self {
            inner: StreamSource::Grpc(Box::new(inner)),
            headers,
            lifetime_guard,
            on_first_transaction_id: None,
        }
    }

    /// Creates a stream from an inbound [`ResponseStream`] backed by an in-memory stub or mock.
    pub(crate) fn from_response_stream<Message>(
        stream: ResponseStream<Message>,
        headers: HeaderMap,
        lifetime_guard: Option<StreamLifetimeGuard>,
    ) -> Self
    where
        Message: ToProto<T, Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Self {
            inner: StreamSource::Stub(Box::new(StubResponseStream::new(stream))),
            headers,
            lifetime_guard,
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
        let raw_message = match &mut self.inner {
            StreamSource::Grpc(streaming) => {
                streaming.message().await.map_err(to_gax_error).transpose()
            }
            StreamSource::Stub(stub_stream) => stub_stream.next_message().await,
        };

        let message = match raw_message {
            Some(Ok(message)) => message,
            Some(Err(err)) => {
                if let Some(guard) = self.lifetime_guard.take() {
                    guard.record_error(&err);
                }
                self.on_first_transaction_id = None;
                return Some(Err(err));
            }
            None => {
                if let Some(guard) = self.lifetime_guard.take() {
                    guard.record_success();
                }
                self.on_first_transaction_id = None;
                return None;
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

/// Internal source for a server streaming RPC, supporting both tonic gRPC streaming and in-memory mock stubs.
pub(crate) enum StreamSource<T> {
    Grpc(Box<Streaming<T>>),
    Stub(Box<dyn DynamicStream<T> + Send + Sync>),
}

impl<T> Debug for StreamSource<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Grpc(streaming) => formatter.debug_tuple("Grpc").field(streaming).finish(),
            Self::Stub(stub_stream) => formatter.debug_tuple("Stub").field(stub_stream).finish(),
        }
    }
}

/// Type-erased trait for asynchronous streaming sources capable of yielding stream messages.
pub(crate) trait DynamicStream<T>: Debug + Send + Sync {
    fn next_message<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Option<Result<T>>> + Send + 'a>>;
}

struct StubResponseStream<Message> {
    stream: Mutex<ResponseStream<Message>>,
}

impl<Message> Debug for StubResponseStream<Message> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.debug_struct("StubResponseStream").finish()
    }
}

impl<Message> StubResponseStream<Message> {
    fn new(stream: ResponseStream<Message>) -> Self {
        Self {
            stream: Mutex::new(stream),
        }
    }
}

impl<Message, ProtoMessage> DynamicStream<ProtoMessage> for StubResponseStream<Message>
where
    Message: ToProto<ProtoMessage, Output = ProtoMessage> + Send + 'static,
    ProtoMessage: Send + 'static,
{
    fn next_message<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Option<Result<ProtoMessage>>> + Send + 'a>> {
        Box::pin(async move {
            match self.stream.get_mut().next().await {
                Some(Ok(message)) => match message.to_proto() {
                    Ok(proto_message) => Some(Ok(proto_message)),
                    Err(error) => Some(Err(Error::deser(error))),
                },
                Some(Err(error)) => Some(Err(error)),
                None => None,
            }
        })
    }
}

/// Trait for stream lifetime drop guards capable of recording RPC error codes for dynamic channel pooling
/// and routing feedback for location-aware routing.
pub(crate) trait StreamGuard: Debug + Send + Sync + 'static {
    fn record_error_code(&self, _code: Code) {}

    fn record_error(&self, error: &Error) {
        if let Some(code) = extract_status_code_from_error(error) {
            self.record_error_code(code);
        }
    }

    fn record_first_response(&self, _latency: Duration) {}

    fn record_success(&self) {}
}

/// Type alias for stream lifetime drop guards.
pub(crate) type StreamLifetimeGuard = Arc<dyn StreamGuard>;

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
    use crate::model::{
        ExecuteSqlRequest, PartialResultSet as ModelPartialResultSet, ResultSetMetadata,
        Transaction as ModelTransaction,
    };
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use bytes::Bytes;
    use gaxi::grpc::tonic::{Response, Status};
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::Status as GaxStatus;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_test_macros::tokio_test_no_panics;
    use spanner_grpc_mock::google::spanner::v1::{
        PartialResultSet as MockPartialResultSet, ResultSetMetadata as MockResultSetMetadata,
        Transaction as MockTransaction,
    };
    use std::fmt::Debug;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc::channel;

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

    #[derive(Debug)]
    struct TestDropGuard {
        dropped: Arc<AtomicBool>,
        recorded_code: Arc<Mutex<Option<Code>>>,
        recorded_success: Arc<AtomicBool>,
    }

    impl StreamGuard for TestDropGuard {
        fn record_error_code(&self, code: Code) {
            *self.recorded_code.lock().expect("lock poisoned") = Some(code);
        }

        fn record_success(&self) {
            self.recorded_success.store(true, Ordering::Release);
        }
    }

    impl Drop for TestDropGuard {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    #[tokio_test_no_panics]
    async fn stream_drop_releases_lifetime_guard() -> anyhow::Result<()> {
        let dropped = Arc::new(AtomicBool::new(false));
        let recorded_code = Arc::new(Mutex::new(None));
        let recorded_success = Arc::new(AtomicBool::new(false));
        let guard = Arc::new(TestDropGuard {
            dropped: Arc::clone(&dropped),
            recorded_code: Arc::clone(&recorded_code),
            recorded_success: Arc::clone(&recorded_success),
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
                .execute_streaming_sql(request, RequestOptions::default(), None)
                .with_lifetime_guard(guard)
                .send()
                .await?;

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
        assert_eq!(
            *recorded_code.lock().expect("lock poisoned"),
            None,
            "No error code should be recorded on normal stream drop"
        );
        assert!(
            !recorded_success.load(Ordering::Acquire),
            "Early stream drop without EOF must not invoke record_success"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn stream_eof_releases_lifetime_guard() -> anyhow::Result<()> {
        let dropped = Arc::new(AtomicBool::new(false));
        let recorded_code = Arc::new(Mutex::new(None));
        let recorded_success = Arc::new(AtomicBool::new(false));
        let guard = Arc::new(TestDropGuard {
            dropped: Arc::clone(&dropped),
            recorded_code: Arc::clone(&recorded_code),
            recorded_success: Arc::clone(&recorded_success),
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
            .execute_streaming_sql(request, RequestOptions::default(), None)
            .with_lifetime_guard(guard)
            .send()
            .await?;

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
        assert_eq!(
            *recorded_code.lock().expect("lock poisoned"),
            None,
            "No error code should be recorded on normal EOF"
        );
        assert!(
            recorded_success.load(Ordering::Acquire),
            "Stream EOF must invoke record_success on lifetime guard"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn stream_error_releases_lifetime_guard() -> anyhow::Result<()> {
        let dropped = Arc::new(AtomicBool::new(false));
        let recorded_code = Arc::new(Mutex::new(None));
        let recorded_success = Arc::new(AtomicBool::new(false));
        let guard = Arc::new(TestDropGuard {
            dropped: Arc::clone(&dropped),
            recorded_code: Arc::clone(&recorded_code),
            recorded_success: Arc::clone(&recorded_success),
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
            .execute_streaming_sql(request, RequestOptions::default(), None)
            .with_lifetime_guard(guard)
            .send()
            .await?;

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
        assert_eq!(
            *recorded_code.lock().expect("lock poisoned"),
            Some(Code::Unavailable),
            "Stream error must record Code::Unavailable on guard"
        );
        assert!(
            !recorded_success.load(Ordering::Acquire),
            "Stream error must not invoke record_success on lifetime guard"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn stream_error_without_guard() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        mock.expect_execute_streaming_sql()
            .return_once(move |_| Ok(Response::from(receiver)));

        let (db_client, _server) = setup_db_client(mock).await;

        let request = ExecuteSqlRequest::default()
            .set_session(db_client.session_name())
            .set_sql("SELECT 1");
        let mut stream = db_client
            .execute_streaming_sql(request, RequestOptions::default(), None)
            .send()
            .await?;

        sender
            .send(Err(Status::unavailable("server unavailable")))
            .await
            .expect("send error");

        let next = stream.next_message().await;
        assert!(next.is_some(), "Stream should yield Some on error");
        assert!(
            next.expect("error message").is_err(),
            "Stream message should be an error"
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
            .execute_streaming_sql(request, RequestOptions::default(), None)
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
                .execute_streaming_sql(request, RequestOptions::default(), None)
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
                .execute_streaming_sql(request, RequestOptions::default(), None)
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

    #[tokio_test_no_panics]
    async fn from_response_stream_yields_messages_and_terminates() -> anyhow::Result<()> {
        let (sender, receiver) = channel(2);
        let partial_result_set =
            ModelPartialResultSet::default().set_values(vec![wkt::Value::from("val1")]);
        sender.send(Ok(partial_result_set)).await.expect("send ok");
        drop(sender);

        let mut stream =
            SpannerServerStream::from_response_stream(receiver.into(), HeaderMap::new(), None);

        let first = stream.next_message().await;
        assert!(first.is_some(), "expected first message");
        let first_message = first.expect("first message present")?;
        assert_eq!(
            first_message.values.len(),
            1,
            "expected 1 value in first message"
        );

        let second = stream.next_message().await;
        assert!(second.is_none(), "expected stream EOF");
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn from_response_stream_invokes_transaction_id_callback() -> anyhow::Result<()> {
        let (sender, receiver) = channel(1);
        let partial_result_set = ModelPartialResultSet::default().set_metadata(
            ResultSetMetadata::default().set_transaction(
                ModelTransaction::default().set_id(Bytes::from_static(b"tx-stub-123")),
            ),
        );
        sender.send(Ok(partial_result_set)).await.expect("send ok");

        let recorded_transaction_id = Arc::new(Mutex::new(None));
        let recorded_transaction_id_clone = Arc::clone(&recorded_transaction_id);
        let callback = TransactionIdCallback::new(move |transaction_id| {
            *recorded_transaction_id_clone
                .lock()
                .expect("lock not poisoned") = Some(transaction_id.to_vec());
        });

        let mut stream =
            SpannerServerStream::from_response_stream(receiver.into(), HeaderMap::new(), None)
                .with_transaction_id_callback(callback);

        let message = stream.next_message().await;
        assert!(message.is_some(), "expected message");
        assert_eq!(
            recorded_transaction_id
                .lock()
                .expect("lock not poisoned")
                .as_deref(),
            Some(b"tx-stub-123".as_slice()),
            "transaction ID callback must be invoked with extracted transaction ID"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn from_response_stream_records_error_and_success_on_guard() -> anyhow::Result<()> {
        let recorded_error = Arc::new(Mutex::new(None));
        let recorded_success = Arc::new(AtomicBool::new(false));
        let guard = Arc::new(TestDropGuard {
            dropped: Arc::new(AtomicBool::new(false)),
            recorded_code: Arc::clone(&recorded_error),
            recorded_success: Arc::clone(&recorded_success),
        });

        let (sender, receiver) = channel::<Result<ModelPartialResultSet>>(1);
        let status = GaxStatus::default()
            .set_code(Code::Unavailable)
            .set_message("stub unavailable");
        sender
            .send(Err(GaxError::service(status)))
            .await
            .expect("send error");

        let mut stream = SpannerServerStream::from_response_stream(
            receiver.into(),
            HeaderMap::new(),
            Some(guard),
        );

        let error_message = stream.next_message().await;
        assert!(error_message.is_some(), "expected error message");
        assert!(
            error_message.expect("message present").is_err(),
            "expected Err"
        );
        assert_eq!(
            *recorded_error.lock().expect("lock not poisoned"),
            Some(Code::Unavailable),
            "guard must record Code::Unavailable on stream error"
        );

        let recorded_error_ok = Arc::new(Mutex::new(None));
        let recorded_success_ok = Arc::new(AtomicBool::new(false));
        let guard_ok = Arc::new(TestDropGuard {
            dropped: Arc::new(AtomicBool::new(false)),
            recorded_code: Arc::clone(&recorded_error_ok),
            recorded_success: Arc::clone(&recorded_success_ok),
        });

        let (sender_ok, receiver_ok) = channel::<Result<ModelPartialResultSet>>(1);
        drop(sender_ok);

        let mut stream_ok = SpannerServerStream::from_response_stream(
            receiver_ok.into(),
            HeaderMap::new(),
            Some(guard_ok),
        );

        let eof = stream_ok.next_message().await;
        assert!(eof.is_none(), "expected EOF");
        assert!(
            recorded_success_ok.load(Ordering::Acquire),
            "guard must record success on EOF"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn stream_debug_formatting() -> anyhow::Result<()> {
        let (_sender, receiver) = channel::<Result<ModelPartialResultSet>>(1);
        let stub_stream =
            SpannerServerStream::from_response_stream(receiver.into(), HeaderMap::new(), None);
        let stub_debug = format!("{stub_stream:?}");
        assert!(
            stub_debug.contains("Stub"),
            "debug format should show Stub variant"
        );
        assert!(
            stub_debug.contains("StubResponseStream"),
            "debug format should show StubResponseStream"
        );

        let mut mock = create_session_mock();
        let (_sender, receiver) = tokio::sync::mpsc::channel(1);
        mock.expect_execute_streaming_sql()
            .return_once(move |_| Ok(Response::from(receiver)));
        let (database_client, _server) = setup_db_client(mock).await;
        let request = ExecuteSqlRequest::default()
            .set_session(database_client.session_name())
            .set_sql("SELECT 1");
        let grpc_stream = database_client
            .execute_streaming_sql(request, RequestOptions::default(), None)
            .send()
            .await?;
        let grpc_debug = format!("{grpc_stream:?}");
        assert!(
            grpc_debug.contains("Grpc"),
            "debug format should show Grpc variant"
        );

        Ok(())
    }

    #[derive(Debug)]
    struct FailingToProto;

    impl ToProto<PartialResultSet> for FailingToProto {
        type Output = PartialResultSet;
        fn to_proto(self) -> std::result::Result<Self::Output, gaxi::prost::ConvertError> {
            Err(gaxi::prost::ConvertError::Unimplemented)
        }
    }

    #[tokio_test_no_panics]
    async fn from_response_stream_to_proto_error() {
        let (sender, receiver) = channel::<Result<FailingToProto>>(1);
        sender.send(Ok(FailingToProto)).await.expect("send ok");
        let mut stream =
            SpannerServerStream::from_response_stream(receiver.into(), HeaderMap::new(), None);
        let message = stream.next_message().await;
        assert!(message.is_some(), "expected message");
        let error = message.expect("message present");
        assert!(error.is_err(), "expected conversion error");
    }
}
