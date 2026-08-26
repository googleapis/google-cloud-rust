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
use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::Streaming;
use http::HeaderMap;
use std::any::Any;

/// Type alias for opaque stream lifetime drop guards.
pub(crate) type StreamLifetimeGuard = Box<dyn Any + Send + Sync>;

/// Generic wrapper around gRPC server-streaming responses with lifetime management.
#[derive(Debug)]
pub(crate) struct SpannerServerStream<T> {
    pub(crate) inner: Streaming<T>,
    pub(crate) headers: HeaderMap,
    pub(crate) lifetime_guard: Option<StreamLifetimeGuard>,
}

impl<T> SpannerServerStream<T> {
    pub(crate) fn new(inner: Streaming<T>, headers: HeaderMap) -> Self {
        Self {
            inner,
            headers,
            lifetime_guard: None,
        }
    }

    /// Attaches an opaque RAII lifetime guard that remains alive for the duration of the stream.
    #[allow(dead_code)]
    pub(crate) fn with_lifetime_guard(mut self, guard: StreamLifetimeGuard) -> Self {
        self.lifetime_guard = Some(guard);
        self
    }

    /// Returns the initial response headers for the stream.
    pub(crate) fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Fetches the next message from the stream.
    ///
    /// Returns `Some(Ok(message))` when a message is successfully received,
    /// `None` when the stream concludes naturally, or `Some(Err(_))` on RPC errors.
    /// Drops the attached lifetime guard on EOF or stream errors to release active in-flight accounting early.
    pub(crate) async fn next_message(&mut self) -> Option<crate::Result<T>> {
        match self.inner.message().await.map_err(to_gax_error).transpose() {
            Some(Ok(message)) => Some(Ok(message)),
            other => {
                self.lifetime_guard = None;
                other
            }
        }
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
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use gaxi::grpc::tonic::{Response, Status};
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_test_macros::tokio_test_no_panics;
    use std::fmt::Debug;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(PartialResultSetStream: Send, Sync, Debug);
        static_assertions::assert_impl_all!(BatchWriteStream: Send, Sync, Debug);
        static_assertions::assert_impl_all!(CacheUpdateStream: Send, Sync, Debug);
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
            let request = crate::model::ExecuteSqlRequest::default()
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

        let request = crate::model::ExecuteSqlRequest::default()
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

        let request = crate::model::ExecuteSqlRequest::default()
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
}
