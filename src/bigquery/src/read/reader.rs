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

//! Helpers to read from a [ReadRows] stream.

use crate::builder::read::ReadRows;
use crate::model::ReadRowsResponse;
use crate::{Error, Result};
use google_cloud_gax::streaming::ResponseStream;

/// State machine for reading from a [`ReadRows`] stream.
///
/// All mutable runtime state for a [`Reader`] (such as the current row offset
/// and active stream) must be stored in [`ReaderState`] rather than as separate
/// fields on [`Reader`], keeping state transitions explicit and self-contained.
#[derive(Debug)]
pub(crate) enum ReaderState {
    /// Connect or reconnect to the BigQuery read stream.
    Connecting { offset: i64 },
    /// Reading messages from an open BigQuery read stream.
    Reading {
        offset: i64,
        stream: ResponseStream<ReadRowsResponse>,
    },
    /// Stream completed cleanly, fatal error occurred, or consumer dropped the receiver.
    Terminated(Option<Error>),
}

/// A stream reader for [`ReadRows`].
#[derive(Debug)]
pub struct Reader {
    request: ReadRows,
    /// Encapsulates all mutable state for the [`Reader`]. Any new mutable
    /// runtime state should be added to [`ReaderState`] rather than [`Reader`].
    state: ReaderState,
}

impl Reader {
    pub(crate) fn new(request: ReadRows) -> Self {
        Self {
            request,
            state: ReaderState::Connecting { offset: 0 },
        }
    }

    /// Advances the state machine by a single transition, returning a
    /// [`ReadRowsResponse`] if one was read during this step.
    async fn step(&mut self) -> Option<ReadRowsResponse> {
        match &mut self.state {
            ReaderState::Connecting { offset } => {
                let offset = *offset;
                let req = self.request.clone().set_offset(offset);
                match req.send().await {
                    Ok(stream) => {
                        self.state = ReaderState::Reading { offset, stream };
                    }
                    Err(err) => {
                        self.state = ReaderState::Terminated(Some(err));
                    }
                }
                None
            }
            ReaderState::Reading { offset, stream } => match stream.next().await {
                Some(Ok(response)) => {
                    *offset += response.row_count;
                    Some(response)
                }
                Some(Err(err)) => {
                    self.state = ReaderState::Terminated(Some(err));
                    None
                }
                None => {
                    self.state = ReaderState::Terminated(None);
                    None
                }
            },
            ReaderState::Terminated(_) => None,
        }
    }

    /// Receives the next [`ReadRowsResponse`] from the stream, or returns `None`
    /// when the stream completes.
    pub async fn next(&mut self) -> Option<Result<ReadRowsResponse>> {
        loop {
            if let Some(response) = self.step().await {
                return Some(Ok(response));
            }
            if let ReaderState::Terminated(err) = &mut self.state {
                return err.take().map(Err);
            }
        }
    }
}

impl ReadRows {
    /// Returns a [`Reader`] for reading rows from the stream.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::builder::read::ReadRows;
    /// # async fn sample(builder: ReadRows) -> google_cloud_bigquery::Result<()> {
    /// let mut rows = builder.into_reader();
    /// while let Some(response) = rows.next().await.transpose()? {
    ///     println!("Read {} rows", response.row_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_reader(self) -> Reader {
        Reader::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Read;
    use crate::model::ReadRowsRequest;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::options::RequestOptions;
    use tokio::sync::mpsc;

    mockall::mock! {
        #[derive(Debug)]
        ReadStub {}
        impl crate::stub::Read for ReadStub {
            async fn read_rows(
                &self,
                req: ReadRowsRequest,
                options: RequestOptions,
            ) -> Result<ResponseStream<ReadRowsResponse>>;
        }
    }

    fn permanent_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("permission denied"),
        )
    }

    #[tokio::test]
    async fn step_connecting_to_reading() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().once().returning(|req, _| {
            assert_eq!(req.read_stream, "streams/1");
            assert_eq!(req.offset, 42);
            let (_tx, rx) = mpsc::channel(1);
            Ok(ResponseStream::from(rx))
        });

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);
        reader.state = ReaderState::Connecting { offset: 42 };

        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Reading { offset, .. } = &reader.state else {
            anyhow::bail!("expected Reading state, got: {:?}", reader.state);
        };
        assert_eq!(*offset, 42);
        Ok(())
    }

    #[tokio::test]
    async fn step_connecting_error_to_terminated() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows()
            .once()
            .returning(|_, _| Err(permanent_error()));

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Terminated(Some(err)) = &reader.state else {
            anyhow::bail!("expected Terminated(Some(_)), got: {:?}", reader.state);
        };
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        Ok(())
    }

    #[tokio::test]
    async fn step_reading_to_reading() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(ReadRowsResponse::new().set_row_count(3)))
            .await?;
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Reading { offset: 5, stream };
        let resp = reader.step().await.expect("expected Some(response)");
        let ReaderState::Reading { offset, .. } = &reader.state else {
            anyhow::bail!("expected Reading state, got: {:?}", reader.state);
        };
        assert_eq!(resp.row_count, 3);
        assert_eq!(*offset, 8);
        Ok(())
    }

    #[tokio::test]
    async fn step_reading_error_to_terminated() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        let (tx, rx) = mpsc::channel(1);
        tx.send(Err(permanent_error())).await?;
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Reading { offset: 0, stream };
        let resp = reader.step().await;
        assert!(resp.is_none());
        let ReaderState::Terminated(Some(err)) = &reader.state else {
            anyhow::bail!("expected Terminated(Some(_)), got: {:?}", reader.state);
        };
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        Ok(())
    }

    #[tokio::test]
    async fn step_reading_eof_to_terminated() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let stream = ResponseStream::from(rx);

        reader.state = ReaderState::Reading { offset: 0, stream };
        let resp = reader.step().await;
        assert!(resp.is_none());
        assert!(
            matches!(reader.state, ReaderState::Terminated(None)),
            "expected Terminated(None), got: {:?}",
            reader.state
        );
        Ok(())
    }

    #[tokio::test]
    async fn step_terminated_stays_terminated() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().never();

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        reader.state = ReaderState::Terminated(None);
        assert!(reader.step().await.is_none());
        assert!(
            matches!(reader.state, ReaderState::Terminated(None)),
            "expected Terminated(None), got: {:?}",
            reader.state
        );

        reader.state = ReaderState::Terminated(Some(permanent_error()));
        assert!(reader.step().await.is_none());
        let ReaderState::Terminated(Some(err)) = &reader.state else {
            anyhow::bail!("expected Terminated(Some(_)), got: {:?}", reader.state);
        };
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        Ok(())
    }

    #[tokio::test]
    async fn read_rows_success() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().once().returning(|req, _| {
            assert_eq!(req.read_stream, "streams/1");
            assert_eq!(req.offset, 0);
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(5))).await;
                let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(3))).await;
            });
            Ok(ResponseStream::from(rx))
        });

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        let r1 = reader.next().await.transpose()?.expect("row batch 1");
        assert_eq!(r1.row_count, 5);
        let r2 = reader.next().await.transpose()?.expect("row batch 2");
        assert_eq!(r2.row_count, 3);
        assert!(reader.next().await.is_none());
        assert!(reader.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn permanent_error_terminates_immediately() -> anyhow::Result<()> {
        let mut mock = MockReadStub::new();
        mock.expect_read_rows().once().returning(|_, _| {
            let (tx, rx) = mpsc::channel(2);
            tokio::spawn(async move {
                let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(5))).await;
                let _ = tx.send(Err(permanent_error())).await;
            });
            Ok(ResponseStream::from(rx))
        });

        let client = Read::from_stub(mock);
        let req = client.read_rows().set_read_stream("streams/1");
        let mut reader = Reader::new(req);

        assert_eq!(reader.next().await.transpose()?.unwrap().row_count, 5);
        let err = reader
            .next()
            .await
            .expect("should return error")
            .expect_err("should be permanent error");
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        assert!(reader.next().await.is_none());
        Ok(())
    }
}
