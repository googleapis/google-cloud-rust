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

use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model::{ReadRowsRequest, ReadRowsResponse};
use google_cloud_bigquery::stub::Read as ReadStub;
use google_cloud_gax::Result as GaxResult;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::streaming::ResponseStream;
use mockall::{Sequence, mock};
use std::time::Duration;
use tokio::sync::mpsc;

mock! {
    #[derive(Debug)]
    pub TestRead {}

    impl ReadStub for TestRead {
        async fn read_rows(
            &self,
            req: ReadRowsRequest,
            options: RequestOptions,
        ) -> GaxResult<ResponseStream<ReadRowsResponse>>;
    }
}

#[derive(Debug)]
struct NoBackoff;

impl BackoffPolicy for NoBackoff {
    fn on_failure(&self, _state: &RetryState) -> Duration {
        Duration::ZERO
    }
}

#[tokio::test]
async fn into_reader_reconnects_at_offset() -> anyhow::Result<()> {
    let mut seq = Sequence::new();
    let mut mock = MockTestRead::new();

    mock.expect_read_rows()
        .once()
        .in_sequence(&mut seq)
        .returning(|req, _| {
            assert_eq!(
                req.read_stream,
                "projects/p/locations/us/sessions/s/streams/1"
            );
            assert_eq!(req.offset, 0);
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(10))).await;
                let _ = tx
                    .send(Err(Error::service(
                        Status::default()
                            .set_code(Code::Unavailable)
                            .set_message("stream reset"),
                    )))
                    .await;
            });
            Ok(ResponseStream::from(rx))
        });

    mock.expect_read_rows()
        .once()
        .in_sequence(&mut seq)
        .returning(|req, _| {
            assert_eq!(
                req.read_stream,
                "projects/p/locations/us/sessions/s/streams/1"
            );
            assert_eq!(req.offset, 10);
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                let _ = tx.send(Ok(ReadRowsResponse::new().set_row_count(5))).await;
            });
            Ok(ResponseStream::from(rx))
        });

    let client = Read::from_stub(mock);
    let mut rows = client
        .read_rows()
        .set_read_stream("projects/p/locations/us/sessions/s/streams/1")
        .into_reader()
        .with_backoff_policy(NoBackoff);

    let r1 = rows.next().await.transpose()?.expect("batch 1");
    assert_eq!(r1.row_count, 10);
    let r2 = rows.next().await.transpose()?.expect("batch 2");
    assert_eq!(r2.row_count, 5);
    assert!(rows.next().await.is_none());

    Ok(())
}
