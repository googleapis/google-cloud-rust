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

use crate::RequestOptions;
use crate::client::Read as ReadClient;
use crate::error::RowError;
use crate::model::{
    ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadRowsResponse, ReadSession,
    arrow_serialization_options::CompressionCodec, read_rows_response::Rows,
    read_rows_response::Schema as ReadRowsResponseSchema,
    read_session::Schema as ReadSessionSchema, read_session::TableReadOptions,
};
use crate::query::arrow::ArrowStreamDecoder;
use crate::query::query_handle::build_get_job;
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::JobReference;
use google_cloud_gax::streaming::ResponseStream;
use std::collections::VecDeque;
use std::sync::Arc;

/// Default threshold below which queries stick to REST pagination.
pub(crate) const DEFAULT_REST_ROW_THRESHOLD: u64 = 10_000;

/// Default threshold above which queries switch from a single job stream to a multi-stream `ReadSession`.
pub(crate) const DEFAULT_MULTI_STREAM_THRESHOLD: u64 = 5_000_000;

// TODO(#7038): remove environment variable override once benchmarks establish the optimal threshold.
pub(crate) fn rest_row_threshold() -> u64 {
    std::env::var("GOOGLE_CLOUD_RUST_BIGQUERY_REST_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_REST_ROW_THRESHOLD)
}

// TODO(#7039): remove environment variable override once benchmarks establish the optimal threshold.
pub(crate) fn multi_stream_threshold() -> u64 {
    std::env::var("GOOGLE_CLOUD_RUST_BIGQUERY_MULTI_STREAM_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MULTI_STREAM_THRESHOLD)
}

#[derive(Debug)]
pub(crate) struct ActiveStream {
    stream: ResponseStream<ReadRowsResponse>,
    decoder: ArrowStreamDecoder,
    has_schema: bool,
}

#[derive(Debug)]
pub(crate) enum ReaderState {
    Unopened,
    SingleStream(ActiveStream),
    // TODO(#7039): spawn concurrent stream reader tasks over a bounded channel.
    MultiStream {
        streams: VecDeque<String>,
        session_schema: Option<bytes::Bytes>,
        current: Option<ActiveStream>,
    },
    Completed,
}

/// Reads query results via the BigQuery Storage Read API (`_default` job stream or multi-stream `ReadSession`).
#[derive(Debug)]
pub(crate) struct StorageReader {
    pub(crate) read_client: Arc<ReadClient>,
    pub(crate) job_service: Option<Arc<JobService>>,
    pub(crate) job_ref: JobReference,
    pub(crate) options: RequestOptions,
    pub(crate) estimated_total_rows: Option<u64>,
    pub(crate) multi_stream_threshold: u64,
    pub(crate) state: ReaderState,
}

impl StorageReader {
    pub(crate) fn new(
        read_client: Arc<ReadClient>,
        job_ref: JobReference,
        job_service: Option<Arc<JobService>>,
        estimated_total_rows: Option<u64>,
        options: RequestOptions,
    ) -> Self {
        Self {
            read_client,
            job_service,
            job_ref,
            options,
            estimated_total_rows,
            multi_stream_threshold: multi_stream_threshold(),
            state: ReaderState::Unopened,
        }
    }

    async fn open_stream(
        read_client: &ReadClient,
        options: &RequestOptions,
        stream_name: String,
    ) -> Result<ActiveStream, RowError> {
        let stream = read_client
            .read_rows()
            .with_options(options.clone())
            .set_read_stream(stream_name)
            .set_offset(0)
            .send()
            .await
            .map_err(|source| RowError::Rpc { source })?;
        Ok(ActiveStream {
            stream,
            decoder: ArrowStreamDecoder::new(),
            has_schema: false,
        })
    }

    async fn try_create_multi_stream_session(
        read_client: &ReadClient,
        job_service: &JobService,
        job_ref: &JobReference,
        options: &RequestOptions,
    ) -> Result<Option<ReaderState>, RowError> {
        let Some(get_job) = build_get_job(job_service, job_ref, options) else {
            return Ok(None);
        };

        let job = get_job.send().await?;

        let dest_table = job
            .configuration
            .and_then(|c| c.query)
            .and_then(|q| q.destination_table);

        let Some(dest_table) = dest_table else {
            return Ok(None);
        };

        if dest_table.project_id.is_empty()
            || dest_table.dataset_id.is_empty()
            || dest_table.table_id.is_empty()
        {
            return Ok(None);
        }

        let table_path = format!(
            "projects/{}/datasets/{}/tables/{}",
            dest_table.project_id, dest_table.dataset_id, dest_table.table_id
        );

        let session_req = CreateReadSessionRequest::new()
            .set_parent(format!("projects/{}", job_ref.project_id))
            .set_read_session(
                ReadSession::new()
                    .set_table(table_path)
                    .set_data_format(DataFormat::Arrow)
                    .set_read_options(
                        TableReadOptions::new().set_arrow_serialization_options(
                            ArrowSerializationOptions::new()
                                .set_buffer_compression(CompressionCodec::Lz4Frame),
                        ),
                    ),
            );

        let session = read_client
            .create_read_session()
            .with_options(options.clone())
            .with_request(session_req)
            .send()
            .await?;

        if session.streams.len() <= 1 {
            return Ok(None);
        }

        let session_schema = match session.schema {
            Some(ReadSessionSchema::ArrowSchema(schema))
                if !schema.serialized_schema.is_empty() =>
            {
                Some(schema.serialized_schema)
            }
            _ => None,
        };
        let streams = session.streams.into_iter().map(|s| s.name).collect();
        Ok(Some(ReaderState::MultiStream {
            streams,
            session_schema,
            current: None,
        }))
    }
    async fn next_record_batch_inner(&mut self) -> Result<Option<RecordBatch>, RowError> {
        loop {
            match &mut self.state {
                ReaderState::Completed => return Ok(None),
                ReaderState::Unopened => {
                    if self
                        .estimated_total_rows
                        .is_some_and(|r| r > self.multi_stream_threshold)
                        && let Some(job_service) = self.job_service.take()
                        && let Some(multi_state) = Self::try_create_multi_stream_session(
                            &self.read_client,
                            &job_service,
                            &self.job_ref,
                            &self.options,
                        )
                        .await?
                    {
                        self.state = multi_state;
                        continue;
                    }

                    let project = &self.job_ref.project_id;
                    let location = self.job_ref.location.as_deref().unwrap_or("US");
                    let job_id = &self.job_ref.job_id;
                    let stream_name = format!(
                        "projects/{project}/locations/{location}/jobs/{job_id}/streams/_default"
                    );
                    let active =
                        Self::open_stream(&self.read_client, &self.options, stream_name).await?;
                    self.state = ReaderState::SingleStream(active);
                }
                ReaderState::SingleStream(active) => {
                    let Some(item) = active.stream.next().await else {
                        return Ok(None);
                    };
                    let response = item.map_err(|source| RowError::Rpc { source })?;

                    let exceeds_threshold = response
                        .total_estimated_row_count
                        .and_then(|c| u64::try_from(c).ok())
                        .is_some_and(|c| c > self.multi_stream_threshold);

                    if exceeds_threshold
                        && let Some(job_service) = self.job_service.take()
                        && let Some(multi_state) = Self::try_create_multi_stream_session(
                            &self.read_client,
                            &job_service,
                            &self.job_ref,
                            &self.options,
                        )
                        .await?
                    {
                        self.state = multi_state;
                        continue;
                    }

                    if let Some(record_batch) = decode_response(
                        &mut active.decoder,
                        &mut active.has_schema,
                        None,
                        response,
                    )? {
                        // Never upgrade to a full-table ReadSession after yielding rows.
                        self.job_service = None;
                        return Ok(Some(record_batch));
                    }
                }
                ReaderState::MultiStream {
                    streams,
                    session_schema,
                    current,
                } => {
                    let Some(active) = current else {
                        let Some(stream_name) = streams.pop_front() else {
                            return Ok(None);
                        };
                        *current = Some(
                            Self::open_stream(&self.read_client, &self.options, stream_name)
                                .await?,
                        );
                        continue;
                    };

                    let Some(item) = active.stream.next().await else {
                        *current = None;
                        continue;
                    };
                    let response = item.map_err(|source| RowError::Rpc { source })?;
                    if let Some(record_batch) = decode_response(
                        &mut active.decoder,
                        &mut active.has_schema,
                        session_schema.as_ref(),
                        response,
                    )? {
                        return Ok(Some(record_batch));
                    }
                }
            }
        }
    }
}

fn decode_response(
    decoder: &mut ArrowStreamDecoder,
    has_schema: &mut bool,
    fallback_schema: Option<&bytes::Bytes>,
    response: ReadRowsResponse,
) -> Result<Option<RecordBatch>, RowError> {
    if let Some(ReadRowsResponseSchema::ArrowSchema(arrow_schema)) = response.schema {
        decoder.set_schema_bytes(arrow_schema.serialized_schema)?;
        *has_schema = true;
    }

    let Some(Rows::ArrowRecordBatch(batch)) = response.rows else {
        return Ok(None);
    };

    if !*has_schema && let Some(schema_bytes) = fallback_schema {
        decoder.set_schema_bytes(schema_bytes.clone())?;
        *has_schema = true;
    }

    Ok(decoder
        .decode_batch(batch.serialized_record_batch)?
        .filter(|rb| rb.num_rows() > 0))
}

impl crate::query::arrow::RecordBatchSource for StorageReader {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        let res = self.next_record_batch_inner().await;
        if res.is_err() || matches!(res, Ok(None)) {
            self.state = ReaderState::Completed;
        }
        res
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::model::{ArrowRecordBatch, ArrowSchema, ReadStream};
    use crate::query::arrow::RecordBatchSource;
    use crate::query::tests::{MockJobService, MockRead, create_job_service};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchemaDef};
    use arrow::ipc::CompressionType;
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
    use google_cloud_bigquery_v2::model::{
        Job, JobConfiguration, JobConfigurationQuery, TableReference,
    };
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;
    use tokio::sync::mpsc;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    pub(crate) fn encode_named_int64_ipc(
        col_name: &str,
        values: &[i64],
        compression: Option<CompressionType>,
    ) -> Result<(bytes::Bytes, bytes::Bytes), arrow::error::ArrowError> {
        let arrow_schema = Arc::new(ArrowSchemaDef::new(vec![Field::new(
            col_name,
            DataType::Int64,
            false,
        )]));
        let options = IpcWriteOptions::default().try_with_compression(compression)?;
        let mut schema_buf = Vec::new();
        let _ =
            StreamWriter::try_new_with_options(&mut schema_buf, &arrow_schema, options.clone())?;
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )?;
        let mut batch_buf = Vec::new();
        let mut writer =
            StreamWriter::try_new_with_options(&mut batch_buf, &arrow_schema, options)?;
        writer.write(&batch)?;
        let batch_bytes = bytes::Bytes::from(batch_buf[schema_buf.len()..].to_vec());
        Ok((bytes::Bytes::from(schema_buf), batch_bytes))
    }

    fn encode_int64_ipc(
        values: &[i64],
        compression: Option<CompressionType>,
    ) -> Result<(bytes::Bytes, bytes::Bytes), arrow::error::ArrowError> {
        encode_named_int64_ipc("number", values, compression)
    }

    pub(crate) fn job_with_dest_table(project_id: &str, dataset_id: &str, table_id: &str) -> Job {
        Job::new().set_configuration(
            JobConfiguration::new().set_query(
                JobConfigurationQuery::new().set_destination_table(
                    TableReference::new()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .set_table_id(table_id),
                ),
            ),
        )
    }

    pub(crate) fn batch_response(
        schema: Option<bytes::Bytes>,
        batch: bytes::Bytes,
    ) -> ReadRowsResponse {
        let res = ReadRowsResponse::new()
            .set_arrow_record_batch(ArrowRecordBatch::new().set_serialized_record_batch(batch));
        match schema {
            Some(s) => res.set_arrow_schema(ArrowSchema::new().set_serialized_schema(s)),
            None => res,
        }
    }

    pub(crate) fn response_stream<I>(responses: I) -> ResponseStream<ReadRowsResponse>
    where
        I: IntoIterator<Item = ReadRowsResponse>,
    {
        let items: Vec<_> = responses.into_iter().collect();
        let (tx, rx) = mpsc::channel(items.len().max(1));
        for item in items {
            tx.try_send(Ok(item)).expect("channel has capacity");
        }
        ResponseStream::from(rx)
    }

    fn int64_values(batch: &RecordBatch) -> &[i64] {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 0 is Int64Array")
            .values()
    }

    #[tokio::test]
    async fn test_storage_reader_reads_batches_successfully() -> TestResult {
        let (schema_buf, batch_buf) = encode_int64_ipc(&[1, 2, 3], None)?;

        let (tx, rx) = mpsc::channel(2);
        // First message: schema only
        tx.send(Ok(ReadRowsResponse::default().set_schema(
            ReadRowsResponseSchema::ArrowSchema(Box::new(
                ArrowSchema::default().set_serialized_schema(schema_buf),
            )),
        )))
        .await?;
        // Second message: rows
        tx.send(Ok(ReadRowsResponse::default().set_rows(
            Rows::ArrowRecordBatch(Box::new(
                ArrowRecordBatch::default().set_serialized_record_batch(batch_buf),
            )),
        )))
        .await?;
        drop(tx);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |req, _| {
                assert_eq!(
                    req.read_stream,
                    "projects/p1/locations/loc1/jobs/j1/streams/_default"
                );
                assert_eq!(req.offset, 0);
                Ok(ResponseStream::from(rx))
            });

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_location("loc1")
            .set_job_id("j1");

        let mut reader =
            StorageReader::new(read_client, job_ref, None, None, RequestOptions::default());

        let batch1 = reader
            .next_record_batch()
            .await?
            .expect("should yield batch");
        assert_eq!(batch1.num_rows(), 3);
        assert_eq!(
            batch1
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2, 3]
        );

        let batch2 = reader.next_record_batch().await?;
        assert!(batch2.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_handles_rpc_error() -> TestResult {
        let mut mock_read = MockRead::new();
        mock_read.expect_read_rows().times(1).returning(|_, _| {
            let status = Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("permission denied on stream");
            Err(GaxError::service(status))
        });

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_job_id("j1");

        let mut reader =
            StorageReader::new(read_client, job_ref, None, None, RequestOptions::default());
        let err = reader.next_record_batch().await.unwrap_err();
        assert!(matches!(err, RowError::Rpc { .. }), "{err:?}");
        assert!(err.to_string().contains("permission denied on stream"));

        // After error, reader is Completed and returns None
        assert!(reader.next_record_batch().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_empty_stream() -> TestResult {
        let (tx, rx) = mpsc::channel(1);
        drop(tx);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |_, _| Ok(ResponseStream::from(rx)));

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_job_id("j1");

        let mut reader =
            StorageReader::new(read_client, job_ref, None, None, RequestOptions::default());
        assert!(reader.next_record_batch().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_handles_decoding_error_and_transitions_to_completed() -> TestResult
    {
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(ReadRowsResponse::default().set_schema(
            ReadRowsResponseSchema::ArrowSchema(Box::new(
                ArrowSchema::default().set_serialized_schema(bytes::Bytes::from_static(
                    b"\xff\xff\xff\xff\x08\x00\x00\x00invalid_schema",
                )),
            )),
        )))
        .await?;
        drop(tx);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |_, _| Ok(ResponseStream::from(rx)));

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_job_id("j1");

        let mut reader =
            StorageReader::new(read_client, job_ref, None, None, RequestOptions::default());
        let err = reader.next_record_batch().await.unwrap_err();
        assert!(matches!(err, RowError::InvalidRowFormat(_)), "{err:?}");

        // After decoding error, reader must be Completed and return None
        assert!(reader.next_record_batch().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_handles_batch_decoding_error_and_transitions_to_completed()
    -> TestResult {
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(ReadRowsResponse::default().set_rows(
            Rows::ArrowRecordBatch(Box::new(
                ArrowRecordBatch::default().set_serialized_record_batch(bytes::Bytes::from_static(
                    b"\xff\xff\xff\xff\x08\x00\x00\x00invalid_batch",
                )),
            )),
        )))
        .await?;
        drop(tx);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |_, _| Ok(ResponseStream::from(rx)));

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_job_id("j1");

        let mut reader =
            StorageReader::new(read_client, job_ref, None, None, RequestOptions::default());
        let err = reader.next_record_batch().await.unwrap_err();
        assert!(matches!(err, RowError::InvalidRowFormat(_)), "{err:?}");

        // After decoding error, reader must be Completed and return None
        assert!(reader.next_record_batch().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_multi_stream_session_reads_lz4_batches() -> TestResult {
        let (schema_buf, batch_buf1) =
            encode_int64_ipc(&[10, 20], Some(CompressionType::LZ4_FRAME))?;
        let (_, batch_buf2) = encode_int64_ipc(&[30, 40], Some(CompressionType::LZ4_FRAME))?;

        let mut mock_job = MockJobService::new();
        mock_job.expect_get_job().times(1).returning(|req, opts| {
            assert_eq!(req.project_id, "billing-proj");
            assert_eq!(req.job_id, "job-123");
            assert_eq!(req.location, "us-central1");
            assert_eq!(opts.quota_project().as_deref(), Some("quota-proj"));
            Ok(Response::from(job_with_dest_table(
                "dest-proj",
                "_anon_ds",
                "anon_tbl",
            )))
        });

        let schema_for_session = schema_buf.clone();
        let mut mock_read = MockRead::new();
        mock_read
            .expect_create_read_session()
            .times(1)
            .return_once(move |req, opts| {
                assert_eq!(req.parent, "projects/billing-proj");
                assert_eq!(opts.quota_project().as_deref(), Some("quota-proj"));
                let session_req = req.read_session.expect("read_session must be set");
                assert_eq!(
                    session_req.table,
                    "projects/dest-proj/datasets/_anon_ds/tables/anon_tbl"
                );
                assert_eq!(session_req.data_format, DataFormat::Arrow);
                let arrow_opts = session_req
                    .read_options
                    .and_then(|o| o.arrow_serialization_options().cloned())
                    .expect("arrow_serialization_options must be set");
                assert_eq!(arrow_opts.buffer_compression, CompressionCodec::Lz4Frame);

                let session = ReadSession::new()
                    .set_arrow_schema(ArrowSchema::new().set_serialized_schema(schema_for_session))
                    .set_streams([
                        ReadStream::new().set_name("sessions/s1/streams/0"),
                        ReadStream::new().set_name("sessions/s1/streams/1"),
                    ]);
                Ok(Response::from(session))
            });

        let mut seq = mockall::Sequence::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |req, opts| {
                assert_eq!(req.read_stream, "sessions/s1/streams/0");
                assert_eq!(opts.quota_project().as_deref(), Some("quota-proj"));
                Ok(response_stream([batch_response(
                    Some(schema_buf),
                    batch_buf1,
                )]))
            });
        // Stream 1 omits per-message schema to verify fallback to ReadSession.schema.
        mock_read
            .expect_read_rows()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |req, _| {
                assert_eq!(req.read_stream, "sessions/s1/streams/1");
                Ok(response_stream([batch_response(None, batch_buf2)]))
            });

        let mut options = RequestOptions::default();
        options.set_quota_project("quota-proj");
        let mut reader = StorageReader::new(
            Arc::new(crate::client::Read::from_stub(mock_read)),
            JobReference::new()
                .set_project_id("billing-proj")
                .set_location("us-central1")
                .set_job_id("job-123"),
            Some(create_job_service(mock_job)),
            Some(DEFAULT_MULTI_STREAM_THRESHOLD + 1),
            options,
        );

        let b1 = reader.next_record_batch().await?.expect("batch 1");
        assert_eq!(int64_values(&b1), &[10, 20]);
        let b2 = reader.next_record_batch().await?.expect("batch 2");
        assert_eq!(int64_values(&b2), &[30, 40]);
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_upgrades_from_single_stream_on_estimated_row_count() -> TestResult
    {
        let (schema_buf, default_batch) = encode_int64_ipc(&[1, 2], None)?;
        let (_, s0_batch) = encode_int64_ipc(&[1, 2], Some(CompressionType::LZ4_FRAME))?;
        let (_, s1_batch) = encode_int64_ipc(&[3, 4], Some(CompressionType::LZ4_FRAME))?;

        let mut mock_job = MockJobService::new();
        mock_job
            .expect_get_job()
            .times(1)
            .returning(|_, _| Ok(Response::from(job_with_dest_table("p1", "ds1", "t1"))));

        let mut mock_read = MockRead::new();
        mock_read
            .expect_create_read_session()
            .times(1)
            .return_once(|_, _| {
                Ok(Response::from(ReadSession::new().set_streams([
                    ReadStream::new().set_name("streams/0"),
                    ReadStream::new().set_name("streams/1"),
                ])))
            });

        let default_res = batch_response(Some(schema_buf.clone()), default_batch)
            .set_total_estimated_row_count((DEFAULT_MULTI_STREAM_THRESHOLD + 10) as i64);
        let s0_res = batch_response(Some(schema_buf.clone()), s0_batch);
        let s1_res = batch_response(Some(schema_buf), s1_batch);

        let mut seq = mockall::Sequence::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |req, _| {
                assert_eq!(
                    req.read_stream,
                    "projects/p1/locations/US/jobs/j1/streams/_default"
                );
                Ok(response_stream([default_res]))
            });
        mock_read
            .expect_read_rows()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |req, _| {
                assert_eq!(req.read_stream, "streams/0");
                Ok(response_stream([s0_res]))
            });
        mock_read
            .expect_read_rows()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |req, _| {
                assert_eq!(req.read_stream, "streams/1");
                Ok(response_stream([s1_res]))
            });

        let mut reader = StorageReader::new(
            Arc::new(crate::client::Read::from_stub(mock_read)),
            JobReference::new().set_project_id("p1").set_job_id("j1"),
            Some(create_job_service(mock_job)),
            None,
            RequestOptions::default(),
        );

        let b1 = reader.next_record_batch().await?.expect("batch 1");
        assert_eq!(int64_values(&b1), &[1, 2]);
        let b2 = reader.next_record_batch().await?.expect("batch 2");
        assert_eq!(int64_values(&b2), &[3, 4]);
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_does_not_upgrade_after_yielding_rows_or_negative_estimate()
    -> TestResult {
        let (schema_buf, batch1) = encode_int64_ipc(&[1], None)?;
        let (_, batch2) = encode_int64_ipc(&[2], None)?;

        let mut mock_job = MockJobService::new();
        mock_job.expect_get_job().never();

        // Negative estimate (-1) must not wrap to u64::MAX, and yielding batch1 disables later upgrades.
        let r1 = batch_response(Some(schema_buf), batch1).set_total_estimated_row_count(-1);
        let r2 = batch_response(None, batch2)
            .set_total_estimated_row_count((DEFAULT_MULTI_STREAM_THRESHOLD + 100) as i64);

        let mut mock_read = MockRead::new();
        mock_read.expect_create_read_session().never();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |_, _| Ok(response_stream([r1, r2])));

        let mut reader = StorageReader::new(
            Arc::new(crate::client::Read::from_stub(mock_read)),
            JobReference::new().set_project_id("p1").set_job_id("j1"),
            Some(create_job_service(mock_job)),
            None,
            RequestOptions::default(),
        );

        assert_eq!(reader.next_record_batch().await?.expect("b1").num_rows(), 1);
        assert_eq!(reader.next_record_batch().await?.expect("b2").num_rows(), 1);
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[test_case::test_case(Job::new(), None; "no destination table")]
    #[test_case::test_case(job_with_dest_table("p1", "ds1", ""), None; "empty destination table id")]
    #[test_case::test_case(job_with_dest_table("p1", "ds1", "t1"), Some(0); "zero session streams")]
    #[test_case::test_case(job_with_dest_table("p1", "ds1", "t1"), Some(1); "single session stream")]
    #[tokio::test]
    async fn test_storage_reader_multi_stream_falls_back_to_single_stream(
        job: Job,
        session_stream_count: Option<usize>,
    ) -> TestResult {
        let (schema_buf, batch_buf) = encode_int64_ipc(&[7, 8], None)?;

        let mut mock_job = MockJobService::new();
        mock_job
            .expect_get_job()
            .times(1)
            .return_once(move |_, _| Ok(Response::from(job)));

        let mut mock_read = MockRead::new();
        match session_stream_count {
            Some(count) => {
                mock_read
                    .expect_create_read_session()
                    .times(1)
                    .return_once(move |_, _| {
                        let streams =
                            (0..count).map(|i| ReadStream::new().set_name(format!("streams/{i}")));
                        Ok(Response::from(ReadSession::new().set_streams(streams)))
                    });
            }
            None => {
                mock_read.expect_create_read_session().never();
            }
        }
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |req, _| {
                assert_eq!(
                    req.read_stream,
                    "projects/p1/locations/US/jobs/j1/streams/_default"
                );
                Ok(response_stream([batch_response(
                    Some(schema_buf),
                    batch_buf,
                )]))
            });

        let mut reader = StorageReader::new(
            Arc::new(crate::client::Read::from_stub(mock_read)),
            JobReference::new().set_project_id("p1").set_job_id("j1"),
            Some(create_job_service(mock_job)),
            Some(DEFAULT_MULTI_STREAM_THRESHOLD + 1),
            RequestOptions::default(),
        );

        let batch = reader.next_record_batch().await?.expect("fallback batch");
        assert_eq!(int64_values(&batch), &[7, 8]);
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[test_case::test_case(true; "get_job rpc error")]
    #[test_case::test_case(false; "create_read_session rpc error")]
    #[tokio::test]
    async fn test_storage_reader_multi_stream_rpc_error_surfaces_and_completes(
        fail_get_job: bool,
    ) -> TestResult {
        let mut mock_job = MockJobService::new();
        let mut mock_read = MockRead::new();
        mock_read.expect_read_rows().never();

        let denied = |msg: &str| {
            GaxError::service(
                Status::default()
                    .set_code(Code::PermissionDenied)
                    .set_message(msg),
            )
        };
        if fail_get_job {
            mock_job
                .expect_get_job()
                .times(1)
                .returning(move |_, _| Err(denied("get_job denied")));
            mock_read.expect_create_read_session().never();
        } else {
            mock_job
                .expect_get_job()
                .times(1)
                .returning(|_, _| Ok(Response::from(job_with_dest_table("p1", "ds1", "t1"))));
            mock_read
                .expect_create_read_session()
                .times(1)
                .returning(move |_, _| Err(denied("create_read_session denied")));
        }

        let mut reader = StorageReader::new(
            Arc::new(crate::client::Read::from_stub(mock_read)),
            JobReference::new().set_project_id("p1").set_job_id("j1"),
            Some(create_job_service(mock_job)),
            Some(DEFAULT_MULTI_STREAM_THRESHOLD + 1),
            RequestOptions::default(),
        );

        let err = reader
            .next_record_batch()
            .await
            .expect_err("should return RPC error");
        assert!(matches!(err, RowError::Rpc { .. }), "{err:?}");
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }
}
