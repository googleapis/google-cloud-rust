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
    ArrowSerializationOptions, DataFormat, ReadRowsResponse, ReadSession,
    arrow_serialization_options::CompressionCodec, read_rows_response::Rows,
    read_rows_response::Schema as ReadRowsResponseSchema,
    read_session::Schema as ReadSessionSchema, read_session::TableReadOptions,
};
use crate::query::arrow::{ArrowStreamDecoder, RecordBatchSource};
use crate::query::query_handle::build_get_job;
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::JobReference;
use google_cloud_gax::streaming::ResponseStream;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Default threshold below which queries stick to REST pagination.
pub(crate) const DEFAULT_REST_ROW_THRESHOLD: u64 = 10_000;

// TODO(#7038): remove environment variable override once benchmarks establish the optimal threshold.
pub(crate) fn rest_row_threshold() -> u64 {
    std::env::var("GOOGLE_CLOUD_RUST_BIGQUERY_REST_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_REST_ROW_THRESHOLD)
}

/// Reads and decodes Arrow [`RecordBatch`]es from a single Storage Read API stream.
#[derive(Debug)]
pub(crate) struct SingleStreamReader {
    stream: ResponseStream<ReadRowsResponse>,
    decoder: ArrowStreamDecoder,
    schema_loaded: bool,
}

impl SingleStreamReader {
    pub(crate) async fn open(
        read_client: &ReadClient,
        stream_name: String,
        session_schema: Option<&bytes::Bytes>,
        options: &RequestOptions,
    ) -> Result<Self, RowError> {
        let mut decoder = ArrowStreamDecoder::new();
        let mut schema_loaded = false;
        if let Some(schema_bytes) = session_schema
            && !schema_bytes.is_empty()
        {
            decoder.set_schema_bytes(schema_bytes.clone())?;
            schema_loaded = true;
        }

        let stream = read_client
            .read_rows()
            .with_options(options.clone())
            .set_read_stream(stream_name)
            .set_offset(0)
            .send()
            .await?;

        Ok(Self {
            stream,
            decoder,
            schema_loaded,
        })
    }
}

impl RecordBatchSource for SingleStreamReader {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        while let Some(response) = self.stream.next().await {
            let response = response?;

            if !self.schema_loaded
                && let Some(ReadRowsResponseSchema::ArrowSchema(arrow_schema)) = response.schema
                && !arrow_schema.serialized_schema.is_empty()
            {
                self.decoder
                    .set_schema_bytes(arrow_schema.serialized_schema)?;
                self.schema_loaded = true;
            }

            if let Some(Rows::ArrowRecordBatch(batch)) = response.rows
                && let Some(record_batch) =
                    self.decoder.decode_batch(batch.serialized_record_batch)?
                && record_batch.num_rows() > 0
            {
                return Ok(Some(record_batch));
            }
        }

        Ok(None)
    }
}

#[derive(Debug)]
pub(crate) enum ReaderState {
    Unopened,
    Streaming(SingleStreamReader),
    Completed,
}

/// Reads query results via a single Storage Read API stream (`_default`).
#[derive(Debug)]
pub(crate) struct StorageReader {
    pub(crate) read_client: Arc<ReadClient>,
    pub(crate) job_ref: JobReference,
    pub(crate) state: ReaderState,
}

impl StorageReader {
    pub(crate) fn new(read_client: Arc<ReadClient>, job_ref: JobReference) -> Self {
        Self {
            read_client,
            job_ref,
            state: ReaderState::Unopened,
        }
    }
}

impl RecordBatchSource for StorageReader {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        loop {
            match &mut self.state {
                ReaderState::Completed => return Ok(None),
                ReaderState::Unopened => {
                    let project = &self.job_ref.project_id;
                    let location = self.job_ref.location.as_deref().unwrap_or("US");
                    let job_id = &self.job_ref.job_id;
                    let stream_name = format!(
                        "projects/{project}/locations/{location}/jobs/{job_id}/streams/_default"
                    );
                    let reader = match SingleStreamReader::open(
                        &self.read_client,
                        stream_name,
                        None,
                        &RequestOptions::default(),
                    )
                    .await
                    {
                        Ok(reader) => reader,
                        Err(err) => {
                            self.state = ReaderState::Completed;
                            return Err(err);
                        }
                    };

                    self.state = ReaderState::Streaming(reader);
                }
                ReaderState::Streaming(reader) => match reader.next_record_batch().await {
                    Ok(Some(batch)) => return Ok(Some(batch)),
                    Ok(None) => {
                        self.state = ReaderState::Completed;
                        return Ok(None);
                    }
                    Err(err) => {
                        self.state = ReaderState::Completed;
                        return Err(err);
                    }
                },
            }
        }
    }
}

// TODO(#7039): wire into StorageReader once concurrent stream reading lands.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReadSessionInfo {
    pub(crate) streams: Vec<String>,
    pub(crate) arrow_schema: Option<bytes::Bytes>,
}

// TODO(#7039): wire into StorageReader once concurrent stream reading lands.
#[allow(dead_code)]
pub(crate) async fn try_create_read_session(
    read_client: &ReadClient,
    job_service: &JobService,
    job_ref: &JobReference,
    options: &RequestOptions,
) -> Result<Option<ReadSessionInfo>, RowError> {
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

    let session = read_client
        .create_read_session()
        .with_options(options.clone())
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
        )
        .send()
        .await?;

    let arrow_schema = match session.schema {
        Some(ReadSessionSchema::ArrowSchema(schema)) if !schema.serialized_schema.is_empty() => {
            Some(schema.serialized_schema)
        }
        _ => None,
    };
    let streams = session.streams.into_iter().map(|s| s.name).collect();
    Ok(Some(ReadSessionInfo {
        streams,
        arrow_schema,
    }))
}

/// Maximum number of Storage Read API streams read concurrently.
#[allow(dead_code)]
const MAX_CONCURRENT_STREAMS: usize = 16;

/// Number of decoded Arrow [`RecordBatch`]es buffered per concurrent stream worker.
#[allow(dead_code)]
const BATCH_BUFFER_PER_STREAM: usize = 2;

/// Reads Arrow [`RecordBatch`]es concurrently from a [`ReadSessionInfo`]'s streams.
///
/// Worker tasks push decoded batches into a bounded channel for backpressure and
/// are aborted automatically when [`MultiStreamReader`] is dropped or encounters
/// an error.
// TODO(#7039): wire into StorageReader.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct MultiStreamReader {
    rx: mpsc::Receiver<Result<RecordBatch, RowError>>,
    tasks: JoinSet<()>,
    completed: bool,
}

#[allow(dead_code)]
impl MultiStreamReader {
    pub(crate) fn open(
        read_client: Arc<ReadClient>,
        session: ReadSessionInfo,
        options: RequestOptions,
    ) -> Self {
        Self::with_concurrency(read_client, session, options, MAX_CONCURRENT_STREAMS)
    }

    fn with_concurrency(
        read_client: Arc<ReadClient>,
        session: ReadSessionInfo,
        options: RequestOptions,
        max_concurrency: usize,
    ) -> Self {
        let worker_count = session.streams.len().min(max_concurrency.max(1));
        if worker_count == 0 {
            let (_tx, rx) = mpsc::channel(1);
            return Self {
                rx,
                tasks: JoinSet::new(),
                completed: true,
            };
        }

        let channel_capacity = worker_count.saturating_mul(BATCH_BUFFER_PER_STREAM).max(1);
        let (tx, rx) = mpsc::channel(channel_capacity);
        let queue = Arc::new(Mutex::new(VecDeque::from(session.streams)));
        let session_schema = session.arrow_schema;
        let options = Arc::new(options);

        let mut tasks = JoinSet::new();
        for _ in 0..worker_count {
            let read_client = Arc::clone(&read_client);
            let queue = Arc::clone(&queue);
            let session_schema = session_schema.clone();
            let options = Arc::clone(&options);
            let tx = tx.clone();

            tasks.spawn(async move {
                loop {
                    if tx.is_closed() {
                        break;
                    }
                    let stream_name = {
                        let mut guard = queue.lock().unwrap_or_else(|e| e.into_inner());
                        guard.pop_front()
                    };
                    let Some(stream_name) = stream_name else {
                        break;
                    };

                    if let Err(err) = forward_stream(
                        &read_client,
                        stream_name,
                        session_schema.as_ref(),
                        &options,
                        &tx,
                    )
                    .await
                    {
                        let _ = tx.send(Err(err)).await;
                        break;
                    }
                }
            });
        }

        Self {
            rx,
            tasks,
            completed: false,
        }
    }
}

/// Opens a single `ReadRows` stream and forwards decoded [`RecordBatch`]es into
/// `tx`, stopping early if the receiver is dropped or closed.
#[allow(dead_code)]
async fn forward_stream(
    read_client: &ReadClient,
    stream_name: String,
    session_schema: Option<&bytes::Bytes>,
    options: &RequestOptions,
    tx: &mpsc::Sender<Result<RecordBatch, RowError>>,
) -> Result<(), RowError> {
    let mut reader =
        SingleStreamReader::open(read_client, stream_name, session_schema, options).await?;
    while let Some(batch) = reader.next_record_batch().await? {
        if tx.send(Ok(batch)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

impl RecordBatchSource for MultiStreamReader {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        if self.completed {
            return Ok(None);
        }

        match self.rx.recv().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(err)) => {
                self.completed = true;
                self.tasks.abort_all();
                self.rx.close();
                Err(err)
            }
            None => {
                self.completed = true;
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ArrowRecordBatch, ArrowSchema, ReadStream};
    use crate::query::arrow::RecordBatchSource;
    use crate::query::tests::{MockJobService, MockRead, create_job_service};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchemaDef};
    use arrow::ipc::writer::StreamWriter;
    use google_cloud_bigquery_v2::model::{
        Job, JobConfiguration, JobConfigurationQuery, TableReference,
    };
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;
    use tokio::sync::mpsc;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn test_storage_reader_reads_batches_successfully() -> TestResult {
        let arrow_schema = Arc::new(ArrowSchemaDef::new(vec![Field::new(
            "number",
            DataType::Int64,
            false,
        )]));

        let mut schema_buf = Vec::new();
        let _ = StreamWriter::try_new(&mut schema_buf, &arrow_schema)?;

        let col = Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(col)])?;

        let mut batch_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut batch_buf, &arrow_schema)?;
        writer.write(&batch)?;
        let batch_buf = batch_buf[schema_buf.len()..].to_vec();

        let (tx, rx) = mpsc::channel(2);
        // First message: schema only
        tx.send(Ok(ReadRowsResponse::default().set_schema(
            ReadRowsResponseSchema::ArrowSchema(Box::new(
                ArrowSchema::default().set_serialized_schema(bytes::Bytes::from(schema_buf)),
            )),
        )))
        .await?;
        // Second message: rows
        tx.send(Ok(ReadRowsResponse::default().set_rows(
            Rows::ArrowRecordBatch(Box::new(
                ArrowRecordBatch::default()
                    .set_serialized_record_batch(bytes::Bytes::from(batch_buf)),
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

        let mut reader = StorageReader::new(read_client, job_ref);

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

        let mut reader = StorageReader::new(read_client, job_ref);
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

        let mut reader = StorageReader::new(read_client, job_ref);
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

        let mut reader = StorageReader::new(read_client, job_ref);
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

        let mut reader = StorageReader::new(read_client, job_ref);
        let err = reader.next_record_batch().await.unwrap_err();
        assert!(matches!(err, RowError::InvalidRowFormat(_)), "{err:?}");

        // After decoding error, reader must be Completed and return None
        assert!(reader.next_record_batch().await?.is_none());

        Ok(())
    }

    fn job_with_dest_table(project_id: &str, dataset_id: &str, table_id: &str) -> Job {
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

    #[test_case::test_case(0; "zero streams")]
    #[test_case::test_case(1; "single stream")]
    #[test_case::test_case(2; "multiple streams")]
    #[tokio::test]
    async fn test_try_create_read_session_creates_arrow_lz4_session(
        stream_count: usize,
    ) -> TestResult {
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

        let expected_streams: Vec<String> = (0..stream_count)
            .map(|i| format!("sessions/s1/streams/{i}"))
            .collect();
        let streams_for_mock = expected_streams.clone();
        let schema_bytes = bytes::Bytes::from_static(b"arrow-schema-bytes");
        let expected_schema = schema_bytes.clone();
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

                let streams = streams_for_mock
                    .into_iter()
                    .map(|name| ReadStream::new().set_name(name));
                let session = ReadSession::new()
                    .set_arrow_schema(ArrowSchema::new().set_serialized_schema(schema_bytes))
                    .set_streams(streams);
                Ok(Response::from(session))
            });

        let job_service = create_job_service(mock_job);
        let read_client = crate::client::Read::from_stub(mock_read);
        let job_ref = JobReference::new()
            .set_project_id("billing-proj")
            .set_location("us-central1")
            .set_job_id("job-123");
        let mut options = RequestOptions::default();
        options.set_quota_project("quota-proj");

        let result =
            try_create_read_session(&read_client, &job_service, &job_ref, &options).await?;

        assert_eq!(
            result,
            Some(ReadSessionInfo {
                streams: expected_streams,
                arrow_schema: Some(expected_schema),
            })
        );
        Ok(())
    }

    #[test_case::test_case(JobReference::new(), None; "empty job reference")]
    #[test_case::test_case(JobReference::new().set_project_id("p1").set_job_id("j1"), Some(Job::new()); "no destination table")]
    #[test_case::test_case(JobReference::new().set_project_id("p1").set_job_id("j1"), Some(job_with_dest_table("p1", "ds1", "")); "empty destination table id")]
    #[tokio::test]
    async fn test_try_create_read_session_returns_none_without_destination_table(
        job_ref: JobReference,
        job_response: Option<Job>,
    ) -> TestResult {
        let mut mock_job = MockJobService::new();
        match job_response {
            Some(job) => {
                mock_job
                    .expect_get_job()
                    .times(1)
                    .return_once(move |_, _| Ok(Response::from(job)));
            }
            None => {
                mock_job.expect_get_job().never();
            }
        }

        let mut mock_read = MockRead::new();
        mock_read.expect_create_read_session().never();

        let job_service = create_job_service(mock_job);
        let read_client = crate::client::Read::from_stub(mock_read);
        let result = try_create_read_session(
            &read_client,
            &job_service,
            &job_ref,
            &RequestOptions::default(),
        )
        .await?;

        assert_eq!(result, None);
        Ok(())
    }

    #[test_case::test_case(true; "get_job rpc error")]
    #[test_case::test_case(false; "create_read_session rpc error")]
    #[tokio::test]
    async fn test_try_create_read_session_propagates_rpc_errors(fail_get_job: bool) -> TestResult {
        let mut mock_job = MockJobService::new();
        let mut mock_read = MockRead::new();
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

        let job_service = create_job_service(mock_job);
        let read_client = crate::client::Read::from_stub(mock_read);
        let job_ref = JobReference::new().set_project_id("p1").set_job_id("j1");
        let err = try_create_read_session(
            &read_client,
            &job_service,
            &job_ref,
            &RequestOptions::default(),
        )
        .await
        .expect_err("should propagate RPC error");

        assert!(matches!(err, RowError::Rpc { .. }), "{err:?}");
        Ok(())
    }

    fn encode_int64_schema_and_batch(
        values: Vec<i64>,
    ) -> Result<(bytes::Bytes, bytes::Bytes), Box<dyn std::error::Error>> {
        let arrow_schema = Arc::new(ArrowSchemaDef::new(vec![Field::new(
            "number",
            DataType::Int64,
            false,
        )]));

        let mut schema_buf = Vec::new();
        let _ = StreamWriter::try_new(&mut schema_buf, &arrow_schema)?;

        let col = Int64Array::from(values);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(col)])?;

        let mut batch_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut batch_buf, &arrow_schema)?;
        writer.write(&batch)?;
        let batch_bytes = batch_buf[schema_buf.len()..].to_vec();

        Ok((
            bytes::Bytes::from(schema_buf),
            bytes::Bytes::from(batch_bytes),
        ))
    }

    #[test]
    fn test_single_stream_reader_is_send() {
        fn assert_send<T: Send>(_: T) {}

        static_assertions::assert_impl_all!(SingleStreamReader: Send);

        let _check_futures = |read_client: &ReadClient,
                              single: &mut SingleStreamReader,
                              options: &RequestOptions| {
            assert_send(SingleStreamReader::open(
                read_client,
                String::new(),
                None,
                options,
            ));
            assert_send(single.next_record_batch());
        };
    }

    #[tokio::test]
    async fn test_single_stream_reader_uses_session_schema_and_options() -> TestResult {
        let (schema_bytes, batch_bytes) = encode_int64_schema_and_batch(vec![10, 20])?;

        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(ReadRowsResponse::default().set_rows(
            Rows::ArrowRecordBatch(Box::new(
                ArrowRecordBatch::default().set_serialized_record_batch(batch_bytes),
            )),
        )))
        .await?;
        drop(tx);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |req, opts| {
                assert_eq!(req.read_stream, "sessions/s1/streams/0");
                assert_eq!(req.offset, 0);
                assert_eq!(opts.quota_project().as_deref(), Some("quota-proj"));
                Ok(ResponseStream::from(rx))
            });

        let read_client = crate::client::Read::from_stub(mock_read);
        let mut options = RequestOptions::default();
        options.set_quota_project("quota-proj");

        let mut reader = SingleStreamReader::open(
            &read_client,
            "sessions/s1/streams/0".to_string(),
            Some(&schema_bytes),
            &options,
        )
        .await?;

        let batch = reader
            .next_record_batch()
            .await?
            .expect("should yield batch");
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array column")
                .values(),
            &[10, 20]
        );
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[test]
    fn test_multi_stream_reader_is_send() {
        fn assert_send<T: Send>(_: T) {}

        static_assertions::assert_impl_all!(MultiStreamReader: Send);

        let _check_futures = |multi: &mut MultiStreamReader| {
            assert_send(multi.next_record_batch());
        };
    }

    #[tokio::test]
    async fn test_multi_stream_reader_zero_streams_completes_immediately() -> TestResult {
        let mut mock_read = MockRead::new();
        mock_read.expect_read_rows().never();

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let session = ReadSessionInfo {
            streams: vec![],
            arrow_schema: None,
        };
        let mut reader = MultiStreamReader::open(read_client, session, RequestOptions::default());

        assert!(reader.next_record_batch().await?.is_none());
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[test_case::test_case(1, true; "single stream with session schema")]
    #[test_case::test_case(3, true; "multiple streams with session schema")]
    #[test_case::test_case(2, false; "multiple streams with inline stream schema")]
    #[tokio::test]
    async fn test_multi_stream_reader_reads_all_streams(
        stream_count: usize,
        schema_in_session: bool,
    ) -> TestResult {
        let (schema_bytes, _) = encode_int64_schema_and_batch(vec![])?;
        let mut batches_by_stream = std::collections::HashMap::new();
        let mut stream_names = Vec::new();
        let mut expected_values = Vec::new();

        for i in 0..stream_count {
            let name = format!("sessions/s1/streams/{i}");
            let base = (i as i64) * 10;
            let values = vec![base + 1, base + 2];
            expected_values.extend_from_slice(&values);
            let (_, batch_bytes) = encode_int64_schema_and_batch(values)?;
            batches_by_stream.insert(name.clone(), batch_bytes);
            stream_names.push(name);
        }

        let schema_for_mock = schema_bytes.clone();
        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(stream_count)
            .returning(move |req, opts| {
                assert_eq!(req.offset, 0);
                assert_eq!(opts.quota_project().as_deref(), Some("quota-proj"));
                let batch_bytes = batches_by_stream
                    .get(&req.read_stream)
                    .expect("unexpected stream name")
                    .clone();
                let (tx, rx) = mpsc::channel(2);
                if !schema_in_session {
                    tx.try_send(Ok(ReadRowsResponse::default().set_schema(
                        ReadRowsResponseSchema::ArrowSchema(Box::new(
                            ArrowSchema::default().set_serialized_schema(schema_for_mock.clone()),
                        )),
                    )))
                    .expect("channel has capacity");
                }
                tx.try_send(Ok(ReadRowsResponse::default().set_rows(
                    Rows::ArrowRecordBatch(Box::new(
                        ArrowRecordBatch::default().set_serialized_record_batch(batch_bytes),
                    )),
                )))
                .expect("channel has capacity");
                Ok(ResponseStream::from(rx))
            });

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let session = ReadSessionInfo {
            streams: stream_names,
            arrow_schema: schema_in_session.then_some(schema_bytes),
        };
        let mut options = RequestOptions::default();
        options.set_quota_project("quota-proj");

        let mut reader = MultiStreamReader::open(read_client, session, options);
        let mut got_values: Vec<i64> = Vec::new();
        while let Some(batch) = reader.next_record_batch().await? {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array column");
            got_values.extend_from_slice(col.values());
        }
        got_values.sort_unstable();
        expected_values.sort_unstable();
        assert_eq!(got_values, expected_values);
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_stream_reader_respects_concurrency_limit() -> TestResult {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (schema_bytes, batch_bytes) = encode_int64_schema_and_batch(vec![42])?;
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let active_for_mock = Arc::clone(&active);
        let peak_for_mock = Arc::clone(&peak);
        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(4)
            .returning(move |_, _| {
                let current = active_for_mock.fetch_add(1, Ordering::SeqCst) + 1;
                peak_for_mock.fetch_max(current, Ordering::SeqCst);

                let (tx, rx) = mpsc::channel(1);
                let batch = batch_bytes.clone();
                let active_done = Arc::clone(&active_for_mock);
                tokio::spawn(async move {
                    tokio::task::yield_now().await;
                    let _ = tx
                        .send(Ok(ReadRowsResponse::default().set_rows(
                            Rows::ArrowRecordBatch(Box::new(
                                ArrowRecordBatch::default().set_serialized_record_batch(batch),
                            )),
                        )))
                        .await;
                    active_done.fetch_sub(1, Ordering::SeqCst);
                });
                Ok(ResponseStream::from(rx))
            });

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let session = ReadSessionInfo {
            streams: (0..4).map(|i| format!("sessions/s1/streams/{i}")).collect(),
            arrow_schema: Some(schema_bytes),
        };
        let mut reader =
            MultiStreamReader::with_concurrency(read_client, session, RequestOptions::default(), 2);

        let mut total_rows = 0;
        while let Some(batch) = reader.next_record_batch().await? {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 4);
        assert!(peak.load(Ordering::SeqCst) <= 2);
        Ok(())
    }

    #[test_case::test_case("open_rpc"; "read_rows open rpc error")]
    #[test_case::test_case("stream_rpc"; "mid-stream rpc error")]
    #[test_case::test_case("decode"; "invalid arrow batch decoding error")]
    #[tokio::test]
    async fn test_multi_stream_reader_propagates_errors_and_completes(kind: &str) -> TestResult {
        let (schema_bytes, _) = encode_int64_schema_and_batch(vec![])?;
        let kind_for_mock = kind.to_string();

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .returning(move |_, _| {
                let denied = || {
                    GaxError::service(
                        Status::default()
                            .set_code(Code::PermissionDenied)
                            .set_message("stream failed"),
                    )
                };
                match kind_for_mock.as_str() {
                    "open_rpc" => Err(denied()),
                    "stream_rpc" => {
                        let (tx, rx) = mpsc::channel(1);
                        tx.try_send(Err(denied())).expect("channel has capacity");
                        Ok(ResponseStream::from(rx))
                    }
                    _ => {
                        let (tx, rx) = mpsc::channel(1);
                        tx.try_send(Ok(ReadRowsResponse::default().set_rows(
                            Rows::ArrowRecordBatch(Box::new(
                                ArrowRecordBatch::default().set_serialized_record_batch(
                                    bytes::Bytes::from_static(
                                        b"\xff\xff\xff\xff\x08\x00\x00\x00invalid_batch",
                                    ),
                                ),
                            )),
                        )))
                        .expect("channel has capacity");
                        Ok(ResponseStream::from(rx))
                    }
                }
            });

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let session = ReadSessionInfo {
            streams: vec!["sessions/s1/streams/0".to_string()],
            arrow_schema: Some(schema_bytes),
        };
        let mut reader = MultiStreamReader::open(read_client, session, RequestOptions::default());

        let err = reader
            .next_record_batch()
            .await
            .expect_err("should surface stream error");
        match kind {
            "open_rpc" | "stream_rpc" => {
                assert!(matches!(err, RowError::Rpc { .. }), "{err:?}");
                assert!(err.to_string().contains("stream failed"));
            }
            "decode" => {
                assert!(matches!(err, RowError::InvalidRowFormat(_)), "{err:?}");
            }
            other => panic!("unexpected test kind: {other}"),
        }

        // Subsequent calls must return Ok(None).
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_stream_reader_aborts_sibling_streams_on_error() -> TestResult {
        let (schema_bytes, _) = encode_int64_schema_and_batch(vec![])?;
        let (sibling_tx, sibling_rx) = mpsc::channel(1);
        let sibling_rx = Mutex::new(Some(sibling_rx));
        let sibling_opened = Arc::new(tokio::sync::Notify::new());
        let opened_for_mock = Arc::clone(&sibling_opened);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(2)
            .returning(move |req, _| {
                if req.read_stream.ends_with("/0") {
                    let rx = sibling_rx
                        .lock()
                        .unwrap()
                        .take()
                        .expect("sibling stream opened once");
                    opened_for_mock.notify_one();
                    return Ok(ResponseStream::from(rx));
                }

                let (err_tx, err_rx) = mpsc::channel(1);
                let opened = Arc::clone(&opened_for_mock);
                tokio::spawn(async move {
                    opened.notified().await;
                    let _ = err_tx
                        .send(Err(GaxError::service(
                            Status::default()
                                .set_code(Code::PermissionDenied)
                                .set_message("sibling error"),
                        )))
                        .await;
                });
                Ok(ResponseStream::from(err_rx))
            });

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let session = ReadSessionInfo {
            streams: vec![
                "sessions/s1/streams/0".to_string(),
                "sessions/s1/streams/1".to_string(),
            ],
            arrow_schema: Some(schema_bytes),
        };
        let mut reader = MultiStreamReader::open(read_client, session, RequestOptions::default());

        let err = reader
            .next_record_batch()
            .await
            .expect_err("should surface stream error");
        assert!(matches!(err, RowError::Rpc { .. }), "{err:?}");

        // The sibling stream worker must be aborted and drop its receiver.
        tokio::time::timeout(std::time::Duration::from_secs(2), sibling_tx.closed()).await?;
        assert!(sibling_tx.is_closed());
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_stream_reader_cancels_tasks_on_drop() -> TestResult {
        let (schema_bytes, batch_bytes) = encode_int64_schema_and_batch(vec![1, 2])?;
        let (stream_tx, stream_rx) = mpsc::channel(2);
        stream_tx
            .send(Ok(ReadRowsResponse::default().set_rows(
                Rows::ArrowRecordBatch(Box::new(
                    ArrowRecordBatch::default().set_serialized_record_batch(batch_bytes),
                )),
            )))
            .await?;

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |_, _| Ok(ResponseStream::from(stream_rx)));

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let session = ReadSessionInfo {
            streams: vec!["sessions/s1/streams/0".to_string()],
            arrow_schema: Some(schema_bytes),
        };
        let mut reader = MultiStreamReader::open(read_client, session, RequestOptions::default());

        let first = reader
            .next_record_batch()
            .await?
            .expect("first batch should be yielded");
        assert_eq!(first.num_rows(), 2);

        // Dropping the reader must abort the background worker and drop its stream receiver.
        drop(reader);
        tokio::time::timeout(std::time::Duration::from_secs(2), stream_tx.closed()).await?;
        assert!(stream_tx.is_closed());
        Ok(())
    }
}
