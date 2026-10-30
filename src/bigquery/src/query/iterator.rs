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
use crate::error::RowError;
#[cfg(any(
    google_cloud_unstable_bigquery_arrow,
    google_cloud_unstable_bigquery_storage_read
))]
use crate::query::arrow::{ArrowReader, ArrowResponseReader, RecordBatchSource};
use crate::query::query_handle::CachedData;
use crate::query::{CompleteQuery, Row, Schema};
#[cfg(any(
    google_cloud_unstable_bigquery_arrow,
    google_cloud_unstable_bigquery_storage_read
))]
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{GetQueryResultsRequest, JobReference};
use std::collections::VecDeque;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, RowError>;
/// An iterator over rows returned by a query.
///
/// [`CompleteQuery::read()`](crate::query::CompleteQuery::read) returns a
/// `RowIterator`.
///
/// `RowIterator` yields rows from the query result set, automatically requesting
/// subsequent pages from BigQuery as needed until the entire result set has been consumed.
///
/// # Example
///
/// ```
/// # use google_cloud_bigquery::client::BigQuery;
/// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
/// let mut rows = client
///     .query("SELECT name, state FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 10")
///     .until_done()
///     .await?
///     .read();
///
/// while let Some(row) = rows.next().await.transpose()? {
///     let name: String = row.get("name")?;
///     let state: String = row.get("state")?;
///     println!("{name} from {state}");
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct RowIterator {
    job_service: Arc<JobService>,
    job_ref: Option<JobReference>,
    schema: Arc<Schema>,
    page_token: Option<String>,
    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    current_batch: Option<Arc<RecordBatch>>,
    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    row_index: usize,
    rows: VecDeque<wkt::Struct>,
    page_size: Option<u32>,
    options: RequestOptions,
    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    arrow_reader: Option<ArrowReader>,
}

impl RowIterator {
    pub(crate) fn new(q: CompleteQuery) -> Self {
        #[cfg(any(
            google_cloud_unstable_bigquery_arrow,
            google_cloud_unstable_bigquery_storage_read
        ))]
        let (rows, schema, page_token, arrow_reader) = {
            let metadata_schema = Arc::new(Schema::new(q.metadata.schema.unwrap_or_default()));

            #[cfg(google_cloud_unstable_bigquery_storage_read)]
            let storage_reader = {
                let has_cached = match &q.cached_data {
                    CachedData::Rows(rows) => !rows.is_empty(),
                    CachedData::Arrow {
                        serialized_record_batch,
                        ..
                    } => !serialized_record_batch.is_empty(),
                };
                let has_remaining_rows = q.page_token.is_some()
                    || (!has_cached && q.metadata.total_rows.unwrap_or(0) > 0);
                let exceeds_rest_threshold = q
                    .metadata
                    .total_rows
                    .is_none_or(|r| r > crate::query::storage_reader::rest_row_threshold());

                if let (Some(read_client), Some(job_ref)) = (q.read_client, q.job_ref.clone()) {
                    if has_remaining_rows && exceeds_rest_threshold {
                        Some(crate::query::storage_reader::StorageReader::new(
                            read_client,
                            job_ref,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            #[cfg(google_cloud_unstable_bigquery_storage_read)]
            let maybe_storage = storage_reader.map(|r| {
                let schema = match &q.cached_data {
                    CachedData::Arrow {
                        serialized_schema, ..
                    } if !serialized_schema.is_empty() => {
                        Arc::new(Schema::from_arrow_schema(serialized_schema).unwrap_or_default())
                    }
                    _ => metadata_schema.clone(),
                };
                (VecDeque::new(), schema, None, Some(ArrowReader::Storage(r)))
            });

            #[cfg(not(google_cloud_unstable_bigquery_storage_read))]
            let maybe_storage: Option<(
                VecDeque<wkt::Struct>,
                Arc<Schema>,
                Option<String>,
                Option<ArrowReader>,
            )> = None;

            if let Some(res) = maybe_storage {
                res
            } else {
                match q.cached_data {
                    CachedData::Rows(rows) => (rows, metadata_schema, q.page_token, None),
                    CachedData::Arrow {
                        serialized_record_batch,
                        serialized_schema,
                    } => {
                        let schema = Arc::new(
                            Schema::from_arrow_schema(&serialized_schema).unwrap_or_default(),
                        );
                        (
                            VecDeque::new(),
                            schema,
                            q.page_token,
                            Some(ArrowReader::Response(ArrowResponseReader::new(
                                serialized_schema,
                                serialized_record_batch,
                            ))),
                        )
                    }
                }
            }
        };
        #[cfg(not(any(
            google_cloud_unstable_bigquery_arrow,
            google_cloud_unstable_bigquery_storage_read
        )))]
        let (rows, schema) = match q.cached_data {
            CachedData::Rows(rows) => {
                let schema = Arc::new(Schema::new(q.metadata.schema.unwrap_or_default()));
                (rows, schema)
            }
            // TODO(#7035): remove once google_cloud_unstable_bigquery_arrow is stabilized
            CachedData::Arrow { .. } => {
                unimplemented!("Arrow IPC decoding is not yet implemented (#7035)")
            }
        };
        Self {
            job_service: q.job_service,
            job_ref: q.job_ref,
            schema,
            #[cfg(any(
                google_cloud_unstable_bigquery_arrow,
                google_cloud_unstable_bigquery_storage_read
            ))]
            page_token,
            #[cfg(not(any(
                google_cloud_unstable_bigquery_arrow,
                google_cloud_unstable_bigquery_storage_read
            )))]
            page_token: q.page_token,
            #[cfg(any(
                google_cloud_unstable_bigquery_arrow,
                google_cloud_unstable_bigquery_storage_read
            ))]
            current_batch: None,
            #[cfg(any(
                google_cloud_unstable_bigquery_arrow,
                google_cloud_unstable_bigquery_storage_read
            ))]
            row_index: 0,
            rows,
            page_size: q.page_size,
            options: q.options,
            #[cfg(any(
                google_cloud_unstable_bigquery_arrow,
                google_cloud_unstable_bigquery_storage_read
            ))]
            arrow_reader,
        }
    }

    /// Sets the maximum number of rows to buffer in memory.
    ///
    /// This controls page size during network fetches when streaming result
    /// sets.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample(client: BigQuery) -> anyhow::Result<()> {
    /// let mut rows = client
    ///     .query("SELECT 1 AS n")
    ///     .until_done()
    ///     .await?
    ///     .read()
    ///     .set_page_size(500);
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_page_size(mut self, page_size: u32) -> Self {
        self.page_size = Some(page_size);
        self
    }

    /// Fetches the next [`Row`](crate::query::Row) from the result set.
    ///
    /// Returns `None` when all rows have been retrieved.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::query::RowIterator;
    /// # async fn sample(mut rows: RowIterator) -> anyhow::Result<()> {
    /// while let Some(row) = rows.next().await.transpose()? {
    ///     let msg: String = row.get("msg")?;
    ///     println!("Message: {msg}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next(&mut self) -> Option<Result<Row>> {
        loop {
            #[cfg(any(
                google_cloud_unstable_bigquery_arrow,
                google_cloud_unstable_bigquery_storage_read
            ))]
            {
                if let Some(batch) = &self.current_batch {
                    if self.row_index < batch.num_rows() {
                        let idx = self.row_index;
                        self.row_index += 1;
                        return Some(Row::try_new_from_arrow(batch, idx, &self.schema));
                    }
                    self.current_batch = None;
                    self.row_index = 0;
                }

                if let Some(ref mut arrow_reader) = self.arrow_reader {
                    match arrow_reader.next_record_batch().await {
                        Ok(Some(batch)) => {
                            self.current_batch = Some(Arc::new(batch));
                            self.row_index = 0;
                            continue;
                        }
                        Ok(None) => {
                            self.arrow_reader = None;
                        }
                        Err(err) => {
                            self.arrow_reader = None;
                            return Some(Err(err));
                        }
                    }
                }
            }

            if let Some(raw_row) = self.rows.pop_front() {
                return Some(Row::try_new(raw_row, &self.schema));
            }

            if let Err(e) = self.try_fetch_page().await {
                return Some(Err(e));
            }

            if self.rows.is_empty() && self.page_token.is_none() {
                return None;
            }
        }
    }

    async fn try_fetch_page(&mut self) -> Result<()> {
        let Some(token) = self.page_token.as_deref() else {
            return Ok(());
        };

        let job_ref = self.job_ref.clone().expect(
            "only queries with a job reference should have page tokens and can fetch more pages",
        );
        let job_service = self.job_service.clone();
        let options = self.options.clone();
        let page_size = self.page_size;

        let (fetched_rows, next_token) =
            Self::fetch_page(&job_service, &options, &job_ref, page_size, token).await?;
        if fetched_rows.is_empty() && next_token.as_deref() == Some(token) {
            self.page_token = None;
        } else {
            self.page_token = next_token;
            self.rows.extend(fetched_rows);
        }
        Ok(())
    }

    // Fetches the next page of results and the next page token.
    async fn fetch_page(
        job_service: &JobService,
        options: &RequestOptions,
        job_ref: &JobReference,
        page_size: Option<u32>,
        token: &str,
    ) -> Result<(Vec<wkt::Struct>, Option<String>)> {
        let mut req = GetQueryResultsRequest::new()
            .set_project_id(job_ref.project_id.clone())
            .set_or_clear_max_results(page_size)
            .set_job_id(job_ref.job_id.clone())
            .set_page_token(token)
            .set_format_options(
                google_cloud_bigquery_v2::model::DataFormatOptions::new()
                    .set_use_int64_timestamp(true),
            );
        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let res = job_service
            .get_query_results()
            .with_options(options.clone())
            .with_request(req)
            .send()
            .await?;

        let page_token = if res.page_token.is_empty() {
            None
        } else {
            Some(res.page_token)
        };

        Ok((res.rows, page_token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::tests::{MockJobService, create_job_service};
    use google_cloud_bigquery_v2::model::{
        DataFormatOptions, GetQueryResultsResponse, JobReference, QueryResponse, TableFieldSchema,
        TableSchema,
    };
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;
    use serde_json::{Map, json};
    use std::sync::Arc;

    type TestResult = anyhow::Result<()>;

    fn create_test_schema() -> TableSchema {
        TableSchema::new().set_fields([TableFieldSchema::new()
            .set_name("col")
            .set_type("STRING")
            .set_mode("NULLABLE")])
    }

    fn create_test_row(val: &str) -> wkt::Struct {
        Map::from_iter([("f".to_string(), json!([{ "v": val }]))])
    }

    fn create_test_job_ref() -> JobReference {
        JobReference::new()
            .set_project_id("test_project")
            .set_job_id("test_job")
    }

    fn create_test_job_ref_with_location(location: &str) -> JobReference {
        create_test_job_ref().set_location(location)
    }

    fn create_test_complete_query(
        job_service: Arc<JobService>,
        job_ref: Option<JobReference>,
        rows: Vec<wkt::Struct>,
        page_token: Option<String>,
    ) -> CompleteQuery {
        let mut res = QueryResponse::new()
            .set_schema(create_test_schema())
            .set_rows(rows);
        if let Some(token) = page_token {
            res = res.set_page_token(token);
        }
        if let Some(job_ref) = job_ref {
            res = res.set_job_reference(job_ref);
        }
        CompleteQuery::from_query_response(job_service, res, None)
    }

    #[tokio::test]
    async fn test_row_iterator_empty_no_token() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let q = create_test_complete_query(job_service, Some(create_test_job_ref()), vec![], None);
        let mut iter = q.read();
        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_cached_rows_only() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let rows = vec![create_test_row("first"), create_test_row("second")];
        let q = create_test_complete_query(job_service, Some(create_test_job_ref()), rows, None);
        let mut iter = q.read();

        let row1 = iter.next().await.expect("should have row 1")?;
        assert_eq!(row1.get::<String>("col")?, "first");

        let row2 = iter.next().await.expect("should have row 2")?;
        assert_eq!(row2.get::<String>("col")?, "second");

        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_row_conversion_error() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let invalid_row = Map::from_iter([("f".to_string(), json!([]))]);
        let q = create_test_complete_query(
            job_service,
            Some(create_test_job_ref()),
            vec![invalid_row],
            None,
        );
        let mut iter = q.read();

        let err = iter.next().await.expect("should return error").unwrap_err();
        assert!(matches!(err, RowError::InvalidRowFormat(_)), "{err:?}");
        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_fetch_multiple_pages() -> TestResult {
        let mut mock = MockJobService::new();
        let mut seq = mockall::Sequence::new();

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.page_token, "token_page_1");
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![
                        create_test_row("page_1_row_1"),
                        create_test_row("page_1_row_2"),
                    ])
                    .set_page_token("token_page_2");
                Ok(Response::from(res))
            });

        mock.expect_get_query_results()
            .in_sequence(&mut seq)
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.page_token, "token_page_2");
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![create_test_row("page_2_row_1")])
                    .set_page_token("");
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let q = create_test_complete_query(
            job_service,
            Some(create_test_job_ref()),
            vec![create_test_row("cached_row")],
            Some("token_page_1".to_string()),
        );
        let mut iter = q.read();

        let row1 = iter.next().await.expect("should have row 1")?;
        assert_eq!(row1.get::<String>("col")?, "cached_row");

        let row2 = iter.next().await.expect("should have row 2")?;
        assert_eq!(row2.get::<String>("col")?, "page_1_row_1");

        let row3 = iter.next().await.expect("should have row 3")?;
        assert_eq!(row3.get::<String>("col")?, "page_1_row_2");

        let row4 = iter.next().await.expect("should have row 4")?;
        assert_eq!(row4.get::<String>("col")?, "page_2_row_1");

        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_fetch_page_request_parameters() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results()
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.project_id, "test_project");
                assert_eq!(req.job_id, "test_job");
                assert_eq!(req.page_token, "token_1");
                assert_eq!(req.max_results, Some(50));
                assert_eq!(req.location, "us-east1");
                assert_eq!(
                    req.format_options,
                    Some(DataFormatOptions::new().set_use_int64_timestamp(true))
                );
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![create_test_row("page_row")])
                    .set_page_token("");
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let q = create_test_complete_query(
            job_service,
            Some(create_test_job_ref_with_location("us-east1")),
            vec![],
            Some("token_1".to_string()),
        );
        let mut iter = q.read().set_page_size(50);

        let row = iter.next().await.expect("should have row")?;
        assert_eq!(row.get::<String>("col")?, "page_row");

        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_inherits_page_size() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results()
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.max_results, Some(25));
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![create_test_row("page_row")])
                    .set_page_token("");
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let res = QueryResponse::new()
            .set_schema(create_test_schema())
            .set_job_reference(create_test_job_ref())
            .set_page_token("token_1");
        let q = CompleteQuery::from_query_response(job_service, res, Some(25));
        let mut iter = q.read();

        let row = iter.next().await.expect("should have row")?;
        assert_eq!(row.get::<String>("col")?, "page_row");
        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_fetch_page_rpc_error() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results().returning(|_, _| {
            let status = Status::default()
                .set_code(Code::Unavailable)
                .set_message("temporary service error");
            Err(GaxError::service(status))
        });

        let job_service = create_job_service(mock);
        let q = create_test_complete_query(
            job_service,
            Some(create_test_job_ref()),
            vec![],
            Some("token_err".to_string()),
        );
        let mut iter = q.read();

        let err = iter.next().await.expect("should return error").unwrap_err();
        assert!(matches!(err, RowError::Rpc { .. }), "{err:?}");
        assert!(
            err.to_string().contains("temporary service error"),
            "{err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_empty_page_handling() -> TestResult {
        let mut mock = MockJobService::new();
        let mut seq = mockall::Sequence::new();

        // empty page with a new token: should continue fetching
        mock.expect_get_query_results()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req, _| {
                assert_eq!(req.page_token, "token_1");
                let res = GetQueryResultsResponse::new()
                    .set_rows(Vec::<wkt::Struct>::new())
                    .set_page_token("token_2");
                Ok(Response::from(res))
            });

        // next page returns rows and advances to token_3
        mock.expect_get_query_results()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req, _| {
                assert_eq!(req.page_token, "token_2");
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![create_test_row("page2_row")])
                    .set_page_token("token_3");
                Ok(Response::from(res))
            });

        // empty page with the same token: should terminate pagination
        mock.expect_get_query_results()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req, _| {
                assert_eq!(req.page_token, "token_3");
                let res = GetQueryResultsResponse::new()
                    .set_rows(Vec::<wkt::Struct>::new())
                    .set_page_token("token_3");
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let q = create_test_complete_query(
            job_service,
            Some(create_test_job_ref()),
            vec![],
            Some("token_1".to_string()),
        );
        let mut iter = q.read();

        let row = iter.next().await.expect("should have row")?;
        assert_eq!(row.get::<String>("col")?, "page2_row");
        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_inherits_user_agent() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results()
            .times(1)
            .returning(|_, options| {
                assert_eq!(options.user_agent().as_deref(), Some("my-app/1.2.3"));
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![create_test_row("page_row")])
                    .set_page_token("");
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let res = QueryResponse::new()
            .set_schema(create_test_schema())
            .set_job_reference(create_test_job_ref())
            .set_page_token("token_1");
        let mut q = CompleteQuery::from_query_response(job_service, res, None);
        q.options.set_user_agent("my-app/1.2.3");
        let mut iter = q.read();

        let row = iter.next().await.expect("should have row")?;
        assert_eq!(row.get::<String>("col")?, "page_row");
        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_inherits_quota_project() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results()
            .times(1)
            .returning(|_, options| {
                assert_eq!(options.quota_project().as_deref(), Some("test_project"));
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![create_test_row("page_row")])
                    .set_page_token("");
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let res = QueryResponse::new()
            .set_schema(create_test_schema())
            .set_job_reference(create_test_job_ref())
            .set_page_token("token_1");
        let mut q = CompleteQuery::from_query_response(job_service, res, None);
        q.options.set_quota_project("test_project");
        let mut iter = q.read();

        let row = iter.next().await.expect("should have row")?;
        assert_eq!(row.get::<String>("col")?, "page_row");
        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    #[tokio::test]
    async fn test_row_iterator_cached_arrow_multiple_batches() -> TestResult {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::ipc::writer::StreamWriter;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("col", DataType::Utf8, false),
            Field::new("num", DataType::Int64, false),
        ]));

        let mut schema_buf = Vec::new();
        let _ = StreamWriter::try_new(&mut schema_buf, &arrow_schema)?;

        let batch1 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["hello", "world"])),
                Arc::new(Int64Array::from(vec![42, 100])),
            ],
        )?;
        let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
        let batch2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["last"])),
                Arc::new(Int64Array::from(vec![999])),
            ],
        )?;

        let mut stream_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut stream_buf, &arrow_schema)?;
        writer.write(&batch1)?;
        writer.write(&empty_batch)?;
        writer.write(&batch2)?;
        writer.finish()?;
        let batch_buf = stream_buf[schema_buf.len()..].to_vec();

        let job_service = create_job_service(MockJobService::new());
        let q = CompleteQuery {
            job_service,
            read_client: None,
            job_ref: None,
            cached_data: CachedData::Arrow {
                serialized_schema: schema_buf.into(),
                serialized_record_batch: batch_buf.into(),
            },
            page_token: None,
            metadata: Default::default(),
            page_size: None,
            options: Default::default(),
        };

        let mut iter = q.read();

        let row1 = iter.next().await.expect("row 1")?;
        assert_eq!(row1.get::<String>("col")?, "hello");
        assert_eq!(row1.get::<i64>("num")?, 42);

        let row2 = iter.next().await.expect("row 2")?;
        assert_eq!(row2.get::<String>("col")?, "world");
        assert_eq!(row2.get::<i64>("num")?, 100);

        let row3 = iter.next().await.expect("row 3")?;
        assert_eq!(row3.get::<String>("col")?, "last");
        assert_eq!(row3.get::<i64>("num")?, 999);

        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    #[tokio::test]
    async fn test_row_iterator_cached_arrow_then_fetch_page() -> TestResult {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::ipc::writer::StreamWriter;

        let mut mock = MockJobService::new();
        mock.expect_get_query_results()
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.page_token, "token_page_1");
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![create_test_row("from_json_page")])
                    .set_page_token("");
                Ok(Response::from(res))
            });

        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "col",
            DataType::Utf8,
            false,
        )]));
        let mut schema_buf = Vec::new();
        let _ = StreamWriter::try_new(&mut schema_buf, &arrow_schema)?;

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["from_arrow_batch"]))],
        )?;
        let mut stream_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut stream_buf, &arrow_schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        let batch_buf = stream_buf[schema_buf.len()..].to_vec();

        let job_service = create_job_service(mock);
        let q = CompleteQuery {
            job_service,
            read_client: None,
            job_ref: Some(create_test_job_ref()),
            cached_data: CachedData::Arrow {
                serialized_schema: schema_buf.into(),
                serialized_record_batch: batch_buf.into(),
            },
            page_token: Some("token_page_1".to_string()),
            metadata: Default::default(),
            page_size: None,
            options: Default::default(),
        };

        let mut iter = q.read();

        let row1 = iter.next().await.expect("row 1")?;
        assert_eq!(row1.get::<String>("col")?, "from_arrow_batch");

        let row2 = iter.next().await.expect("row 2")?;
        assert_eq!(row2.get::<String>("col")?, "from_json_page");

        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    #[tokio::test]
    async fn test_row_iterator_cached_arrow_empty_batch_then_json_pagination() -> TestResult {
        let mut mock = MockJobService::new();
        mock.expect_get_query_results()
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.page_token, "token_page_1");
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![create_test_row("from_json_page")])
                    .set_page_token("");
                Ok(Response::from(res))
            });

        let job_service = create_job_service(mock);
        let q = CompleteQuery {
            job_service,
            read_client: None,
            job_ref: Some(create_test_job_ref()),
            cached_data: CachedData::Arrow {
                serialized_schema: create_test_arrow_schema_bytes(),
                serialized_record_batch: bytes::Bytes::new(),
            },
            page_token: Some("token_page_1".to_string()),
            metadata: Default::default(),
            page_size: None,
            options: Default::default(),
        };

        let mut iter = q.read();

        let row = iter.next().await.expect("row 1")?;
        assert_eq!(row.get::<String>("col")?, "from_json_page");

        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    fn create_test_arrow_schema_bytes() -> bytes::Bytes {
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::ipc::writer::StreamWriter;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "col",
            DataType::Utf8,
            false,
        )]));
        let mut schema_buf = Vec::new();
        let _ =
            StreamWriter::try_new(&mut schema_buf, &arrow_schema).expect("valid test arrow schema");
        schema_buf.into()
    }

    #[cfg(any(
        google_cloud_unstable_bigquery_arrow,
        google_cloud_unstable_bigquery_storage_read
    ))]
    #[test_case::test_case(
        bytes::Bytes::from_static(b"\xff\xff\xff\xff\x08\x00\x00\x00invalid_schema"),
        bytes::Bytes::from_static(b"batch");
        "malformed schema"
    )]
    #[test_case::test_case(
        create_test_arrow_schema_bytes(),
        bytes::Bytes::from_static(b"\xff\xff\xff\xff\x08\x00\x00\x00invalid_batch");
        "malformed record batch"
    )]
    #[test_case::test_case(
        create_test_arrow_schema_bytes(),
        bytes::Bytes::from_static(b"\xff\xff\xff\xff\x10\x00\x00\x00truncated");
        "truncated stream"
    )]
    #[tokio::test]
    async fn test_row_iterator_cached_arrow_malformed_ipc(
        serialized_schema: bytes::Bytes,
        serialized_record_batch: bytes::Bytes,
    ) -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let q = CompleteQuery {
            job_service,
            read_client: None,
            job_ref: None,
            cached_data: CachedData::Arrow {
                serialized_schema,
                serialized_record_batch,
            },
            page_token: None,
            metadata: Default::default(),
            page_size: None,
            options: Default::default(),
        };

        let mut iter = q.read();
        let err = iter.next().await.expect("should return error").unwrap_err();
        assert!(matches!(err, RowError::InvalidRowFormat(_)), "{err:?}");
        assert!(iter.next().await.is_none(), "{iter:?}");
        Ok(())
    }

    #[test]
    fn test_row_iterator_next_future_is_send() {
        fn assert_send<T: Send>(_: T) {}
        fn check(mut iter: RowIterator) {
            assert_send(iter.next());
        }
        let _ = check;
    }

    #[cfg(google_cloud_unstable_bigquery_storage_read)]
    #[tokio::test]
    async fn test_row_iterator_storage_read_success() -> TestResult {
        use crate::model::{ArrowRecordBatch, ArrowSchema};
        use crate::query::tests::MockRead;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchemaDef};
        use arrow::ipc::writer::StreamWriter;
        use google_cloud_gax::streaming::ResponseStream;
        use tokio::sync::mpsc;

        let arrow_schema = Arc::new(ArrowSchemaDef::new(vec![Field::new(
            "col",
            DataType::Int64,
            false,
        )]));
        let mut schema_buf = Vec::new();
        let _ = StreamWriter::try_new(&mut schema_buf, &arrow_schema)?;

        let batch1 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )?;
        let mut batch_buf1 = Vec::new();
        let mut writer1 = StreamWriter::try_new(&mut batch_buf1, &arrow_schema)?;
        writer1.write(&batch1)?;
        let batch_buf1 = batch_buf1[schema_buf.len()..].to_vec();

        let batch2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![30]))],
        )?;
        let mut batch_buf2 = Vec::new();
        let mut writer2 = StreamWriter::try_new(&mut batch_buf2, &arrow_schema)?;
        writer2.write(&batch2)?;
        let batch_buf2 = batch_buf2[schema_buf.len()..].to_vec();

        let (tx, rx) = mpsc::channel(3);
        tx.send(Ok(crate::model::ReadRowsResponse::default().set_schema(
            crate::model::read_rows_response::Schema::ArrowSchema(Box::new(
                ArrowSchema::default().set_serialized_schema(bytes::Bytes::from(schema_buf)),
            )),
        )))
        .await?;
        tx.send(Ok(crate::model::ReadRowsResponse::default().set_rows(
            crate::model::read_rows_response::Rows::ArrowRecordBatch(Box::new(
                ArrowRecordBatch::default()
                    .set_serialized_record_batch(bytes::Bytes::from(batch_buf1)),
            )),
        )))
        .await?;
        tx.send(Ok(crate::model::ReadRowsResponse::default().set_rows(
            crate::model::read_rows_response::Rows::ArrowRecordBatch(Box::new(
                ArrowRecordBatch::default()
                    .set_serialized_record_batch(bytes::Bytes::from(batch_buf2)),
            )),
        )))
        .await?;
        drop(tx);

        let mut mock_job = MockJobService::new();
        mock_job.expect_get_query_results().never();

        let mut mock_read = MockRead::new();
        mock_read.expect_read_rows().times(1).return_once(|req, _| {
            assert_eq!(
                req.read_stream,
                "projects/test_project/locations/US/jobs/test_job/streams/_default"
            );
            Ok(ResponseStream::from(rx))
        });

        let job_service = create_job_service(mock_job);
        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));

        let mut q = create_test_complete_query(
            job_service,
            Some(create_test_job_ref()),
            vec![create_test_row("cached_rest_row")],
            Some("rest_page_token".to_string()),
        );
        q.read_client = Some(read_client);
        q.metadata.total_rows = Some(20_000);

        let mut iter = q.read();
        let row1 = iter.next().await.expect("row 1")?;
        assert_eq!(row1.get::<i64>("col")?, 10);
        let row2 = iter.next().await.expect("row 2")?;
        assert_eq!(row2.get::<i64>("col")?, 20);
        let row3 = iter.next().await.expect("row 3")?;
        assert_eq!(row3.get::<i64>("col")?, 30);
        assert!(iter.next().await.is_none(), "{iter:?}");

        Ok(())
    }

    #[cfg(google_cloud_unstable_bigquery_storage_read)]
    #[tokio::test]
    async fn test_row_iterator_empty_query_with_storage_read_does_not_open_stream() -> TestResult {
        use crate::query::tests::MockRead;

        for total_rows in [None, Some(0)] {
            let mut mock_job = MockJobService::new();
            mock_job.expect_get_query_results().never();

            let mut mock_read = MockRead::new();
            mock_read.expect_read_rows().never();
            mock_read.expect_create_read_session().never();

            let job_service = create_job_service(mock_job);
            let read_client = Arc::new(crate::client::Read::from_stub(mock_read));

            let mut q =
                create_test_complete_query(job_service, Some(create_test_job_ref()), vec![], None);
            q.read_client = Some(read_client);
            q.metadata.total_rows = total_rows;

            let mut iter = q.read();
            assert!(iter.next().await.is_none(), "{iter:?}");
        }

        Ok(())
    }

    #[cfg(google_cloud_unstable_bigquery_storage_read)]
    #[tokio::test]
    async fn test_row_iterator_storage_read_error_surfaces_without_fallback() -> TestResult {
        use crate::query::tests::MockRead;
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status};

        let mut mock_job = MockJobService::new();
        mock_job.expect_get_query_results().never();

        let mut mock_read = MockRead::new();
        mock_read.expect_read_rows().times(1).returning(|req, _| {
            assert_eq!(
                req.read_stream,
                "projects/test_project/locations/US/jobs/test_job/streams/_default"
            );
            let status = Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("storage read permission denied");
            Err(GaxError::service(status))
        });

        let job_service = create_job_service(mock_job);
        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));

        let mut q = create_test_complete_query(
            job_service,
            Some(create_test_job_ref()),
            vec![create_test_row("cached_rest_row")],
            Some("rest_page_token".to_string()),
        );
        q.read_client = Some(read_client);
        q.metadata.total_rows = Some(20_000);

        let mut iter = q.read();
        let err = iter
            .next()
            .await
            .expect("should return an item")
            .unwrap_err();
        assert!(matches!(err, RowError::Rpc { .. }), "{err:?}");
        assert!(
            err.to_string().contains("storage read permission denied"),
            "{err:?}"
        );
        assert!(iter.next().await.is_none(), "{iter:?}");

        Ok(())
    }

    #[cfg(google_cloud_unstable_bigquery_storage_read)]
    #[tokio::test]
    async fn test_row_iterator_storage_read_under_threshold_uses_rest() -> TestResult {
        use crate::query::tests::MockRead;

        let mut mock_job = MockJobService::new();
        mock_job
            .expect_get_query_results()
            .times(1)
            .returning(|req, _| {
                assert_eq!(req.page_token, "rest_token_1");
                let res = GetQueryResultsResponse::new()
                    .set_rows(vec![create_test_row("from_json_rest")])
                    .set_page_token("");
                Ok(Response::from(res))
            });

        let mut mock_read = MockRead::new();
        mock_read.expect_read_rows().never();

        let job_service = create_job_service(mock_job);
        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));

        let mut q = create_test_complete_query(
            job_service,
            Some(create_test_job_ref()),
            vec![create_test_row("cached_rest_row")],
            Some("rest_token_1".to_string()),
        );
        q.read_client = Some(read_client);
        q.metadata.total_rows = Some(5_000); // under 10,000 threshold

        let mut iter = q.read();
        let row1 = iter.next().await.expect("row 1")?;
        assert_eq!(row1.get::<String>("col")?, "cached_rest_row");
        let row2 = iter.next().await.expect("row 2")?;
        assert_eq!(row2.get::<String>("col")?, "from_json_rest");
        assert!(iter.next().await.is_none(), "{iter:?}");

        Ok(())
    }
}
