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
        // DDL/DML or Arrow queries may have no TableSchema in metadata initially.
        let schema = Arc::new(Schema::new(q.metadata.schema.unwrap_or_default()));
        #[cfg(any(
            google_cloud_unstable_bigquery_arrow,
            google_cloud_unstable_bigquery_storage_read
        ))]
        let (rows, arrow_reader) = match q.cached_data {
            CachedData::Rows(rows) => (rows, None),
            CachedData::Arrow {
                serialized_record_batch,
                serialized_schema,
            } => (
                VecDeque::new(),
                Some(ArrowReader::Response(ArrowResponseReader::new(
                    serialized_schema,
                    serialized_record_batch,
                ))),
            ),
        };
        #[cfg(not(any(
            google_cloud_unstable_bigquery_arrow,
            google_cloud_unstable_bigquery_storage_read
        )))]
        let rows = match q.cached_data {
            CachedData::Rows(rows) => rows,
            // TODO(#7035): remove once google_cloud_unstable_bigquery_arrow is stabilized
            CachedData::Arrow { .. } => {
                unimplemented!("Arrow IPC decoding is not yet implemented (#7035)")
            }
        };
        Self {
            job_service: q.job_service,
            job_ref: q.job_ref,
            schema,
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
                        Ok(batch) => {
                            if self.schema.len() == 0
                                && let Some(arrow_schema) = arrow_reader.schema()
                            {
                                self.schema = Arc::new(Schema::from_arrow_schema(&arrow_schema));
                            }
                            if let Some(batch) = batch {
                                self.current_batch = Some(Arc::new(batch));
                                self.row_index = 0;
                                continue;
                            }
                            self.arrow_reader = None;
                        }
                        Err(_err) if arrow_reader.can_fallback_to_rest() => {
                            // Fall back to JSON pagination if read stream opening failed
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

        let (fetched_rows, next_token) = self.fetch_page(token).await?;
        if fetched_rows.is_empty() && next_token.as_deref() == Some(token) {
            self.page_token = None;
        } else {
            self.page_token = next_token;
            self.rows.extend(fetched_rows);
        }
        Ok(())
    }

    // Fetches the next page of results and the next page token.
    async fn fetch_page(&self, token: &str) -> Result<(Vec<wkt::Struct>, Option<String>)> {
        let job_ref = self.job_ref.as_ref().expect(
            "only queries with a job reference should have page tokens and can fetch more pages",
        );

        let mut req = GetQueryResultsRequest::new()
            .set_project_id(job_ref.project_id.clone())
            .set_or_clear_max_results(self.page_size)
            .set_job_id(job_ref.job_id.clone())
            .set_page_token(token)
            .set_format_options(
                google_cloud_bigquery_v2::model::DataFormatOptions::new()
                    .set_use_int64_timestamp(true),
            );
        if let Some(location) = job_ref.location.clone() {
            req = req.set_location(location);
        }

        let res = self
            .job_service
            .get_query_results()
            .with_options(self.options.clone())
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
        bytes::Bytes::from_static(b"invalid arrow schema bytes"),
        bytes::Bytes::new();
        "malformed schema"
    )]
    #[test_case::test_case(
        create_test_arrow_schema_bytes(),
        bytes::Bytes::from_static(b"\xff\xff\xff\xff\x10\x00\x00\x00corrupted_batch_header");
        "malformed record batch"
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
}
