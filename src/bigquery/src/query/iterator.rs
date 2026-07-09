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

use crate::error::RowError;
use crate::query::{CompleteQuery, Row, Schema};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::JobReference;
use std::collections::VecDeque;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, RowError>;
/// An iterator over rows returned by a query.
#[derive(Debug)]
pub struct RowIterator {
    job_service: Arc<JobService>,
    job_ref: Option<JobReference>,
    schema: Arc<Schema>,
    page_token: Option<String>,
    rows: VecDeque<wkt::Struct>,
    is_done: bool,
}

impl RowIterator {
    pub(crate) fn new(q: CompleteQuery) -> Self {
        let rows = q.cached_rows;
        let is_done = rows.is_empty() && q.page_token.is_none();

        Self {
            job_service: q.job_service,
            job_ref: q.job_ref,
            schema: q.schema,
            page_token: q.page_token,
            rows,
            is_done,
        }
    }

    /// Fetches the next row from the result set.
    ///
    /// Returns `None` when all rows have been retrieved.
    pub async fn next(&mut self) -> Option<Result<Row>> {
        if let Some(raw_row) = self.rows.pop_front() {
            let row = Row::try_new(raw_row, &self.schema);
            return Some(row);
        }

        if self.is_done {
            return None;
        }

        if let Some(token) = self.page_token.take() {
            match self.fetch_page(&token).await {
                Ok((fetched_rows, next_token)) => {
                    self.page_token = next_token;
                    self.rows.extend(fetched_rows);
                    if let Some(raw_row) = self.rows.pop_front() {
                        return Some(Row::try_new(raw_row, &self.schema));
                    }
                }
                Err(e) => {
                    self.page_token = Some(token);
                    return Some(Err(e));
                }
            }
        }

        self.is_done = true;
        None
    }

    // Fetches the next page of results and the next page token.
    async fn fetch_page(&self, _token: &str) -> Result<(Vec<wkt::Struct>, Option<String>)> {
        // TODO(#5592): implement page fetching with jobs.getQueryResults
        unimplemented!("pagination support not yet implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::tests::{MockJobService, create_job_service};
    use google_cloud_bigquery_v2::model::{
        JobReference, QueryResponse, TableFieldSchema, TableSchema,
    };
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
        CompleteQuery::from_query_response(job_service, job_ref, res)
    }

    #[tokio::test]
    async fn test_row_iterator_empty_no_token() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let q = create_test_complete_query(job_service, Some(create_test_job_ref()), vec![], None);
        let mut iter = q.read();
        assert!(iter.next().await.is_none(), "{iter:?}");
        assert!(iter.is_done, "{iter:?}");
        Ok(())
    }

    #[tokio::test]
    async fn test_row_iterator_cached_rows_only() -> TestResult {
        let job_service = create_job_service(MockJobService::new());
        let rows = vec![create_test_row("first"), create_test_row("second")];
        let q = create_test_complete_query(job_service, Some(create_test_job_ref()), rows, None);
        let mut iter = q.read();

        let row1 = iter.next().await.expect("should have row 1")?;
        assert_eq!(row1.get::<String, _>("col"), "first");

        let row2 = iter.next().await.expect("should have row 2")?;
        assert_eq!(row2.get::<String, _>("col"), "second");

        assert!(iter.next().await.is_none(), "{iter:?}");
        assert!(iter.is_done, "{iter:?}");
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
}
