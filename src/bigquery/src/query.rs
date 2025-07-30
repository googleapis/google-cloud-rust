// Copyright 2025 Google LLC
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
use crate::iterator::RowIterator;
use crate::schema::Schema;
use bigquery_v2::client::JobService;
use bigquery_v2::model::{self, DataFormatOptions};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Query {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) project_id: String,
    pub(crate) job_id: String,
    pub(crate) location: String,
    pub(crate) query_id: Option<String>,
    pub(crate) page_token: String,
    pub(crate) schema: Option<Schema>,
    pub(crate) total_rows: u64,
    pub(crate) completed: bool,
    pub(crate) cached_rows: VecDeque<wkt::Struct>,
    pub(crate) num_dml_affected_rows: i64,
}

/// Represents errors that can occur when running queries.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum QueryError {
    /// Only complete Query Jobs can be read.
    #[error("Query is not complete: Only complete Query Jobs can be read.")]
    NotComplete,
}

impl Query {
    pub(crate) fn new(job_service: Arc<JobService>, res: model::QueryResponse) -> Self {
        let mut query = Self {
            job_service,
            project_id: String::default(),
            job_id: String::default(),
            location: String::default(),
            page_token: String::default(),
            query_id: None,
            schema: None,
            total_rows: 0,
            completed: false,
            cached_rows: VecDeque::new(),
            num_dml_affected_rows: 0,
        };
        query.consume_query_response(from_get_query_response(res.clone()));
        if !res.query_id.is_empty() {
            query.query_id = Some(res.query_id);
        }
        query
    }

    pub fn job_reference(&self) -> Option<model::JobReference> {
        if self.job_id.is_empty() {
            return None;
        }
        Some(
            model::JobReference::new()
                .set_project_id(self.project_id.clone())
                .set_job_id(self.job_id.clone())
                .set_location(self.location.clone()),
        )
    }

    pub fn query_id(&self) -> Option<&str> {
        self.query_id.as_deref()
    }

    pub fn schema(&self) -> Option<model::TableSchema> {
        self.schema.clone().map(|s| s.schema)
    }

    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub fn completed(&self) -> bool {
        self.completed
    }

    pub fn num_dml_affected_rows(&self) -> i64 {
        self.num_dml_affected_rows
    }

    pub async fn wait(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }
        let sleep = async |d| tokio::time::sleep(d).await;
        loop {
            let complete = self.poll_job().await?;
            if complete {
                return Ok(());
            }
            // TODO: exponential backoff
            sleep(Duration::from_millis(300)).await;
        }
    }

    pub async fn read(self) -> Result<RowIterator> {
        if !self.completed {
            return Err(crate::Error::ser(QueryError::NotComplete));
        }
        RowIterator::try_new(Arc::new(self)).await
    }

    async fn poll_job(&mut self) -> Result<bool> {
        let max_results: u32 = 0;
        let res = self
            .job_service
            .get_query_results()
            .set_project_id(&self.project_id)
            .set_job_id(&self.job_id)
            .set_location(&self.location)
            .set_format_options(DataFormatOptions::new().set_use_int64_timestamp(true))
            .set_max_results(max_results)
            .send()
            .await?;
        self.consume_query_response(res);
        Ok(self.completed)
    }

    pub(crate) fn consume_query_response(&mut self, res: model::GetQueryResultsResponse) {
        if let Some(job_ref) = res.job_reference {
            self.job_id = job_ref.job_id;
            if let Some(location) = job_ref.location {
                self.location = location;
            }
        }
        if let Some(job_complete) = res.job_complete {
            self.completed = job_complete;
        }
        if let Some(schema) = res.schema {
            self.schema = Some(Schema::new(schema));
        }

        if let Some(total_rows) = res.total_rows {
            self.total_rows = total_rows;
        }
        if let Some(num_dml_affected_rows) = res.num_dml_affected_rows {
            self.num_dml_affected_rows = num_dml_affected_rows;
        }
        self.page_token = res.page_token;
        // rows are only present if query is complete as has schema
        self.cached_rows = res.rows.into_iter().map(|r| r).collect();
    }
}

fn from_get_query_response(res: model::QueryResponse) -> model::GetQueryResultsResponse {
    model::GetQueryResultsResponse::new()
        .set_page_token(res.page_token)
        .set_rows(res.rows)
        .set_errors(res.errors)
        .set_or_clear_schema(res.schema)
        .set_or_clear_cache_hit(res.cache_hit)
        .set_or_clear_job_reference(res.job_reference)
        .set_or_clear_total_rows(res.total_rows)
        .set_or_clear_total_bytes_processed(res.total_bytes_processed)
        .set_or_clear_job_complete(res.job_complete)
        .set_or_clear_num_dml_affected_rows(res.num_dml_affected_rows)
}
