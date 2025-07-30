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

use bigquery_v2::client::JobService;
use bigquery_v2::model::DataFormatOptions;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::Result;
use crate::query::Query;
use crate::row::Row;
use crate::schema::Schema;

pub struct RowIterator {
    job_service: Arc<JobService>,
    project_id: String,
    job_id: String,
    location: String,
    page_token: String,
    schema: Option<Schema>,
    total_rows: u64,
    rows: std::collections::VecDeque<Row>,
}

/// Represents errors that can occur when reading query results.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum IteratorError {
    /// Only complete Query Jobs can be read.
    #[error("Only complete Query Jobs with schema can be read.")]
    MissingSchema,
}

impl RowIterator {
    pub(crate) async fn try_new(query: Arc<Query>) -> Result<Self> {
        if query.schema.is_none() {
            return Err(crate::Error::ser(IteratorError::MissingSchema));
        }
        let schema = query.schema.clone().unwrap();
        Ok(Self {
            job_service: query.job_service.clone(),
            project_id: query.project_id.clone(),
            job_id: query.job_id.clone(),
            location: query.location.clone(),
            page_token: query.page_token.clone(),
            schema: Some(schema.clone()),
            rows: query
                .cached_rows
                .clone()
                .into_iter()
                .map(|row| Row::try_new(row, schema.clone()))
                .collect::<Result<VecDeque<_>>>()?,
            total_rows: query.total_rows,
        })
    }

    pub async fn next(&mut self) -> Option<Result<Row>> {
        if let Some(row) = self.rows.pop_front() {
            return Some(Ok(row));
        }
        if self.page_token.is_empty() {
            return None;
        }

        match self.fetch_next_page().await {
            Ok(_) => self.rows.pop_front().map(Ok),
            Err(e) => Some(Err(e)),
        }
    }

    async fn fetch_next_page(&mut self) -> Result<()> {
        let res = self
            .job_service
            .get_query_results()
            .set_project_id(&self.project_id)
            .set_job_id(&self.job_id)
            .set_location(&self.location)
            .set_format_options(DataFormatOptions::new().set_use_int64_timestamp(true))
            .set_page_token(self.page_token.clone())
            .send()
            .await?;

        if let Some(schema) = res.schema.clone() {
            self.schema = Some(Schema::new(schema));
        }

        if let Some(total_rows) = res.total_rows {
            self.total_rows = total_rows;
        }

        self.page_token = res.page_token;
        if let Some(schema) = self.schema.clone() {
            for row in res.rows {
                self.rows.push_back(Row::try_new(row, schema.clone())?);
            }
        }
        Ok(())
    }
}
