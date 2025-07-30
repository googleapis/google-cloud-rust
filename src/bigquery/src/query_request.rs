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

use bigquery_v2::model;

pub trait IntoPostQueryRequest {
    fn into_post_query_request(self) -> bigquery_v2::model::PostQueryRequest;
}

impl IntoPostQueryRequest for bigquery_v2::model::QueryRequest {
    fn into_post_query_request(self) -> bigquery_v2::model::PostQueryRequest {
        bigquery_v2::model::PostQueryRequest::new().set_query_request(self)
    }
}

impl IntoPostQueryRequest for String {
    fn into_post_query_request(self) -> bigquery_v2::model::PostQueryRequest {
        query_request_from_sql(&self).into_post_query_request()
    }
}

pub fn query_request_from_sql<T>(sql: T) -> bigquery_v2::model::QueryRequest
where
    T: Into<String>,
{
    bigquery_v2::model::QueryRequest::new()
        .set_query(sql.into())
        .set_use_legacy_sql(false)
        .set_format_options(model::DataFormatOptions::new().set_use_int64_timestamp(true))
}

pub fn post_query_request_from_sql<T>(project_id: T, sql: T) -> bigquery_v2::model::PostQueryRequest
where
    T: Into<String>,
{
    bigquery_v2::model::PostQueryRequest::new()
        .set_project_id(project_id)
        .set_query_request(query_request_from_sql(sql.into()))
}
