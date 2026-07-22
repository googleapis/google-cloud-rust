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

// [START bigquery_query_params_named_types]
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::model::{QueryParameter, QueryParameterType, QueryParameterValue};

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    let mut rows = client
        .query(
            "SELECT \
        @date_param AS date_col, \
        @numeric_param AS num_col, \
        @bool_param AS bool_col;",
        )
        .with_project_id(project_id)
        .set_parameter_mode("NAMED")
        .set_query_parameters([
            QueryParameter::new()
                .set_name("date_param")
                .set_parameter_type(QueryParameterType::new().set_type("DATE"))
                .set_parameter_value(QueryParameterValue::new().set_value("2026-01-15")),
            QueryParameter::new()
                .set_name("numeric_param")
                .set_parameter_type(QueryParameterType::new().set_type("NUMERIC"))
                .set_parameter_value(QueryParameterValue::new().set_value("12345.67")),
            QueryParameter::new()
                .set_name("bool_param")
                .set_parameter_type(QueryParameterType::new().set_type("BOOL"))
                .set_parameter_value(QueryParameterValue::new().set_value("true")),
        ])
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?
        .read();

    while let Some(row) = rows.next().await.transpose()? {
        let date_col: String = row.get("date_col");
        let num_col: String = row.get("num_col");
        let bool_col: bool = row.get("bool_col");
        println!("Date: {date_col}, Numeric: {num_col}, Bool: {bool_col}");
    }
    Ok(())
}
// [END bigquery_query_params_named_types]
