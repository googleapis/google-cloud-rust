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

// [START bigquery_query_params_positional_types]
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::model::{QueryParameter, QueryParameterType, QueryParameterValue};

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    let mut rows = client
        .query("SELECT ? AS str_col, ? AS int_col, ? AS float_col;")
        .with_project_id(project_id)
        .set_parameter_mode("POSITIONAL")
        .set_query_parameters([
            QueryParameter::new()
                .set_parameter_type(QueryParameterType::new().set_type("STRING"))
                .set_parameter_value(QueryParameterValue::new().set_value("example string")),
            QueryParameter::new()
                .set_parameter_type(QueryParameterType::new().set_type("INT64"))
                .set_parameter_value(QueryParameterValue::new().set_value("42")),
            QueryParameter::new()
                .set_parameter_type(QueryParameterType::new().set_type("FLOAT64"))
                .set_parameter_value(QueryParameterValue::new().set_value("3.14159")),
        ])
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?
        .read();

    while let Some(row) = rows.next().await.transpose()? {
        let str_col: String = row.get("str_col");
        let int_col: i64 = row.get("int_col");
        let float_col: f64 = row.get("float_col");
        println!("String: {str_col}, Int: {int_col}, Float: {float_col}");
    }
    Ok(())
}
// [END bigquery_query_params_positional_types]
