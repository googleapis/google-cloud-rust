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

// [START bigquery_query_params_arrays]
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::model::{QueryParameter, QueryParameterType, QueryParameterValue};

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    let param = QueryParameter::new()
        .set_name("states")
        .set_parameter_type(
            QueryParameterType::new()
                .set_type("ARRAY")
                .set_array_type(QueryParameterType::new().set_type("STRING")),
        )
        .set_parameter_value(QueryParameterValue::new().set_array_values(vec![
            QueryParameterValue::new().set_value("TX"),
            QueryParameterValue::new().set_value("CA"),
        ]));

    let mut rows = client
        .query(
            "SELECT \
        name FROM `bigquery-public-data.usa_names.usa_1910_2013` \
        WHERE state IN UNNEST(@states) \
        LIMIT 100",
        )
        .with_project_id(project_id)
        .set_parameter_mode("NAMED")
        .set_query_parameters(vec![param])
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?
        .read();

    while let Some(row) = rows.next().await.transpose()? {
        let name: String = row.get("name");
        println!("Name: {name}");
    }
    Ok(())
}
// [END bigquery_query_params_arrays]
