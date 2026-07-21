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

// [START bigquery_query_params_structs]
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::model::{
    QueryParameter, QueryParameterStructType, QueryParameterType, QueryParameterValue,
};

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    let mut rows = client
        .query(
            "SELECT name, number \
        FROM `bigquery-public-data.usa_names.usa_1910_2013` \
        WHERE state = @record.state AND gender = @record.gender \
        LIMIT 10",
        )
        .with_project_id(project_id)
        .set_parameter_mode("NAMED")
        .set_query_parameters([QueryParameter::new()
            .set_name("record")
            .set_parameter_type(
                QueryParameterType::new()
                    .set_type("STRUCT")
                    .set_struct_types([
                        QueryParameterStructType::new()
                            .set_name("state")
                            .set_type(QueryParameterType::new().set_type("STRING")),
                        QueryParameterStructType::new()
                            .set_name("gender")
                            .set_type(QueryParameterType::new().set_type("STRING")),
                    ]),
            )
            .set_parameter_value(QueryParameterValue::new().set_struct_values([
                ("state", QueryParameterValue::new().set_value("TX")),
                ("gender", QueryParameterValue::new().set_value("F")),
            ]))])
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?
        .read();

    while let Some(row) = rows.next().await.transpose()? {
        let name: String = row.get("name");
        let number: i64 = row.get("number");
        println!("Name: {name}, Number: {number}");
    }
    Ok(())
}
// [END bigquery_query_params_structs]
