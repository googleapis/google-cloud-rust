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

#[cfg(all(test, feature = "run-integration-tests"))]
mod spanner {
    use google_cloud_spanner::client::Kind;
    use google_cloud_spanner::client::Spanner;
    use google_cloud_spanner::client::Statement;

    const PROJECT_ID: &str = "test-project";
    const INSTANCE_ID: &str = "test-instance";
    const DATABASE_ID: &str = "test-db";

    fn get_emulator_host() -> Option<String> {
        std::env::var("SPANNER_EMULATOR_HOST").ok()
    }

    // Waits for up to 10 seconds for the Spanner Emulator to be available.
    async fn wait_for_emulator(endpoint: &str) {
        let mut connected = false;
        for _ in 0..10 {
            if tokio::net::TcpStream::connect(endpoint).await.is_ok() {
                connected = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        if !connected {
            panic!("Failed to connect to emulator at {}", endpoint);
        }
    }

    // Provisions the Spanner Emulator with a test instance and database.
    // ALREADY_EXISTS errors that are returned when creating an instance or database are ignored.
    async fn provision_emulator(endpoint: &str) {
        // TODO(#4973): Re-write this to use the admin clients once those also support the Emulator.
        let rest_endpoint = endpoint.replace("9010", "9020");
        let rest_endpoint =
            if rest_endpoint.starts_with("http://") || rest_endpoint.starts_with("https://") {
                rest_endpoint
            } else {
                format!("http://{}", rest_endpoint)
            };
        let client = reqwest::Client::new();

        // Create a test instance and ignore any ALREADY_EXISTS errors.
        let instance_payload = serde_json::json!({
            "instanceId": INSTANCE_ID,
            "instance": {
                "config": "emulator-config",
                "displayName": "Test Instance",
                "nodeCount": 1
            }
        });
        let res = client
            .post(format!(
                "{}/v1/projects/{}/instances",
                rest_endpoint, PROJECT_ID
            ))
            .json(&instance_payload)
            .send()
            .await
            .expect("Failed to send create instance request");
        assert!(
            res.status().is_success() || res.status() == reqwest::StatusCode::CONFLICT,
            "Failed to create instance: {}",
            res.text().await.unwrap()
        );

        // Create a test database and ignore any ALREADY_EXISTS errors.
        let database_payload = serde_json::json!({
            "createStatement": format!("CREATE DATABASE `{}`", DATABASE_ID)
        });
        let res = client
            .post(format!(
                "{}/v1/projects/{}/instances/{}/databases",
                rest_endpoint, PROJECT_ID, INSTANCE_ID
            ))
            .json(&database_payload)
            .send()
            .await
            .expect("Failed to send create database request");
        assert!(
            res.status().is_success() || res.status() == reqwest::StatusCode::CONFLICT,
            "Failed to create database: {}",
            res.text().await.unwrap()
        );
    }

    /// Creates a database client for the test instance and database.
    /// Returns None if the SPANNER_EMULATOR_HOST environment variable is not set.
    /// This indicates that integration tests should be skipped.
    async fn create_database_client() -> Option<google_cloud_spanner::client::DatabaseClient> {
        let endpoint = match get_emulator_host() {
            Some(host) => host,
            None => {
                println!("Skipping emulator E2E test as SPANNER_EMULATOR_HOST is not set");
                return None;
            }
        };

        wait_for_emulator(&endpoint).await;
        provision_emulator(&endpoint).await;

        let client = Spanner::builder()
            .build()
            .await
            .expect("Failed to create Spanner client");

        let db_client = client
            .database_client(format!(
                "projects/{}/instances/{}/databases/{}",
                PROJECT_ID, INSTANCE_ID, DATABASE_ID
            ))
            .build()
            .await
            .expect("Failed to build database client");

        Some(db_client)
    }

    #[tokio::test]
    async fn test_simple_query() -> Result<(), Box<dyn std::error::Error>> {
        let db_client = match create_database_client().await {
            Some(client) => client,
            None => return Ok(()),
        };

        let rot = db_client.single_use().build();

        let sql = r#"
SELECT
  1 AS col_int64,
  CAST(1.0 AS FLOAT64) AS col_float64,
  CAST(1.0 AS FLOAT32) AS col_float32,
  TRUE AS col_bool,
  'One' AS col_string,
  CAST('One' AS BYTES) AS col_bytes,
  JSON '{"value": 1}' AS col_json,
  NUMERIC '1.0' AS col_numeric,
  CAST('2026-03-09' AS DATE) AS col_date,
  CAST('2026-03-09T16:20:00Z' AS TIMESTAMP) AS col_timestamp,
  [1] AS col_array_int64,
  [CAST(1.0 AS FLOAT64)] AS col_array_float64,
  [CAST(1.0 AS FLOAT32)] AS col_array_float32,
  [TRUE] AS col_array_bool,
  ['One'] AS col_array_string,
  [CAST('One' AS BYTES)] AS col_array_bytes,
  [JSON '{"value": 1}'] AS col_array_json,
  [NUMERIC '1.0'] AS col_array_numeric,
  [CAST('2026-03-09' AS DATE)] AS col_array_date,
  [CAST('2026-03-09T16:20:00Z' AS TIMESTAMP)] AS col_array_timestamp
UNION ALL
SELECT
  2 AS col_int64,
  CAST(2.0 AS FLOAT64) AS col_float64,
  CAST(2.0 AS FLOAT32) AS col_float32,
  FALSE AS col_bool,
  'Two' AS col_string,
  CAST('Two' AS BYTES) AS col_bytes,
  JSON '{"value": 2}' AS col_json,
  NUMERIC '2.0' AS col_numeric,
  CAST('2026-03-10' AS DATE) AS col_date,
  CAST('2026-03-10T16:20:00Z' AS TIMESTAMP) AS col_timestamp,
  [2, 3] AS col_array_int64,
  [CAST(2.0 AS FLOAT64), CAST(3.0 AS FLOAT64)] AS col_array_float64,
  [CAST(2.0 AS FLOAT32), CAST(3.0 AS FLOAT32)] AS col_array_float32,
  [FALSE, TRUE] AS col_array_bool,
  ['Two', 'Three'] AS col_array_string,
  [CAST('Two' AS BYTES), CAST('Three' AS BYTES)] AS col_array_bytes,
  [JSON '{"value": 2}', JSON '{"value": 3}'] AS col_array_json,
  [NUMERIC '2.0', NUMERIC '3.0'] AS col_array_numeric,
  [CAST('2026-03-10' AS DATE), CAST('2026-03-11' AS DATE)] AS col_array_date,
  [CAST('2026-03-10T16:20:00Z' AS TIMESTAMP), CAST('2026-03-11T16:20:00Z' AS TIMESTAMP)] AS col_array_timestamp
UNION ALL
SELECT
  CAST(NULL AS INT64) AS col_int64,
  CAST(NULL AS FLOAT64) AS col_float64,
  CAST(NULL AS FLOAT32) AS col_float32,
  CAST(NULL AS BOOL) AS col_bool,
  CAST(NULL AS STRING) AS col_string,
  CAST(NULL AS BYTES) AS col_bytes,
  CAST(NULL AS JSON) AS col_json,
  CAST(NULL AS NUMERIC) AS col_numeric,
  CAST(NULL AS DATE) AS col_date,
  CAST(NULL AS TIMESTAMP) AS col_timestamp,
  CAST(NULL AS ARRAY<INT64>) AS col_array_int64,
  CAST(NULL AS ARRAY<FLOAT64>) AS col_array_float64,
  CAST(NULL AS ARRAY<FLOAT32>) AS col_array_float32,
  CAST(NULL AS ARRAY<BOOL>) AS col_array_bool,
  CAST(NULL AS ARRAY<STRING>) AS col_array_string,
  CAST(NULL AS ARRAY<BYTES>) AS col_array_bytes,
  CAST(NULL AS ARRAY<JSON>) AS col_array_json,
  CAST(NULL AS ARRAY<NUMERIC>) AS col_array_numeric,
  CAST(NULL AS ARRAY<DATE>) AS col_array_date,
  CAST(NULL AS ARRAY<TIMESTAMP>) AS col_array_timestamp
ORDER BY col_int64
"#;

        let stmt = Statement::builder(sql).build();
        let mut rs = rot
            .execute_query(stmt)
            .await
            .expect("Failed to execute query");

        let mut rows = Vec::new();
        while let Some(row) = rs.next().await.transpose()? {
            rows.push(row);
        }

        let (row1, row2, row3) = match &rows[..] {
            [r1, r2, r3] => (r1, r2, r3),
            _ => panic!(
                "unexpected number of rows, got={}, want=3\n{rows:?}",
                rows.len()
            ),
        };

        // Spanner sorts NULLs first.
        verify_null_row(row1);
        verify_row_1(row2);
        verify_row_2(row3);

        Ok(())
    }

    #[tokio::test]
    async fn test_query_with_parameters() -> Result<(), Box<dyn std::error::Error>> {
        let db_client = match create_database_client().await {
            Some(client) => client,
            None => return Ok(()),
        };

        let rot = db_client.single_use().build();

        let sql = r#"
        WITH Data AS (
            SELECT 1 as id, 'Alice' as name 
            UNION ALL 
            SELECT 2 as id, 'Bob' as name
        ) 
        SELECT name FROM Data WHERE id = @id
        "#;

        let stmt = Statement::builder(sql).add_param("id", &2).build();
        let mut rs = rot
            .execute_query(stmt)
            .await
            .expect("Failed to execute query");

        let mut rows = Vec::new();
        while let Some(row) = rs.next().await.transpose()? {
            rows.push(row);
        }

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].raw_values()[0].as_string(), "Bob");

        Ok(())
    }

    fn verify_null_row(row: &google_cloud_spanner::client::Row) {
        let raw_values = row.raw_values();
        assert_eq!(raw_values.len(), 20, "Row should have exactly 20 columns");
        assert!(
            raw_values.iter().all(|v| v.kind() == Kind::Null),
            "Expected all columns to be NULL"
        );
    }

    fn verify_row_1(row: &google_cloud_spanner::client::Row) {
        let raw_values = row.raw_values();
        assert_eq!(raw_values.len(), 20, "Row should have exactly 20 columns");
        assert_eq!(raw_values[0].as_string(), "1"); // INT64 is encoded as string
        assert_eq!(raw_values[1].as_f64(), 1.0);
        assert_eq!(raw_values[2].as_f64(), 1.0); // FLOAT32 is encoded as f64
        assert!(raw_values[3].as_bool());
        assert_eq!(raw_values[4].as_string(), "One");
        assert_eq!(raw_values[5].as_string(), "T25l"); // Base64 'One'
        assert_eq!(raw_values[6].as_string(), "{\"value\":1}"); // JSON
        assert_eq!(raw_values[7].as_string(), "1"); // NUMERIC is encoded as string
        assert_eq!(raw_values[8].as_string(), "2026-03-09");
        assert_eq!(raw_values[9].as_string(), "2026-03-09T16:20:00Z");

        assert_eq!(raw_values[10].as_list().len(), 1);
        assert_eq!(raw_values[10].as_list().get(0).unwrap().as_string(), "1");
        assert_eq!(raw_values[11].as_list().len(), 1);
        assert_eq!(raw_values[11].as_list().get(0).unwrap().as_f64(), 1.0);
        assert_eq!(raw_values[12].as_list().len(), 1);
        assert_eq!(raw_values[12].as_list().get(0).unwrap().as_f64(), 1.0);
        assert_eq!(raw_values[13].as_list().len(), 1);
        assert!(raw_values[13].as_list().get(0).unwrap().as_bool());
        assert_eq!(raw_values[14].as_list().len(), 1);
        assert_eq!(raw_values[14].as_list().get(0).unwrap().as_string(), "One");
        assert_eq!(raw_values[15].as_list().len(), 1);
        assert_eq!(raw_values[15].as_list().get(0).unwrap().as_string(), "T25l");
        assert_eq!(raw_values[16].as_list().len(), 1);
        assert_eq!(
            raw_values[16].as_list().get(0).unwrap().as_string(),
            "{\"value\":1}"
        );
        assert_eq!(raw_values[17].as_list().len(), 1);
        assert_eq!(raw_values[17].as_list().get(0).unwrap().as_string(), "1");
        assert_eq!(raw_values[18].as_list().len(), 1);
        assert_eq!(
            raw_values[18].as_list().get(0).unwrap().as_string(),
            "2026-03-09"
        );
        assert_eq!(raw_values[19].as_list().len(), 1);
        assert_eq!(
            raw_values[19].as_list().get(0).unwrap().as_string(),
            "2026-03-09T16:20:00Z"
        );
    }

    fn verify_row_2(row: &google_cloud_spanner::client::Row) {
        let raw_values = row.raw_values();
        assert_eq!(raw_values.len(), 20, "Row should have exactly 20 columns");
        assert_eq!(raw_values[0].as_string(), "2");
        assert_eq!(raw_values[1].as_f64(), 2.0);
        assert_eq!(raw_values[2].as_f64(), 2.0);
        assert!(!raw_values[3].as_bool());
        assert_eq!(raw_values[4].as_string(), "Two");
        assert_eq!(raw_values[5].as_string(), "VHdv"); // Base64 'Two'
        assert_eq!(raw_values[6].as_string(), "{\"value\":2}");
        assert_eq!(raw_values[7].as_string(), "2");
        assert_eq!(raw_values[8].as_string(), "2026-03-10");
        assert_eq!(raw_values[9].as_string(), "2026-03-10T16:20:00Z");

        assert_eq!(raw_values[10].as_list().len(), 2);
        assert_eq!(raw_values[10].as_list().get(0).unwrap().as_string(), "2");
        assert_eq!(raw_values[10].as_list().get(1).unwrap().as_string(), "3");
        assert_eq!(raw_values[11].as_list().len(), 2);
        assert_eq!(raw_values[11].as_list().get(0).unwrap().as_f64(), 2.0);
        assert_eq!(raw_values[11].as_list().get(1).unwrap().as_f64(), 3.0);
        assert_eq!(raw_values[12].as_list().len(), 2);
        assert_eq!(raw_values[12].as_list().get(0).unwrap().as_f64(), 2.0);
        assert_eq!(raw_values[12].as_list().get(1).unwrap().as_f64(), 3.0);
        assert_eq!(raw_values[13].as_list().len(), 2);
        assert!(!raw_values[13].as_list().get(0).unwrap().as_bool());
        assert!(raw_values[13].as_list().get(1).unwrap().as_bool());
        assert_eq!(raw_values[14].as_list().len(), 2);
        assert_eq!(raw_values[14].as_list().get(0).unwrap().as_string(), "Two");
        assert_eq!(
            raw_values[14].as_list().get(1).unwrap().as_string(),
            "Three"
        );
        assert_eq!(raw_values[15].as_list().len(), 2);
        assert_eq!(raw_values[15].as_list().get(0).unwrap().as_string(), "VHdv");
        assert_eq!(
            raw_values[15].as_list().get(1).unwrap().as_string(),
            "VGhyZWU="
        );
        assert_eq!(raw_values[16].as_list().len(), 2);
        assert_eq!(
            raw_values[16].as_list().get(0).unwrap().as_string(),
            "{\"value\":2}"
        );
        assert_eq!(
            raw_values[16].as_list().get(1).unwrap().as_string(),
            "{\"value\":3}"
        );
        assert_eq!(raw_values[17].as_list().len(), 2);
        assert_eq!(raw_values[17].as_list().get(0).unwrap().as_string(), "2");
        assert_eq!(raw_values[17].as_list().get(1).unwrap().as_string(), "3");
        assert_eq!(raw_values[18].as_list().len(), 2);
        assert_eq!(
            raw_values[18].as_list().get(0).unwrap().as_string(),
            "2026-03-10"
        );
        assert_eq!(
            raw_values[18].as_list().get(1).unwrap().as_string(),
            "2026-03-11"
        );
        assert_eq!(raw_values[19].as_list().len(), 2);
        assert_eq!(
            raw_values[19].as_list().get(0).unwrap().as_string(),
            "2026-03-10T16:20:00Z"
        );
        assert_eq!(
            raw_values[19].as_list().get(1).unwrap().as_string(),
            "2026-03-11T16:20:00Z"
        );
    }
}
