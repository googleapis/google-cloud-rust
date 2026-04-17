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

use google_cloud_spanner::client::{KeySet, Mutation, Spanner};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner as MockSpannerTrait;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;

const PROJECT_ID: &str = "test-project";
const INSTANCE_ID: &str = "test-instance";

pub fn get_emulator_host() -> Option<String> {
    std::env::var("SPANNER_EMULATOR_HOST").ok()
}

// Waits for up to 10 seconds for the Spanner Emulator to be available.
pub async fn wait_for_emulator(endpoint: &str) {
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

static PROVISION_EMULATOR: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static DATABASE_ID: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();

pub async fn get_database_id() -> &'static str {
    DATABASE_ID
        .get_or_init(|| async {
            std::env::var("SPANNER_EMULATOR_TEST_DB")
                .unwrap_or_else(|_| format!("db-{}", LowercaseAlphanumeric.random_string(20)))
        })
        .await
}

// Provisions the Spanner Emulator with a test instance and database.
// ALREADY_EXISTS errors that are returned when creating an instance or database are ignored.
pub async fn provision_emulator(endpoint: &str) {
    PROVISION_EMULATOR
        .get_or_init(|| async {
            do_provision_emulator(endpoint).await;
        })
        .await;
}

pub fn get_emulator_rest_endpoint(grpc_endpoint: &str) -> String {
    let rest_endpoint = std::env::var("SPANNER_EMULATOR_REST_HOST")
        .unwrap_or_else(|_| grpc_endpoint.replace("9010", "9020"));
    if rest_endpoint.starts_with("http://") || rest_endpoint.starts_with("https://") {
        rest_endpoint
    } else {
        format!("http://{}", rest_endpoint)
    }
}

async fn do_provision_emulator(endpoint: &str) {
    // TODO(#4973): Re-write this to use the admin clients once those also support the Emulator.
    let rest_endpoint = get_emulator_rest_endpoint(endpoint);
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
    let res: reqwest::Response = client
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
        "createStatement": format!("CREATE DATABASE `{}`", get_database_id().await),
        "extraStatements": [
            "CREATE TABLE AllTypes ( \
                Id STRING(MAX) NOT NULL, \
                ColBool BOOL, \
                ColInt64 INT64, \
                ColFloat32 FLOAT32, \
                ColFloat64 FLOAT64, \
                ColNumeric NUMERIC, \
                ColString STRING(MAX), \
                ColBytes BYTES(MAX), \
                ColDate DATE, \
                ColTimestamp TIMESTAMP, \
                ColJson JSON, \
                ColArrayBool ARRAY<BOOL>, \
                ColArrayInt64 ARRAY<INT64>, \
                ColArrayFloat32 ARRAY<FLOAT32>, \
                ColArrayFloat64 ARRAY<FLOAT64>, \
                ColArrayNumeric ARRAY<NUMERIC>, \
                ColArrayString ARRAY<STRING(MAX)>, \
                ColArrayBytes ARRAY<BYTES(MAX)>, \
                ColArrayDate ARRAY<DATE>, \
                ColArrayTimestamp ARRAY<TIMESTAMP>, \
                ColArrayJson ARRAY<JSON> \
             ) PRIMARY KEY (Id)",
            "CREATE INDEX Idx_AllTypes_ColString ON AllTypes (ColString)"
        ]
    });
    let res: reqwest::Response = client
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

    let spanner_client = Spanner::builder()
        .build()
        .await
        .expect("Failed to create Spanner client in provision_emulator");
    let db_client = spanner_client
        .database_client(format!(
            "projects/{}/instances/{}/databases/{}",
            PROJECT_ID,
            INSTANCE_ID,
            get_database_id().await
        ))
        .build()
        .await
        .expect("Failed to build database client in provision_emulator");

    let write_tx = db_client.write_only_transaction().build();
    let mutation = Mutation::delete("AllTypes", KeySet::all());
    write_tx
        .write_at_least_once(vec![mutation])
        .await
        .expect("Failed to delete all data from AllTypes");
}

/// Creates a database client for the test instance and database.
/// Returns None if the SPANNER_EMULATOR_HOST environment variable is not set.
/// This indicates that integration tests should be skipped.
pub async fn create_database_client() -> Option<google_cloud_spanner::client::DatabaseClient> {
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
            PROJECT_ID,
            INSTANCE_ID,
            get_database_id().await
        ))
        .build()
        .await
        .expect("Failed to build database client");

    Some(db_client)
}

/// Updates the database DDL by executing the given statement on the Spanner Emulator.
///
/// This method uses the emulator's REST API directly. It includes a retry loop to handle
/// transient "Schema change operation rejected" errors that can occur in the emulator
/// if multiple schema changes are executed in parallel, or if schema changes are executed
/// in parallel with read/write transactions.
pub async fn update_database_ddl(statement: String) -> anyhow::Result<()> {
    let emulator_host = get_emulator_host().expect("SPANNER_EMULATOR_HOST must be set");
    let rest_endpoint = get_emulator_rest_endpoint(&emulator_host);
    let db_path = format!(
        "projects/{}/instances/{}/databases/{}",
        PROJECT_ID,
        INSTANCE_ID,
        get_database_id().await
    );
    let url = format!("{}/v1/{}/ddl", rest_endpoint, db_path);
    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "statements": [statement]
    });

    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 25;

    loop {
        attempts += 1;
        let res = client.patch(&url).json(&payload).send().await?;

        let status = res.status();
        let text = res.text().await?;

        if status.is_success() {
            return Ok(());
        }

        // Check if the error is the specific one we want to retry.
        // Code 9 is FailedPrecondition.
        if text.contains("\"code\":9") && text.contains("Schema change operation rejected") {
            if attempts >= MAX_ATTEMPTS {
                anyhow::bail!(
                    "Failed to update DDL after {} attempts. Last error: {}",
                    attempts,
                    text
                );
            }
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        anyhow::bail!("Failed to update DDL: status={}, body={}", status, text);
    }
}

/// A guard that aborts the server task when dropped.
pub struct ServerGuard(JoinHandle<()>);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Helper to start a mock server and return a drop guard.
pub async fn start_guarded_server<T>(
    address: &str,
    service: T,
) -> anyhow::Result<(String, ServerGuard)>
where
    T: MockSpannerTrait + Send + 'static,
{
    let (uri, handle) = spanner_grpc_mock::start(address, service).await?;
    Ok((uri, ServerGuard(handle)))
}
