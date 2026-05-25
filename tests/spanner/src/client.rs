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

use anyhow::Result;
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_lro::Poller;
use google_cloud_spanner::client::{DatabaseClient, KeySet, Mutation, Spanner};
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use google_cloud_wkt::Timestamp;
use reqwest::{Client, Response, StatusCode};
use spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner as MockSpannerTrait;
use std::env::var;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{info, warn};

const EMULATOR_PROJECT_ID: &str = "test-project";
const EMULATOR_INSTANCE_ID: &str = "test-instance";

const EXTRA_STATEMENTS: [&str; 2] = [
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
    "CREATE INDEX Idx_AllTypes_ColString ON AllTypes (ColString)",
];

const PG_EXTRA_STATEMENTS: [&str; 1] = ["CREATE TABLE AllTypes ( \
        Id VARCHAR NOT NULL, \
        ColBool BOOLEAN, \
        ColInt64 BIGINT, \
        ColFloat32 REAL, \
        ColFloat64 DOUBLE PRECISION, \
        ColNumeric NUMERIC, \
        ColString VARCHAR, \
        ColBytes BYTEA, \
        ColDate DATE, \
        ColTimestamp TIMESTAMPTZ, \
        ColJson JSONB, \
        ColUuid UUID, \
        ColArrayBool BOOLEAN[], \
        ColArrayInt64 BIGINT[], \
        ColArrayFloat32 REAL[], \
        ColArrayFloat64 DOUBLE PRECISION[], \
        ColArrayNumeric NUMERIC[], \
        ColArrayString VARCHAR[], \
        ColArrayBytes BYTEA[], \
        ColArrayDate DATE[], \
        ColArrayTimestamp TIMESTAMPTZ[], \
        ColArrayJson JSONB[], \
        ColArrayUuid UUID[], \
        PRIMARY KEY (Id) \
     )"];

pub fn get_emulator_host() -> Option<String> {
    var("SPANNER_EMULATOR_HOST").ok()
}

pub fn get_real_spanner_config() -> Option<(String, String)> {
    let project = var("GOOGLE_CLOUD_PROJECT").ok()?;
    let instance = var("GOOGLE_CLOUD_RUST_SPANNER_TEST_INSTANCE").ok()?;
    Some((project, instance))
}

// Waits for up to 10 seconds for the Spanner Emulator to be available.
pub async fn wait_for_emulator(endpoint: &str) {
    let mut connected = false;
    for _ in 0..10 {
        if TcpStream::connect(endpoint).await.is_ok() {
            connected = true;
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    if !connected {
        panic!("Failed to connect to emulator at {}", endpoint);
    }
}

static PROVISION_EMULATOR: OnceCell<()> = OnceCell::const_new();
static PROVISION_REAL_SPANNER: OnceCell<()> = OnceCell::const_new();
static DATABASE_ID: OnceCell<String> = OnceCell::const_new();

pub async fn get_database_id() -> &'static str {
    DATABASE_ID
        .get_or_init(|| async {
            if let Ok(fixed_db) = var("GOOGLE_CLOUD_RUST_SPANNER_TEST_DATABASE") {
                return fixed_db;
            }
            let prefix = var("GOOGLE_CLOUD_RUST_SPANNER_TEST_DATABASE_PREFIX")
                .unwrap_or_else(|_| "testdb".to_string());
            format!("{}-{}", prefix, LowercaseAlphanumeric.random_string(20))
        })
        .await
}

static PG_DATABASE_ID: OnceCell<String> = OnceCell::const_new();
static PROVISION_EMULATOR_PG: OnceCell<()> = OnceCell::const_new();
static PROVISION_REAL_SPANNER_PG: OnceCell<()> = OnceCell::const_new();

pub async fn get_pg_database_id() -> &'static str {
    PG_DATABASE_ID
        .get_or_init(|| async {
            if let Ok(fixed_db) = var("GOOGLE_CLOUD_RUST_SPANNER_TEST_PG_DATABASE") {
                return fixed_db;
            }
            let prefix = var("GOOGLE_CLOUD_RUST_SPANNER_TEST_DATABASE_PREFIX")
                .unwrap_or_else(|_| "testdb".to_string());
            format!("{}-pg-{}", prefix, LowercaseAlphanumeric.random_string(17))
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

pub async fn provision_real_spanner(project: &str, instance: &str) {
    PROVISION_REAL_SPANNER
        .get_or_init(|| async {
            do_provision_real_spanner(project, instance).await;
        })
        .await;
}

pub fn get_emulator_rest_endpoint(grpc_endpoint: &str) -> String {
    let rest_endpoint =
        var("SPANNER_EMULATOR_REST_HOST").unwrap_or_else(|_| grpc_endpoint.replace("9010", "9020"));
    if rest_endpoint.starts_with("http://") || rest_endpoint.starts_with("https://") {
        rest_endpoint
    } else {
        format!("http://{}", rest_endpoint)
    }
}

async fn ensure_emulator_instance_created(client: &Client, rest_endpoint: &str) {
    let instance_payload = serde_json::json!({
        "instanceId": EMULATOR_INSTANCE_ID,
        "instance": {
            "config": "emulator-config",
            "displayName": "Test Instance",
            "nodeCount": 1
        }
    });
    let res: Response = client
        .post(format!(
            "{}/v1/projects/{}/instances",
            rest_endpoint, EMULATOR_PROJECT_ID
        ))
        .json(&instance_payload)
        .send()
        .await
        .expect("Failed to send create instance request");
    assert!(
        res.status().is_success() || res.status() == StatusCode::CONFLICT,
        "Failed to create instance: {}",
        res.text().await.expect("Failed to extract response text")
    );
}

async fn ensure_emulator_database_created(
    client: &Client,
    rest_endpoint: &str,
    payload: &serde_json::Value,
    dialect_name: &str,
) {
    let res = client
        .post(format!(
            "{}/v1/projects/{}/instances/{}/databases",
            rest_endpoint, EMULATOR_PROJECT_ID, EMULATOR_INSTANCE_ID
        ))
        .json(payload)
        .send()
        .await
        .expect("Failed to send create database request");
    assert!(
        res.status().is_success() || res.status() == StatusCode::CONFLICT,
        "Failed to create {} database: {}",
        dialect_name,
        res.text().await.expect("Failed to extract response text")
    );
}

async fn delete_all_data_from_all_types(database_id: &str) {
    let spanner_client = Spanner::builder()
        .build()
        .await
        .expect("Failed to create Spanner client for data cleanup");
    let db_client = spanner_client
        .database_client(format!(
            "projects/{}/instances/{}/databases/{}",
            EMULATOR_PROJECT_ID, EMULATOR_INSTANCE_ID, database_id
        ))
        .build()
        .await
        .expect("Failed to build database client for data cleanup");

    let write_tx = db_client.write_only_transaction().build();
    let mutation = Mutation::delete("AllTypes", KeySet::all());
    write_tx
        .write_at_least_once(vec![mutation])
        .await
        .expect("Failed to delete all data from AllTypes");
}

async fn do_provision_emulator(endpoint: &str) {
    // TODO(#4973): Re-write this to use the admin clients once those also support the Emulator.
    let rest_endpoint = get_emulator_rest_endpoint(endpoint);
    let client = Client::new();

    // Ensure the test instance is created
    ensure_emulator_instance_created(&client, &rest_endpoint).await;

    // Create a test database and ignore any ALREADY_EXISTS errors.
    let database_payload = serde_json::json!({
        "createStatement": format!("CREATE DATABASE `{}`", get_database_id().await),
        "extraStatements": EXTRA_STATEMENTS,
    });
    ensure_emulator_database_created(&client, &rest_endpoint, &database_payload, "GoogleSQL").await;

    // Clean up any leftover data from previous runs
    delete_all_data_from_all_types(get_database_id().await).await;
}

async fn cleanup_stale_databases(
    client: &DatabaseAdmin,
    project: &str,
    instance: &str,
) -> Result<()> {
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(24 * 60 * 60);
    let stale_deadline = Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let prefix = var("GOOGLE_CLOUD_RUST_SPANNER_TEST_DATABASE_PREFIX")
        .unwrap_or_else(|_| "testdb".to_string());
    let prefix_path = format!("/databases/{}", prefix);

    let mut list = client
        .list_databases()
        .set_parent(format!("projects/{}/instances/{}", project, instance))
        .by_item();

    while let Some(db) = list.next().await.transpose()? {
        if db.name.contains(&prefix_path) && db.create_time.is_some_and(|t| t < stale_deadline) {
            info!("Cleaning up stale database: {}", db.name);
            let _ = client.drop_database().set_database(db.name).send().await;
        }
    }

    Ok(())
}

async fn do_provision_real_spanner(project: &str, instance: &str) {
    let admin_client = DatabaseAdmin::builder()
        .build()
        .await
        .expect("Failed to create DatabaseAdmin client");

    // Clean up stale databases from previous aborted runs.
    if let Err(e) = cleanup_stale_databases(&admin_client, project, instance).await {
        warn!("failed to clean up stale databases: {e:?}");
    }

    let parent = format!("projects/{}/instances/{}", project, instance);
    let db_id = get_database_id().await;
    let create_statement = format!("CREATE DATABASE `{}`", db_id);

    info!(
        "Creating real Spanner database: {}/databases/{}",
        parent, db_id
    );

    let _db = admin_client
        .create_database()
        .set_parent(parent)
        .set_create_statement(create_statement)
        .set_extra_statements(EXTRA_STATEMENTS)
        .poller()
        .until_done()
        .await
        .expect("Failed to create real Spanner database");

    info!("Successfully created real Spanner database.");
}

pub async fn cleanup_real_spanner() {
    if var("GOOGLE_CLOUD_RUST_SPANNER_TEST_DATABASE").is_ok() {
        info!("GOOGLE_CLOUD_RUST_SPANNER_TEST_DATABASE is set. Skipping test database drop.");
        return;
    }

    let Some((project, instance)) = get_real_spanner_config() else {
        return;
    };

    let admin_client = match DatabaseAdmin::builder().build().await {
        Ok(c) => c,
        Err(e) => {
            warn!("failed to create DatabaseAdmin client in cleanup: {e:?}");
            return;
        }
    };

    let db_path = format!(
        "projects/{}/instances/{}/databases/{}",
        project,
        instance,
        get_database_id().await
    );

    info!("Dropping real Spanner database: {}", db_path);
    let _ = admin_client
        .drop_database()
        .set_database(db_path)
        .send()
        .await;
}

async fn do_provision_emulator_pg(endpoint: &str) {
    let rest_endpoint = get_emulator_rest_endpoint(endpoint);
    let client = Client::new();

    // Ensure the test instance is created
    ensure_emulator_instance_created(&client, &rest_endpoint).await;

    let database_payload = serde_json::json!({
        "createStatement": format!("CREATE DATABASE \"{}\"", get_pg_database_id().await),
        "databaseDialect": "POSTGRESQL",
    });
    ensure_emulator_database_created(&client, &rest_endpoint, &database_payload, "PostgreSQL")
        .await;

    let ddl_payload = serde_json::json!({
        "statements": PG_EXTRA_STATEMENTS
    });
    let res = client
        .patch(format!(
            "{}/v1/projects/{}/instances/{}/databases/{}/ddl",
            rest_endpoint,
            EMULATOR_PROJECT_ID,
            EMULATOR_INSTANCE_ID,
            get_pg_database_id().await
        ))
        .json(&ddl_payload)
        .send()
        .await
        .expect("Failed to apply PG DDL schema");
    assert!(res.status().is_success(), "Failed to apply PG schema DDL");

    // Clean up any leftover data from previous runs
    delete_all_data_from_all_types(get_pg_database_id().await).await;
}

async fn do_provision_real_spanner_pg(project: &str, instance: &str) {
    use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;

    let admin_client = DatabaseAdmin::builder()
        .build()
        .await
        .expect("Failed to create DatabaseAdmin client");

    let parent = format!("projects/{}/instances/{}", project, instance);
    let db_id = get_pg_database_id().await;

    info!(
        "Creating real Spanner PG database: {}/databases/{}",
        parent, db_id
    );

    let _db = admin_client
        .create_database()
        .set_parent(parent)
        .set_create_statement(format!("CREATE DATABASE \"{}\"", db_id))
        .set_database_dialect(DatabaseDialect::Postgresql)
        .poller()
        .until_done()
        .await
        .expect("Failed to create PG database on real Spanner");

    let db_path = format!(
        "projects/{}/instances/{}/databases/{}",
        project, instance, db_id
    );

    admin_client
        .update_database_ddl()
        .set_database(db_path)
        .set_statements(PG_EXTRA_STATEMENTS.iter().map(|s| s.to_string()))
        .poller()
        .until_done()
        .await
        .expect("Failed to apply PG schema DDL on real Spanner");
}

pub async fn cleanup_real_spanner_pg() {
    if var("GOOGLE_CLOUD_RUST_SPANNER_TEST_PG_DATABASE").is_ok() {
        return;
    }
    let Some((project, instance)) = get_real_spanner_config() else {
        return;
    };
    let admin_client = match DatabaseAdmin::builder().build().await {
        Ok(c) => c,
        Err(e) => {
            warn!("failed to create DatabaseAdmin client in PG cleanup: {e:?}");
            return;
        }
    };
    let db_path = format!(
        "projects/{}/instances/{}/databases/{}",
        project,
        instance,
        get_pg_database_id().await
    );
    let _ = admin_client
        .drop_database()
        .set_database(db_path)
        .send()
        .await;
}

/// Creates a database client for the test instance and database.
/// Returns None if neither SPANNER_EMULATOR_HOST nor GOOGLE_CLOUD_RUST_SPANNER_TEST_INSTANCE is set.
/// This indicates that integration tests should be skipped.
pub async fn create_database_client() -> Option<DatabaseClient> {
    if let Some(host) = get_emulator_host() {
        return create_emulator_database_client(&host).await;
    }

    if let Some((project, instance)) = get_real_spanner_config() {
        return create_real_spanner_database_client(&project, &instance).await;
    }

    info!(
        "Skipping Spanner E2E test as neither SPANNER_EMULATOR_HOST nor GOOGLE_CLOUD_RUST_SPANNER_TEST_INSTANCE is set"
    );
    None
}

async fn create_emulator_database_client(host: &str) -> Option<DatabaseClient> {
    wait_for_emulator(host).await;
    provision_emulator(host).await;

    let client = Spanner::builder()
        .build()
        .await
        .expect("Failed to create Spanner client");

    let db_client = client
        .database_client(format!(
            "projects/{}/instances/{}/databases/{}",
            EMULATOR_PROJECT_ID,
            EMULATOR_INSTANCE_ID,
            get_database_id().await
        ))
        .build()
        .await
        .expect("Failed to build database client");

    Some(db_client)
}

async fn create_real_spanner_database_client(
    project: &str,
    instance: &str,
) -> Option<DatabaseClient> {
    provision_real_spanner(project, instance).await;

    let client = Spanner::builder()
        .build()
        .await
        .expect("Failed to create Spanner client");

    let db_client = client
        .database_client(format!(
            "projects/{}/instances/{}/databases/{}",
            project,
            instance,
            get_database_id().await
        ))
        .build()
        .await
        .expect("Failed to build database client");

    Some(db_client)
}

pub async fn create_pg_database_client() -> Option<DatabaseClient> {
    if let Some(host) = get_emulator_host() {
        return create_emulator_pg_database_client(&host).await;
    }

    if let Some((project, instance)) = get_real_spanner_config() {
        return create_real_spanner_pg_database_client(&project, &instance).await;
    }

    None
}

async fn create_emulator_pg_database_client(host: &str) -> Option<DatabaseClient> {
    wait_for_emulator(host).await;
    PROVISION_EMULATOR_PG
        .get_or_init(|| async {
            do_provision_emulator_pg(host).await;
        })
        .await;

    let spanner_client = Spanner::builder()
        .build()
        .await
        .expect("Failed to create Spanner client");
    let db_client = spanner_client
        .database_client(format!(
            "projects/{}/instances/{}/databases/{}",
            EMULATOR_PROJECT_ID,
            EMULATOR_INSTANCE_ID,
            get_pg_database_id().await
        ))
        .build()
        .await
        .expect("Failed to build database client");
    Some(db_client)
}

async fn create_real_spanner_pg_database_client(
    project: &str,
    instance: &str,
) -> Option<DatabaseClient> {
    PROVISION_REAL_SPANNER_PG
        .get_or_init(|| async {
            do_provision_real_spanner_pg(project, instance).await;
        })
        .await;

    let spanner_client = Spanner::builder()
        .build()
        .await
        .expect("Failed to create Spanner client");
    let db_client = spanner_client
        .database_client(format!(
            "projects/{}/instances/{}/databases/{}",
            project,
            instance,
            get_pg_database_id().await
        ))
        .build()
        .await
        .expect("Failed to build database client");
    Some(db_client)
}

/// Updates the database DDL by executing the given statement on the Spanner Emulator or real Spanner instance.
pub async fn update_database_ddl(statement: String) -> Result<()> {
    update_database_ddl_batch(vec![statement]).await
}

/// Updates the database DDL by executing the given batch of statements on the Spanner Emulator or real Spanner instance.
pub async fn update_database_ddl_batch(statements: Vec<String>) -> Result<()> {
    if let Some(emulator_host) = get_emulator_host() {
        return update_emulator_ddl(&emulator_host, statements).await;
    }

    if let Some((project, instance)) = get_real_spanner_config() {
        return update_real_spanner_ddl(&project, &instance, statements).await;
    }

    anyhow::bail!(
        "Neither SPANNER_EMULATOR_HOST nor GOOGLE_CLOUD_RUST_SPANNER_TEST_INSTANCE is set"
    );
}

/// Updates the database DDL by executing the given statements on the Spanner Emulator.
///
/// This method uses the emulator's REST API directly. It includes a retry loop to handle
/// transient "Schema change operation rejected" errors that can occur in the emulator
/// if multiple schema changes are executed in parallel, or if schema changes are executed
/// in parallel with read/write transactions.
async fn update_emulator_ddl(emulator_host: &str, statements: Vec<String>) -> Result<()> {
    let rest_endpoint = get_emulator_rest_endpoint(emulator_host);
    let db_path = format!(
        "projects/{}/instances/{}/databases/{}",
        EMULATOR_PROJECT_ID,
        EMULATOR_INSTANCE_ID,
        get_database_id().await
    );
    let url = format!("{}/v1/{}/ddl", rest_endpoint, db_path);
    let client = Client::new();
    let payload = serde_json::json!({
        "statements": statements
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

/// Updates the database DDL by executing the given statements on a real Spanner instance.
async fn update_real_spanner_ddl(
    project: &str,
    instance: &str,
    statements: Vec<String>,
) -> Result<()> {
    let admin_client = DatabaseAdmin::builder().build().await?;
    let db_path = format!(
        "projects/{}/instances/{}/databases/{}",
        project,
        instance,
        get_database_id().await
    );
    admin_client
        .update_database_ddl()
        .set_database(db_path)
        .set_statements(statements)
        .poller()
        .until_done()
        .await?;
    Ok(())
}

static COMPLETED_TESTS: AtomicUsize = AtomicUsize::new(0);

pub async fn finish_test(total_test_suites: usize) {
    let prev = COMPLETED_TESTS.fetch_add(1, Ordering::SeqCst);
    if prev + 1 == total_test_suites {
        info!(
            "All {} integration test suites completed. Executing final cleanup.",
            total_test_suites
        );
        cleanup_real_spanner().await;
        cleanup_real_spanner_pg().await;
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
pub async fn start_guarded_server<T>(address: &str, service: T) -> Result<(String, ServerGuard)>
where
    T: MockSpannerTrait + Send + 'static,
{
    let (uri, handle) = spanner_grpc_mock::start(address, service).await?;
    Ok((uri, ServerGuard(handle)))
}
