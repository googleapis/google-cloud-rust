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

use crate::client::Spanner;
use crate::database_client::DatabaseClient;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousBuilder;
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, LazyLock, Mutex};

/// Parsed Spanner Data Source Name (DSN).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParsedDsn {
    /// Optional custom host/endpoint.
    pub host: Option<String>,
    /// Google Cloud Project ID.
    pub project: String,
    /// Cloud Spanner Instance ID.
    pub instance: String,
    /// Cloud Spanner Database name.
    pub database: String,
    /// Connection parameters.
    pub params: HashMap<String, String>,
}

/// Parse a Spanner DSN connection string without regular expressions.
/// Supports formats:
/// - `projects/{project}/instances/{instance}/databases/{database}`
/// - `host/projects/{project}/instances/{instance}/databases/{database}`
/// - Either format optionally followed by query parameter string starting with `?` or `;`.
pub(crate) fn parse_dsn(dsn: &str) -> Result<ParsedDsn, crate::Error> {
    let (path_part, params_part) = if let Some(idx) = dsn.find('?') {
        (&dsn[..idx], &dsn[idx + 1..])
    } else if let Some(idx) = dsn.find(';') {
        (&dsn[..idx], &dsn[idx + 1..])
    } else {
        (dsn, "")
    };

    let params = extract_connector_params(params_part)?;

    let projects_idx = path_part
        .find("projects/")
        .ok_or_else(|| crate::Error::deser("Connection DSN must contain 'projects/' segment"))?;

    let host = if projects_idx > 0 {
        let prefix = &path_part[..projects_idx];
        if let Some(stripped) = prefix.strip_suffix('/') {
            Some(stripped.to_string())
        } else {
            Some(prefix.to_string())
        }
    } else {
        None
    };

    let mut path = &path_part[projects_idx..];
    if path.ends_with('/') {
        path = &path[..path.len() - 1];
    }
    let segments: Vec<&str> = path.split('/').collect();
    if segments.len() != 6
        || segments[0] != "projects"
        || segments[1].is_empty()
        || segments[2] != "instances"
        || segments[3].is_empty()
        || segments[4] != "databases"
        || segments[5].is_empty()
    {
        return Err(crate::Error::deser(format!(
            "Invalid Spanner database path format: '{}'. Expected 'projects/{{project}}/instances/{{instance}}/databases/{{database}}'",
            path
        )));
    }

    Ok(ParsedDsn {
        host,
        project: segments[1].to_string(),
        instance: segments[3].to_string(),
        database: segments[5].to_string(),
        params,
    })
}

fn extract_connector_params(params_str: &str) -> Result<HashMap<String, String>, crate::Error> {
    let mut params = HashMap::new();
    if params_str.is_empty() {
        return Ok(params);
    }

    let chars: Vec<char> = params_str.chars().collect();
    let mut i = 0;
    let len = chars.len();

    while i < len {
        while i < len && (chars[i].is_whitespace() || chars[i] == ';' || chars[i] == '&') {
            i += 1;
        }
        if i >= len {
            break;
        }

        let mut key = String::new();
        while i < len
            && chars[i] != '='
            && !chars[i].is_whitespace()
            && chars[i] != ';'
            && chars[i] != '&'
        {
            key.push(chars[i]);
            i += 1;
        }

        while i < len && chars[i].is_whitespace() {
            i += 1;
        }

        if i >= len || chars[i] != '=' {
            return Err(crate::Error::deser(format!(
                "Invalid DSN parameter: expected '=' after key '{}'",
                key
            )));
        }
        i += 1; // Eat '='

        while i < len && chars[i].is_whitespace() {
            i += 1;
        }

        let mut val = String::new();
        if i < len && (chars[i] == '"' || chars[i] == '\'') {
            let quote = chars[i];
            i += 1; // Eat quote
            while i < len && chars[i] != quote {
                if chars[i] == '\\' && i + 1 < len && chars[i + 1] == quote {
                    val.push(quote);
                    i += 2;
                } else {
                    val.push(chars[i]);
                    i += 1;
                }
            }
            if i >= len {
                return Err(crate::Error::deser(format!(
                    "Unterminated quoted value for DSN parameter '{}'",
                    key
                )));
            }
            i += 1; // Eat closing quote
        } else {
            while i < len && chars[i] != ';' && chars[i] != '&' {
                val.push(chars[i]);
                i += 1;
            }
            val = val.trim().to_string();
        }

        params.insert(key.trim().to_lowercase(), val);
    }

    Ok(params)
}

/// Cache key uniquely identifying a Spanner client channel pool configuration.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ClientPoolKey {
    /// Host/endpoint address.
    pub endpoint: Option<String>,
    /// Optional credentials file path.
    pub credentials_file: Option<String>,
    /// Custom universe domain if specified.
    pub universe_domain: Option<String>,
    /// Whether to use plaintext communication (sets anonymous credentials).
    pub use_plaintext: bool,
}

impl ClientPoolKey {
    /// Map connection string parameters to the client pool cache key.
    pub(crate) fn from_dsn(dsn: &ParsedDsn) -> Self {
        let use_plaintext = dsn
            .params
            .get("useplaintext")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let mut endpoint = dsn
            .host
            .clone()
            .or_else(|| dsn.params.get("endpoint").cloned());
        let needs_http = use_plaintext
            && endpoint
                .as_ref()
                .is_some_and(|ep| !ep.starts_with("http://") && !ep.starts_with("https://"));
        if needs_http {
            let ep = endpoint.as_mut().unwrap();
            *ep = format!("http://{}", ep);
        }
        Self {
            endpoint,
            credentials_file: dsn.params.get("credentials").cloned(),
            universe_domain: dsn.params.get("universe_domain").cloned(),
            use_plaintext,
        }
    }
}

/// A thread-safe Spanner client pool that shares client instances.
pub(crate) struct ClientPool {
    pub(crate) clients: HashMap<ClientPoolKey, Arc<tokio::sync::OnceCell<Spanner>>>,
    pub(crate) db_clients:
        HashMap<(ClientPoolKey, String), Arc<tokio::sync::OnceCell<DatabaseClient>>>,
    pub(crate) admin_clients: HashMap<ClientPoolKey, Arc<tokio::sync::OnceCell<DatabaseAdmin>>>,
}

impl ClientPool {
    fn new() -> Self {
        Self {
            clients: HashMap::new(),
            db_clients: HashMap::new(),
            admin_clients: HashMap::new(),
        }
    }

    /// Retrieve or create a Spanner client for the given configuration.
    ///
    /// Note: This method is only intended for testing where we want to have predictable
    /// behavior for when a new client is created and when it is reused. In production,
    /// get_or_create_global should be used instead.
    #[allow(dead_code)]
    pub(crate) async fn get_or_create(
        &mut self,
        key: &ClientPoolKey,
    ) -> Result<Spanner, crate::Error> {
        let cell = self
            .clients
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone();
        let client = cell.get_or_try_init(|| build_spanner_client(key)).await?;
        Ok(client.clone())
    }

    /// Retrieve or create a Spanner client for the global static pool without holding a MutexGuard across await.
    pub(crate) async fn get_or_create_global(key: &ClientPoolKey) -> Result<Spanner, crate::Error> {
        let cell = {
            let mut pool = CLIENT_POOL
                .lock()
                .map_err(|e| crate::Error::deser(format!("Client pool lock error: {}", e)))?;
            pool.clients
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };

        let client = cell.get_or_try_init(|| build_spanner_client(key)).await?;
        Ok(client.clone())
    }

    /// Retrieve or create a DatabaseClient for the global static pool.
    pub(crate) async fn get_or_create_db_client(
        key: &ClientPoolKey,
        db_path: &str,
    ) -> Result<DatabaseClient, crate::Error> {
        let spanner = Self::get_or_create_global(key).await?;

        let cache_key = (key.clone(), db_path.to_string());
        let cell = {
            let mut pool = CLIENT_POOL
                .lock()
                .map_err(|e| crate::Error::deser(format!("Client pool lock error: {}", e)))?;
            pool.db_clients
                .entry(cache_key)
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };

        let client = cell
            .get_or_try_init(|| async {
                spanner
                    .database_client(db_path)
                    .build()
                    .await
                    .map_err(crate::Error::connect)
            })
            .await?;

        Ok(client.clone())
    }

    /// Retrieve or create a DatabaseAdmin client for the global static pool.
    pub(crate) async fn get_or_create_admin_global(
        key: &ClientPoolKey,
        spanner: &Spanner,
    ) -> Result<DatabaseAdmin, crate::Error> {
        let cell = {
            let mut pool = CLIENT_POOL
                .lock()
                .map_err(|e| crate::Error::deser(format!("Client pool lock error: {}", e)))?;
            pool.admin_clients
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };

        let admin_client = cell
            .get_or_try_init(|| async {
                spanner
                    .database_admin_builder()
                    .build()
                    .await
                    .map_err(crate::Error::connect)
            })
            .await?;

        Ok(admin_client.clone())
    }
}

async fn build_spanner_client(key: &ClientPoolKey) -> Result<Spanner, crate::Error> {
    let mut builder = Spanner::builder();

    if let Some(ref ep) = key.endpoint {
        builder = builder.with_endpoint(ep);
    }

    if let Some(ref path) = key.credentials_file {
        let creds = load_credentials_from_file(path)?;
        builder = builder.with_credentials(creds);
    } else if key.use_plaintext {
        let creds = AnonymousBuilder::new().build();
        builder = builder.with_credentials(creds);
    }

    if let Some(ref ud) = key.universe_domain {
        builder = builder.with_universe_domain(ud);
    }

    builder.build().await.map_err(crate::Error::deser)
}

/// Global client pool manager.
pub(crate) static CLIENT_POOL: LazyLock<Mutex<ClientPool>> =
    LazyLock::new(|| Mutex::new(ClientPool::new()));

fn load_credentials_from_file(path: &str) -> Result<gaxi::options::Credentials, crate::Error> {
    let content = fs::read_to_string(path).map_err(|e| {
        crate::Error::deser(format!("Failed to read credentials file {}: {}", path, e))
    })?;
    let json: Value = serde_json::from_str(&content)
        .map_err(|e| crate::Error::deser(format!("Failed to parse credentials JSON: {}", e)))?;

    let cred_type = json.get("type").and_then(Value::as_str).unwrap_or("");
    match cred_type {
        "service_account" => {
            let builder = google_cloud_auth::credentials::service_account::Builder::new(json);
            builder.build().map_err(|e| {
                crate::Error::deser(format!(
                    "Failed to build service account credentials: {}",
                    e
                ))
            })
        }
        "authorized_user" => {
            let builder = google_cloud_auth::credentials::user_account::Builder::new(json);
            builder.build().map_err(|e| {
                crate::Error::deser(format!("Failed to build user credentials: {}", e))
            })
        }
        _ => Err(crate::Error::deser(format!(
            "Unsupported credentials type: {}",
            cred_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_client_pooling_cache() {
        unsafe {
            std::env::set_var("SPANNER_EMULATOR_HOST", "localhost:9010");
        }

        let key1 = ClientPoolKey {
            endpoint: Some("localhost:9010".to_string()),
            credentials_file: None,
            universe_domain: None,
            use_plaintext: false,
        };

        let key2 = ClientPoolKey {
            endpoint: Some("localhost:9010".to_string()),
            credentials_file: None,
            universe_domain: None,
            use_plaintext: false,
        };

        let key3 = ClientPoolKey {
            endpoint: Some("localhost:9020".to_string()),
            credentials_file: None,
            universe_domain: None,
            use_plaintext: false,
        };

        let mut pool = ClientPool::new();
        assert_eq!(pool.clients.len(), 0);

        let c1 = pool
            .get_or_create(&key1)
            .await
            .expect("c1 creation should succeed");
        assert_eq!(pool.clients.len(), 1);

        let c2 = pool
            .get_or_create(&key2)
            .await
            .expect("c2 retrieval should succeed");
        assert_eq!(pool.clients.len(), 1, "should reuse cached client instance");

        assert_eq!(c1.config.endpoint, c2.config.endpoint);

        let _c3 = pool
            .get_or_create(&key3)
            .await
            .expect("c3 creation should succeed");
        assert_eq!(
            pool.clients.len(),
            2,
            "different configuration should create new client instance"
        );

        unsafe {
            std::env::remove_var("SPANNER_EMULATOR_HOST");
        }
    }

    #[tokio::test]
    async fn test_load_credentials_from_file() {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("spanner_mock_creds.json");

        let service_account_json = r#"{
            "type": "service_account",
            "project_id": "my-project",
            "private_key_id": "key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC3\n-----END PRIVATE KEY-----\n",
            "client_email": "service-account@my-project.iam.gserviceaccount.com",
            "client_id": "client-id",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account%40my-project.iam.gserviceaccount.com"
        }"#;

        fs::write(&file_path, service_account_json).expect("should write temp file");

        let creds_res = load_credentials_from_file(file_path.to_str().expect("valid path"));
        let _ = fs::remove_file(file_path);

        assert!(
            creds_res.is_ok(),
            "should successfully parse valid service account json"
        );
    }

    #[test]
    fn test_parse_dsn_success() {
        let dsn = "localhost:9010/projects/test-project/instances/test-instance/databases/test-database;useplaintext=true;credentials=/path/to/key.json;universe_domain=domain.gcp";
        let parsed = parse_dsn(dsn).expect("should parse");

        assert_eq!(parsed.host.as_deref(), Some("localhost:9010"));
        assert_eq!(parsed.project, "test-project");
        assert_eq!(parsed.instance, "test-instance");
        assert_eq!(parsed.database, "test-database");
        assert_eq!(
            parsed.params.get("useplaintext").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            parsed.params.get("credentials").map(String::as_str),
            Some("/path/to/key.json")
        );
        assert_eq!(
            parsed.params.get("universe_domain").map(String::as_str),
            Some("domain.gcp")
        );

        let key = ClientPoolKey::from_dsn(&parsed);
        assert_eq!(key.endpoint.as_deref(), Some("http://localhost:9010"));
        assert_eq!(key.credentials_file.as_deref(), Some("/path/to/key.json"));
        assert_eq!(key.universe_domain.as_deref(), Some("domain.gcp"));
        assert!(key.use_plaintext);
    }

    #[test]
    fn test_parse_dsn_invalid() {
        assert!(parse_dsn("invalid_dsn_string").is_err());
        assert!(parse_dsn("projects/p/instances/i").is_err());
        assert!(
            parse_dsn("projects/p/instances/i/databases/d/extra").is_err(),
            "extra segments must be rejected"
        );
        assert!(
            parse_dsn("projects/p/instances/i/databases/").is_err(),
            "empty database name must be rejected"
        );
        assert!(
            parse_dsn("projects//instances/i/databases/d").is_err(),
            "empty project ID must be rejected"
        );
        assert!(
            parse_dsn("project/p/instances/i/databases/d").is_err(),
            "incorrect projects keyword must be rejected"
        );
        assert!(
            parse_dsn("projects/p/instance/i/databases/d").is_err(),
            "incorrect instances keyword must be rejected"
        );
        assert!(
            parse_dsn("projects/p/instances/i/database/d").is_err(),
            "incorrect databases keyword must be rejected"
        );

        // Verify trailing slash gets stripped and parses successfully
        let valid_with_slash = parse_dsn("projects/p/instances/i/databases/d/");
        assert!(
            valid_with_slash.is_ok(),
            "trailing slash should be stripped and allowed"
        );
        let parsed = valid_with_slash.unwrap();
        assert_eq!(parsed.project, "p");
        assert_eq!(parsed.instance, "i");
        assert_eq!(parsed.database, "d");
    }

    #[test]
    fn test_parse_dsn_quoted_values() {
        let dsn = "projects/my-project/instances/my-instance/databases/my-database?my_prop=\"my value\";my_other_prop=\"little Bobby drop tables;\"";
        let parsed = parse_dsn(dsn).expect("should parse successfully");

        assert_eq!(parsed.project, "my-project");
        assert_eq!(
            parsed.params.get("my_prop").map(String::as_str),
            Some("my value")
        );
        assert_eq!(
            parsed.params.get("my_other_prop").map(String::as_str),
            Some("little Bobby drop tables;")
        );
    }
}
