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

//! Spanner Omni instance types and configuration utilities.

pub use crate::client::SpannerBuilderExt;

/// Specifies the type of Spanner instance to connect to (`Cloud` or `Omni`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum InstanceType {
    /// Google Cloud Spanner instance (default).
    #[default]
    Cloud,
    /// Spanner Omni instance.
    Omni,
}

/// Helper function to check if an endpoint string uses plaintext (`http://`).
pub(crate) fn is_plaintext_endpoint(endpoint: &str) -> bool {
    let trimmed = endpoint.trim();
    if let Ok(parsed_url) = url::Url::parse(trimmed) {
        parsed_url.scheme() == "http"
    } else {
        trimmed.starts_with("http://")
    }
}

/// Helper function to format database resource names for Spanner Omni.
///
/// If project or instance IDs are omitted, defaults to `projects/default/instances/default/databases/{database}`.
pub(crate) fn format_database_name(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return trimmed.to_string();
    }

    let parts: Vec<&str> = trimmed.split('/').collect();
    match parts.as_slice() {
        [
            "projects",
            _project,
            "instances",
            _instance,
            "databases",
            _database,
        ] => trimmed.to_string(),
        ["instances", instance, "databases", database] => {
            format!(
                "projects/default/instances/{}/databases/{}",
                instance, database
            )
        }
        ["databases", database] => {
            format!("projects/default/instances/default/databases/{}", database)
        }
        [database] => {
            format!("projects/default/instances/default/databases/{}", database)
        }
        _ => trimmed.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;

    #[test]
    fn test_format_database_name() {
        assert_eq!(
            format_database_name("projects/p/instances/i/databases/d"),
            "projects/p/instances/i/databases/d"
        );
        assert_eq!(
            format_database_name("instances/i/databases/d"),
            "projects/default/instances/i/databases/d"
        );
        assert_eq!(
            format_database_name("databases/d"),
            "projects/default/instances/default/databases/d"
        );
        assert_eq!(
            format_database_name("retail-sample"),
            "projects/default/instances/default/databases/retail-sample"
        );
    }

    #[test]
    fn test_is_plaintext_endpoint() {
        assert!(is_plaintext_endpoint("http://localhost:15000"));
        assert!(!is_plaintext_endpoint("https://spanner.internal:15000"));
        assert!(!is_plaintext_endpoint("127.0.0.1:15000"));
        assert!(is_plaintext_endpoint("http://not a valid url:1234"));
    }

    #[tokio::test]
    async fn test_spanner_with_instance_type() {
        let spanner = Spanner::builder()
            .with_instance_type(InstanceType::Omni)
            .build()
            .await
            .expect("build client");
        assert_eq!(spanner.instance_type(), InstanceType::Omni);
    }

    #[tokio::test]
    #[ignore = "requires live Omni instance at localhost:15000"]
    async fn test_query_local_omni_instance() {
        let spanner = Spanner::builder()
            .with_endpoint("http://localhost:15000")
            .with_instance_type(InstanceType::Omni)
            .build()
            .await
            .expect("build client");

        let db_client = spanner
            .database_client("retail-sample")
            .build()
            .await
            .expect("build db client");

        let tx = db_client.single_use().build();
        let mut rs = tx
            .execute_query("SELECT * FROM Products LIMIT 5")
            .await
            .expect("execute query");

        let mut count = 0;
        while let Some(row) = rs.next().await {
            let row = row.expect("read row");
            println!("Fetched Omni row {}: {:?}", count, row);
            count += 1;
        }
        println!("Successfully queried Omni! Total rows fetched: {}", count);
    }
}
