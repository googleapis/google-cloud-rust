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

/// A SQL statement for execution on Spanner.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::Statement;
/// # async fn test_doc() -> Result<(), google_cloud_spanner::Error> {
/// let client = Spanner::builder().build().await.unwrap();
/// let db = client.database_client("projects/p/instances/i/databases/d").build().await.unwrap();
///
/// let tx = db.single_use().build();
/// let mut rs = tx.execute_query(Statement::new("SELECT 1")).await?;
///
/// while let Some(row) = rs.next().await {
///     let row = row?;
///     // process row
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Statement {
    pub sql: String,
    // TODO(#4970): Add support for query parameters.
}

impl Statement {
    /// Creates a new statement.
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(Statement: Clone, std::fmt::Debug, PartialEq, Send, Sync);
    }
}
