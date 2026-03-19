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

use crate::key::KeySet;

/// Represents an incomplete read operation that requires specifying keys.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::ReadRequest;
/// # use google_cloud_spanner::client::KeySet;
/// # use google_cloud_spanner::key;
/// // Read all rows from a table using its primary key.
/// let read_all = ReadRequest::builder("Users", vec!["Id", "Name"])
///     .with_keys(KeySet::all())
///     .build();
///
/// // Read specific rows using an index.
/// let read_by_id = ReadRequest::builder("Users", vec!["Id", "Name"])
///     .with_index("UsersByIndex", key![1_i64])
///     .with_limit(10)
///     .build();
/// ```
///
/// Use `ReadRequest::builder` to define the table and columns to be read.
/// Keys must be supplied using `with_keys` (for the primary key) or `with_index` (for an index)
/// to obtain an executable `ReadRequest`.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadRequestBuilder {
    table: String,
    columns: Vec<String>,
}

impl ReadRequestBuilder {
    /// Supplies the `KeySet` targeting the table's primary key.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{ReadRequest, KeySet};
    /// let request = ReadRequest::builder("Users", vec!["Id", "Name"]).with_keys(KeySet::all());
    /// ```
    ///
    /// The `keys` parameter identifies the rows to be yielded by naming the primary keys
    /// of the rows in the table. Rows are yielded in table primary key order.
    pub fn with_keys(self, keys: impl Into<KeySet>) -> ConfiguredReadRequestBuilder {
        ConfiguredReadRequestBuilder {
            table: self.table,
            index: None,
            keys: keys.into(),
            columns: self.columns,
            limit: None,
        }
    }

    /// Supplies an index name and its associated `KeySet`.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{ReadRequest, KeySet};
    /// # use google_cloud_spanner::key;
    /// let request = ReadRequest::builder("Users", vec!["Id", "Name"]).with_index("UsersByIndex", key![1_i64]);
    /// ```
    ///
    /// The `keys` parameter identifies the rows to be yielded by naming the index keys
    /// in the provided `index`. Rows are yielded in index key order.
    pub fn with_index(
        self,
        index: impl Into<String>,
        keys: impl Into<KeySet>,
    ) -> ConfiguredReadRequestBuilder {
        ConfiguredReadRequestBuilder {
            table: self.table,
            index: Some(index.into()),
            keys: keys.into(),
            columns: self.columns,
            limit: None,
        }
    }
}

/// A fully configured read request that is ready to be built.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfiguredReadRequestBuilder {
    table: String,
    index: Option<String>,
    keys: KeySet,
    columns: Vec<String>,
    limit: Option<i64>,
}

impl ConfiguredReadRequestBuilder {
    /// Sets an optional limit on how many rows could be retrieved.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{ReadRequest, KeySet};
    /// let request = ReadRequest::builder("Users", vec!["Id"])
    ///     .with_keys(KeySet::all())
    ///     .with_limit(10)
    ///     .build();
    /// ```
    ///
    /// If fewer rows are found, only the matching rows will be returned.
    pub fn with_limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Builds the configured `ReadRequest`.
    pub fn build(self) -> ReadRequest {
        ReadRequest {
            table: self.table,
            index: self.index,
            keys: self.keys,
            columns: self.columns,
            limit: self.limit,
        }
    }
}

/// Represents a configured read request ready for execution.
///
/// Contains the table, optional index, keys, and columns.
/// Allows configuring optional parameters on the read operation, such as a limit.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadRequest {
    pub(crate) table: String,
    pub(crate) index: Option<String>,
    pub(crate) keys: KeySet,
    pub(crate) columns: Vec<String>,
    pub(crate) limit: Option<i64>,
}

impl ReadRequest {
    /// Creates a new read operation builder for a specific table.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::ReadRequest;
    /// let builder = ReadRequest::builder("Users", vec!["Id", "Name"]);
    /// ```
    ///
    /// The table name and columns to retrieve are required to initiate a read.
    pub fn builder(
        table: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> ReadRequestBuilder {
        ReadRequestBuilder {
            table: table.into(),
            columns: columns.into_iter().map(|s| s.into()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(ReadRequestBuilder: Send, Sync, Clone, std::fmt::Debug, PartialEq);
        static_assertions::assert_impl_all!(ConfiguredReadRequestBuilder: Send, Sync, Clone, std::fmt::Debug, PartialEq);
        static_assertions::assert_impl_all!(ReadRequest: Send, Sync, Clone, std::fmt::Debug, PartialEq);
    }

    #[test]
    fn read_with_keys() {
        let keys = KeySet::all();
        let req = ReadRequest::builder("MyTable", vec!["col1", "col2"])
            .with_keys(keys.clone())
            .build();
        assert_eq!(req.table, "MyTable");
        assert_eq!(req.index, None);
        assert_eq!(req.keys, keys);
        assert_eq!(req.columns, vec!["col1", "col2"]);
        assert_eq!(req.limit, None);
    }

    #[test]
    fn read_with_index() {
        let keys = KeySet::all();
        let req = ReadRequest::builder("MyTable", vec!["col1", "col2"])
            .with_index("MyIndex", keys.clone())
            .build();
        assert_eq!(req.table, "MyTable");
        assert_eq!(req.index, Some("MyIndex".to_string()));
        assert_eq!(req.keys, keys);
        assert_eq!(req.columns, vec!["col1", "col2"]);
        assert_eq!(req.limit, None);
    }

    #[test]
    fn with_limit() {
        let req = ReadRequest::builder("MyTable", vec!["col1"])
            .with_keys(KeySet::all())
            .with_limit(42)
            .build();
        assert_eq!(req.limit, Some(42));
    }
}
