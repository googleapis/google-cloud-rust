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
use crate::model::DirectedReadOptions;
use crate::model::read_request::{LockHint, OrderBy};
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use std::time::Duration;

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
            request_options: None,
            directed_read_options: None,
            order_by: None,
            lock_hint: None,
            gax_options: GaxRequestOptions::default(),
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
            request_options: None,
            directed_read_options: None,
            order_by: None,
            lock_hint: None,
            gax_options: GaxRequestOptions::default(),
        }
    }
}

/// A fully configured read request that is ready to be built.
#[derive(Clone, Debug)]
pub struct ConfiguredReadRequestBuilder {
    table: String,
    index: Option<String>,
    keys: KeySet,
    columns: Vec<String>,
    limit: Option<i64>,
    request_options: Option<crate::model::RequestOptions>,
    directed_read_options: Option<DirectedReadOptions>,
    order_by: Option<OrderBy>,
    lock_hint: Option<LockHint>,
    gax_options: GaxRequestOptions,
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

    /// Sets the request tag to use for this read.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{ReadRequest, KeySet};
    /// let request = ReadRequest::builder("Users", vec!["Id"])
    ///     .with_keys(KeySet::all())
    ///     .with_request_tag("my-tag")
    ///     .build();
    /// ```
    ///
    /// See also: [Troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
    pub fn with_request_tag(mut self, tag: impl Into<String>) -> Self {
        self.request_options
            .get_or_insert_with(crate::model::RequestOptions::default)
            .request_tag = tag.into();
        self
    }

    /// Sets the directed read options for this request.
    ///
    /// ```
    /// # use google_cloud_spanner::client::ReadRequest;
    /// # use google_cloud_spanner::client::KeySet;
    /// # use google_cloud_spanner::model::DirectedReadOptions;
    /// let dro = DirectedReadOptions::default();
    /// let req = ReadRequest::builder("MyTable", vec!["col1"])
    ///     .with_keys(KeySet::all())
    ///     .with_directed_read_options(dro)
    ///     .build();
    /// ```
    ///
    /// DirectedReadOptions can only be specified for a read-only transaction,
    /// otherwise Spanner returns an INVALID_ARGUMENT error.
    pub fn with_directed_read_options(mut self, options: DirectedReadOptions) -> Self {
        self.directed_read_options = Some(options);
        self
    }

    /// Sets the order in which rows are returned.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{ReadRequest, KeySet};
    /// # use google_cloud_spanner::model::read_request::OrderBy;
    /// let request = ReadRequest::builder("Users", vec!["Id"])
    ///     .with_keys(KeySet::all())
    ///     .with_order_by(OrderBy::NoOrder);
    /// ```
    ///
    /// By default, Spanner returns result rows in primary key order (or index key
    /// order if reading via an index) except for `PartitionRead` requests.
    ///
    /// For applications that don't require rows to be returned in primary key
    /// (`ORDER_BY_PRIMARY_KEY`) order, setting `ORDER_BY_NO_ORDER` option allows
    /// Spanner to optimize row retrieval, resulting in lower latencies in certain
    /// cases (for example, bulk point lookups).
    pub fn with_order_by(mut self, order_by: OrderBy) -> Self {
        self.order_by = Some(order_by);
        self
    }

    /// Sets the lock hint for this read.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{ReadRequest, KeySet};
    /// # use google_cloud_spanner::model::read_request::LockHint;
    /// let request = ReadRequest::builder("Users", vec!["Id"])
    ///     .with_keys(KeySet::all())
    ///     .with_lock_hint(LockHint::Exclusive);
    /// ```
    ///
    /// Lock hints can only be used with read-write transactions.
    ///
    /// By default, Spanner acquires shared read locks, which allows other reads to
    /// still access the data until your transaction is ready to commit.
    ///
    /// Requesting exclusive locks (`LOCK_HINT_EXCLUSIVE`) is beneficial if you observe
    /// high write contention. It prevents deadlocks by avoiding the situation where
    /// multiple transactions initially acquire shared locks and then both try to upgrade
    /// to exclusive locks at the same time.
    ///
    /// Request exclusive locks judiciously because they block others from reading that
    /// data for the entire transaction, rather than just when the writes are being performed.
    pub fn with_lock_hint(mut self, lock_hint: LockHint) -> Self {
        self.lock_hint = Some(lock_hint);
        self
    }

    /// Sets the per-attempt timeout for this read request.
    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.gax_options.set_attempt_timeout(timeout);
        self
    }

    /// Sets the retry policy for this read request.
    pub fn with_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.gax_options.set_retry_policy(policy);
        self
    }

    /// Sets the backoff policy for this read request.
    pub fn with_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.gax_options.set_backoff_policy(policy);
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
            request_options: self.request_options,
            directed_read_options: self.directed_read_options,
            order_by: self.order_by,
            lock_hint: self.lock_hint,
            gax_options: self.gax_options,
        }
    }
}

/// Represents a configured read request ready for execution.
///
/// Contains the table, optional index, keys, and columns.
/// Allows configuring optional parameters on the read operation, such as a limit.
#[derive(Clone, Debug)]
pub struct ReadRequest {
    pub(crate) table: String,
    pub(crate) index: Option<String>,
    pub(crate) keys: KeySet,
    pub(crate) columns: Vec<String>,
    pub(crate) limit: Option<i64>,
    pub(crate) request_options: Option<crate::model::RequestOptions>,
    pub(crate) directed_read_options: Option<DirectedReadOptions>,
    pub(crate) order_by: Option<OrderBy>,
    pub(crate) lock_hint: Option<LockHint>,
    pub(crate) gax_options: GaxRequestOptions,
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

    pub(crate) fn into_request(self) -> crate::model::ReadRequest {
        crate::model::ReadRequest::default()
            .set_table(self.table)
            .set_columns(self.columns)
            .set_key_set(self.keys.into_proto())
            .set_index(self.index.unwrap_or_default())
            .set_limit(self.limit.unwrap_or_default())
            .set_or_clear_request_options(self.request_options)
            .set_or_clear_directed_read_options(self.directed_read_options)
            .set_order_by(self.order_by.unwrap_or_default())
            .set_lock_hint(self.lock_hint.unwrap_or_default())
    }

    pub(crate) fn into_partition_read_request(self) -> crate::model::PartitionReadRequest {
        crate::model::PartitionReadRequest::default()
            .set_table(self.table)
            .set_columns(self.columns)
            .set_key_set(self.keys.into_proto())
            .set_index(self.index.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(ReadRequestBuilder: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(ConfiguredReadRequestBuilder: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(ReadRequest: Send, Sync, Clone, std::fmt::Debug);
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

    #[test]
    fn with_request_tag() {
        let req = ReadRequest::builder("MyTable", vec!["col1"])
            .with_keys(KeySet::all())
            .with_request_tag("tag1")
            .build();
        assert_eq!(
            req.request_options
                .expect("request options missing")
                .request_tag,
            "tag1"
        );
    }

    #[test]
    fn with_directed_read_options() {
        let dro = DirectedReadOptions::default();
        let req = ReadRequest::builder("MyTable", vec!["col1"])
            .with_keys(KeySet::all())
            .with_directed_read_options(dro.clone())
            .build();
        assert_eq!(req.directed_read_options, Some(dro));
    }

    #[test]
    fn with_order_by() {
        let req = ReadRequest::builder("MyTable", vec!["col1"])
            .with_keys(KeySet::all())
            .with_order_by(OrderBy::PrimaryKey)
            .build();
        assert_eq!(req.order_by, Some(OrderBy::PrimaryKey));
    }

    #[test]
    fn with_lock_hint() {
        let req = ReadRequest::builder("MyTable", vec!["col1"])
            .with_keys(KeySet::all())
            .with_lock_hint(LockHint::Exclusive)
            .build();
        assert_eq!(req.lock_hint, Some(LockHint::Exclusive));
    }

    #[test]
    fn with_gax_options() -> anyhow::Result<()> {
        use google_cloud_gax::exponential_backoff::ExponentialBackoff;
        use google_cloud_gax::retry_policy::NeverRetry;
        use std::time::Duration;

        let req = ReadRequest::builder("MyTable", vec!["col1"])
            .with_keys(KeySet::all())
            .with_attempt_timeout(Duration::from_secs(10))
            .with_retry_policy(NeverRetry)
            .with_backoff_policy(ExponentialBackoff::default())
            .build();

        assert_eq!(
            req.gax_options.attempt_timeout(),
            &Some(Duration::from_secs(10))
        );
        assert!(req.gax_options.retry_policy().is_some());
        assert!(req.gax_options.backoff_policy().is_some());

        Ok(())
    }
}
