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

use crate::value::Value;

/// Represents a primary key or index key for Cloud Spanner.
///
/// # Example
/// ```rust
/// use google_cloud_spanner::key;
///
/// let key = key![1_i64, "Alice"];
/// ```
///
/// Keys are constructed by appending values in the correct column order.
#[macro_export]
macro_rules! key {
    ($($val:expr),* $(,)?) => {
        $crate::client::Key::new(vec![
            $($crate::client::ToValue::to_value(&$val)),*
        ])
    };
}

#[derive(Clone, Debug, PartialEq)]
pub struct Key {
    pub(crate) values: Vec<Value>,
}

impl Key {
    /// Creates a new Key from a vector of values.
    pub fn new(values: Vec<Value>) -> Self {
        Key { values }
    }
}

impl Default for Key {
    fn default() -> Self {
        Self::new(vec![])
    }
}

impl From<Key> for Vec<serde_json::Value> {
    fn from(key: Key) -> Self {
        key.values
            .into_iter()
            .map(|v| v.into_serde_value())
            .collect()
    }
}

/// Defines whether a boundary of a key range is open (exclusive) or closed (inclusive).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Endpoint {
    Closed,
    Open,
}

/// Represents a contiguous range of keys.
///
/// # Example
/// ```rust
/// use google_cloud_spanner::client::KeyRange;
/// use google_cloud_spanner::key;
///
/// let range = KeyRange::closed_open(key![1_i64], key![10_i64]);
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct KeyRange {
    pub(crate) start: Key,
    pub(crate) start_endpoint: Endpoint,
    pub(crate) end: Key,
    pub(crate) end_endpoint: Endpoint,
}

impl KeyRange {
    /// Constructs a closed-open `KeyRange`.
    ///
    /// # Example
    /// ```rust
    /// use google_cloud_spanner::client::KeyRange;
    /// use google_cloud_spanner::key;
    ///
    /// // Creates a key for the range [1, 10)
    /// let range = KeyRange::closed_open(key![1_i64], key![10_i64]);
    /// ```
    ///
    /// Creates a key for the range [start, end).
    pub fn closed_open(start: Key, end: Key) -> Self {
        KeyRange {
            start,
            start_endpoint: Endpoint::Closed,
            end,
            end_endpoint: Endpoint::Open,
        }
    }

    /// Constructs a closed-closed `KeyRange`.
    ///
    /// # Example
    /// ```rust
    /// use google_cloud_spanner::client::KeyRange;
    /// use google_cloud_spanner::key;
    ///
    /// // Creates a key for the range [1, 10]
    /// let range = KeyRange::closed_closed(key![1_i64], key![10_i64]);
    /// ```
    ///
    /// Creates a key for the range [start, end].
    pub fn closed_closed(start: Key, end: Key) -> Self {
        KeyRange {
            start,
            start_endpoint: Endpoint::Closed,
            end,
            end_endpoint: Endpoint::Closed,
        }
    }

    /// Constructs an open-closed `KeyRange`.
    ///
    /// # Example
    /// ```rust
    /// use google_cloud_spanner::client::KeyRange;
    /// use google_cloud_spanner::key;
    ///
    /// // Creates a key for the range (1, 10]
    /// let range = KeyRange::open_closed(key![1_i64], key![10_i64]);
    /// ```
    ///
    /// Creates a key for the range (start, end].
    pub fn open_closed(start: Key, end: Key) -> Self {
        KeyRange {
            start,
            start_endpoint: Endpoint::Open,
            end,
            end_endpoint: Endpoint::Closed,
        }
    }

    /// Constructs an open-open `KeyRange`.
    ///
    /// # Example
    /// ```rust
    /// use google_cloud_spanner::client::KeyRange;
    /// use google_cloud_spanner::key;
    ///
    /// // Creates a key for the range (1, 10)
    /// let range = KeyRange::open_open(key![1_i64], key![10_i64]);
    /// ```
    ///
    /// Creates a key for the range (start, end).
    pub fn open_open(start: Key, end: Key) -> Self {
        KeyRange {
            start,
            start_endpoint: Endpoint::Open,
            end,
            end_endpoint: Endpoint::Open,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_proto(self) -> crate::model::KeyRange {
        let mut proto = crate::model::KeyRange::new();

        proto = match self.start_endpoint {
            Endpoint::Closed => {
                proto.set_start_closed(Into::<Vec<serde_json::Value>>::into(self.start))
            }
            Endpoint::Open => {
                proto.set_start_open(Into::<Vec<serde_json::Value>>::into(self.start))
            }
        };

        match self.end_endpoint {
            Endpoint::Closed => {
                proto.set_end_closed(Into::<Vec<serde_json::Value>>::into(self.end))
            }
            Endpoint::Open => proto.set_end_open(Into::<Vec<serde_json::Value>>::into(self.end)),
        }
    }
}

/// A collection of Spanner keys and key ranges.
///
/// # Example
/// ```rust
/// use google_cloud_spanner::client::{KeySet, KeyRange};
/// use google_cloud_spanner::key;
///
/// let keyset = KeySet::builder()
///     .add_key(key![1_i64])
///     .add_range(KeyRange::closed_open(key![10_i64], key![100_i64]))
///     .build();
/// ```
///
/// Defines a collection of Cloud Spanner keys and/or key ranges. All the keys are expected to be in
/// the same table or index. The keys need not be sorted in any particular way.
///
/// `KeySet`s are used for delete mutations and reads.
///
/// If the same key is specified multiple times in the set (for example if two ranges, two keys,
/// or a key and a range overlap), the Cloud Spanner backend behaves as if the key were only
/// specified once. `KeySet` instances are immutable.
#[derive(Clone, Debug, PartialEq)]
pub struct KeySet {
    pub(crate) keys: Vec<Key>,
    pub(crate) ranges: Vec<KeyRange>,
    pub(crate) all: bool,
}

impl KeySet {
    /// Creates a builder for `KeySet`.
    pub fn builder() -> KeySetBuilder {
        KeySetBuilder::new()
    }

    /// Creates a `KeySet` that matches all rows.
    pub fn all() -> Self {
        KeySet {
            keys: vec![],
            ranges: vec![],
            all: true,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_proto(self) -> crate::model::KeySet {
        let mut proto = crate::model::KeySet::new();
        if self.all {
            proto = proto.set_all(true);
        }

        // Convert keys
        let keys_proto: Vec<Vec<serde_json::Value>> =
            self.keys.into_iter().map(|k| k.into()).collect();
        if !keys_proto.is_empty() {
            proto = proto.set_keys(keys_proto);
        }

        // Convert ranges
        let ranges_proto: Vec<crate::model::KeyRange> =
            self.ranges.into_iter().map(|r| r.into_proto()).collect();
        if !ranges_proto.is_empty() {
            proto = proto.set_ranges(ranges_proto);
        }

        proto
    }
}

/// A builder for constructing a `KeySet`.
#[derive(Clone, Debug)]
pub struct KeySetBuilder {
    keys: Vec<Key>,
    ranges: Vec<KeyRange>,
}

impl KeySetBuilder {
    /// Creates a new, empty builder.
    pub fn new() -> Self {
        KeySetBuilder {
            keys: vec![],
            ranges: vec![],
        }
    }

    /// Adds a key to the key set.
    pub fn add_key(mut self, key: Key) -> Self {
        self.keys.push(key);
        self
    }

    /// Adds a range to the key set.
    pub fn add_range(mut self, range: KeyRange) -> Self {
        self.ranges.push(range);
        self
    }

    /// Builds the `KeySet`.
    pub fn build(self) -> KeySet {
        KeySet {
            keys: self.keys,
            ranges: self.ranges,
            all: false,
        }
    }
}

impl Default for KeySetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for KeySet {
    fn default() -> Self {
        KeySet::builder().build()
    }
}

impl From<Key> for KeySet {
    fn from(key: Key) -> Self {
        KeySet::builder().add_key(key).build()
    }
}

impl From<KeyRange> for KeySet {
    fn from(range: KeyRange) -> Self {
        KeySet::builder().add_range(range).build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(Key: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(KeyRange: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(KeySet: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(KeySetBuilder: Send, Sync, Clone, std::fmt::Debug);
    }

    #[test]
    fn test_key_builder() {
        let key = key![1_i64, "test"];
        assert_eq!(key.values.len(), 2);
    }

    #[test]
    fn test_key_default() {
        let key = Key::default();
        assert_eq!(key.values.len(), 0);
    }

    #[test]
    fn test_key_from_into() {
        let key = key![1_i64];
        let values: Vec<serde_json::Value> = key.into();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], serde_json::json!("1"));
    }

    #[test]
    fn test_keyrange_factories() {
        let start = key![1_i64];
        let end = key![10_i64];

        let r1 = KeyRange::closed_open(start.clone(), end.clone());
        assert_eq!(r1.start_endpoint, Endpoint::Closed);
        assert_eq!(r1.end_endpoint, Endpoint::Open);

        let r2 = KeyRange::closed_closed(start.clone(), end.clone());
        assert_eq!(r2.start_endpoint, Endpoint::Closed);
        assert_eq!(r2.end_endpoint, Endpoint::Closed);

        let r3 = KeyRange::open_closed(start.clone(), end.clone());
        assert_eq!(r3.start_endpoint, Endpoint::Open);
        assert_eq!(r3.end_endpoint, Endpoint::Closed);

        let r4 = KeyRange::open_open(start.clone(), end.clone());
        assert_eq!(r4.start_endpoint, Endpoint::Open);
        assert_eq!(r4.end_endpoint, Endpoint::Open);
    }

    #[test]
    fn test_keyrange_into_proto() {
        let r1 = KeyRange::closed_open(key![1_i64], key![10_i64]);
        let proto1 = r1.into_proto();
        assert_eq!(proto1.start_closed().unwrap().len(), 1);
        assert_eq!(proto1.end_open().unwrap().len(), 1);

        let r2 = KeyRange::open_closed(key![1_i64], key![10_i64]);
        let proto2 = r2.into_proto();
        assert_eq!(proto2.start_open().unwrap().len(), 1);
        assert_eq!(proto2.end_closed().unwrap().len(), 1);
    }

    #[test]
    fn test_keyset_builder() {
        let key = key![1_i64];
        let range = KeyRange::closed_open(key![2_i64], key![5_i64]);
        let keyset = KeySet::builder().add_key(key).add_range(range).build();

        assert_eq!(keyset.keys.len(), 1);
        assert_eq!(keyset.ranges.len(), 1);
        assert!(!keyset.all);
    }

    #[test]
    fn test_keyset_builder_default() {
        let builder = KeySetBuilder::default();
        let keyset = builder.build();
        assert_eq!(keyset.keys.len(), 0);
        assert_eq!(keyset.ranges.len(), 0);
    }

    #[test]
    fn test_keyset_default() {
        let keyset = KeySet::default();
        assert_eq!(keyset.keys.len(), 0);
        assert_eq!(keyset.ranges.len(), 0);
        assert!(!keyset.all);
    }

    #[test]
    fn test_keyset_from_key() {
        let keyset: KeySet = key![1_i64].into();
        assert_eq!(keyset.keys.len(), 1);
        assert_eq!(keyset.ranges.len(), 0);
    }

    #[test]
    fn test_keyset_from_keyrange() {
        let range = KeyRange::closed_open(key![2_i64], key![5_i64]);
        let keyset: KeySet = range.into();
        assert_eq!(keyset.keys.len(), 0);
        assert_eq!(keyset.ranges.len(), 1);
    }

    #[test]
    fn test_keyset_into_proto() {
        let keyset = KeySet::all();
        let proto = keyset.into_proto();
        assert!(proto.all);

        let keyset2 = KeySet::builder()
            .add_key(key![1_i64])
            .add_range(KeyRange::closed_open(key![2_i64], key![5_i64]))
            .build();
        let proto2 = keyset2.into_proto();
        assert_eq!(proto2.keys.len(), 1);
        assert_eq!(proto2.ranges.len(), 1);
    }
}
