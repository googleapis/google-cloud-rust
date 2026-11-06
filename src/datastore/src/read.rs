// Copyright 2025 Google LLC
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

//! Point read operations (`get` and `get_multi`) and read consistency options.

use crate::Result;
use crate::client::Datastore;
use crate::entity::{Entity, FromEntity};
use crate::key::Key;

/// Read consistency mode for non-transactional reads and queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadConsistency {
    /// Strongly consistent read (returns the latest committed state).
    Strong,
    /// Eventually consistent read (may return slightly stale data with lower latency).
    Eventual,
}

/// Options controlling read consistency or snapshot timestamp for reads and queries.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadOptions {
    pub(crate) consistency: Option<ReadConsistency>,
    pub(crate) read_time: Option<wkt::Timestamp>,
}

impl ReadOptions {
    /// Creates `ReadOptions` requesting strong read consistency.
    pub fn strong() -> Self {
        Self {
            consistency: Some(ReadConsistency::Strong),
            read_time: None,
        }
    }

    /// Creates `ReadOptions` requesting eventual read consistency.
    pub fn eventual() -> Self {
        Self {
            consistency: Some(ReadConsistency::Eventual),
            read_time: None,
        }
    }

    /// Creates `ReadOptions` reading from a historical snapshot at `read_time`.
    pub fn at_read_time(read_time: wkt::Timestamp) -> Self {
        Self {
            consistency: None,
            read_time: Some(read_time),
        }
    }

    /// Returns the configured [`ReadConsistency`], if any.
    pub fn consistency(&self) -> Option<ReadConsistency> {
        self.consistency
    }

    /// Returns the configured snapshot `read_time`, if any.
    pub fn read_time(&self) -> Option<&wkt::Timestamp> {
        self.read_time.as_ref()
    }
}

/// Request builder for [`Datastore::get`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct GetBuilder {
    pub(crate) client: Datastore,
    pub(crate) key: Key,
    pub(crate) read_options: ReadOptions,
    pub(crate) options: crate::RequestOptions,
}

impl GetBuilder {
    /// Configures the [`ReadConsistency`] for this lookup.
    pub fn with_consistency(mut self, consistency: ReadConsistency) -> Self {
        self.read_options = ReadOptions {
            consistency: Some(consistency),
            read_time: None,
        };
        self
    }

    /// Configures a historical snapshot `read_time` for this lookup.
    pub fn with_read_time(mut self, read_time: wkt::Timestamp) -> Self {
        self.read_options = ReadOptions::at_read_time(read_time);
        self
    }

    /// Configures the [`ReadOptions`] for this lookup.
    pub fn with_read_options(mut self, read_options: ReadOptions) -> Self {
        self.read_options = read_options;
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Executes the lookup and returns `Ok(Some(entity))` if found, or `Ok(None)` if missing.
    pub async fn send(self) -> Result<Option<Entity>> {
        unimplemented!()
    }

    /// Executes the lookup and deserializes the resulting entity into `T` via [`FromEntity`].
    pub async fn send_as<T: FromEntity>(self) -> Result<Option<T>> {
        unimplemented!()
    }
}

/// Request builder for [`Datastore::get_multi`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct GetMultiBuilder {
    pub(crate) client: Datastore,
    pub(crate) keys: Vec<Key>,
    pub(crate) read_options: ReadOptions,
    pub(crate) options: crate::RequestOptions,
}

impl GetMultiBuilder {
    /// Configures the [`ReadConsistency`] for this batch lookup.
    pub fn with_consistency(mut self, consistency: ReadConsistency) -> Self {
        self.read_options = ReadOptions {
            consistency: Some(consistency),
            read_time: None,
        };
        self
    }

    /// Configures a historical snapshot `read_time` for this batch lookup.
    pub fn with_read_time(mut self, read_time: wkt::Timestamp) -> Self {
        self.read_options = ReadOptions::at_read_time(read_time);
        self
    }

    /// Configures the [`ReadOptions`] for this batch lookup.
    pub fn with_read_options(mut self, read_options: ReadOptions) -> Self {
        self.read_options = read_options;
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Executes the batch lookup (including automatic `deferred` key retries) and returns
    /// results in the exact order of the input keys (`None` for missing keys).
    pub async fn send(self) -> Result<Vec<Option<Entity>>> {
        unimplemented!()
    }

    /// Executes the batch lookup and deserializes each found entity into `T` via [`FromEntity`].
    pub async fn send_as<T: FromEntity>(self) -> Result<Vec<Option<T>>> {
        unimplemented!()
    }
}
