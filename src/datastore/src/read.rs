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

use crate::client::Datastore;
use crate::entity::{Entity, FromEntity};
use crate::key::Key;
use crate::{Error, Result};
use bytes::Bytes;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::retry_state::RetryState;
use std::collections::HashMap;
use std::time::Instant;

const MAX_DEFERRED_RETRIES: u32 = 10;

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

    pub(crate) fn to_proto(&self) -> Option<crate::model::ReadOptions> {
        if let Some(read_time) = self.read_time {
            return Some(crate::model::ReadOptions::new().set_read_time(read_time));
        }
        match self.consistency {
            Some(ReadConsistency::Strong) => Some(
                crate::model::ReadOptions::new()
                    .set_read_consistency(crate::model::read_options::ReadConsistency::Strong),
            ),
            Some(ReadConsistency::Eventual) => Some(
                crate::model::ReadOptions::new()
                    .set_read_consistency(crate::model::read_options::ReadConsistency::Eventual),
            ),
            None => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum LookupReadMode {
    NonTransactional(ReadOptions),
    Transaction(Bytes),
    NewTransaction(crate::model::TransactionOptions),
}

impl LookupReadMode {
    fn to_proto(&self, started_tx: Option<&Bytes>) -> Option<crate::model::ReadOptions> {
        if let Some(tx_id) = started_tx {
            return Some(crate::model::ReadOptions::new().set_transaction(tx_id.clone()));
        }
        match self {
            Self::NonTransactional(opts) => opts.to_proto(),
            Self::Transaction(tx_id) => {
                Some(crate::model::ReadOptions::new().set_transaction(tx_id.clone()))
            }
            Self::NewTransaction(tx_opts) => {
                Some(crate::model::ReadOptions::new().set_new_transaction(tx_opts.clone()))
            }
        }
    }
}

pub(crate) async fn execute_lookup(
    client: &Datastore,
    keys: &[Key],
    read_mode: LookupReadMode,
    options: crate::RequestOptions,
) -> Result<(Vec<Option<Entity>>, Option<Bytes>)> {
    if keys.is_empty() {
        return Ok((Vec::new(), None));
    }

    let default_partition = client.partition_id();
    let mut key_to_indices: HashMap<Key, Vec<usize>> = HashMap::with_capacity(keys.len());
    let mut unique_proto_keys: Vec<crate::model::Key> = Vec::with_capacity(keys.len());

    for (idx, key) in keys.iter().enumerate() {
        let normalized = key
            .normalize(&default_partition)
            .map_err(|e| Error::ser(Box::new(e)))?;
        let indices = key_to_indices.entry(normalized.clone()).or_default();
        if indices.is_empty() {
            unique_proto_keys.push(normalized.to_proto());
        }
        indices.push(idx);
    }

    let mut results: Vec<Option<Entity>> = vec![None; keys.len()];
    let mut pending_keys = unique_proto_keys;
    let mut started_transaction: Option<Bytes> = None;
    let backoff = crate::retry_policy::default_exponential_backoff();
    let loop_start = Instant::now();
    let mut attempt: u32 = 0;

    loop {
        let mut req = client
            .inner
            .gapic
            .lookup()
            .set_project_id(client.project_id())
            .set_database_id(client.database_id())
            .set_keys(pending_keys)
            .with_options(options.clone());

        if let Some(ro) = read_mode.to_proto(started_transaction.as_ref()) {
            req = req.set_read_options(ro);
        }

        let response = req.send().await?;
        if !response.transaction.is_empty() {
            started_transaction = Some(response.transaction.clone());
        }

        for entity_result in response.found {
            let Some(proto_entity) = entity_result.entity else {
                continue;
            };
            let mut entity =
                Entity::from_proto(proto_entity).map_err(|e| Error::deser(Box::new(e)))?;
            if entity_result.version != 0 {
                entity.version = Some(entity_result.version);
            }
            if let Some(found_key) = entity.key() {
                let normalized_found = found_key
                    .normalize(&default_partition)
                    .unwrap_or_else(|_| found_key.clone());
                if let Some(indices) = key_to_indices.get(&normalized_found) {
                    for &idx in indices {
                        results[idx] = Some(entity.clone());
                    }
                }
            }
        }

        if response.deferred.is_empty() {
            return Ok((results, started_transaction));
        }

        attempt += 1;
        if attempt > MAX_DEFERRED_RETRIES {
            return Err(Error::deser(format!(
                "Lookup response still contained {} deferred key(s) after {MAX_DEFERRED_RETRIES} retries",
                response.deferred.len()
            )));
        }

        let delay = backoff.on_failure(
            &RetryState::new(true)
                .set_start(loop_start)
                .set_attempt_count(attempt),
        );
        tokio::time::sleep(delay).await;
        pending_keys = response.deferred;
    }
}

/// Request builder for [`Datastore::get`].
#[derive(Clone, Debug)]
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
        let (mut results, _) = execute_lookup(
            &self.client,
            std::slice::from_ref(&self.key),
            LookupReadMode::NonTransactional(self.read_options),
            self.options,
        )
        .await?;
        Ok(results.pop().flatten())
    }

    /// Executes the lookup and deserializes the resulting entity into `T` via [`FromEntity`].
    pub async fn send_as<T: FromEntity>(self) -> Result<Option<T>> {
        match self.send().await? {
            Some(entity) => T::from_entity(&entity)
                .map(Some)
                .map_err(|e| Error::deser(Box::new(e))),
            None => Ok(None),
        }
    }
}

/// Request builder for [`Datastore::get_multi`].
#[derive(Clone, Debug)]
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
        let (results, _) = execute_lookup(
            &self.client,
            &self.keys,
            LookupReadMode::NonTransactional(self.read_options),
            self.options,
        )
        .await?;
        Ok(results)
    }

    /// Executes the batch lookup and deserializes each found entity into `T` via [`FromEntity`].
    pub async fn send_as<T: FromEntity>(self) -> Result<Vec<Option<T>>> {
        let entities = self.send().await?;
        entities
            .into_iter()
            .map(|opt| match opt {
                Some(entity) => T::from_entity(&entity)
                    .map(Some)
                    .map_err(|e| Error::deser(Box::new(e))),
                None => Ok(None),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::gapic_dataplane::stub::Datastore as DatastoreStub;
    use crate::key::PartitionId;
    use crate::model::{EntityResult, LookupRequest, LookupResponse};
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response;

    mockall::mock! {
        #[derive(Debug)]
        DatastoreMock {}
        impl DatastoreStub for DatastoreMock {
            async fn lookup(
                &self,
                req: LookupRequest,
                options: RequestOptions,
            ) -> Result<Response<LookupResponse>>;
        }
    }

    fn test_client(mock: MockDatastoreMock) -> Datastore {
        Datastore::from_stub("test-proj", "test-db", "test-ns", mock)
    }

    fn make_entity_result(key: &Key, name: &str, version: i64) -> EntityResult {
        let mut entity = Entity::with_key(key.clone());
        entity.set("name", name);
        EntityResult::new()
            .set_entity(entity.to_proto(key.partition_id()).expect("valid entity"))
            .set_version(version)
    }

    #[tokio::test]
    async fn get_multi_empty_short_circuits() {
        let mut mock = MockDatastoreMock::new();
        mock.expect_lookup().never();
        let client = test_client(mock);

        let results = client
            .get_multi(Vec::<Key>::new())
            .send()
            .await
            .expect("empty lookup succeeds");
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn get_single_found_and_missing_with_options() {
        let mut mock = MockDatastoreMock::new();
        let read_time = wkt::Timestamp::clamp(1_700_000_000, 0);

        mock.expect_lookup()
            .times(1)
            .withf(move |req, _| {
                req.project_id == "test-proj"
                    && req.database_id == "test-db"
                    && req.keys.len() == 1
                    && req.read_options.as_ref().and_then(|ro| ro.read_time())
                        == Some(&Box::new(read_time))
            })
            .returning(|req, _| {
                let key = Key::from_proto(req.keys[0].clone()).expect("valid key");
                let resp = LookupResponse::new().set_found([make_entity_result(&key, "Ada", 42)]);
                Ok(Response::from(resp))
            });

        mock.expect_lookup()
            .times(1)
            .withf(|req, _| {
                req.read_options
                    .as_ref()
                    .and_then(|ro| ro.read_consistency())
                    == Some(&crate::model::read_options::ReadConsistency::Eventual)
            })
            .returning(|_, _| Ok(Response::from(LookupResponse::new())));

        let client = test_client(mock);
        let key = client
            .key_factory("User")
            .with_name("ada")
            .expect("valid key");

        let found = client
            .get(key.clone())
            .with_read_time(read_time)
            .send()
            .await
            .expect("get succeeds")
            .expect("entity found");
        assert_eq!(found.get::<String>("name").expect("name"), "Ada");
        assert_eq!(found.version(), Some(42));

        let missing = client
            .get(key)
            .with_consistency(ReadConsistency::Eventual)
            .send()
            .await
            .expect("get succeeds");
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn get_multi_deduplicates_keys_preserves_order_and_retries_deferred() {
        let mut mock = MockDatastoreMock::new();

        // Input order: [k2, k1, k2, k_missing, k3]
        // Unique keys sent on round 1: [k2, k1, k_missing, k3] (4 keys, not 5).
        // Round 1 returns found=[k1], missing=[k_missing], deferred=[k2, k3].
        mock.expect_lookup()
            .times(1)
            .withf(|req, _| req.keys.len() == 4)
            .returning(|req, _| {
                let mut found = Vec::new();
                let mut deferred = Vec::new();
                for proto_key in req.keys {
                    let key = Key::from_proto(proto_key.clone()).expect("valid key");
                    match key.id().as_name() {
                        Some("k1") => {
                            found.push(make_entity_result(&key, "Entity 1", 10));
                        }
                        Some("k2") | Some("k3") => {
                            deferred.push(proto_key);
                        }
                        _ => {}
                    }
                }
                Ok(Response::from(
                    LookupResponse::new()
                        .set_found(found)
                        .set_deferred(deferred),
                ))
            });

        // Round 2 receives only the 2 deferred keys [k2, k3] and resolves them both.
        mock.expect_lookup()
            .times(1)
            .withf(|req, _| req.keys.len() == 2)
            .returning(|req, _| {
                let mut found = Vec::new();
                for proto_key in req.keys {
                    let key = Key::from_proto(proto_key).expect("valid key");
                    match key.id().as_name() {
                        Some("k2") => {
                            found.push(make_entity_result(&key, "Entity 2", 20));
                        }
                        Some("k3") => {
                            found.push(make_entity_result(&key, "Entity 3", 30));
                        }
                        _ => {}
                    }
                }
                Ok(Response::from(LookupResponse::new().set_found(found)))
            });

        let client = test_client(mock);
        let kf = client.key_factory("Task");
        let k1 = kf.with_name("k1").expect("valid key");
        let k2 = kf.with_name("k2").expect("valid key");
        let k3 = kf.with_name("k3").expect("valid key");
        let k_missing = kf.with_name("missing").expect("valid key");

        let results = client
            .get_multi([k2.clone(), k1, k2, k_missing, k3])
            .send()
            .await
            .expect("get_multi succeeds");

        assert_eq!(results.len(), 5);
        assert_eq!(
            results[0]
                .as_ref()
                .and_then(|e| e.get::<String>("name").ok())
                .as_deref(),
            Some("Entity 2")
        );
        assert_eq!(
            results[1]
                .as_ref()
                .and_then(|e| e.get::<String>("name").ok())
                .as_deref(),
            Some("Entity 1")
        );
        assert_eq!(
            results[2]
                .as_ref()
                .and_then(|e| e.get::<String>("name").ok())
                .as_deref(),
            Some("Entity 2")
        );
        assert!(results[3].is_none());
        assert_eq!(
            results[4]
                .as_ref()
                .and_then(|e| e.get::<String>("name").ok())
                .as_deref(),
            Some("Entity 3")
        );
    }

    #[tokio::test]
    async fn get_rejects_partition_mismatch() {
        let mut mock = MockDatastoreMock::new();
        mock.expect_lookup().never();
        let client = test_client(mock);

        let wrong_key = Key::with_name("Task", "k1")
            .expect("valid key")
            .with_partition_id(PartitionId::new("other-proj"));
        let err = client
            .get(wrong_key)
            .send()
            .await
            .expect_err("mismatched partition should fail");
        assert!(err.is_serialization());
    }
}
