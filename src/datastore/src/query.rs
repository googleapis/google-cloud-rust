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

//! Structured queries (`Query`), filters (`Filter`), cursors (`Cursor`), and multi-batch iteration (`QueryStream`).

use crate::client::Datastore;
use crate::entity::{Entity, FromEntity};
use crate::error::{KeyError, ValueError};
use crate::explain::{ExplainMetrics, ExplainOptions};
use crate::key::{Key, PartitionId};
use crate::read::{LookupReadMode, ReadConsistency, ReadOptions};
use crate::value::{IntoValue, Value, ValueKind};
use crate::{Error, Result};
use bytes::Bytes;
use std::collections::VecDeque;

/// Comparison operator for a [`PropertyFilter`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Operator {
    /// Strict less-than (`<`).
    LessThan,
    /// Less-than-or-equal (`<=`).
    LessThanOrEqual,
    /// Strict greater-than (`>`).
    GreaterThan,
    /// Greater-than-or-equal (`>=`).
    GreaterThanOrEqual,
    /// Equality (`=`).
    Equal,
    /// Set membership (`IN`).
    In,
    /// Inequality (`!=`).
    NotEqual,
    /// Ancestor hierarchy match (`HAS_ANCESTOR`).
    HasAncestor,
    /// Set non-membership (`NOT_IN`).
    NotIn,
}

impl Operator {
    pub(crate) fn to_proto(self) -> crate::model::property_filter::Operator {
        use crate::model::property_filter::Operator as ProtoOp;
        match self {
            Self::LessThan => ProtoOp::LessThan,
            Self::LessThanOrEqual => ProtoOp::LessThanOrEqual,
            Self::GreaterThan => ProtoOp::GreaterThan,
            Self::GreaterThanOrEqual => ProtoOp::GreaterThanOrEqual,
            Self::Equal => ProtoOp::Equal,
            Self::In => ProtoOp::In,
            Self::NotEqual => ProtoOp::NotEqual,
            Self::HasAncestor => ProtoOp::HasAncestor,
            Self::NotIn => ProtoOp::NotIn,
        }
    }
}

/// A filter comparing a single entity property against a [`Value`].
#[derive(Clone, Debug, PartialEq)]
pub struct PropertyFilter {
    pub(crate) property: String,
    pub(crate) operator: Operator,
    pub(crate) value: Value,
}

impl PropertyFilter {
    /// Creates a new [`PropertyFilter`].
    pub fn new(property: impl Into<String>, operator: Operator, value: impl IntoValue) -> Self {
        Self {
            property: property.into(),
            operator,
            value: value.into_value(),
        }
    }

    /// Returns the target property name.
    pub fn property(&self) -> &str {
        &self.property
    }

    /// Returns the comparison [`Operator`].
    pub fn operator(&self) -> Operator {
        self.operator
    }

    /// Returns the comparison [`Value`].
    pub fn value(&self) -> &Value {
        &self.value
    }

    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> std::result::Result<crate::model::PropertyFilter, ValueError> {
        Ok(crate::model::PropertyFilter::new()
            .set_property(crate::model::PropertyReference::new().set_name(self.property.clone()))
            .set_op(self.operator.to_proto())
            .set_value(self.value.to_proto(default_partition)?))
    }
}

/// Logical operator combining multiple sub-filters in a [`CompositeFilter`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompositeOperator {
    /// Logical conjunction (`AND`).
    And,
    /// Logical disjunction (`OR`).
    Or,
}

impl CompositeOperator {
    pub(crate) fn to_proto(self) -> crate::model::composite_filter::Operator {
        use crate::model::composite_filter::Operator as ProtoOp;
        match self {
            Self::And => ProtoOp::And,
            Self::Or => ProtoOp::Or,
        }
    }
}

/// A composite filter combining multiple [`Filter`]s with [`CompositeOperator::And`] or [`CompositeOperator::Or`].
#[derive(Clone, Debug, PartialEq)]
pub struct CompositeFilter {
    pub(crate) operator: CompositeOperator,
    pub(crate) filters: Vec<Filter>,
}

impl CompositeFilter {
    /// Combines `filters` with [`CompositeOperator::And`].
    pub fn and<I>(filters: I) -> Self
    where
        I: IntoIterator<Item = Filter>,
    {
        Self {
            operator: CompositeOperator::And,
            filters: filters.into_iter().collect(),
        }
    }

    /// Combines `filters` with [`CompositeOperator::Or`].
    pub fn or<I>(filters: I) -> Self
    where
        I: IntoIterator<Item = Filter>,
    {
        Self {
            operator: CompositeOperator::Or,
            filters: filters.into_iter().collect(),
        }
    }

    /// Returns the [`CompositeOperator`].
    pub fn operator(&self) -> CompositeOperator {
        self.operator
    }

    /// Returns the sub-filters in this composite filter.
    pub fn filters(&self) -> &[Filter] {
        &self.filters
    }

    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> std::result::Result<crate::model::CompositeFilter, ValueError> {
        let mut proto_filters = Vec::with_capacity(self.filters.len());
        for f in &self.filters {
            proto_filters.push(f.to_proto(default_partition)?);
        }
        Ok(crate::model::CompositeFilter::new()
            .set_op(self.operator.to_proto())
            .set_filters(proto_filters))
    }
}

/// A query filter tree (either a [`PropertyFilter`] or a [`CompositeFilter`]).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Filter {
    /// A single property filter.
    Property(PropertyFilter),
    /// A composite (`AND` / `OR`) filter.
    Composite(CompositeFilter),
}

impl Filter {
    /// Creates an equality (`=`) filter on `property`.
    pub fn equal(property: impl Into<String>, value: impl IntoValue) -> Self {
        Self::Property(PropertyFilter::new(property, Operator::Equal, value))
    }

    /// Creates a strict less-than (`<`) filter on `property`.
    pub fn less_than(property: impl Into<String>, value: impl IntoValue) -> Self {
        Self::Property(PropertyFilter::new(property, Operator::LessThan, value))
    }

    /// Creates a less-than-or-equal (`<=`) filter on `property`.
    pub fn less_than_or_equal(property: impl Into<String>, value: impl IntoValue) -> Self {
        Self::Property(PropertyFilter::new(
            property,
            Operator::LessThanOrEqual,
            value,
        ))
    }

    /// Creates a strict greater-than (`>`) filter on `property`.
    pub fn greater_than(property: impl Into<String>, value: impl IntoValue) -> Self {
        Self::Property(PropertyFilter::new(property, Operator::GreaterThan, value))
    }

    /// Creates a greater-than-or-equal (`>=`) filter on `property`.
    pub fn greater_than_or_equal(property: impl Into<String>, value: impl IntoValue) -> Self {
        Self::Property(PropertyFilter::new(
            property,
            Operator::GreaterThanOrEqual,
            value,
        ))
    }

    /// Creates an inequality (`!=`) filter on `property`.
    pub fn not_equal(property: impl Into<String>, value: impl IntoValue) -> Self {
        Self::Property(PropertyFilter::new(property, Operator::NotEqual, value))
    }

    /// Creates an `IN` filter matching `property` against any element in `values`.
    pub fn in_values<I, V>(property: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: IntoValue,
    {
        let arr: Vec<Value> = values.into_iter().map(IntoValue::into_value).collect();
        Self::Property(PropertyFilter::new(
            property,
            Operator::In,
            Value::new(ValueKind::Array(arr)),
        ))
    }

    /// Creates a `NOT_IN` filter excluding entities where `property` matches any element in `values`.
    pub fn not_in_values<I, V>(property: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: IntoValue,
    {
        let arr: Vec<Value> = values.into_iter().map(IntoValue::into_value).collect();
        Self::Property(PropertyFilter::new(
            property,
            Operator::NotIn,
            Value::new(ValueKind::Array(arr)),
        ))
    }

    /// Creates an ancestor (`HAS_ANCESTOR`) filter matching entities descended from `ancestor`.
    pub fn has_ancestor(ancestor: Key) -> Self {
        Self::Property(PropertyFilter::new(
            "__key__",
            Operator::HasAncestor,
            Value::new(ValueKind::Key(ancestor)),
        ))
    }

    /// Combines multiple filters with a logical `AND`.
    pub fn and<I>(filters: I) -> Self
    where
        I: IntoIterator<Item = Filter>,
    {
        Self::Composite(CompositeFilter::and(filters))
    }

    /// Combines multiple filters with a logical `OR`.
    pub fn or<I>(filters: I) -> Self
    where
        I: IntoIterator<Item = Filter>,
    {
        Self::Composite(CompositeFilter::or(filters))
    }

    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> std::result::Result<crate::model::Filter, ValueError> {
        match self {
            Self::Property(pf) => Ok(
                crate::model::Filter::new().set_property_filter(pf.to_proto(default_partition)?)
            ),
            Self::Composite(cf) => {
                Ok(crate::model::Filter::new()
                    .set_composite_filter(cf.to_proto(default_partition)?))
            }
        }
    }
}

/// Sort direction for a property [`Order`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Direction {
    /// Ascending sort order.
    Ascending,
    /// Descending sort order.
    Descending,
}

impl Direction {
    pub(crate) fn to_proto(self) -> crate::model::property_order::Direction {
        use crate::model::property_order::Direction as ProtoDir;
        match self {
            Self::Ascending => ProtoDir::Ascending,
            Self::Descending => ProtoDir::Descending,
        }
    }
}

/// A sort order specification on a single property.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Order {
    pub(crate) property: String,
    pub(crate) direction: Direction,
}

impl Order {
    /// Creates an ascending sort order on `property`.
    pub fn ascending(property: impl Into<String>) -> Self {
        Self {
            property: property.into(),
            direction: Direction::Ascending,
        }
    }

    /// Creates a descending sort order on `property`.
    pub fn descending(property: impl Into<String>) -> Self {
        Self {
            property: property.into(),
            direction: Direction::Descending,
        }
    }

    /// Returns the property name.
    pub fn property(&self) -> &str {
        &self.property
    }

    /// Returns the sort [`Direction`].
    pub fn direction(&self) -> Direction {
        self.direction
    }

    pub(crate) fn to_proto(&self) -> crate::model::PropertyOrder {
        crate::model::PropertyOrder::new()
            .set_property(crate::model::PropertyReference::new().set_name(self.property.clone()))
            .set_direction(self.direction.to_proto())
    }
}

/// An opaque pagination cursor returned by a Datastore query.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Cursor(pub(crate) Bytes);

impl Cursor {
    /// Wraps raw cursor bytes into a [`Cursor`].
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        Self(bytes.into())
    }

    /// Returns the raw cursor bytes as a slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the [`Cursor`] and returns the underlying [`Bytes`].
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// Encodes this cursor as an unpadded URL-safe Base64 string.
    pub fn to_url_safe(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&self.0)
    }

    /// Decodes a [`Cursor`] from an unpadded (or padded) URL-safe Base64 string.
    pub fn from_url_safe(encoded: &str) -> std::result::Result<Self, KeyError> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(encoded))
            .map_err(|e| KeyError::InvalidUrlSafeEncoding(e.to_string()))?;
        Ok(Self(Bytes::from(bytes)))
    }
}

/// Terminal state of a query after iteration completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BatchMoreResults {
    /// Additional batches exist on the server.
    NotFinished,
    /// The query stopped because the `limit` clause was reached; more matching entities exist.
    MoreResultsAfterLimit,
    /// The query stopped because the `end_cursor` was reached; more matching entities exist.
    MoreResultsAfterCursor,
    /// All matching entities have been returned.
    NoMoreResults,
}

impl BatchMoreResults {
    pub(crate) fn from_proto(
        proto: &crate::model::query_result_batch::MoreResultsType,
    ) -> Option<Self> {
        use crate::model::query_result_batch::MoreResultsType as P;
        match proto {
            P::NotFinished => Some(Self::NotFinished),
            P::MoreResultsAfterLimit => Some(Self::MoreResultsAfterLimit),
            P::MoreResultsAfterCursor => Some(Self::MoreResultsAfterCursor),
            P::NoMoreResults => Some(Self::NoMoreResults),
            _ => None,
        }
    }
}

/// An entity yielded by a [`QueryStream`] together with its per-item cursor and version metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityItem {
    /// The returned [`Entity`].
    pub entity: Entity,
    /// The per-item pagination [`Cursor`], if returned by the server.
    pub cursor: Option<Cursor>,
    /// The entity version number on the server.
    pub version: i64,
    /// The entity creation timestamp, if present.
    pub create_time: Option<wkt::Timestamp>,
    /// The entity last-update timestamp, if present.
    pub update_time: Option<wkt::Timestamp>,
}

/// A structured Cloud Datastore query (`google.datastore.v1.Query`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Query {
    pub(crate) kind: Option<String>,
    pub(crate) namespace_id: Option<String>,
    pub(crate) filter: Option<Filter>,
    pub(crate) order: Vec<Order>,
    pub(crate) projection: Vec<String>,
    pub(crate) distinct_on: Vec<String>,
    pub(crate) start_cursor: Option<Cursor>,
    pub(crate) end_cursor: Option<Cursor>,
    pub(crate) offset: i32,
    pub(crate) limit: Option<i32>,
}

impl Query {
    /// Creates a new [`Query`] over entities of the given `kind`.
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: Some(kind.into()),
            ..Self::default()
        }
    }

    /// Creates a kindless [`Query`] matching entities across all kinds.
    pub fn kindless() -> Self {
        Self::default()
    }

    /// Overrides the namespace ID for this query (defaults to the client's `namespace_id`).
    pub fn with_namespace_id(mut self, namespace_id: impl Into<String>) -> Self {
        self.namespace_id = Some(namespace_id.into());
        self
    }

    /// Sets the [`Filter`] for this query. If a filter is already set, combines them with `AND`.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(match self.filter.take() {
            Some(existing) => Filter::and([existing, filter]),
            None => filter,
        });
        self
    }

    /// Adds an ascending sort order on `property`.
    pub fn order_by_ascending(mut self, property: impl Into<String>) -> Self {
        self.order.push(Order::ascending(property));
        self
    }

    /// Adds a descending sort order on `property`.
    pub fn order_by_descending(mut self, property: impl Into<String>) -> Self {
        self.order.push(Order::descending(property));
        self
    }

    /// Adds a custom [`Order`] clause to this query.
    pub fn order_by(mut self, order: Order) -> Self {
        self.order.push(order);
        self
    }

    /// Configures a projection query returning only the specified property names.
    pub fn select<I, S>(mut self, properties: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = properties.into_iter().map(Into::into).collect();
        self
    }

    /// Configures a keys-only query (`SELECT __key__`), which avoids reading entity properties.
    pub fn keys_only(mut self) -> Self {
        self.projection = vec!["__key__".to_string()];
        self
    }

    /// Deduplicates results by the given `properties` (`DISTINCT ON`).
    pub fn distinct_on<I, S>(mut self, properties: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.distinct_on = properties.into_iter().map(Into::into).collect();
        self
    }

    /// Resumes the query after `cursor`.
    pub fn start_cursor(mut self, cursor: Cursor) -> Self {
        self.start_cursor = Some(cursor);
        self
    }

    /// Stops the query at `cursor`.
    pub fn end_cursor(mut self, cursor: Cursor) -> Self {
        self.end_cursor = Some(cursor);
        self
    }

    /// Skips the first `offset` matching entities before returning results.
    pub fn offset(mut self, offset: i32) -> Self {
        self.offset = offset;
        self
    }

    /// Caps the maximum number of entities returned by the query.
    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> std::result::Result<crate::model::Query, ValueError> {
        let mut proto = crate::model::Query::new().set_offset(self.offset);
        if let Some(ref kind) = self.kind {
            proto = proto.set_kind([crate::model::KindExpression::new().set_name(kind.clone())]);
        }
        if let Some(ref filter) = self.filter {
            proto = proto.set_filter(filter.to_proto(default_partition)?);
        }
        if !self.order.is_empty() {
            proto = proto.set_order(self.order.iter().map(Order::to_proto));
        }
        if !self.projection.is_empty() {
            proto = proto.set_projection(self.projection.iter().map(|p| {
                crate::model::Projection::new()
                    .set_property(crate::model::PropertyReference::new().set_name(p.clone()))
            }));
        }
        if !self.distinct_on.is_empty() {
            proto = proto.set_distinct_on(
                self.distinct_on
                    .iter()
                    .map(|p| crate::model::PropertyReference::new().set_name(p.clone())),
            );
        }
        if let Some(ref start_cursor) = self.start_cursor {
            proto = proto.set_start_cursor(start_cursor.0.clone());
        }
        if let Some(ref end_cursor) = self.end_cursor {
            proto = proto.set_end_cursor(end_cursor.0.clone());
        }
        if let Some(limit) = self.limit {
            proto = proto.set_limit(limit);
        }
        Ok(proto)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum InitialQuerySpec {
    Structured(Box<crate::model::Query>),
    Gql(crate::model::GqlQuery),
}

pub(crate) async fn execute_initial_query(
    client: Datastore,
    partition_id: PartitionId,
    initial_spec: InitialQuerySpec,
    read_mode: LookupReadMode,
    explain_options: Option<ExplainOptions>,
    options: crate::RequestOptions,
) -> Result<(QueryStream, Option<Bytes>)> {
    let proto_partition = partition_id.to_proto();
    let proto_explain =
        explain_options.map(|eo| crate::model::ExplainOptions::new().set_analyze(eo.analyze));

    let compiled_query = match &initial_spec {
        InitialQuerySpec::Structured(q) => Some((**q).clone()),
        InitialQuerySpec::Gql(_) => None,
    };

    let mut req = client
        .inner
        .gapic
        .run_query()
        .set_project_id(client.project_id())
        .set_database_id(client.database_id())
        .set_partition_id(proto_partition.clone())
        .with_options(options.clone());

    req = match initial_spec {
        InitialQuerySpec::Structured(q) => req.set_query(*q),
        InitialQuerySpec::Gql(gql) => req.set_gql_query(gql),
    };

    if let Some(ro) = read_mode.to_proto(None) {
        req = req.set_read_options(ro);
    }
    if let Some(ref eo) = proto_explain {
        req = req.set_explain_options(eo.clone());
    }

    let response = req.send().await?;
    let started_tx = (!response.transaction.is_empty()).then(|| response.transaction.clone());

    let mut stream = QueryStream {
        client,
        partition_id: proto_partition,
        read_mode,
        started_transaction: started_tx.clone(),
        explain_options: proto_explain,
        options,
        compiled_query,
        items: VecDeque::new(),
        end_cursor: None,
        more_results: None,
        read_time: None,
        explain_metrics: None,
    };
    stream.apply_response(response)?;
    Ok((stream, started_tx))
}

/// Asynchronous multi-batch iterator over entities returned by [`Datastore::run_query`] or [`Datastore::run_gql_query`].
#[derive(Debug)]
pub struct QueryStream {
    pub(crate) client: Datastore,
    pub(crate) partition_id: crate::model::PartitionId,
    pub(crate) read_mode: LookupReadMode,
    pub(crate) started_transaction: Option<Bytes>,
    pub(crate) explain_options: Option<crate::model::ExplainOptions>,
    pub(crate) options: crate::RequestOptions,
    pub(crate) compiled_query: Option<crate::model::Query>,
    pub(crate) items: VecDeque<EntityItem>,
    pub(crate) end_cursor: Option<Cursor>,
    pub(crate) more_results: Option<BatchMoreResults>,
    pub(crate) read_time: Option<wkt::Timestamp>,
    pub(crate) explain_metrics: Option<ExplainMetrics>,
}

impl QueryStream {
    fn apply_response(&mut self, response: crate::model::RunQueryResponse) -> Result<()> {
        if !response.transaction.is_empty() {
            self.started_transaction = Some(response.transaction);
        }
        if self.compiled_query.is_none() && response.query.is_some() {
            self.compiled_query = response.query;
        }
        if let Some(proto_metrics) = response.explain_metrics {
            self.explain_metrics = Some(ExplainMetrics::from_proto(proto_metrics));
        }

        let Some(batch) = response.batch else {
            self.more_results = Some(BatchMoreResults::NoMoreResults);
            return Ok(());
        };

        if batch.read_time.is_some() {
            self.read_time = batch.read_time;
        }
        self.more_results = BatchMoreResults::from_proto(&batch.more_results);

        let resume_cursor = if !batch.end_cursor.is_empty() {
            self.end_cursor = Some(Cursor::from_bytes(batch.end_cursor.clone()));
            Some(batch.end_cursor)
        } else if !batch.skipped_cursor.is_empty() {
            self.end_cursor = Some(Cursor::from_bytes(batch.skipped_cursor.clone()));
            Some(batch.skipped_cursor)
        } else {
            None
        };

        let batch_count = i32::try_from(batch.entity_results.len()).unwrap_or(i32::MAX);
        if let Some(ref mut q) = self.compiled_query {
            if batch.skipped_results > 0 {
                q.offset = (q.offset - batch.skipped_results).max(0);
            }
            if let Some(ref mut lim) = q.limit {
                *lim = (*lim - batch_count).max(0);
            }
            if let Some(cursor_bytes) = resume_cursor {
                q.start_cursor = cursor_bytes;
            }
        }

        for entity_result in batch.entity_results {
            let proto_entity = entity_result.entity.unwrap_or_default();
            let mut entity =
                Entity::from_proto(proto_entity).map_err(|e| Error::deser(Box::new(e)))?;
            if entity_result.version != 0 {
                entity.version = Some(entity_result.version);
            }
            let cursor = (!entity_result.cursor.is_empty())
                .then(|| Cursor::from_bytes(entity_result.cursor));
            self.items.push_back(EntityItem {
                entity,
                cursor,
                version: entity_result.version,
                create_time: entity_result.create_time,
                update_time: entity_result.update_time,
            });
        }

        Ok(())
    }

    /// Returns the next [`Entity`] in the query result stream, automatically fetching
    /// continuation batches when the server returns `NOT_FINISHED`.
    pub async fn next_entity(&mut self) -> Result<Option<Entity>> {
        Ok(self.next_item().await?.map(|item| item.entity))
    }

    /// Returns the next entity in the stream deserialized into `T` via [`FromEntity`].
    pub async fn next_entity_as<T: FromEntity>(&mut self) -> Result<Option<T>> {
        match self.next_entity().await? {
            Some(entity) => T::from_entity(&entity)
                .map(Some)
                .map_err(|e| Error::deser(Box::new(e))),
            None => Ok(None),
        }
    }

    /// Returns the next [`EntityItem`] (including its per-item cursor and version metadata).
    pub async fn next_item(&mut self) -> Result<Option<EntityItem>> {
        loop {
            if let Some(item) = self.items.pop_front() {
                return Ok(Some(item));
            }

            if self.more_results != Some(BatchMoreResults::NotFinished) {
                return Ok(None);
            }

            if self
                .compiled_query
                .as_ref()
                .and_then(|q| q.limit)
                .is_some_and(|l| l <= 0)
            {
                self.more_results = Some(BatchMoreResults::MoreResultsAfterLimit);
                return Ok(None);
            }

            let Some(next_query) = self.compiled_query.clone() else {
                return Err(Error::deser(
                    "RunQueryResponse returned NOT_FINISHED without a compiled Query for continuation",
                ));
            };

            let mut req = self
                .client
                .inner
                .gapic
                .run_query()
                .set_project_id(self.client.project_id())
                .set_database_id(self.client.database_id())
                .set_partition_id(self.partition_id.clone())
                .set_query(next_query)
                .with_options(self.options.clone());

            if let Some(ro) = self.read_mode.to_proto(self.started_transaction.as_ref()) {
                req = req.set_read_options(ro);
            }
            if let Some(ref eo) = self.explain_options {
                req = req.set_explain_options(eo.clone());
            }

            let response = req.send().await?;
            self.apply_response(response)?;
        }
    }

    /// Returns the latest pagination `end_cursor` for resuming this query in a subsequent request.
    pub fn end_cursor(&self) -> Option<&Cursor> {
        self.end_cursor.as_ref()
    }

    /// Returns the terminal [`BatchMoreResults`] state once received from the server.
    pub fn more_results(&self) -> Option<BatchMoreResults> {
        self.more_results
    }

    /// Returns the snapshot `read_time` at which the query was executed, if reported by the server.
    pub fn read_time(&self) -> Option<&wkt::Timestamp> {
        self.read_time.as_ref()
    }

    /// Returns the [`ExplainMetrics`] for the query (populated on the final batch when [`ExplainOptions`] is set).
    pub fn explain_metrics(&self) -> Option<&ExplainMetrics> {
        self.explain_metrics.as_ref()
    }

    /// Converts this [`QueryStream`] into a [`futures::Stream`] of `Result<Entity>`.
    pub fn into_stream(self) -> impl futures::Stream<Item = Result<Entity>> {
        futures::stream::unfold(self, |mut stream| async move {
            match stream.next_entity().await {
                Ok(Some(entity)) => Some((Ok(entity), stream)),
                Ok(None) => None,
                Err(err) => Some((Err(err), stream)),
            }
        })
    }
}

/// Request builder for [`Datastore::run_query`].
#[derive(Clone, Debug)]
pub struct RunQueryBuilder {
    pub(crate) client: Datastore,
    pub(crate) query: Query,
    pub(crate) namespace_id: Option<String>,
    pub(crate) read_options: ReadOptions,
    pub(crate) transaction_id: Option<Bytes>,
    pub(crate) explain_options: Option<ExplainOptions>,
    pub(crate) options: crate::RequestOptions,
}

impl RunQueryBuilder {
    /// Overrides the namespace ID for this query execution.
    pub fn with_namespace_id(mut self, namespace_id: impl Into<String>) -> Self {
        self.namespace_id = Some(namespace_id.into());
        self
    }

    /// Configures the [`ReadConsistency`] for this query.
    pub fn with_consistency(mut self, consistency: ReadConsistency) -> Self {
        self.read_options = ReadOptions {
            consistency: Some(consistency),
            read_time: None,
        };
        self
    }

    /// Configures a historical snapshot `read_time` for this query.
    pub fn with_read_time(mut self, read_time: wkt::Timestamp) -> Self {
        self.read_options = ReadOptions::at_read_time(read_time);
        self
    }

    /// Configures the [`ReadOptions`] for this query.
    pub fn with_read_options(mut self, read_options: ReadOptions) -> Self {
        self.read_options = read_options;
        self
    }

    /// Requests query planning and/or execution profiling via [`ExplainOptions`].
    pub fn with_explain_options(mut self, explain_options: ExplainOptions) -> Self {
        self.explain_options = Some(explain_options);
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Executes the initial `RunQuery` RPC and returns a [`QueryStream`] over all matching entities.
    pub async fn send(self) -> Result<QueryStream> {
        let mut partition_id = self.client.partition_id();
        if let Some(ns) = self
            .namespace_id
            .or_else(|| self.query.namespace_id.clone())
        {
            partition_id = partition_id.with_namespace_id(ns);
        }
        let proto_query = self
            .query
            .to_proto(&partition_id)
            .map_err(|e| Error::ser(Box::new(e)))?;
        let read_mode = match self.transaction_id {
            Some(tx_id) => LookupReadMode::Transaction(tx_id),
            None => LookupReadMode::NonTransactional(self.read_options),
        };
        let (stream, _) = execute_initial_query(
            self.client,
            partition_id,
            InitialQuerySpec::Structured(Box::new(proto_query)),
            read_mode,
            self.explain_options,
            self.options,
        )
        .await?;
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::gapic_dataplane::stub::Datastore as DatastoreStub;
    use crate::model::{
        EntityResult, QueryResultBatch, RunQueryRequest, RunQueryResponse,
        query_result_batch::MoreResultsType,
    };
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response;

    mockall::mock! {
        #[derive(Debug)]
        DatastoreMock {}
        impl DatastoreStub for DatastoreMock {
            async fn run_query(
                &self,
                req: RunQueryRequest,
                options: RequestOptions,
            ) -> Result<Response<RunQueryResponse>>;
        }
    }

    fn test_client(mock: MockDatastoreMock) -> Datastore {
        Datastore::from_stub("test-proj", "test-db", "test-ns", mock)
    }

    fn make_entity_result(id: i64, title: &str, cursor: &'static [u8]) -> EntityResult {
        let key = Key::with_id("Task", id)
            .unwrap()
            .with_partition_id(PartitionId::new("test-proj"));
        let mut entity = Entity::with_key(key.clone());
        entity.set("title", title);
        EntityResult::new()
            .set_entity(entity.to_proto(key.partition_id()).unwrap())
            .set_version(id)
            .set_cursor(Bytes::from_static(cursor))
    }

    #[tokio::test]
    async fn structured_query_multi_batch_pagination_adjusts_offset_limit_and_cursor() {
        let mut mock = MockDatastoreMock::new();

        // Batch 1: offset=2, limit=3, filter on done=false, order by priority asc.
        // Server skips 2 results, returns 1 entity, end_cursor="c-batch-1", more_results=NOT_FINISHED.
        mock.expect_run_query()
            .times(1)
            .withf(|req, _| {
                let Some(q) = req.query() else {
                    return false;
                };
                q.kind.len() == 1
                    && q.kind[0].name == "Task"
                    && q.offset == 2
                    && q.limit == Some(3)
                    && q.start_cursor.is_empty()
                    && q.filter.is_some()
                    && q.order.len() == 1
            })
            .returning(|_, _| {
                let batch = QueryResultBatch::new()
                    .set_skipped_results(2)
                    .set_entity_results([make_entity_result(1, "Task 1", b"c-item-1")])
                    .set_end_cursor(Bytes::from_static(b"c-batch-1"))
                    .set_more_results(MoreResultsType::NotFinished);
                Ok(Response::from(RunQueryResponse::new().set_batch(batch)))
            });

        // Batch 2 continuation: offset=0 (2 - 2), limit=2 (3 - 1), start_cursor="c-batch-1".
        // Server returns 2 entities, end_cursor="c-batch-2", more_results=MORE_RESULTS_AFTER_LIMIT.
        mock.expect_run_query()
            .times(1)
            .withf(|req, _| {
                let Some(q) = req.query() else {
                    return false;
                };
                q.offset == 0
                    && q.limit == Some(2)
                    && q.start_cursor == Bytes::from_static(b"c-batch-1")
            })
            .returning(|_, _| {
                let batch = QueryResultBatch::new()
                    .set_entity_results([
                        make_entity_result(2, "Task 2", b"c-item-2"),
                        make_entity_result(3, "Task 3", b"c-item-3"),
                    ])
                    .set_end_cursor(Bytes::from_static(b"c-batch-2"))
                    .set_more_results(MoreResultsType::MoreResultsAfterLimit);
                Ok(Response::from(RunQueryResponse::new().set_batch(batch)))
            });

        let client = test_client(mock);
        let query = Query::new("Task")
            .filter(Filter::equal("done", false))
            .filter(Filter::greater_than_or_equal("priority", 1_i64))
            .order_by_ascending("priority")
            .offset(2)
            .limit(3);

        let mut stream = client.run_query(query).send().await.expect("run_query");

        let mut titles = Vec::new();
        while let Some(entity) = stream.next_entity().await.expect("next_entity") {
            titles.push(entity.get::<String>("title").expect("title"));
        }

        assert_eq!(titles, vec!["Task 1", "Task 2", "Task 3"]);
        assert_eq!(
            stream.more_results(),
            Some(BatchMoreResults::MoreResultsAfterLimit)
        );
        assert_eq!(
            stream.end_cursor().map(Cursor::as_bytes),
            Some(&b"c-batch-2"[..])
        );
    }
}
