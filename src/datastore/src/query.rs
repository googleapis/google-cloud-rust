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

use crate::Result;
use crate::client::Datastore;
use crate::entity::{Entity, FromEntity};
use crate::error::KeyError;
use crate::explain::{ExplainMetrics, ExplainOptions};
use crate::key::Key;
use crate::read::{ReadConsistency, ReadOptions};
use crate::value::{IntoValue, Value, ValueKind};
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
#[allow(dead_code)]
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
}

/// Asynchronous multi-batch iterator over entities returned by [`Datastore::run_query`] or [`Datastore::run_gql_query`].
#[derive(Debug)]
#[allow(dead_code)]
pub struct QueryStream {
    pub(crate) client: Datastore,
    pub(crate) items: VecDeque<EntityItem>,
    pub(crate) end_cursor: Option<Cursor>,
    pub(crate) more_results: Option<BatchMoreResults>,
    pub(crate) read_time: Option<wkt::Timestamp>,
    pub(crate) explain_metrics: Option<ExplainMetrics>,
}

impl QueryStream {
    /// Returns the next [`Entity`] in the query result stream, automatically fetching
    /// continuation batches when the server returns `NOT_FINISHED`.
    pub async fn next_entity(&mut self) -> Result<Option<Entity>> {
        unimplemented!()
    }

    /// Returns the next entity in the stream deserialized into `T` via [`FromEntity`].
    pub async fn next_entity_as<T: FromEntity>(&mut self) -> Result<Option<T>> {
        unimplemented!()
    }

    /// Returns the next [`EntityItem`] (including its per-item cursor and version metadata).
    pub async fn next_item(&mut self) -> Result<Option<EntityItem>> {
        unimplemented!()
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
#[allow(dead_code)]
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
        unimplemented!()
    }
}
