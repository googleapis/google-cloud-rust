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
use crate::model::batch_write_request::MutationGroup as ProtoMutationGroup;
use crate::model::mutation::Operation;
use crate::to_value::ToValue;
use crate::value::Value;
use rand::seq::IteratorRandom;
use std::slice::Iter;
use std::vec::IntoIter;

/// Represents an individual table modification to be applied to Cloud Spanner.
///
/// # Example
/// ```rust
/// use google_cloud_spanner::mutation::Mutation;
///
/// let mutation = Mutation::new_insert_builder("Users")
///     .set("UserId").to(&1)
///     .set("UserName").to(&"Alice")
///     .build();
/// ```
///
/// Use the methods on `Mutation` to create a builder for the desired operation type.
#[derive(Clone, Debug, PartialEq)]
pub struct Mutation {
    pub(crate) inner: InternalMutation,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum InternalMutation {
    /// Inserts a new row in a table. If the row already exists, the write or transaction fails with
    /// `ALREADY_EXISTS`.
    Insert(Write),
    /// Updates an existing row in a table. If the row does not already exist, the transaction fails
    /// with error `NOT_FOUND`.
    Update(Write),
    /// Like `Insert`, except that if the row already exists, then its column values are
    /// overwritten with the ones provided.
    InsertOrUpdate(Write),
    /// Like `Insert`, except that if the row already exists, it is deleted, and the column
    /// values provided are inserted instead.
    Replace(Write),
    /// Deletes rows from a table. Succeeds whether or not the referenced rows were present.
    Delete(Delete),
}

/// A mutation that inserts, updates, or replaces rows in a table.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Write {
    pub(crate) table: String,
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<Value>,
}

/// A mutation that deletes rows from a table.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Delete {
    pub(crate) table: String,
    // This will be replaced with the KeySet definition from the
    // spanner-keys branch once it has been merged.
    pub(crate) key_set: KeySet,
}

impl Mutation {
    /// Returns a builder that can be used to construct an `Insert` mutation against `table`.
    ///
    /// # Example
    /// ```rust
    /// use google_cloud_spanner::mutation::Mutation;
    /// let mutation = Mutation::new_insert_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .build();
    /// ```
    pub fn new_insert_builder(table: impl Into<String>) -> WriteBuilder {
        WriteBuilder::new(table, MutationType::Insert)
    }

    /// Returns a builder that can be used to construct an `Update` mutation against `table`.
    ///
    /// # Example
    /// ```rust
    /// use google_cloud_spanner::mutation::Mutation;
    /// let mutation = Mutation::new_update_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .set("UserName").to(&"Bob")
    ///     .build();
    /// ```
    pub fn new_update_builder(table: impl Into<String>) -> WriteBuilder {
        WriteBuilder::new(table, MutationType::Update)
    }

    /// Returns a builder that can be used to construct an `InsertOrUpdate` mutation against `table`.
    ///
    /// # Example
    /// ```rust
    /// use google_cloud_spanner::mutation::Mutation;
    /// let mutation = Mutation::new_insert_or_update_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .set("UserName").to(&"Bob")
    ///     .build();
    /// ```
    pub fn new_insert_or_update_builder(table: impl Into<String>) -> WriteBuilder {
        WriteBuilder::new(table, MutationType::InsertOrUpdate)
    }

    /// Returns a builder that can be used to construct a `Replace` mutation against `table`.
    ///
    /// # Example
    /// ```rust
    /// use google_cloud_spanner::mutation::Mutation;
    /// let mutation = Mutation::new_replace_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .set("UserName").to(&"Bob")
    ///     .build();
    /// ```
    pub fn new_replace_builder(table: impl Into<String>) -> WriteBuilder {
        WriteBuilder::new(table, MutationType::Replace)
    }

    /// Returns a mutation that will delete all rows with primary keys covered by `key_set`.
    ///
    /// # Example
    /// ```text
    /// // Example omitted temporarily until the new KeySet API is merged
    /// ```
    pub fn delete(table: impl Into<String>, key_set: KeySet) -> Mutation {
        Mutation {
            inner: InternalMutation::Delete(Delete {
                table: table.into(),
                key_set,
            }),
        }
    }

    pub(crate) fn build_proto(self) -> crate::model::Mutation {
        match self.inner {
            InternalMutation::Insert(write) => {
                crate::model::Mutation::new().set_insert(write.into_proto())
            }
            InternalMutation::Update(write) => {
                crate::model::Mutation::new().set_update(write.into_proto())
            }
            InternalMutation::InsertOrUpdate(write) => {
                crate::model::Mutation::new().set_insert_or_update(write.into_proto())
            }
            InternalMutation::Replace(write) => {
                crate::model::Mutation::new().set_replace(write.into_proto())
            }
            InternalMutation::Delete(delete) => {
                crate::model::Mutation::new().set_delete(delete.into_proto())
            }
        }
    }

    /// Selects the best mutation to act as a routing `mutation_key`.
    /// Prefers any non-`Insert` variation (like `Update`, `InsertOrUpdate`, `Replace`, `Delete`)
    /// since inserts more often use auto-generated columns (e.g. for primary key generation).
    /// Using a mutation with only non-generated values as the mutation key is preferred, as it reduces
    /// the overhead internally in Spanner.
    /// If only `Insert` mutations are present, it selects the insert mutation with the largest number of rows.
    pub(crate) fn select_mutation_key(
        mutations: &[crate::model::Mutation],
    ) -> Option<crate::model::Mutation> {
        if mutations.is_empty() {
            return None;
        }

        // 1. Filter for any mutations other than Operation::Insert, Send, or Ack, selecting one randomly.
        let selected_non_insert = mutations
            .iter()
            .filter(|m| {
                m.operation.as_ref().is_some_and(|op| {
                    !matches!(
                        op,
                        Operation::Insert(_) | Operation::Send(_) | Operation::Ack(_)
                    )
                })
            })
            .choose(&mut rand::rng())
            .cloned();

        if selected_non_insert.is_some() {
            return selected_non_insert;
        }

        // 2. If only Inserts are present, choose the one with the largest number of values (rows).
        let max_insert = mutations
            .iter()
            .filter_map(|m| match &m.operation {
                Some(Operation::Insert(write)) => Some((m, write.values.len())),
                _ => None,
            })
            .max_by_key(|&(_, rows)| rows)
            .map(|(m, _)| m);

        max_insert.cloned().or_else(|| mutations.first().cloned())
    }
}

impl Write {
    fn into_proto(self) -> crate::model::mutation::Write {
        crate::model::mutation::Write::new()
            .set_table(self.table)
            .set_columns(self.columns)
            .set_values(vec![
                self.values
                    .into_iter()
                    .map(Value::into_serde_value)
                    .collect::<wkt::ListValue>(),
            ])
    }
}

impl Delete {
    fn into_proto(self) -> crate::model::mutation::Delete {
        crate::model::mutation::Delete::new()
            .set_table(self.table)
            .set_key_set(self.key_set.into_proto())
    }
}

/// A builder for constructing `Write` mutations fluently.
pub struct WriteBuilder {
    table: String,
    mutation_type: MutationType,
    columns: Vec<String>,
    values: Vec<Value>,
}

enum MutationType {
    Insert,
    Update,
    InsertOrUpdate,
    Replace,
}

impl WriteBuilder {
    fn new(table: impl Into<String>, mutation_type: MutationType) -> Self {
        Self {
            table: table.into(),
            mutation_type,
            columns: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Returns a binder to set the value of `column_name` that should be applied by the mutation.
    ///
    /// # Example
    /// ```rust
    /// use google_cloud_spanner::mutation::Mutation;
    /// let mutation = Mutation::new_insert_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .build();
    /// ```
    pub fn set(self, column_name: impl Into<String>) -> ValueBinder {
        ValueBinder {
            builder: self,
            column: column_name.into(),
        }
    }

    /// Builds and returns the finalized `Mutation`.
    pub fn build(self) -> Mutation {
        let write = Write {
            table: self.table,
            columns: self.columns,
            values: self.values,
        };
        let inner = match self.mutation_type {
            MutationType::Insert => InternalMutation::Insert(write),
            MutationType::Update => InternalMutation::Update(write),
            MutationType::InsertOrUpdate => InternalMutation::InsertOrUpdate(write),
            MutationType::Replace => InternalMutation::Replace(write),
        };
        Mutation { inner }
    }
}

/// A binder that associates a column name with a value within a `WriteBuilder`.
pub struct ValueBinder {
    builder: WriteBuilder,
    column: String,
}

impl ValueBinder {
    /// Sets the value for the column.
    pub fn to<T: ToValue + ?Sized>(self, value: &T) -> WriteBuilder {
        self.to_value(value.to_value())
    }

    /// Sets the value for the column, taking ownership of the value.
    ///
    /// This behaves like [`to`](Self::to) but accepts an owned value (anything convertible into a
    /// [`Value`]). When the caller already holds a `Value`, this avoids the deep clone that
    /// [`to`](Self::to) performs via [`ToValue::to_value`].
    pub fn to_value(mut self, value: impl Into<Value>) -> WriteBuilder {
        self.builder.columns.push(self.column);
        self.builder.values.push(value.into());
        self.builder
    }
}

/// A group of mutations that are applied atomically in a [crate::batch::BatchWriteTransaction].
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct MutationGroup {
    mutations: Vec<Mutation>,
}

impl MutationGroup {
    /// Creates a new mutation group from a list of mutations.
    pub fn new(mutations: Vec<Mutation>) -> Self {
        Self { mutations }
    }

    /// Returns a reference to the collection of mutations in this group.
    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }

    #[allow(dead_code)]
    pub(crate) fn build_proto(self) -> ProtoMutationGroup {
        ProtoMutationGroup::new().set_mutations(self.mutations.into_iter().map(|m| m.build_proto()))
    }
}

impl IntoIterator for MutationGroup {
    type Item = Mutation;
    type IntoIter = IntoIter<Mutation>;

    fn into_iter(self) -> Self::IntoIter {
        self.mutations.into_iter()
    }
}

impl<'a> IntoIterator for &'a MutationGroup {
    type Item = &'a Mutation;
    type IntoIter = Iter<'a, Mutation>;

    fn into_iter(self) -> Self::IntoIter {
        self.mutations.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(Mutation: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(Write: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(Delete: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(WriteBuilder: Send, Sync);
        static_assertions::assert_impl_all!(ValueBinder: Send, Sync);
        static_assertions::assert_impl_all!(MutationGroup: Send, Sync, Clone, std::fmt::Debug);
    }

    #[test]
    fn mutation_group() {
        let mutation1 = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .build();
        let mutation2 = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&2)
            .build();
        let group = MutationGroup::new(vec![mutation1.clone(), mutation2.clone()]);
        assert_eq!(group.mutations.len(), 2);
        assert_eq!(group.mutations[0], mutation1);
        assert_eq!(group.mutations[1], mutation2);
    }

    #[test]
    fn mutation_group_into_iter() {
        let mutation1 = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .build();
        let mutation2 = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&2)
            .build();
        let group = MutationGroup::new(vec![mutation1.clone(), mutation2.clone()]);

        let mutations: Vec<_> = group.into_iter().collect();
        assert_eq!(mutations, vec![mutation1, mutation2]);
    }

    #[test]
    fn mutation_group_iter_ref() {
        let mutation1 = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .build();
        let mutation2 = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&2)
            .build();
        let group = MutationGroup::new(vec![mutation1.clone(), mutation2.clone()]);

        let mutations: Vec<_> = (&group).into_iter().collect();
        assert_eq!(mutations, vec![&mutation1, &mutation2]);
    }

    #[test]
    fn value_binder_to_value_owned() {
        let by_ref = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .build();
        let by_value = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to_value(1.to_value())
            .build();
        assert_eq!(by_ref, by_value);
    }

    #[test]
    fn insert_builder() {
        let mutation = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .set("UserName")
            .to(&"Alice")
            .build();

        match mutation.inner {
            InternalMutation::Insert(write) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId", "UserName"]);
                assert_eq!(write.values.len(), 2);
                assert_eq!(write.values[0].as_string(), "1");
                assert_eq!(write.values[1].as_string(), "Alice");
            }
            _ => panic!("Expected Insert mutation"),
        }
    }

    #[test]
    fn update_builder() {
        let mutation = Mutation::new_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        match mutation.inner {
            InternalMutation::Update(write) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId"]);
                assert_eq!(write.values.len(), 1);
                assert_eq!(write.values[0].as_string(), "1");
            }
            _ => panic!("Expected Update mutation"),
        }
    }

    #[test]
    fn insert_or_update_builder() {
        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        match mutation.inner {
            InternalMutation::InsertOrUpdate(write) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId"]);
                assert_eq!(write.values.len(), 1);
                assert_eq!(write.values[0].as_string(), "1");
            }
            _ => panic!("Expected InsertOrUpdate mutation"),
        }
    }

    #[test]
    fn replace_builder() {
        let mutation = Mutation::new_replace_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        match mutation.inner {
            InternalMutation::Replace(write) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId"]);
                assert_eq!(write.values.len(), 1);
                assert_eq!(write.values[0].as_string(), "1");
            }
            _ => panic!("Expected Replace mutation"),
        }
    }

    #[test]
    fn build_proto_insert() {
        let mutation = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .set("UserName")
            .to(&"Alice")
            .build();
        let proto = mutation.build_proto();
        match proto.operation {
            Some(Operation::Insert(write)) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId", "UserName"]);
                assert_eq!(write.values.len(), 1);
                assert_eq!(write.values[0].len(), 2);
                assert_eq!(write.values[0][0], serde_json::json!("1"));
                assert_eq!(write.values[0][1], serde_json::json!("Alice"));
            }
            _ => panic!("Expected Insert operation, got {:?}", proto.operation),
        }
    }

    #[test]
    fn build_proto_update() {
        let mutation = Mutation::new_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();
        let proto = mutation.build_proto();
        match proto.operation {
            Some(Operation::Update(write)) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId"]);
                assert_eq!(write.values.len(), 1);
            }
            _ => panic!("Expected Update operation, got {:?}", proto.operation),
        }
    }

    #[test]
    fn build_proto_insert_or_update() {
        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();
        let proto = mutation.build_proto();
        match proto.operation {
            Some(Operation::InsertOrUpdate(write)) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId"]);
                assert_eq!(write.values.len(), 1);
            }
            _ => panic!(
                "Expected InsertOrUpdate operation, got {:?}",
                proto.operation
            ),
        }
    }

    #[test]
    fn build_proto_replace() {
        let mutation = Mutation::new_replace_builder("Users")
            .set("UserId")
            .to(&1)
            .build();
        let proto = mutation.build_proto();
        match proto.operation {
            Some(Operation::Replace(write)) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId"]);
                assert_eq!(write.values.len(), 1);
            }
            _ => panic!("Expected Replace operation, got {:?}", proto.operation),
        }
    }

    #[test]
    fn build_proto_delete() {
        let key_set = crate::key::KeySet::builder().build();
        let mutation = Mutation::delete("Users", key_set);
        let proto = mutation.build_proto();
        match proto.operation {
            Some(Operation::Delete(delete)) => {
                assert_eq!(delete.table, "Users");
            }
            _ => panic!("Expected Delete operation, got {:?}", proto.operation),
        }
    }

    #[test]
    fn test_select_mutation_key_empty() {
        let mutations = vec![];
        let key = Mutation::select_mutation_key(&mutations);
        assert!(key.is_none());
    }

    #[test]
    fn test_select_mutation_key_prefers_insert_or_update_over_insert() {
        let m1 = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .build()
            .build_proto();
        let m2 = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&2)
            .build()
            .build_proto();
        let mutations = vec![m1.clone(), m2.clone()];
        let key = Mutation::select_mutation_key(&mutations);
        assert_eq!(key, Some(m2));
    }

    #[test]
    fn test_select_mutation_key_only_insert_prefers_largest() {
        let m1 = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .build()
            .build_proto();

        // Create an insert mutation with two rows (larger than m1 which has one row)
        let row1 = vec![serde_json::json!("2")]
            .into_iter()
            .collect::<wkt::ListValue>();
        let row2 = vec![serde_json::json!("3")]
            .into_iter()
            .collect::<wkt::ListValue>();
        let write2 = crate::model::mutation::Write::new()
            .set_table("Users")
            .set_columns(vec!["UserId".to_string()])
            .set_values(vec![row1, row2]);
        let m2 = crate::model::Mutation::new().set_insert(write2);

        let mutations = vec![m1.clone(), m2.clone()];
        let key = Mutation::select_mutation_key(&mutations);
        assert_eq!(key, Some(m2));
    }

    #[test]
    fn test_select_mutation_key_mix() {
        let m1 = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .build()
            .build_proto();
        let m2 = Mutation::new_update_builder("Users")
            .set("UserId")
            .to(&2)
            .build()
            .build_proto();
        let m3 = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&3)
            .build()
            .build_proto();
        let mutations = vec![m1.clone(), m2.clone(), m3.clone()];
        let key = Mutation::select_mutation_key(&mutations).expect("Expected a key");
        // Either of the non-insert mutations (m2 or m3) can be selected randomly.
        assert!(
            key == m2 || key == m3,
            "Expected either m2 or m3 to be selected, got {:?}",
            key
        );
    }

    #[test]
    fn test_select_mutation_key_only_non_insert() {
        let m1 = Mutation::new_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build()
            .build_proto();
        let m2 = Mutation::new_replace_builder("Users")
            .set("UserId")
            .to(&2)
            .build()
            .build_proto();
        let mutations = vec![m1.clone(), m2.clone()];
        let key = Mutation::select_mutation_key(&mutations).expect("Expected a key");
        // Either non-insert mutation can be selected randomly.
        assert!(
            key == m1 || key == m2,
            "Expected either m1 or m2 to be selected, got {:?}",
            key
        );
    }

    #[test]
    fn test_select_mutation_key_operation_none() {
        let m1 = crate::model::Mutation::default();
        let m2 = crate::model::Mutation::default();
        let mutations = vec![m1.clone(), m2.clone()];
        let key = Mutation::select_mutation_key(&mutations);
        assert_eq!(key, Some(m1));
    }
}
