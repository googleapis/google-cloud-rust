use crate::model::KeySet;
use crate::value::ToValue;
use serde_json::Value;
use std::collections::HashSet;

/// Represents an individual table modification to be applied to Cloud Spanner.
#[derive(Clone, Debug, PartialEq)]
pub enum Mutation {
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

#[derive(Clone, Debug, PartialEq)]
pub struct Write {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Delete {
    pub table: String,
    pub key_set: KeySet,
}

impl Mutation {
    /// Returns a builder that can be used to construct an `Insert` mutation against `table`.
    pub fn new_insert_builder(table: impl Into<String>) -> WriteBuilder {
        WriteBuilder::new(table, MutationType::Insert)
    }

    /// Returns a builder that can be used to construct an `Update` mutation against `table`.
    pub fn new_update_builder(table: impl Into<String>) -> WriteBuilder {
        WriteBuilder::new(table, MutationType::Update)
    }

    /// Returns a builder that can be used to construct an `InsertOrUpdate` mutation against `table`.
    pub fn new_insert_or_update_builder(table: impl Into<String>) -> WriteBuilder {
        WriteBuilder::new(table, MutationType::InsertOrUpdate)
    }

    /// Returns a builder that can be used to construct a `Replace` mutation against `table`.
    pub fn new_replace_builder(table: impl Into<String>) -> WriteBuilder {
        WriteBuilder::new(table, MutationType::Replace)
    }

    /// Returns a mutation that will delete all rows with primary keys covered by `key_set`.
    pub fn delete(table: impl Into<String>, key_set: KeySet) -> Mutation {
        Mutation::Delete(Delete {
            table: table.into(),
            key_set,
        })
    }

    pub(crate) fn build_proto(self) -> crate::model::Mutation {
        match self {
            Mutation::Insert(write) => crate::model::Mutation::new().set_insert(write.to_proto()),
            Mutation::Update(write) => crate::model::Mutation::new().set_update(write.to_proto()),
            Mutation::InsertOrUpdate(write) => {
                crate::model::Mutation::new().set_insert_or_update(write.to_proto())
            }
            Mutation::Replace(write) => crate::model::Mutation::new().set_replace(write.to_proto()),
            Mutation::Delete(delete) => crate::model::Mutation::new().set_delete(delete.to_proto()),
        }
    }
}

impl Write {
    fn to_proto(self) -> crate::model::mutation::Write {
        let mut write = crate::model::mutation::Write::new();
        write = write.set_table(self.table);
        write = write.set_columns(self.columns);
        write.set_values(vec![self.values])
    }
}

impl Delete {
    fn to_proto(self) -> crate::model::mutation::Delete {
        let mut delete = crate::model::mutation::Delete::new();
        delete = delete.set_table(self.table);
        delete.set_key_set(self.key_set)
    }
}

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
        match self.mutation_type {
            MutationType::Insert => Mutation::Insert(write),
            MutationType::Update => Mutation::Update(write),
            MutationType::InsertOrUpdate => Mutation::InsertOrUpdate(write),
            MutationType::Replace => Mutation::Replace(write),
        }
    }
}

pub struct ValueBinder {
    builder: WriteBuilder,
    column: String,
}

impl ValueBinder {
    /// Sets the value for the column.
    pub fn to<T: ToValue + ?Sized>(mut self, value: &T) -> WriteBuilder {
        self.builder.columns.push(self.column);
        self.builder.values.push(value.to_value());
        self.builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_builder() {
        let mutation = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .set("UserName")
            .to(&"Alice")
            .build();

        match mutation {
            Mutation::Insert(write) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId", "UserName"]);
                assert_eq!(write.values.len(), 2);
            }
            _ => panic!("Expected Insert mutation"),
        }
    }

    #[test]
    fn test_update_builder() {
        let mutation = Mutation::new_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        match mutation {
            Mutation::Update(write) => {
                assert_eq!(write.table, "Users");
                assert_eq!(write.columns, vec!["UserId"]);
            }
            _ => panic!("Expected Update mutation"),
        }
    }

    #[test]
    fn test_build_proto_insert() {
        let mutation = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
            .set("UserName")
            .to(&"Alice")
            .build();
        let proto = mutation.build_proto();
        match proto.operation {
            Some(crate::model::mutation::Operation::Insert(write)) => {
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
    fn test_build_proto_delete() {
        let key_set = KeySet {
            keys: vec![vec![1.into()]],
            ..Default::default()
        };
        let mutation = Mutation::delete("Users", key_set.clone());
        let proto = mutation.build_proto();
        match proto.operation {
            Some(crate::model::mutation::Operation::Delete(delete)) => {
                assert_eq!(delete.table, "Users");
                assert_eq!(delete.key_set.unwrap(), key_set);
            }
            _ => panic!("Expected Delete operation, got {:?}", proto.operation),
        }
    }
}
