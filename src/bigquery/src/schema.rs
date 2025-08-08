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

use std::slice::Iter;

use bigquery_v2::model::{TableFieldSchema, TableSchema};

#[derive(Clone, Debug, Default)]
pub(crate) struct Schema {
    pub schema: TableSchema,
}

impl Schema {
    pub(crate) fn new(schema: TableSchema) -> Self {
        Self {
            schema: TableSchema::new().set_fields(schema.fields),
        }
    }

    pub(crate) fn new_from_field(field: TableFieldSchema) -> Self {
        Self {
            schema: TableSchema::new().set_fields(field.fields),
        }
    }

    pub(crate) fn get_field(&self, name: &str) -> Option<&TableFieldSchema> {
        self.fields().find(|f| f.name == name)
    }

    pub(crate) fn get_field_by_index(&self, index: usize) -> Option<&TableFieldSchema> {
        self.fields().nth(index)
    }

    pub(crate) fn fields(&self) -> Iter<TableFieldSchema> {
        self.schema.fields.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.schema.fields.len()
    }
}
