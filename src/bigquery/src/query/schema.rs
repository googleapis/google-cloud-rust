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

use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};

/// Schema of a table.
#[derive(Clone, Debug)]
pub(crate) struct Schema(TableSchema);

impl Schema {
    pub(crate) fn new(schema: TableSchema) -> Self {
        Self(TableSchema::new().set_fields(schema.fields))
    }

    pub(crate) fn new_from_field(field: TableFieldSchema) -> Self {
        Self(TableSchema::new().set_fields(field.fields))
    }

    pub(crate) fn get_field_index_by_name(&self, name: &str) -> Option<usize> {
        self.0.fields.iter().position(|f| f.name == name)
    }

    pub(crate) fn get_field_by_index(&self, index: usize) -> Option<&TableFieldSchema> {
        self.0.fields.get(index)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.fields.len()
    }
}
