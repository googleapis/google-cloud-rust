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

#![allow(dead_code, unused_imports)]

use crate::query::Schema;
use serde_json::Map;
use std::sync::Arc;

/// A container for a single row within a query result set.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Row {
    pub(crate) values: wkt::Value,
    pub(crate) schema: Arc<Schema>,
}

impl Row {
    pub(crate) fn new(st: Map<String, wkt::Value>, schema: Arc<Schema>) -> Self {
        Self {
            values: wkt::Value::Object(st),
            schema: schema.clone(),
        }
    }
}
