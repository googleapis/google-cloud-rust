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

use crate::row::Row;
use prost_types::value::Kind;

#[derive(Clone)]
pub(crate) struct ChecksumCalculator {
    context: md5::Context,
    is_first_row: bool,
}

impl std::fmt::Debug for ChecksumCalculator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChecksumCalculator")
            .field("is_first_row", &self.is_first_row)
            .finish_non_exhaustive()
    }
}

impl ChecksumCalculator {
    pub fn new() -> Self {
        Self {
            context: md5::Context::new(),
            is_first_row: true,
        }
    }

    pub fn update_row(&mut self, row: &Row) {
        if self.is_first_row {
            for (name, col_type) in row
                .metadata
                .column_names
                .iter()
                .zip(row.metadata.column_types.iter())
            {
                self.context.consume(name.as_bytes());
                let code_i32: i32 = col_type.code().into();
                self.context.consume([code_i32 as u8]);
            }
            self.is_first_row = false;
        }

        for val in row.raw_values() {
            if let Some(ref kind) = val.0.kind {
                match kind {
                    Kind::NullValue(n) => {
                        self.context.consume([0]);
                        self.context.consume(n.to_le_bytes());
                    }
                    Kind::NumberValue(f) => {
                        self.context.consume([1]);
                        self.context.consume(f.to_le_bytes());
                    }
                    Kind::StringValue(s) => {
                        self.context.consume([2]);
                        self.context.consume(s.as_bytes());
                    }
                    Kind::BoolValue(b) => {
                        self.context.consume([3]);
                        self.context.consume([*b as u8]);
                    }
                    Kind::StructValue(s) => {
                        self.context.consume([4]);
                        self.update_struct(s);
                    }
                    Kind::ListValue(l) => {
                        self.context.consume([5]);
                        self.update_list(l);
                    }
                }
            } else {
                self.context.consume([0]);
            }
        }
    }

    fn update_struct(&mut self, s: &prost_types::Struct) {
        for (k, v) in &s.fields {
            self.context.consume(k.as_bytes());
            self.update_proto_value(v);
        }
    }

    fn update_list(&mut self, l: &prost_types::ListValue) {
        for v in &l.values {
            self.update_proto_value(v);
        }
    }

    fn update_proto_value(&mut self, v: &prost_types::Value) {
        if let Some(ref kind) = v.kind {
            match kind {
                Kind::NullValue(n) => {
                    self.context.consume([0]);
                    self.context.consume(n.to_le_bytes());
                }
                Kind::NumberValue(f) => {
                    self.context.consume([1]);
                    self.context.consume(f.to_le_bytes());
                }
                Kind::StringValue(s) => {
                    self.context.consume([2]);
                    self.context.consume(s.as_bytes());
                }
                Kind::BoolValue(b) => {
                    self.context.consume([3]);
                    self.context.consume([*b as u8]);
                }
                Kind::StructValue(s) => {
                    self.context.consume([4]);
                    self.update_struct(s);
                }
                Kind::ListValue(l) => {
                    self.context.consume([5]);
                    self.update_list(l);
                }
            }
        } else {
            self.context.consume([0]);
        }
    }

    pub fn finalize(self) -> [u8; 16] {
        let digest = self.context.finalize();
        digest.0
    }
}
