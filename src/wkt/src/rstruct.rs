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

/// Protobuf (and consequently the Google Cloud APIs) use `Struct` to represent
/// JSON objects. We need a type that might be referenced from the generated
/// code.
pub type Struct = serde_json::Map<String, serde_json::Value>;

/// Protobuf (and consequently the Google Cloud APIs) use `Value` to represent
/// JSON values. We need a type that might be referenced from the generated
/// code.
pub type Value = serde_json::Value;

/// Protobuf (and consequently the Google Cloud APIs) use `ListValue` to
/// represent a list of JSON values. We need a type that might be referenced
/// from the generated code.
pub type ListValue = Vec<serde_json::Value>;
