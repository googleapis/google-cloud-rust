// Copyright 2024 Google LLC
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

/// Defines traits and helpers to serialize query parameters.
///
/// Query parameters in the Google APIs can be types other than strings and
/// integers. We need a helper to efficiently serialize parameters of different
/// types. We also want the generator to be relatively simple.
///
/// The Rust SDK generator produces query parameters as optional fields in the
/// request object. The generator code can be simplified if all the query
/// parameters can be treated uniformly, without any conditionally generated
/// code to handle different types.
///
/// This module defines some traits and helpers to simplify the code generator.
///
/// The types are not intended for application developers to use. They are
/// public because we will generate many crates (roughly one per service), and
/// most of these crates will use these helpers.
pub mod query_parameter;