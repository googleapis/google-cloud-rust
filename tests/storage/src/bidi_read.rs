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

//! Bidirectional Read Integration Tests.
//!
//! Separated into two distinct test suites:
//! - [conformance]: Formal cross-SDK conformance tests for Bidi Read sessions (Suite 1).
//! - [features]: Client builder options, encodings, and backward-compatibility regression tests.

pub mod conformance;
pub mod features;
