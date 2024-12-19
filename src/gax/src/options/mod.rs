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

//! Per request options.

/// A set of options configuring a single request.
///
/// Application only use this class directly in mocks, where they may want to
/// verify their application has configured all the right request parameters and
/// options.
///
/// All other code uses this type indirectly, via the per-request builders.
#[derive(Clone, Debug, Default)]
pub struct RequestOptions;
