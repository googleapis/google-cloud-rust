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

//! This crate contains a number of guides showing how to use the
//! Google Cloud Client Libraries for Rust.

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub mod binding_errors;
pub mod compute;
pub mod error_handling;
pub mod examine_error_details;
pub mod gemini;
pub mod pagination;
pub mod retry_policies;
pub mod update_resource;
