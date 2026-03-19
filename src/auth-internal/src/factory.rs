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

use crate::Result;
use crate::credentials::Credentials;
use std::sync::OnceLock;

pub type CredentialsBuilderFn = Box<dyn Fn() -> Result<Credentials> + Send + Sync>;

static BUILDER: OnceLock<CredentialsBuilderFn> = OnceLock::new();

/// Registers a builder function for default credentials.
///
/// This is typically called by the `google-cloud-auth` crate to register itself
/// as the provider of default credentials.
pub fn set_default_credentials_builder(f: CredentialsBuilderFn) {
    let _ = BUILDER.set(f);
}

/// Builds the default credentials using the registered builder function.
///
/// If no builder function has been registered, this returns an error.
pub fn build_default_credentials() -> Result<Credentials> {
    match BUILDER.get() {
        Some(f) => f(),
        None => Err(crate::errors::non_retryable_from_str(
            "no default credentials builder registered. \
             Ensure google-cloud-auth is used or a builder is registered via \
             set_default_credentials_builder()",
        )),
    }
}
