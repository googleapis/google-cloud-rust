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

use crate::credentials::InternalCredentials;
use crate::errors::InternalCredentialsError;
use std::sync::{Arc, OnceLock};

pub type CredentialsBuilderFn = Box<
    dyn Fn() -> std::result::Result<Arc<dyn InternalCredentials>, InternalCredentialsError>
        + Send
        + Sync,
>;

static BUILDER: OnceLock<CredentialsBuilderFn> = OnceLock::new();

/// Registers a builder function for default credentials.
pub fn set_default_credentials_builder(f: CredentialsBuilderFn) {
    let _ = BUILDER.set(f);
}

/// Builds the default credentials using the registered builder function.
pub fn build_default_credentials()
-> std::result::Result<Arc<dyn InternalCredentials>, InternalCredentialsError> {
    match BUILDER.get() {
        Some(f) => f(),
        None => Err(InternalCredentialsError::from_msg(
            false,
            "no default credentials builder registered",
        )),
    }
}
