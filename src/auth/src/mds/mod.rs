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

pub(crate) mod client;

pub(crate) const MDS_DEFAULT_URI: &str = "/computeMetadata/v1/instance/service-accounts/default";
pub(crate) const METADATA_FLAVOR_VALUE: &str = "Google";
pub(crate) const METADATA_FLAVOR: &str = "metadata-flavor";
pub(crate) const METADATA_ROOT: &str = "http://metadata.google.internal";
pub(crate) const GCE_METADATA_HOST_ENV_VAR: &str = "GCE_METADATA_HOST";
