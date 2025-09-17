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

use super::*;

impl std::fmt::Debug for crate::model::ExternalAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("ExternalAccountKey");
        debug_struct.field("name", &self.name);
        debug_struct.field("key_id", &self.key_id);
        debug_struct.field("b64_mac_key", &self.b64_mac_key);
        if !self._unknown_fields.is_empty() {
            debug_struct.field("_unknown_fields", &self._unknown_fields);
        }
        debug_struct.finish()
    }
}

impl std::fmt::Debug for crate::model::CreateExternalAccountKeyRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("CreateExternalAccountKeyRequest");
        debug_struct.field("parent", &self.parent);
        debug_struct.field("external_account_key", &self.external_account_key);
        if !self._unknown_fields.is_empty() {
            debug_struct.field("_unknown_fields", &self._unknown_fields);
        }
        debug_struct.finish()
    }
}
