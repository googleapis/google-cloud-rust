# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

variable "project" {
  type = string
}

# To test `SetIamPolicy()` calls we typically want to add bindings. We use this
# account in such tests. The account is 
# an existing account.
resource "google_service_account" "set-iam-test-only" {
  account_id   = "set-iam-test-only"
  display_name = "Used in testing of set_iam_policy() and similar RPCs."
  disabled     = true
}
