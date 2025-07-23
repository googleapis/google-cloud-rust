# Copyright 2025 Google LLC
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
  default = "rust-auth-testing"
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-f"
}

variable "external_account_project" {
  type    = string
  default = "rust-external-account-joonix"
}

variable "external_account_service_account_id" {
  description = "The ID of the service account used for external account tests."
  type        = string
  default     = "testsa"
}

variable "workload_identity_pool_id" {
  description = "The ID for the workload identity pool."
  type        = string
  default     = "google-idp"
}

variable "impersonation_target_account_id" {
  description = "The account ID for the impersonation target service account."
  type        = string
  default     = "impersonation-target"
}

variable "build_runner_account_id" {
  description = "The account ID for the integration test runner service account."
  type        = string
  default     = "integration-test-runner"
}


