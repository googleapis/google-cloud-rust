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
  type = string
}

variable "runner_project_id" {
  description = "The project ID where the integration-test-runner service account lives."
  type        = string
}

variable "service_account_id" {
  type = string
}

variable "workload_identity_pool_id" {
  description = "The ID for the workload identity pool."
  type        = string
}

variable "impersonation_target_account_id" {
  description = "The account ID for the impersonation target service account."
  type        = string
}

variable "build_runner_account_id" {
  description = "The account ID for the integration test runner service account."
  type        = string
}

provider "google" {
  alias   = "external_account_project"
  project = var.project
}

resource "google_service_account" "service_account" {
  provider     = google.external_account_project
  project      = var.project
  account_id   = var.service_account_id
  display_name = "External Account Test Service Account"
}

data "google_service_account" "build_runner_service_account" {
  provider   = google
  project    = var.runner_project_id
  account_id = var.build_runner_account_id
}

# Allow the build runner service account to create OIDC tokens for the
# test service account.
resource "google_service_account_iam_member" "token_creator" {
  provider           = google.external_account_project
  service_account_id = google_service_account.service_account.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${data.google_service_account.build_runner_service_account.email}"
}

resource "google_iam_workload_identity_pool" "pool" {
  provider                  = google.external_account_project
  project                   = var.project
  workload_identity_pool_id = var.workload_identity_pool_id
  display_name              = "External Account Test Pool"
  description               = "For external account integration tests"
}

resource "google_iam_workload_identity_pool_provider" "provider" {
  provider                           = google.external_account_project
  project                            = var.project
  workload_identity_pool_id          = google_iam_workload_identity_pool.pool.workload_identity_pool_id
  workload_identity_pool_provider_id = var.workload_identity_pool_id
  display_name                       = "External Account Test Provider"
  description                        = "Trust Google as an OIDC provider"

  oidc {
    issuer_uri = "https://accounts.google.com"
  }

  attribute_mapping = {
    "google.subject" = "assertion.sub"
  }
}

# Allow principals from the pool that match the test service account's unique ID
# to impersonate the test service account.
resource "google_service_account_iam_member" "workload_identity_user" {
  provider           = google.external_account_project
  service_account_id = google_service_account.service_account.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principal://iam.googleapis.com/${google_iam_workload_identity_pool.pool.name}/subject/${google_service_account.service_account.unique_id}"
}

resource "google_service_account" "impersonation_target" {
  provider     = google.external_account_project
  project      = var.project
  account_id   = var.impersonation_target_account_id
  display_name = "Impersonation Target Service Account"
}

resource "google_service_account_iam_member" "impersonation_token_creator" {
  provider           = google.external_account_project
  service_account_id = google_service_account.impersonation_target.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "principal://iam.googleapis.com/${google_iam_workload_identity_pool.pool.name}/subject/${google_service_account.service_account.unique_id}"
}

# Allow the external account to create tokens
resource "google_service_account_iam_member" "self_token_creator" {
  provider           = google.external_account_project
  service_account_id = google_service_account.service_account.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.service_account.email}"
}

output "audience" {
  value = "//iam.googleapis.com/${google_iam_workload_identity_pool.pool.name}/providers/${google_iam_workload_identity_pool_provider.provider.workload_identity_pool_provider_id}"
}
