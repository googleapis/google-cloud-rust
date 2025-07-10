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

variable "service_account_id" {
  type = string
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
  account_id = "integration-test-runner"
}

# Grant the GCB runner service account the ability to mint tokens for any
# service account in this project. This allows the test code to use the
# `projects/-/serviceAccounts/...` syntax, which works for both local
# user credentials and the GCB service account.
resource "google_project_iam_member" "token_creator" {
  provider = google.external_account_project
  project  = var.project
  role     = "roles/iam.serviceAccountTokenCreator"
  member   = "serviceAccount:${data.google_service_account.build_runner_service_account.email}"
}

resource "google_iam_workload_identity_pool" "pool" {
  provider                  = google.external_account_project
  project                   = var.project
  workload_identity_pool_id = "test-pool"
  display_name              = "Test Workload Identity Pool"
  description               = "For external account integration tests"
}

resource "google_iam_workload_identity_pool_provider" "provider" {
  provider                           = google.external_account_project
  project                            = var.project
  workload_identity_pool_id          = google_iam_workload_identity_pool.pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "google-idp"
  display_name                       = "Google IDP"
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

output "audience" {
  value = "//iam.googleapis.com/${google_iam_workload_identity_pool.pool.name}/providers/${google_iam_workload_identity_pool_provider.provider.workload_identity_pool_provider_id}"
}

