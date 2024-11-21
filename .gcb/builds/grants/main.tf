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

data "google_project" "project" {
}

locals {
  gce_service_account = "${data.google_project.project.number}-compute@developer.gserviceaccount.com"
  gcb_service_account = "${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

# We need a service account to run the builds.
# We use a dedicated account, as opposed to reusing the GCE or GCB account,
# because we want full control over its permissions.
resource "google_service_account" "integration-test-runner" {
  account_id   = "integration-test-runner"
  display_name = "Build and Run Integration Tests"
}

# The service account will need to write logs. That is needed so we can see the
# build output.
resource "google_project_iam_member" "sa-can-write-logs" {
  project = var.project
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.integration-test-runner.email}"
}

# The service account will need to read tarballs uploaded by `gcloud submit`.
resource "google_storage_bucket_iam_member" "sa-can-read-build-tarballs" {
  bucket = "${var.project}_cloudbuild"
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.integration-test-runner.email}"
}

# We will run integration tests related to secret manager. These require full
# control over the secrets.
resource "google_project_iam_member" "run-secret-manager-integration-tests" {
  project = var.project
  role    = "roles/secretmanager.admin"
  member  = "serviceAccount:${google_service_account.integration-test-runner.email}"
}

output "runner" {
  value = google_service_account.integration-test-runner.id
}
