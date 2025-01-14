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

# This service account is created externally. It is used for all the builds.
data "google_service_account" "integration-test-runner" {
  account_id = "integration-test-runner"
}

# The service account will need to read tarballs uploaded by `gcloud submit`.
resource "google_storage_bucket_iam_member" "sa-can-read-build-tarballs" {
  bucket = "${var.project}_cloudbuild"
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${data.google_service_account.integration-test-runner.email}"
}

# This service account is created externally. It is used to create test
# workflows. The integration tests runner needs permissions to act as this
# account.
data "google_service_account" "test-workflow-runner" {
  account_id = "test-workflow-runner"
}

# The service account will need to read tarballs uploaded by `gcloud submit`.
resource "google_service_account_iam_member" "sa-can-use-test-workflows-sa" {
  service_account_id = data.google_service_account.test-workflow-runner.id
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${data.google_service_account.integration-test-runner.email}"
}

output "runner" {
  value = data.google_service_account.integration-test-runner.id
}
