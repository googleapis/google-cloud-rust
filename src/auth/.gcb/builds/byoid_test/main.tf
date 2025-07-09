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

variable "byoid_project" {
  type = string
}

provider "google" {
  alias   = "byoid_project"
  project = var.byoid_project
}

data "google_service_account" "service_account" {
  project    = var.byoid_project
  account_id = "testsa"
}

# Key rotation for the service account key.
resource "time_rotating" "key_rotation" {
  rotation_days = 60
}

resource "google_service_account_key" "key" {
  service_account_id = data.google_service_account.service_account.name
  keepers = {
    rotation_time = time_rotating.key_rotation.rotation_rfc3339
  }
}

resource "google_secret_manager_secret" "secret" {
  project   = var.project
  secret_id = "byoid-sa-key"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "version" {
  secret      = google_secret_manager_secret.secret.id
  secret_data = base64decode(google_service_account_key.key.private_key)
}

resource "google_project_iam_member" "secret_accessor" {
  project = var.project
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${data.google_service_account.service_account.email}"
}

output "sa_key_secret_resource_id" {
  description = "The resource ID of the Secret Manager secret containing the service account key."
  value       = google_secret_manager_secret.secret.id
}