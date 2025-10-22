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

variable "project" {}

data "google_project" "project" {
}

# The service account we use as our principal for testing service account credentials.
data "google_service_account" "test-sa-creds-principal" {
  account_id = "test-sa-creds"
}

# Key rotation for the service account key.
resource "time_rotating" "key_rotation" {
  rotation_days = 60
}

# Generate a key for the test service account credentials.
resource "google_service_account_key" "test-sa-creds-principal-key" {
  service_account_id = data.google_service_account.test-sa-creds-principal.name

  # Service account keys expire after 90 days, due to our org policy. So have
  # terraform rotate this key after 60 days.
  #
  # Note that someone/something must run terraform periodically.
  # TODO(#926) - Ideally we set up a job for this.
  keepers = {
    rotation_time = time_rotating.key_rotation.rotation_rfc3339
  }
}

# This secret stores the ADC json for the principal testing service account credentials.
resource "google_secret_manager_secret" "test-sa-creds-json-secret" {
  secret_id = "test-sa-creds-json"
  replication {
    auto {}
  }
}

# Store the test service account key in secret manager.
resource "google_secret_manager_secret_version" "test-sa-creds-json-secret-version" {
  secret         = google_secret_manager_secret.test-sa-creds-json-secret.id
  secret_data_wo = base64decode(google_service_account_key.test-sa-creds-principal-key.private_key)
}

# The "secret" that will be accessed by the principal testing service account
# credentials.
#
# Note that this is not really a "secret", in that we are not trying to hide its
# contents.
#
# In order to validate our credentials types, we need a GCP resource we can set
# fine-grained ACL on. We have picked Secret Manager secrets for this purpose.
#
resource "google_secret_manager_secret" "test-sa-creds-secret" {
  secret_id = "test-sa-creds-secret"
  replication {
    auto {}
  }
}

# Add a value to the secret.
resource "google_secret_manager_secret_version" "test-sa-creds-secret-version" {
  secret = google_secret_manager_secret.test-sa-creds-secret.id

  # We do not care that the value is public. We are just testing ACLs.
  secret_data_wo = "service_account"
}

# Set up secret permissions for service account credentials.
resource "google_secret_manager_secret_iam_member" "test-sa-creds-secret-member" {
  project   = var.project
  secret_id = google_secret_manager_secret.test-sa-creds-secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${data.google_service_account.test-sa-creds-principal.email}"
}

output "adc_secret" {
  value = google_secret_manager_secret.test-sa-creds-json-secret
}
