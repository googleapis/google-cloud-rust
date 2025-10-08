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

# The API Keys service is used to create an API key
resource "google_project_service" "apikeys" {
  project = var.project
  service = "apikeys.googleapis.com"

  timeouts {
    create = "30m"
    update = "40m"
  }

  disable_dependent_services = true
}

# The language service is used to verify the API key
resource "google_project_service" "language" {
  project = var.project
  service = "language.googleapis.com"

  timeouts {
    create = "30m"
    update = "40m"
  }

  disable_dependent_services = true
}

resource "google_apikeys_key" "test-api-key" {
  name         = "test-key"
  display_name = "Test API Key"
  project      = var.project

  # Restrict the API Key to the one RPC we use to verify the credentials.
  restrictions {
    api_targets {
      service = "language.googleapis.com"
      methods = ["AnalyzeSentiment"]
    }
  }

  depends_on = [google_project_service.apikeys]
}

# This secret stores the test API key.
resource "google_secret_manager_secret" "test-api-key-secret" {
  secret_id = "test-api-key"
  replication {
    auto {}
  }
}

# Store the test API key in secret manager.
resource "google_secret_manager_secret_version" "test-api-key-secret-version" {
  secret         = google_secret_manager_secret.test-api-key-secret.id
  secret_data_wo = google_apikeys_key.test-api-key.key_string
}

output "secret" {
  value = google_secret_manager_secret.test-api-key-secret
}
