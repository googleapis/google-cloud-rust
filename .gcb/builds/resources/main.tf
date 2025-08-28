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

variable "region" {
  type = string
}

# Create a bucket to cache build artifacts.
resource "google_storage_bucket" "build-cache" {
  name          = "${var.project}-build-cache"
  force_destroy = false
  # This prevents Terraform from deleting the bucket. Any plan to do so is
  # rejected. If we really need to delete the bucket we must take additional
  # steps.
  lifecycle {
    prevent_destroy = true
  }

  # The bucket configuration.
  location                    = "US-CENTRAL1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  versioning {
    enabled = false
  }
  # Remove objects older than 90d. It is unlikely that any build artifact is
  # usefull after that long, and we can always rebuild them if needed.
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# Create a Firestore database for the integration tests.
resource "google_firestore_database" "default" {
  project     = var.project
  name        = "(default)"
  location_id = "us-central1"
  type        = "FIRESTORE_NATIVE"
}

# Create a KMS Key Ring and key for the storage sample tests.
resource "google_kms_key_ring" "us-central1" {
  name     = "us-central1"
  location = "us-central1"
}

# A crypto key for the storage examples.
resource "google_kms_crypto_key" "storage-examples" {
  name     = "storage-examples"
  key_ring = google_kms_key_ring.us-central1.id
  # Rotate every 10 days
  rotation_period = "864000s"
}

# Get the service account for Cloud Storage in the current project.
data "google_storage_project_service_account" "gcs-account" {
}

# Grant Google Cloud Storage (in the project) permissions to use the example key.
resource "google_kms_crypto_key_iam_member" "storage-examples" {
  crypto_key_id = google_kms_crypto_key.storage-examples.id
  role          = "roles/cloudkms.cryptoKeyEncrypter"
  member        = "serviceAccount:${data.google_storage_project_service_account.gcs-account.email_address}"
}

output "build-cache" {
  value = resource.google_storage_bucket.build-cache.id
}
