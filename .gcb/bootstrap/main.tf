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

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.44.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

# Re-import the state of the bucket from GCP. Normally one would store
# terraform's state in a global backend, such as Google Cloud Storage. But this
# is the terraform configuration to bootstrap such a backend. While re-importing
# the state of each resource would not scale as the number of resources grows,
# re-importing a single bootstrap resource seems manageable.
import {
  to = google_storage_bucket.terraform
  id = "${var.project}-terraform"
}

# Create a bucket to store the Terraform data.
resource "google_storage_bucket" "terraform" {
  name          = "${var.project}-terraform"
  force_destroy = false
  # This prevents Terraform from deleting the bucket. Any plan to do so is
  # rejected. If we really need to delete the bucket we must take additional
  # steps.
  lifecycle {
    prevent_destroy = true
  }

  # The bucket configuration.
  location                    = "US"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  # Keep multiple versions of each object so we can recover if needed.
  versioning {
    enabled = true
  }
  # Tidy up archived objects after a year. They are small, so there is no need
  # to rush.
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 365
      with_state                 = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
}
