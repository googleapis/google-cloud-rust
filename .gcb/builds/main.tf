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
      version = "~> 6.0.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

# Enable services used by the integration tests.
module "services" {
  source  = "./services"
  project = var.project
}

# Create the resources we will need to run integration tests on.
module "resources" {
  source  = "./resources"
  project = var.project
  region  = var.region
}

# Create the service account needed for GCB and grant it the necessary
# permissions.
module "grants" {
  source      = "./grants"
  project     = var.project
  build_cache = module.resources.build-cache
}

# Create the GCB triggers.
module "triggers" {
  depends_on      = [module.services, module.resources, module.grants]
  source          = "./triggers"
  project         = var.project
  region          = var.region
  service_account = module.grants.runner
}
