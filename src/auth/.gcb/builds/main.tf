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

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project               = var.project
  region                = var.region
  zone                  = var.zone
  user_project_override = true
  billing_project       = var.project
}

# Enable SecretManager
module "services" {
  source  = "./services"
  project = var.project
}

# Set up for the service account integration test.
module "service_account_test" {
  source  = "./service_account_test"
  project = var.project
}

# Set up for the API key integration test.
module "api_key_test" {
  source  = "./api_key_test"
  project = var.project
}

# Set up for the External Account integration test.
module "external_account_test" {
  source             = "./external_account_test"
  project            = var.external_account_project
  runner_project_id  = var.project
  service_account_id = var.external_account_service_account_id
}

# Create the GCB resources, connection, triggers, etc.
module "triggers" {
  depends_on                          = [module.service_account_test, module.api_key_test, module.external_account_test]
  source                              = "./triggers"
  project                             = var.project
  region                              = var.region
  sa_adc_secret                       = module.service_account_test.adc_secret
  api_key_secret                      = module.api_key_test.secret
  external_account_project            = var.external_account_project
  external_account_service_account_id = var.external_account_service_account_id
  workload_identity_audience          = module.external_account_test.audience
}
