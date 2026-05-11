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

variable "project" {}

locals {
  services = [
    "aiplatform.googleapis.com",
    "bigquery.googleapis.com",
    "compute.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com",
    "dns.googleapis.com",
    "firestore.googleapis.com",
    "cloudkms.googleapis.com",
    "language.googleapis.com",
    "pubsub.googleapis.com",
    "secretmanager.googleapis.com",
    "workflows.googleapis.com",
    "speech.googleapis.com",
    "storage.googleapis.com",
    "sqladmin.googleapis.com",
    "telemetry.googleapis.com",
    "cloudtrace.googleapis.com",
  ]
}

resource "google_project_service" "services" {
  for_each = toset(local.services)

  project = var.project
  service = each.value

  timeouts {
    create = "30m"
    update = "40m"
  }

  disable_dependent_services = true
}

moved {
  from = google_project_service.aiplatform
  to   = google_project_service.services["aiplatform.googleapis.com"]
}

moved {
  from = google_project_service.bigquery
  to   = google_project_service.services["bigquery.googleapis.com"]
}

moved {
  from = google_project_service.compute
  to   = google_project_service.services["compute.googleapis.com"]
}

moved {
  from = google_project_service.cloudbuild
  to   = google_project_service.services["cloudbuild.googleapis.com"]
}

moved {
  from = google_project_service.cloudscheduler
  to   = google_project_service.services["cloudscheduler.googleapis.com"]
}

moved {
  from = google_project_service.dns
  to   = google_project_service.services["dns.googleapis.com"]
}

moved {
  from = google_project_service.firestore
  to   = google_project_service.services["firestore.googleapis.com"]
}

moved {
  from = google_project_service.kms
  to   = google_project_service.services["cloudkms.googleapis.com"]
}

moved {
  from = google_project_service.language
  to   = google_project_service.services["language.googleapis.com"]
}

moved {
  from = google_project_service.pubsub
  to   = google_project_service.services["pubsub.googleapis.com"]
}

moved {
  from = google_project_service.secretmanager
  to   = google_project_service.services["secretmanager.googleapis.com"]
}

moved {
  from = google_project_service.workflows
  to   = google_project_service.services["workflows.googleapis.com"]
}

moved {
  from = google_project_service.speech
  to   = google_project_service.services["speech.googleapis.com"]
}

moved {
  from = google_project_service.storage
  to   = google_project_service.services["storage.googleapis.com"]
}

moved {
  from = google_project_service.sqladmin
  to   = google_project_service.services["sqladmin.googleapis.com"]
}

moved {
  from = google_project_service.telemetry
  to   = google_project_service.services["telemetry.googleapis.com"]
}

moved {
  from = google_project_service.cloudtrace
  to   = google_project_service.services["cloudtrace.googleapis.com"]
}
