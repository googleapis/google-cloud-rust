# Copyright 2026 Google LLC
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

output "services" {
  value = [
    google_project_service.aiplatform.id,
    google_project_service.bigquery.id,
    google_project_service.compute.id,
    google_project_service.cloudbuild.id,
    google_project_service.cloudscheduler.id,
    google_project_service.dns.id,
    google_project_service.firestore.id,
    google_project_service.kms.id,
    google_project_service.language.id,
    google_project_service.pubsub.id,
    google_project_service.secretmanager.id,
    google_project_service.workflows.id,
    google_project_service.speech.id,
    google_project_service.storage.id,
    google_project_service.sqladmin.id,
    google_project_service.telemetry.id,
    google_project_service.cloudtrace.id,
  ]
}
