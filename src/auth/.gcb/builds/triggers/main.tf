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
variable "region" {}
variable "sa_adc_secret" {}
variable "api_key_secret" {}
variable "external_account_project" {}
variable "external_account_service_account_id" {}
variable "workload_identity_audience" {}

# This is used to retrieve the project number. The project number is embedded in
# certain P4 (Per-product per-project) service accounts.
data "google_project" "project" {
}

resource "google_project_service" "cloudbuild" {
  project = var.project
  service = "cloudbuild.googleapis.com"

  timeouts {
    create = "30m"
    update = "40m"
  }

  disable_dependent_services = true
}

# Create a bucket used by Cloud Build.
resource "google_storage_bucket" "cloudbuild" {
  name                        = "${var.project}_cloudbuild"
  uniform_bucket_level_access = true
  force_destroy               = false
  # This prevents Terraform from deleting the bucket. Any plan to do so is
  # rejected. If we really need to delete the bucket we must take additional
  # steps.
  lifecycle {
    prevent_destroy = true
  }

  # The bucket configuration.
  location      = "US-CENTRAL1"
  storage_class = "STANDARD"
  versioning {
    enabled = false
  }
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

# This service account is created externally. It is used for integration test builds.
data "google_service_account" "integration-test-runner" {
  account_id = "integration-test-runner"
}

# This service account is created externally. It is used for the terraform build.
data "google_service_account" "terraform-runner" {
  account_id = "terraform-runner"
}

# The service account will need to read tarballs uploaded by `gcloud submit`.
resource "google_storage_bucket_iam_member" "sa-can-read-build-tarballs" {
  bucket = google_storage_bucket.cloudbuild.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${data.google_service_account.integration-test-runner.email}"
}

# The service account will need to read and write into the build cache.
resource "google_storage_bucket_iam_member" "sa-can-use-build-cache" {
  bucket = google_storage_bucket.build-cache.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${data.google_service_account.integration-test-runner.email}"
}

# The integration test runner needs access to the ADC JSON secrets
resource "google_secret_manager_secret_iam_member" "test-adc-json-secret-member" {
  project   = var.project
  secret_id = var.sa_adc_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${data.google_service_account.integration-test-runner.email}"
}

# The integration test runner needs access to the API key secret
resource "google_secret_manager_secret_iam_member" "test-api-key-secret-member" {
  project   = var.project
  secret_id = var.api_key_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${data.google_service_account.integration-test-runner.email}"
}

locals {
  # Google Cloud Build installs an application on the GitHub organization or
  # repository. This id is hard-coded here because there is no easy way [^1] to
  # manage that installation via terraform.
  #
  # [^1]: there is a way, described in [Connecting a Gitub host programmatically]
  #     but I would not call that "easy". It requires (for example) manually
  #     creating a personally access token (PAT) on GitHub, and storing that
  #     in the Terraform file.
  # [Connecting a Gitub host programmatically]: https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github?generation=2nd-gen#terraform
  #
  # There is one GCB App installation shared between `rust-auth-testing` and
  # `rust-sdk-testing`.
  #
  gcb_app_installation_id = 1168573

  # Google Cloud uses Secret Manager to save the Github access token. Similar to
  # the previous problem. It is much easier to use the UI to create the
  # connection and just record it here.
  #
  # This secret is shared between `rust-auth-testing` and `rust-sdk-testing`.
  # It was manually copied from one test project to the other.
  #
  # ```
  # SECRET=github-github-oauthtoken-319d75
  # gcloud --project=rust-sdk-testing secrets versions access latest --secret=${SECRET} | \
  #     gcloud --project=rust-auth-testing secrets create ${SECRET} --data-file=-
  # ```
  #
  gcb_secret_name = "projects/${var.project}/secrets/github-github-oauthtoken-319d75/versions/latest"

  # Add to this list if you want to have more triggers.
  builds = {
    integration = {}
  }
}

resource "google_cloudbuildv2_connection" "github" {
  project  = var.project
  location = var.region
  name     = "github"

  github_config {
    app_installation_id = local.gcb_app_installation_id
    authorizer_credential {
      oauth_token_secret_version = local.gcb_secret_name
    }
  }
}

resource "google_cloudbuildv2_repository" "main" {
  project           = var.project
  location          = var.region
  name              = "googleapis-google-cloud-rust"
  parent_connection = google_cloudbuildv2_connection.github.name
  remote_uri        = "https://github.com/googleapis/google-cloud-rust.git"
}

resource "google_cloudbuild_trigger" "pull-request" {
  for_each = tomap(local.builds)
  location = var.region
  name     = "gcb-pr-auth-${each.key}"
  filename = "src/auth/.gcb/${each.key}.yaml"
  tags     = ["pull-request", "name:${each.key}"]

  service_account = data.google_service_account.integration-test-runner.id
  substitutions = {
    _EXTERNAL_ACCOUNT_PROJECT               = var.external_account_project
    _EXTERNAL_ACCOUNT_SERVICE_ACCOUNT_EMAIL = "${var.external_account_service_account_id}@${var.external_account_project}.iam.gserviceaccount.com"
    _WORKLOAD_IDENTITY_AUDIENCE             = var.workload_identity_audience
  }

  repository_event_config {
    repository = google_cloudbuildv2_repository.main.id
    pull_request {
      branch          = "^main$"
      comment_control = "COMMENTS_ENABLED_FOR_EXTERNAL_CONTRIBUTORS_ONLY"
    }
  }

  include_build_logs = "INCLUDE_BUILD_LOGS_WITH_STATUS"
}

resource "google_cloudbuild_trigger" "post-merge" {
  for_each = tomap(local.builds)
  location = var.region
  name     = "gcb-pm-auth-${each.key}"
  filename = "src/auth/.gcb/${each.key}.yaml"
  tags     = ["post-merge", "push", "name:${each.key}"]

  service_account = data.google_service_account.integration-test-runner.id
  substitutions = {
    _EXTERNAL_ACCOUNT_PROJECT               = var.external_account_project
    _EXTERNAL_ACCOUNT_SERVICE_ACCOUNT_EMAIL = "${var.external_account_service_account_id}@${var.external_account_project}.iam.gserviceaccount.com"
    _WORKLOAD_IDENTITY_AUDIENCE             = var.workload_identity_audience
  }

  repository_event_config {
    repository = google_cloudbuildv2_repository.main.id
    push {
      branch = "^main$"
    }
  }

  include_build_logs = "INCLUDE_BUILD_LOGS_WITH_STATUS"
}

resource "google_pubsub_topic" "terraform_runner_topic" {
  name = "terraform-runner"
}

resource "google_pubsub_subscription" "terraform_runner_sub" {
  name  = "terraform-sub"
  topic = google_pubsub_topic.terraform_runner_topic.name
}

resource "google_cloud_scheduler_job" "job" {
  name        = "terraform-job"
  description = "Periodically sync terraform build"
  schedule    = "0 0 * * 0" # Once a week at midnight on Sunday.

  pubsub_target {
    topic_name = google_pubsub_topic.terraform_runner_topic.id
    data       = base64encode("sync")
  }
}

resource "google_cloudbuild_trigger" "pubsub-trigger" {
  location = var.region
  name     = "gcb-pubsub-terraform"
  filename = "src/auth/.gcb/terraform.yaml"
  tags     = ["scheduler", "name:terraform"]

  service_account = data.google_service_account.terraform-runner.id

  pubsub_config {
    topic = google_pubsub_topic.terraform_runner_topic.id
  }

  source_to_build {
    repository = google_cloudbuildv2_repository.main.id
    ref        = "refs/heads/main"
    repo_type  = "GITHUB"
  }
}
