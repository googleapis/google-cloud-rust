# Auth Integration Tests

## Running Integration Tests

### In `rust-auth-testing`

The resources needed should already exist. We can just run the tests.

```sh
env GOOGLE_CLOUD_PROJECT=rust-auth-testing \
  cargo test --features run-integration-tests -p auth-integration-tests
```

### In your own test project

#### Create the Test Resources

Set your test project

```sh
PROJECT=$(gcloud config get project)
```

Create test service accounts. Our terraform configuration expects these to
already exist, for org policy reasons.

```sh
gcloud iam service-accounts create test-sa-creds \
    --display-name "Principal for testing service account credentials"
```

Navigate to the terraform root. For example:

```sh
cd ${HOME}/google-cloud-rust/src/auth/.gcb/builds
```

The terraform state for `rust-auth-testing` is stored in a GCS bucket. We tell
terraform to use a local backend when using our test project.

Override backend, and reinitialize terraform:

```sh
cat > backend_override.tf <<EOF
terraform {
  backend "local" {
    path = "${HOME}/${PROJECT}-rust-auth.tfstate"
  }
}
EOF
terraform init -reconfigure
```

Create the test resources only. We skip over any GCB set up which is irrelevant
to a test project.

```sh
terraform plan \
    -var="project=${PROJECT}" \
    -out="/tmp/builds.plan" \
    -target="module.api_key_test" \
    -target="module.service_account_test"

env GOOGLE_CLOUD_QUOTA_PROJECT=${PROJECT} \
    terraform apply "/tmp/builds.plan"
```

Run the tests:

```sh
env GOOGLE_CLOUD_PROJECT=${PROJECT} \
    cargo test --features run-integration-tests -p auth-integration-tests
```

If you are done with the resources, you can destroy them with:

```sh
terraform plan \
    -var="project=${PROJECT}" \
    -out="/tmp/builds.plan" \
    -target="module.api_key_test" \
    -target="module.service_account_test" \
    -destroy

env GOOGLE_CLOUD_QUOTA_PROJECT=${PROJECT} \
    terraform apply "/tmp/builds.plan"
```

## Test Design

For access token credentials, there are integration tests for each type of
principal (service account, authorized user, etc.).

For each principal we have:

- a secret in [SecretManager] containing the [ADC] JSON for this principal
  - the test runner service account can access this
  - so can any owners in the GCP project
- a secret in [SecretManager] containing test data
  - the test runner service account **cannot** access this
  - only the principal can access this secret

The steps in the test are:

1. The principal running the build pulls the ADC JSON from SecretManager.
1. We create a credential object from the ADC JSON.
1. We create a SecretManager client using these credentials.
1. We use this client to access the principal-specific secret.

[adc]: https://cloud.google.com/docs/authentication/application-default-credentials
[secretmanager]: https://cloud.google.com/security/products/secret-manager
