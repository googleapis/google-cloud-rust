# Terraform for Integration Test Resources

This directory contains Terraform configurations to set up the resources needed for integration tests (Firestore, KMS, Buckets, etc.).

## Safe Usage for Personal Test Projects

By default, this configuration uses a remote GCS backend which tracks the state of the shared `rust-sdk-testing` project. **Do not run `terraform apply` directly with the remote backend if you are targeting a personal test project, as it may interfere with the shared state or disable services in the shared project.**

To safely use these Terraform scripts to set up resources in your personal test project:

1.  **Set your project variable**:
    ```bash
    PROJECT=$(gcloud config get project)
    ```

2.  **Override the backend to use local state**:
    Create a `backend_override.tf` file. This tells Terraform to store the state locally on your machine instead of the shared GCS bucket.
    ```bash
    cat > backend_override.tf <<EOF
    terraform {
      backend "local" {
        path = "${HOME}/${PROJECT}-rust-sdk.tfstate"
      }
    }
    EOF
    ```

3.  **Initialize Terraform**:
    ```bash
    terraform init -reconfigure
    ```

4.  **Plan and Apply**:
    
    ### Option A: For Local Testing (Recommended)
    If you only need the resources for running tests locally (and do not need to set up GCB triggers), target only the `services` and `resources` modules. This avoids the need to create CI-specific service accounts:
    
    ```bash
    terraform plan -var="project=${PROJECT}" -target=module.services -target=module.resources -out="/tmp/builds.plan"
    terraform apply "/tmp/builds.plan"
    ```
    
    ### Option B: For Full CI Replication
    If you want to replicate the full CI environment including triggers, you must first create a service account named `integration-test-runner` in your project, as the `grants` module expects it to exist:
    
    ```bash
    gcloud iam service-accounts create integration-test-runner \
        --display-name "Integration Test Runner"
    ```
    
    Then run the full apply:
    ```bash
    terraform plan -var="project=${PROJECT}" -out="/tmp/builds.plan"
    terraform apply "/tmp/builds.plan"
    ```

## Known Issues

-   **Services not enabled error**: If Terraform fails with an error that services like Firestore or KMS are not enabled, you may need to enable them manually using `gcloud services enable firestore.googleapis.com cloudkms.googleapis.com` and retry. This can happen because Terraform checks if the service is active before the enablement has fully propagated in the Google Cloud backend.
