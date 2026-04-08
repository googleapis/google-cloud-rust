# Highlights the observability features in the Rust SDK

This directory contains a demo application demonstrating how to deploy Rust
applications to Cloud Run and monitor them with Google Cloud AppHub.

## Building and Deploying

Because this application relies on other crates in the Rust workspace, you must
build the Docker image from the root of the workspace.

1. Ensure you are authenticated with Google Cloud:

   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
   ```

1. Create an Artifact Registry repository (if you don't already have one):

   ```bash
   gcloud artifacts repositories create cloud-run-apps \
     --repository-format=docker \
     --location=us-central1 \
     --description="Docker repository for Cloud Run apps"
   ```

1. Grant Cloud Run permission to read from the repository (using the default
   Compute Engine service account):

   ```bash
   PROJECT_NUMBER=$(gcloud projects describe ${GOOGLE_CLOUD_PROJECT} --format="value(projectNumber)")
   gcloud artifacts repositories add-iam-policy-binding cloud-run-apps \
     --location=us-central1 \
     --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
     --role="roles/artifactregistry.reader"
   ```

1. Build the Docker image using Google Cloud Build (run from the workspace
   root):

   ```bash
   gcloud builds submit . --config demos/cloud-run-o11y/cloudbuild.yaml
   ```

1. Deploy the built image to Cloud Run:

   ```bash
   gcloud run deploy cloud-run-o11y \
     --image us-central1-docker.pkg.dev/${GOOGLE_CLOUD_PROJECT}/cloud-run-apps/demo-cloud-run-o11y \
     --allow-unauthenticated \
     --region us-central1 \
     --set-env-vars=GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT}
   ```
