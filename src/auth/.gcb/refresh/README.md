# A Cloud Run job to rerun terraform

We want to periodically rerun terraform in order to:

1. Rotate service account keys, which eventually expire

1. Make sure our terraform configuration is up-to-date

To do this, we will deploy a job to Cloud Run. We follow the instructions in
[Build and create a Shell job in Cloud Run][run-quickstart]. The service account
running the job has permissions to create and update integration test resources.

## Deploy

Deploy the image:

```sh
gcloud run jobs deploy refresh \
    --project=rust-auth-testing \
    --region us-central1 \
    --source . \
    --tasks 1 \
    --max-retries 0 \
    --service-account=terraform-runner@rust-auth-testing.iam.gserviceaccount.com
```

The default of 512 MiB is not enough apparently. Ask for more memory:

```sh
gcloud run jobs update refresh \
    --project=rust-auth-testing \
    --region=us-central1 \
    --memory=2GiB
```

## Run

We can manually trigger the job with:

```sh
gcloud run jobs execute refresh \
    --project=rust-auth-testing \
    --region=us-central1
```

## Next steps

1. Run the function on a schedule via Cloud Scheduler

1. (stretch) Have terraform control the deployment and scheduling

   We manually deployed the function once. It could probably be managed via
   terraform. It doesn't have to be though.

[run-quickstart]: https://cloud.google.com/run/docs/quickstarts/jobs/build-create-shell
