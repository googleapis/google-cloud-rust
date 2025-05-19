# Google Cloud client libraries for Rust: endurance test

This directory contains a test to verify the client libraries work well in
long-running applications. Our unit and integration tests are (and should be)
short-lived. These test may miss bugs that only manifest themselves when the
application runs for a long time, examples include:

- Failure to refresh access tokens in the authentication library.
- Transient errors that appear rarely and are not handled correctly.
- Race conditions that only appear rarely.
- Resource leaks, including memory, file descriptors, or any other resource.

While Rust makes it hard to introduce some of the problems described above, it
is not impossible to do so. While running a test for a long time does not
guarantee that such bugs will be found, it makes it less likely that such bugs
do exist. In the worst case, such a test provides scaffolding to reproduce any
bugs reported by our customers.

## Basic Deployment

For a one-time run we can use GCE to run the program, and manually configure the
resources and permissions to run the program. If we wanted to run this program
continuously, then we should consider a more advanced deployment, such as GKE.

## Pre-requisites

You will need a project with billing, the secret manager, and GCE enabled. You
will need a GCE VM instance, and the default GCE service account will need to
have the secret manager admin role.

Capture the project id:

```shell
export PROJECT_ID=$(gcloud config get project)
```

Make sure the service account has the necessary privileges:

```shell
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')
ACCOUNT=${PROJECT_NUMBER}-compute@developer.gserviceaccount.com
gcloud projects add-iam-policy-binding ${PROJECT_ID} --role=roles/secretmanager.viewer  --member=serviceAccount:${ACCOUNT}
gcloud projects add-iam-policy-binding ${PROJECT_ID} --role=roles/secretmanager.secretAccessor  --member=serviceAccount:${ACCOUNT}
gcloud projects add-iam-policy-binding ${PROJECT_ID} --role=roles/secretmanager.secretVersionManager  --member=serviceAccount:${ACCOUNT}
```

Create the testing resources:

```shell
for i in $(seq 0 19); do
    id=$(printf "secret-%03d" $i)
    gcloud secrets create --labels=endurance-test=true ${id}
done
```

## Deployment

On a GCE instance. Install the development tools

```shell
sudo apt install gcc rustup git vim
rustup update stable
```

Clone the code and run a test:

```shell
git clone https://github.com/googleapis/google-cloud-rust.git
cd google-cloud-rust
cargo run --release -p endurance-test
```

That should print some progress metrics every 10 seconds or so. If it fails to
start or cannot successfully update and access the secrets, then check the
permissions.

Once it is working we want to run it in the background, with the logs going to
Cloud Logging. First, create a service unit file:

```shell
sed "s/@PROJECT@/$PROJECT_ID/" src/endurance-test/endurance-test.service >~/.config/systemd/user/endurance-test.service
```

Start the program as a background task:

```shell
systemctl --user status endurance-test.service
```

## Future Work

It would be nice to report metrics, such as successful request counts and
request latency to Cloud Monitoring.

If we wanted to deploy and run this continuously, it would be nice to use GKE
and Cloud Build to automatically deploy new versions.
