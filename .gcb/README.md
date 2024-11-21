# Google Cloud Build support

This directory contains configuration files and scripts to support GCB (Google
Cloud Build) builds.

We generally prefer GHA (GitHub Actions) for CI (Continuous Integration):
building the code, running unit test, run any linters or formatters.

We use GCB for integration tests against production.

GCB can perform these operations with relatively simple management for
authentication and authorization: the builds run using a service account
specific to our project. We can grant this service account the necessary
permissions to act on the test resources.

In contrast, if we wanted to use GHA for the same role, we would need to either
(1) download a service account key file and install it as a GHA secret, and
manually rotate this secret, or (2) configure workload identify federation
between GitHub and our project. Neither approach is very easy to reason about
from a security perspective.

## Managing Resources for Integration Test

Integration tests need resources in production. We will need pre-existing
databases, storage buckets, service accounts, and the configuration for the
builds themselves.

We have chosen Terraform to manage these resources. That makes it easy to audit
them, recreate the resources when needed, and we can always change to a
different IaaC platform if needed.
