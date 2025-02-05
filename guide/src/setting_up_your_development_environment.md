# Setting up your development environment

Prepare your environment for [Rust] app development and deployment on
Google Cloud by installing the following tools.

## Install Rust

1. To install Rust, see [Getting Started][rust-getting-started].

1. Confirm that you have the most recent version of Rust installed:

   ```shell
   cargo --version
   ```

## Install an editor

The [Getting Started][rust-getting-started] guide links popular editor plugins
and IDEs, which provide the following features:

- Fully integrated debugging capabilities
- Syntax highlighting
- Code completion

## Install the Google Cloud CLI

The [Google Cloud CLI] is a set of tools for Google Cloud. It contains the
[`gcloud`](https://cloud.google.com/sdk/gcloud/)
and [`bq`](https://cloud.google.com/bigquery/docs/bq-command-line-tool)
command-line tools used to access Compute Engine, Cloud Storage,
BigQuery, and other services from the command line. You can run these
tools interactively or in your automated scripts.

To install the gcloud CLI, see [Installing the gcloud CLI](https://cloud.google.com/sdk/install).

## Install the Cloud Client Libraries for Rust

The [Cloud Client Libraries for Rust] is the idiomatic way for Rust developers
to integrate with Google Cloud services, such as Firestore and Secret Manager.

For example, to install the package for an individual API, such as the
Secret Manager API, do the following:

1. Change to your Rust project directory.

1. Use the Secret Manager package in your project:

   ```shell
   cargo add gcp-sdk-secretmanager-v1
   ```

Note: The source of the Cloud Client Libraries for Rust is
[on GitHub](https://github.com/googleapis/google-cloud-rust).

## Set up authentication

To use the Cloud Client Libraries in a local development environment, set
up Application Default Credentials.

```shell
gcloud auth application-default login
```

For more information, see
[Authenticate for using client libraries][authn-client-libraries].

## What's next

- Explore [authentication methods at Google].
- Browse the [documentation for Google Cloud products].

[authentication methods at google]: https://cloud.google.com/docs/authentication
[authn-client-libraries]: https://cloud.google.com/docs/authentication/client-libraries
[documentation for google cloud products]: https://cloud.google.com/products
[google cloud cli]: https://cloud.google.com/sdk/
[rust]: https://www.rust-lang.org/
[rust-getting-started]: https://www.rust-lang.org/learn/get-started
