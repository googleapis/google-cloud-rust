<!-- 
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Setting up your development environment

Prepare your environment for [Rust] app development and deployment on Google
Cloud by installing the following tools.

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
[`gcloud`](https://cloud.google.com/sdk/gcloud/) and
[`bq`](https://cloud.google.com/bigquery/docs/bq-command-line-tool) command-line
tools used to access Compute Engine, Cloud Storage, BigQuery, and other services
from the command line. You can run these tools interactively or in your
automated scripts.

To install the gcloud CLI, see
[Installing the gcloud CLI](https://cloud.google.com/sdk/install).

## Install the Cloud Client Libraries for Rust in a new project

The Cloud Client Libraries for Rust is the idiomatic way for Rust developers to
integrate with Google Cloud services, such as Secret Manager and Workflows.

For example, to use the package for an individual API, such as the Secret
Manager API, do the following:

1. Create a new Rust project:

   ```shell
   cargo new my-project
   ```

1. Change your directory to the new project:

   ```shell
   cd my-project
   ```

1. Add the [Secret Manager] client library to the new project:

   ```shell
   cargo add google-cloud-secretmanager-v1
   ```

   If you haven't already enabled the Secret Manager API, enable it in
   [APIs and services](https://console.cloud.google.com/apis) or by running the
   following command:

   ```shell
   gcloud services enable secretmanager.googleapis.com
   ```

1. Add the [google-cloud-gax] crate to the new project:

   ```shell
   cargo add google-cloud-gax
   ```

1. Add the [tokio] crate to the new project:

   ```shell
   cargo add tokio --features macros
   ```

1. Edit `src/main.rs` in your project to use the Secret Manager client library:

```rust,ignore,noplayground
{{#include ../samples/src/bin/getting_started.rs:all}}
```

<!-- markdownlint-disable MD029 -->

6. Build your program:

   ```shell
   cargo build
   ```

   The program should build without errors.

<!-- markdownlint-enable MD029 -->

Note: The source of the Cloud Client Libraries for Rust is
[on GitHub](https://github.com/googleapis/google-cloud-rust).

### Running the program

1. To use the Cloud Client Libraries in a local development environment, set up
   Application Default Credentials.

   ```shell
   gcloud auth application-default login
   ```

   For more information, see
   [Authenticate for using client libraries][authn-client-libraries].

1. Run your program, replacing `[PROJECT ID]` with the id of your project:

   ```shell
   cargo run [PROJECT ID]
   ```

   The program will print the secrets associated with your project ID. If you
   don't see any secrets, you might not have any in Secret Manager. You can
   [create a secret] and rerun the program, and you should see the secret
   printed in the output.

## What's next

- Explore [authentication methods at Google].
- Browse the [documentation for Google Cloud products].

[authentication methods at google]: https://cloud.google.com/docs/authentication
[authn-client-libraries]: https://cloud.google.com/docs/authentication/client-libraries
[create a secret]: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
[documentation for google cloud products]: https://cloud.google.com/products
[google cloud cli]: https://cloud.google.com/sdk/
[google-cloud-gax]: https://crates.io/crates/google-cloud-gax
[rust]: https://www.rust-lang.org/
[rust-getting-started]: https://www.rust-lang.org/learn/get-started
[secret manager]: https://cloud.google.com/secret-manager/docs/overview
[tokio]: https://crates.io/crates/tokio
