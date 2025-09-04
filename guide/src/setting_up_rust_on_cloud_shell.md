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

# Setting up Rust on Cloud Shell

Cloud Shell is a great environment to run small examples and tests. This guide
shows you how to configure Rust and install one of the Cloud Client Libraries in
Cloud Shell.

## Start up Cloud Shell

1. In the Google Cloud console [project selector], select a project.

1. Open <https://shell.cloud.google.com> to start a new shell. You might be
   prompted to [authorize Cloud Shell] to use your credentials for Google Cloud
   API calls.

## Configure Rust

1. [Cloud Shell] comes with [rustup] pre-installed. You can use it to install
   and configure the default version of Rust:

   ```shell
   rustup default stable
   ```

1. Confirm that you have the most recent version of Rust installed:

   ```shell
   cargo --version
   ```

## Install Rust client libraries in Cloud Shell

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

1. Run your program, supplying your Google Cloud Platform project's ID:

   ```shell
   PROJECT_ID=$(gcloud config get project)
   cargo run ${PROJECT_ID}
   ```

   The program will print the secrets associated with your project ID. If you
   don't see any secrets, you might not have any in Secret Manager. You can
   [create a secret] and rerun the program, and you should see the secret
   printed in the output.

   You might see a "no space left on device" error. Run the following to remove
   build artifacts:

   ```shell
   cargo clean
   ```

   Alternatively, you can build in release mode, which should also use less disk
   space:

   ```shell
   cargo build --release
   ```

<!-- markdownlint-enable MD029 -->

[authorize cloud shell]: https://cloud.google.com/shell/docs/auth
[cloud shell]: https://cloud.google.com/shell
[create a secret]: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
[google-cloud-gax]: https://crates.io/crates/google-cloud-gax
[project selector]: https://console.cloud.google.com/projectselector2/home/dashboard
[rustup]: https://rust-lang.github.io/rustup/
[secret manager]: https://cloud.google.com/secret-manager/docs/overview
[tokio]: https://crates.io/crates/tokio
