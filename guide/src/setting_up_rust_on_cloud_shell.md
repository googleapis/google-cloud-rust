# Setting up Rust on Cloud Shell

Cloud Shell is a great environment to run small examples and tests.

## Start up Cloud Shell

1. Open <https://shell.cloud.google.com> to start a new shell.

1. Select a project.

## Configure Rust

1. [Cloud Shell] comes with [rustup] pre-installed. You can use it to install and configure the default version of Rust:

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

1. Add the [Secret Manager] client library to the new project

   ```shell
   cargo add gcp-sdk-secretmanager-v1 --features unstable-stream
   ```

1. Add the [tokio] crate to the new project

   ```shell
   cargo add tokio --features macros
   ```

1. Edit `src/main.rs` in your project to use the Secret Manager client library:

```rust,ignore,noplayground
{{#include ../samples/src/bin/getting_started.rs:all}}
```

<!-- markdownlint-disable MD029 -->
6. Run your program, replacing `[PROJECT ID]` with the id of your project:

   ```shell
   cargo run [PROJECT ID]
   ```
<!-- markdownlint-enable MD029 -->

[cloud shell]: https://cloud.google.com/shell
[rustup]: https://rust-lang.github.io/rustup/
