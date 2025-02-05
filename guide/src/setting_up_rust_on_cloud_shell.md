# Setting up Rust on Cloud Shell

Cloud Shell is a great environment to run small examples and tests.

## Start up Cloud Shell

1. Open <https://shell.cloud.google.com> to start a new shell.

1. Select a project.

## Configure Rust

1. [Cloud Shell] comes with [Rust] pre-installed. Configure the default version:

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

1. Edit your project to use the Secret Manager client library:

   ```shell
   cat >src/main.rs <<_EOF_
   #[tokio::main]
   async fn main() -> Result<(), Box<dyn std::error::Error>> {
       use gcp_sdk_secretmanager_v1::client::SecretManagerService;
       let project_id = std::env::args().nth(1).unwrap();
       let client = SecretManagerService::new().await?;

       let mut items = client
           .list_secrets(format!("projects/{project_id}"))
           .stream().await.items();
       while let Some(item) = items.next().await {
           println!("{}", item?.name);
       }
       Ok(())
   }
   _EOF_
   ```

1. Run your program, replacing `[PROJECT ID]` with the id of your project:

   ```shell
   cargo run [PROJECT ID]
   ```

[rust]: https://www.rust-lang.org/
