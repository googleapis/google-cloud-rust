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

# How to initialize a client

The Google Cloud Client Libraries for Rust use "clients" as the main abstraction
to interface with specific services. Clients are implemented as Rust structs,
with methods corresponding to each RPC offered by the service. In other words,
to use a Google Cloud service using the Rust client libraries you need to first
initialize a client.

## Prerequisites

In this guide we will initialize a client and then use the client to make a
simple RPC. To make this guide concrete, we will use the [Secret Manager API].
The same structure applies to any other service in Google Cloud.

We recommend you follow one of the "Getting Started" guides for Secret Manager
before attempting to use the client library, such as how to [Create a secret].
These guides cover service specific concepts in more detail, and provide
detailed instructions with respect to project prerequisites than we can fit in
this guide.

We also recommend you follow the instructions in the
[Authenticate for using client libraries] guide. This guide will show you how to
login to configure the [Application Default Credentials] used in this guide.

## Dependencies

As it is usual with Rust, you must declare the dependency in your `Cargo.toml`
file. We use:

```toml
{{#include ../samples/Cargo.toml:secretmanager}}
```

You (1) call `Client::builder()` to obtain an appropriate
[`ClientBuilder`][gax-client-builder] and (2) call `build()` on that builder to
create a client.

The following creates a client with the default configuration, which is designed
to meet requirements for most use cases.

```rust,ignore,noplayground
{{#include ../samples/tests/initialize_client.rs:new-client}}
```

Once successfully initialized, you can use this client to make RPCs:

```rust,ignore,noplayground
{{#include ../samples/tests/initialize_client.rs:make-rpc}}
```

______________________________________________________________________

## Full program

Putting all this code together into a full program looks as follows:

```rust,ignore,noplayground
{{#include ../samples/tests/initialize_client.rs:all}}
```

[application default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
[authenticate for using client libraries]: https://cloud.google.com/docs/authentication/client-libraries
[create a secret]: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
[gax-client-builder]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/client_builder/struct.ClientBuilder.html
[secret manager api]: https://cloud.google.com/secret-manager
