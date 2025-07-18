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

The Google Cloud Client Libraries for Rust use *clients* as the main abstraction
to interface with specific services. Clients are implemented as Rust structs,
with methods corresponding to each RPC offered by the service. To use a Google
Cloud service using the Rust client libraries, you need to first initialize a
client.

## Prerequisites

In this guide you'll initialize a client and then use the client to make a
simple RPC. For this tutorial, you'll use the [Secret Manager API]. The same
structure applies to any other service in Google Cloud.

We recommend that you follow one of the Secret Manager getting started guides,
such as how to [Create a secret], before attempting to use the client library.
These guides cover service specific concepts in more detail, and provide
detailed guidance on project prerequisites.

We also recommend that you follow the instructions in the
[Authenticate for using client libraries] guide. This guide will show you how to
log in to configure the [Application Default Credentials] used in this guide.

## Dependencies

As usual with Rust, you must declare the dependency in your `Cargo.toml` file:

```toml
{{#include ../samples/Cargo.toml:secretmanager}}
```

To initialize a client, you first call `Client::builder()` to obtain an
appropriate [`ClientBuilder`][gax-client-builder] and then call `build()` on
that builder to create a client.

The following creates a client with the default configuration, which is designed
to meet requirements for most use cases.

```rust,ignore,noplayground
{{#include ../samples/tests/initialize_client.rs:new-client}}
```

Once the client is successfully initialized, you can use it to make RPCs:

```rust,ignore,noplayground
{{#include ../samples/tests/initialize_client.rs:make-rpc}}
```

This example shows a call to `list_locations`, which returns information about
the supported locations for the service (in this case, Secret Manager). The
output of the example should look something like this:

```shell
projects/123456789012/locations/europe-west8
projects/123456789012/locations/europe-west9
projects/123456789012/locations/us-east5
...
```

______________________________________________________________________

## Full program

Putting all this code together into a full program looks as follows:

```rust,ignore,noplayground
{{#include ../samples/tests/initialize_client.rs:all}}
```

## What's next

This guide showed you how to initialize a client using the Google Cloud Client
Libraries for Rust. For a more complex example of working with a service, check
out [Generate text using the Vertex AI Gemini API].

[application default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
[authenticate for using client libraries]: https://cloud.google.com/docs/authentication/client-libraries
[create a secret]: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
[gax-client-builder]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/client_builder/struct.ClientBuilder.html
[generate text using the vertex ai gemini api]: generate_text_using_the_vertex_ai_gemini_api.md
[secret manager api]: https://cloud.google.com/secret-manager
