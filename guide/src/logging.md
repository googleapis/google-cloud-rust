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

# Enable debug logs

Sometimes it is easier to troubleshoot applications if all the client library
requests and responses are logged to the console. This guide shows you how to
enable the logging facilities in the client library.

<div class="warning">
These logs are meant to be used for debugging applications. The logs include
full requests and response messages, which may include sensitive information.
If you decide to enable these logs in production, consider the security and
privacy implications before doing so.
</div>

## Prerequisites

This guide uses the [Secret Manager API]. The same concepts apply to the client
libraries for other services. You may want to follow the [service quickstart],
which shows you how to enable the service.

For complete setup instructions for the Rust libraries, see
[Setting up your development environment](/setting_up_your_development_environment.md).

### Dependencies

You must declare the dependencies in your `Cargo.toml` file:

```shell
cargo add google-cloud-secretmanager-v1 google-cloud-gax
```

## Enable logging

The Rust client libraries use Tokio's [tracing] crate to collect scoped,
structured, and async-aware diagnostics. The tracing separate sources of
diagnostics (such as the Rust client libraries) from the components that collect
these diagnostics using the `Subscriber` trait. There are many implementations
available for the `Subscriber`. In this example we will use the [fmt] subscriber
included with the `tracing-subscriber` crate.

First, add a dependency on the `tracing-subscriber` crate:

```shell
cargo add tracing tracing-subscriber
```

This example receives the project id as a function parameter:

```rust,ignore
{{#include ../samples/src/logging.rs:rust_logging_parameters}}
```

Introduce a few use declarations to make the example more readable:

```rust,ignore
{{#include ../samples/src/logging.rs:rust_logging_parameters}}
```

Initialize the default tracing subscriber:

```rust,ignore
{{#include ../samples/src/logging.rs:rust_logging_init}}
```

Initialize a client with tracing enabled. Note the call to `.with_tracing()`:

```rust,ignore
{{#include ../samples/src/logging.rs:rust_logging_client}}
```

Then use the client to send a request:

```rust,ignore
{{#include ../samples/src/logging.rs:rust_logging_call}}
```

### Expected output

The output (slightly edited for readability) will include a line such as:

```text
2025-11-03T14:17:31.759452Z  INFO list_secrets{self=SecretManagerService ...
```

This line includes the request:

```text
req=ListSecretsRequest { parent: "projects/... }
```

and the response:

```text
return=Ok(Response { parts: ..., body: ListSecretsResponse { ...
```

## More information

The default subscriber created via `tracing_subscriber::fmt::init()` can be
configured dynamically using the `RUST_LOG` environment variable. See
[its documentation][fmt::init] for details.

[fmt]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/index.html
[fmt::init]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/fn.init.html
[secret manager api]: https://cloud.google.com/secret-manager
[service quickstart]: https://cloud.google.com/secret-manager/docs/quickstart
[tracing]: https://docs.rs/tracing/latest/tracing/index.html
