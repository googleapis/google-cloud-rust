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

# Override the default endpoint

The Google Cloud client libraries for Rust automatically configure the endpoint
for each service. Some applications may need to override the default endpoint
either because their network has specific requirements, or because they wish to
use regional versions of the service. This guide shows you how to override the
default.

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

## The default endpoint

First, review how to use the client libraries with the default endpoint. Add
some use declarations to simplify the rest of the example:

```rust,ignore
{{#include ../samples/src/endpoint/default.rs:rust_endpoint_default_use}}
```

Initialize the client using the defaults:

```rust,ignore
{{#include ../samples/src/endpoint/default.rs:rust_endpoint_default_client}}
```

Use this client as usual:

```rust,ignore
{{#include ../samples/src/endpoint/default.rs:rust_endpoint_default_call}}
```

The project ID is received as a parameter:

```rust,ignore
{{#include ../samples/src/endpoint/default.rs:rust_endpoint_default_parameters}}
```

## Override the default endpoint

In this example we configure the client library to use secret manager's
[regional endpoints]. The same override can be used to configure the endpoint
with one of the [private access options], or for [locational endpoints] in the
services that support them.

As before, write an example that receives the project ID and region as
parameters:

```rust,ignore
{{#include ../samples/src/endpoint/regional.rs:rust_endpoint_regional_parameters}}
```

Add some use declarations to simplify the code:

```rust,ignore
{{#include ../samples/src/endpoint/regional.rs:rust_endpoint_regional_use}}
```

Initialize the client using the target endpoint:

```rust,ignore
{{#include ../samples/src/endpoint/regional.rs:rust_endpoint_regional_client}}
```

Use this client as usual:

```rust,ignore
{{#include ../samples/src/endpoint/regional.rs:rust_endpoint_regional_call}}
```

[locational endpoints]: https://docs.cloud.google.com/storage/docs/locational-endpoints
[private access options]: https://docs.cloud.google.com/vpc/docs/private-access-options
[regional endpoints]: https://cloud.google.com/sovereign-controls-by-partners/docs/regional-endpoints
[secret manager api]: https://cloud.google.com/secret-manager
[service quickstart]: https://cloud.google.com/secret-manager/docs/quickstart
