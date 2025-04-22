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

# Iterating Google API List methods

The standard Google API List method follows the pagination guideline defined by
[AIP-158](https://google.aip.dev/158). Each call to a List method for a resource
returns a "page" of resource items (e.g. Secrets) along with a
"next-page token" that can be passed to the List method to retrieve the next
page.

The Google Cloud Client Libraries for Rust provide an adapter to converts the
list RPCs as defined by [AIP-4233](https://google.aip.dev/client-libraries/4233)
into a \[futures::Stream\] that can be iterated over in an async fashion.

## Prerequisites

The guide uses the [Secret Manager] service. That makes the examples more
concrete and therefore easier to follow. With that said, the same ideas work for
any other service.

You may want to follow the service [quickstart]. This guide will walk you
through the steps necessary to enable the service, ensure you have logged in,
and that your account has the necessary permissions.

## Dependencies

As it is usual with Rust, you must declare the dependency in your
`Cargo.toml` file. We use:

```toml
{{#include ../samples/Cargo.toml:secretmanager}}
```

This guide also requires the optional feature `unstable-stream` in the
`google-cloud-gax` dependency.

```toml
{{#include ../samples/Cargo.toml:gax}}
```

## Iterating List methods

To iterate the pages of a List method, we use the provided Paginator `next`
function. Paginator will fill in the next page token of the next List RPC as
needed.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-iterate-pages}}
```

To iterate as a stream, we use the provided optional feature `into_stream`
function.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-stream-pages}}
```

Paginator also provides the `items` function to iterate the resource items
(e.g. Secrets).

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-iterate-items}}
```

Similarly, use the `into_stream` function to stream the items.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-stream-items}}
```

## Resuming List methods by setting next page token

In some cases such as an interrupted List operation, we can set the next page
token to start resume paginating from a specific page.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-page-token}}
```

[quickstart]: https://cloud.google.com/secret-manager/docs/quickstart
[secret manager]: https://cloud.google.com/secret-manager
