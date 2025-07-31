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

# Working with List operations

Some services return potentially large lists of items, such as rows or resource
descriptions. To keep CPU and memory usage under control, services return these
resources in `pages`: smaller subsets of the items with a continuation token to
request the next subset.

Iterating over items in this way can be tedious. The client libraries provide
adapters to convert the pages into asynchronous iterators. This guide will show
you how to work with these adapters.

## Prerequisites

The guide uses the [Secret Manager] service. That makes the examples more
concrete and therefore easier to follow. With that said, the same ideas work for
any other service.

You may want to follow the service [quickstart]. This guide will walk you
through the steps necessary to enable the service, ensure you have logged in,
and that your account has the necessary permissions.

## Dependencies

As it is usual with Rust, you must declare the dependency in your `Cargo.toml`
file. We use:

```shell
cargo add google-cloud-secretmanager-v1
```

## Iterating List methods

To help iterate the items in a list method, the APIs return an implementation of
the `Paginator` trait. We need to introduce it in scope via a `use` declaration:

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-use}}
```

To iterate the items, we use the provided Paginator `items` function.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-iterate-items}}
```

In rare cases, pages contain extra information outside of the items that you may
need access to. Or you may need to checkpoint your progress across processes. In
these cases, you can do so by iterating over the full pages instead of the
individual items.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-iterate-pages}}
```

### Working with futures::Stream

You may want to use these APIs in the larger Rust ecosystem of asynchronous
streams, such as `tokio::Stream`. This is readily done, but you must first
enable the `unstable-streams` feature in the `google_cloud_gax` crate:

```shell
cargo add google-cloud-gax --features unstable-stream
```

The name of this feature is intended to convey that we consider these APIs
unstable, because they are! You should only use them if you are prepared to deal
with any breaks that result from incompatible changes to the
[`futures::Stream`][future-stub] trait.

The examples will also use the `futures::stream::StreamExt` trait, so we must
add the crate that defines it.

```shell
cargo add futures
```

We use the `into_stream` function to convert the Paginator into a
`futures::Stream` of items.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-stream-items}}
```

Similarly, we can use the `into_stream` function to convert the Paginator into
`futures::Stream` of pages.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-stream-pages}}
```

## Resuming List methods by setting next page token

In some cases such as an interrupted List operation, we can set the next page
token to resume paginating from a specific page.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-page-token}}
```

## Additional paginator technical details

The standard Google API List method follows the pagination guideline defined by
[AIP-158]. Each call to a List method for a resource returns a "page" of
resource items (e.g. Secrets) along with a "next-page token" that can be passed
to the List method to retrieve the next page.

The Google Cloud Client Libraries for Rust provide an adapter to convert the
list RPCs as defined by [AIP-4233] into a streams that can be iterated over in
an async fashion.

[aip-158]: https://google.aip.dev/158
[aip-4233]: https://google.aip.dev/client-libraries/4233
[future-stub]: https://docs.rs/futures/latest/futures/stream/
[quickstart]: https://cloud.google.com/secret-manager/docs/quickstart
[secret manager]: https://cloud.google.com/secret-manager
