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

# Working with list operations

Some services return potentially large lists of items, such as rows or resource
descriptions. To keep CPU and memory usage under control, services return these
resources in `pages`: smaller subsets of the items with a continuation token to
request the next subset.

Iterating over items by page can be tedious. The client libraries provide
adapters to convert the pages into asynchronous iterators. This guide shows you
how to work with these adapters.

## Prerequisites

This guide uses the [Secret Manager] service to demonstrate list operations, but
the concepts apply to other services as well.

You may want to follow the service [quickstart], which shows you how to enable
the service and ensure that you've logged in and that your account has the
necessary permissions.

For complete setup instructions for the Rust libraries, see
[Setting up your development environment].

## Dependencies

Add the Secret Manager library to your `Cargo.toml` file:

```shell
cargo add google-cloud-secretmanager-v1
```

## Iterating list methods

To help iterate the items in a list method, the APIs return an implementation of
the `ItemPaginator` trait. Introduce it into scope via a `use` declaration:

```rust,ignore
{{#include ../samples/src/pagination.rs:item-paginator-use}}
```

To iterate the items, use the `by_item` function.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-iterate-items}}
```

In rare cases, pages might contain extra information that you need access to. Or
you may need to checkpoint your progress across processes. In these cases, you
can iterate over full pages instead of individual items.

First introduce `Paginator` into scope via a `use` declaration:

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-use}}
```

Then iterate over the pages using `by_page`:

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-iterate-pages}}
```

### Working with `futures::Stream`

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

The following examples also use the `futures::stream::StreamExt` trait, which
you enable by adding the `futures` crate.

```shell
cargo add futures
```

Add the required `use` declarations:

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-stream-items-use}}
```

Then use the `into_stream` function to convert `ItemPaginator` into a
`futures::Stream` of items.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-stream-items}}
```

Similarly, you can use the `into_stream` function to convert `Paginator` into a
`futures::Stream` of pages.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-stream-pages}}
```

## Resuming list methods by setting next page token

In some cases, such as an interrupted list operation, you can set the next page
token to resume paginating from a specific page.

```rust,ignore
{{#include ../samples/src/pagination.rs:paginator-page-token}}
```

## Additional paginator technical details

The standard [Google API List] method follows the pagination guideline defined
by [AIP-158]. Each call to a `List` method for a resource returns a page of
resource items (e.g. secrets) along with a next-page token that can be passed
to the `List` method to retrieve the next page.

The Google Cloud Client Libraries for Rust provide an adapter to convert the
list RPCs as defined by [AIP-4233] into a streams that can be iterated over in
an async fashion.

## What's next

Learn more about working with the Cloud Client Libraries for Rust:

- [Working with long-running operations]
- [Configuring polling policies]

[aip-158]: https://google.aip.dev/158
[aip-4233]: https://google.aip.dev/client-libraries/4233
[configuring polling policies]: configuring_polling_policies.md
[future-stub]: https://docs.rs/futures/latest/futures/stream/
[google api list]: https://google.aip.dev/132
[quickstart]: https://cloud.google.com/secret-manager/docs/quickstart
[secret manager]: https://cloud.google.com/secret-manager
[setting up your development environment]: setting_up_your_development_environment.md
[working with long-running operations]: working_with_long_running_operations.md