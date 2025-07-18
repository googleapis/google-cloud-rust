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

# Working with enums

This guide will show you how to use enumerations in the Google Cloud client
libraries for Rust, including working with enumeration values introduced after
the library was released.

## Background

Google Cloud services use enumerations for fields that only accept or provide a
discrete and limited set of values. While at any point in time the set of
allowed values is known, this list may change over time.

The client libraries are prepared to receive and send enumerations, and support
working with values introduced after the library release.

## Prerequisites

This guide does not make any calls to Google Cloud services. You can run the
examples without having a project or account in Google Cloud. The guide will use
the client library for [Secret Manager]. The same principles apply to any other
enumeration in any other client library.

## Dependencies

As it is usual with Rust, you must declare the dependency in your `Cargo.toml`
file. We use:

```toml
{{#include ../samples/Cargo.toml:secretmanager}}
{{#include ../samples/Cargo.toml:serde_json}}
```

## Handling known values

When using known values, you can use the enumeration as usual:

```rust,ignore
{{#include ../samples/tests/enums.rs:known}}
```

## Handling unknown values

When using unknown string values, the `.value()` function return `None`, but
everything else works as normal:

```rust,ignore
{{#include ../samples/tests/enums.rs:unknown_string}}
```

The same principle applies to unknown integer values:

```rust,ignore
{{#include ../samples/tests/enums.rs:unknown_string}}
```

## Preparing for upgrades

As mentioned above, the Rust enumerations in the client libraries may gain new
variants in future releases. To avoid breaking applications we mark these
enumerations as `#[non_exhaustive]`.

If you use a match expression for non-exhaustive enumerations then you must
include the [wildcard pattern] in your match. This will prevent compilation
problems when new variants are included in the enumeration.

```rust,ignore
{{#include ../samples/tests/enums.rs:use}}
{{#include ../samples/tests/enums.rs:match_with_wildcard}}
```

Nevertheless, you may want a warning or error if new variants appear, at least
so you can examine the code and decide if it must be updated. If that is the
case, consider using the [`wildcard_enum_match_arm`] clippy warning:

```rust,ignore
{{#include ../samples/tests/enums.rs:use}}
{{#include ../samples/tests/enums.rs:match_with_warnings}}
```

You may also consider the (currently unstable)
[`non_exhaustive_omitted_patterns`] lint.

[secret manager]: https://cloud.google.com/secret-manager
[wildcard pattern]: https://doc.rust-lang.org/reference/patterns.html#wildcard-pattern
[`non_exhaustive_omitted_patterns`]: https://github.com/rust-lang/rust/issues/89554
[`wildcard_enum_match_arm`]: https://rust-lang.github.io/rust-clippy/master/#wildcard_enum_match_arm
