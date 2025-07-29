# Google Cloud API Client Libraries for Rust

[![build status](https://github.com/googleapis/google-cloud-rust/actions/workflows/sdk.yaml/badge.svg)](https://github.com/googleapis/google-cloud-rust/actions/workflows/sdk.yaml)
[![dependency status](https://deps.rs/repo/github/googleapis/google-cloud-rust/status.svg)](https://deps.rs/repo/github/googleapis/google-cloud-rust)

Idiomatic Rust client libraries for
[Google Cloud Platform](https://cloud.google.com/) services.

> **NOTE:** this project is under development, all APIs are subject to change
> without notice. Some documentation is aspirational.

## Getting Started

The [User Guide] includes basic tutorials to get started with the Google Cloud
client libraries for Rust.

## Minimum Supported Rust Version (MSRV)

We require Rust >= 1.85. We will compile the development branch with this
version until at least 2026-03-01. Once we update our MSRV beyond 1.85, we plan
to update the MSRV periodically. However, the development branch will always
compile with the `rustc` versions released within the previous year[^1].

## Semantic versioning

We will make every effort to avoid breaking changes once a library reaches 1.0.

With that said, many of the crates in this project are automatically generated
from the service specification. From time to time these service specifications
may introduce breaking changes. We make reasonable efforts, and use tooling, to
detect such breaking changes. When we detect a breaking change we will bump the
major version (or minor for crates still at `0.x`).

We do not consider changes to the MSRV to be breaking changes. We only bump the
MSRV after the `rustc` version has been EOL for a while, and therefore out of
support itself.

We do not consider changes to our dependencies, or the features enabled in our
dependencies, to be breaking changes. You should add any dependencies to your
application directly, and enable any non-default features of these dependencies
explicitly.

### Non-public API

Some crates export types and functions that are intended only for use as
implementation details of other crates in this repository. Chief amongst these
are all the symbols exported by the `google-cloud-gax-internal` crate. As the
name indicates, this crate is intended to hold internal-only types. No
application should take a direct dependency on this crate or use their types in
their code. We reserve the right to make changes to this crate without notice.
The crate major version will be `0.x` for the foreseeable future, and its minor
version may change without notice.

In other crates, any public symbol that is not part of the public API have the
`#[doc(hidden)]` attribute set. This is conventional in Rust, and such symbols
are not considered part of the public API. They may change without increasing
the major version number.

In addition, we also use a number of crate features prefixed with `_internal-`.
None of these features (or the symbols they enable) are intended for application
developers to use. They may change without notice, but we intend to increase the
major version of the corresponding library when they change.

## Unstable APIs

Rust does not have a stable API for asynchronous iterators (also known as
asynchronous streams, or simply streams). Where needed, we provide
implementations of the [futures::stream::Stream] trait.

For crates that offer implementations of the trait, the functionality is gated
by the `unstable-streams` feature. As the name indicates, this feature is
unstable. When Rust stabilizes the streams trait, we may choose to rename the
feature, and may even need to change the trait implementation.

## Contributing

Contributions to this library are always welcome and highly encouraged.

See [CONTRIBUTING] for more information on how to get started. You may also find
the [Set Up Development Environment] and the [Architecture] guides useful.

## License

Apache 2.0 - See [LICENSE] for more information.

[^1]: That is, once 1.85 is at least a year old we will bump to 1.86 or higher,
    and we will bump from 1.86 to 1.87 once 1.86 is at least a year old.

[architecture]: ARCHITECTURE.md
[contributing]: CONTRIBUTING.md
[futures::stream::stream]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
[license]: LICENSE
[set up development environment]: doc/contributor/howto-guide-set-up-development-environment.md
[user guide]: https://googleapis.github.io/google-cloud-rust
