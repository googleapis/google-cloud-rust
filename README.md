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

## Non-public API

Some crates export "public" types and functions that are intended only for use
as implementation details of other crates in this repository. Chief amongst
these are all the symbols exported by the `google-cloud-gax-internal` crate. As
the name indicates, this crate is intended to hold internal-only types. No
application should take a direct dependency on this crate or use their types in
their code. We reserve the right to make changes to this crate without notice.

In other crates, any public symbol that is not part of the public API have the
`#[doc(hidden)]` attribute set. This is conventional in Rust.

In addition, we also use a number of crate features prefixed with `_internal-`.
None of these features (or the symbols they enable) are intended for application
developers to use.

## Unstable APIs

Rust does not have a stable API for asynchronous iterators (also known as
asynchronous streams, or simply streams). Where needed, we provide
implementations of the [futures::stream::Stream] trait.

For crates that offer implementations of the trait, the functionality is gated
by the `unstable-streams` feature. As the name indicates, this feature is
unstable. When Rust stabilizes the streams trait, we may choose to rename the
feature, and may even need to change the trait implementation.

## Minimum Supported Rust Version

We require Rust >= 1.85, as we anticipate this will be at least six months old
by the time this project is stabilized (GA may be even later).

However, we have not defined a policy on how often we will update this minimum
version. We do intend to publish our policy before declaring that the project is
generally available.

## Contributing

Contributions to this library are always welcome and highly encouraged.

See [CONTRIBUTING] for more information on how to get started. You may also find
the [Set Up Development Environment] and the [Architecture] guides useful.

## License

Apache 2.0 - See [LICENSE] for more information.

[architecture]: ARCHITECTURE.md
[contributing]: CONTRIBUTING.md
[futures::stream::stream]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
[license]: LICENSE
[set up development environment]: doc/contributor/howto-guide-set-up-development-environment.md
[user guide]: https://googleapis.github.io/google-cloud-rust
