# Google Cloud API Client Libraries for Rust

[![build status](https://github.com/googleapis/google-cloud-rust/actions/workflows/sdk.yaml/badge.svg)](https://github.com/googleapis/google-cloud-rust/actions/workflows/sdk.yaml)
[![dependency status](https://deps.rs/repo/github/googleapis/google-cloud-rust/status.svg)](https://deps.rs/repo/github/googleapis/google-cloud-rust)

Idiomatic Rust client libraries for [Google Cloud Platform](https://cloud.google.com/) services.

> **NOTE:** this project is under development, all APIs are subject to change
> without notice. Some documentation is aspirational.

## Supported Rust Versions

We have not defined policies with respect to the Minimum Supported Rust Version
or Minimum Supported Rust Edition.

## Contributing

Contributions to this library are always welcome and highly encouraged.

See [CONTRIBUTING] for more information on how to get started.

## License

Apache 2.0 - See [LICENSE] for more information.

## Components

### Authentication (`src/auth`)

The `auth` crate provides authentication support for the Google Cloud Client Libraries for Rust. It includes various credential types such as API key, service account, and user credentials.

For more information, see the [auth README](src/auth/README.md).

### Base (`src/base`)

The `base` crate contains common components and utilities used by the Google Cloud Client Libraries for Rust.

For more information, see the [base README](src/base/README.md).

### GAX (`src/gax`)

The `gax` crate contains common components used by the Google Cloud Client Libraries for Rust, such as retry policies, backoff policies, and HTTP client utilities.

For more information, see the [gax README](src/gax/README.md).

### Long-running Operations (`src/lro`)

The `lro` crate provides support for long-running operations in the Google Cloud Client Libraries for Rust.

For more information, see the [lro README](src/lro/README.md).

### Well Known Types (`src/wkt`)

The `wkt` crate contains well-known types used by many Google Cloud services, such as `Any`, `Duration`, `Timestamp`, and more.

For more information, see the [wkt README](src/wkt/README.md).

### Sidekick (`generator`)

The `sidekick` tool automates most activities around generating and maintaining SDKs for Google Cloud.

For more information, see the [sidekick README](generator/README.md).

### Googleapis Protos (`generator/testdata/googleapis`)

This directory contains a small subset of the [googleapis] protos for use in testing the generator.

For more information, see the [googleapis README](generator/testdata/googleapis/README.md).

[contributing]: CONTRIBUTING.md
[license]: LICENSE
