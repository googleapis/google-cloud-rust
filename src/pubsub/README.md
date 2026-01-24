# Google Cloud Client Libraries for Rust - Pub/Sub

**WARNING:** this crate is under active development. We expect multiple breaking
changes in the upcoming releases. Testing is also incomplete, we do **not**
recommend that you use this crate in production. We welcome feedback about the
APIs, documentation, missing features, bugs, etc.

This crate contains traits, types, and functions to interact with [Pub/Sub].
Most applications will use the structs defined in the client module.

Receiving messages is not yet supported by this crate.

> This crate used to contain a different implementation, with a different
> surface. [@yoshidan](https://github.com/yoshidan) generously donated the crate
> name to Google. Their crate continues to live as [gcloud-pubsub].

## Features

- `default-rustls-provider`: enabled by default. Use the default rustls crypto
  provider ([ring]) for TLS and authentication. Applications with specific
  requirements for cryptography (such as exclusively using the [aws-lc-rs], or
  [ring] crates) should disable this default and install the default crypto
  provider in `rustls` to fit their requirements.
- `unstable-stream`: enable the (unstable) features to convert several types to
  a `future::Stream`.

[aws-lc-rs]: https://crates.io/crates/aws-lc-rs
[gcloud-pubsub]: https://crates.io/crates/gcloud-pubsub
[pub/sub]: https://cloud.google.com/pubsub
[ring]: https://crates.io/crates/ring
