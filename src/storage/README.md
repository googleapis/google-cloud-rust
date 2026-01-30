# Google Cloud Client Libraries for Rust - Storage

This crate contains traits, types, and functions to interact with
[Google Cloud Storage].

To read and write objects, use the [Storage] client. To perform admin
operations, use the [StorageControl] client.

To get started using this crate, refer to the [Using Google Cloud Storage]
section of the Google Cloud Client Libraries for Rust user guide.

> This crate used to contain a different implementation, with a different
> surface. [@yoshidan](https://github.com/yoshidan) generously donated the crate
> name to Google. Their crate continues to live as [gcloud-storage].

The client library types and functions are stable and not expected to change.
Please note that Google Cloud services do change from time to time. The client
libraries are designed to preserve backwards compatibility when the service
changes in compatible ways. For example, adding RPCs, or fields to messages
should not introduce breaking changes to the client libraries.

## Features

- `default-rustls-provider`: enabled by default. Use the default rustls crypto
  provider ([aws-lc-rs]) for TLS and authentication. Applications with specific
  requirements for cryptography (such as exclusively using the [ring] crate)
  should disable this default and call
  `rustls::crypto::CryptoProvider::install_default()`.
- `unstable-stream`: enable the (unstable) features to convert several types to
  a `future::Stream`.

## More Information

- Read the [crate's documentation](https://docs.rs/google-cloud-storage/latest)

[aws-lc-rs]: https://crates.io/crates/aws-lc-rs
[gcloud-storage]: https://crates.io/crates/gcloud-storage
[google cloud storage]: https://cloud.google.com/storage
[ring]: https://crates.io/crates/ring
[storage]: https://docs.rs/google-cloud-storage/latest/google_cloud_storage/client/struct.Storage.html
[storagecontrol]: https://docs.rs/google-cloud-storage/latest/google_cloud_storage/client/struct.StorageControl.html
[using google cloud storage]: https://googleapis.github.io/google-cloud-rust/storage.html
