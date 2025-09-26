# Google Cloud Client Libraries for Rust - Storage

This crate contains traits, types, and functions to interact with
[Google Cloud Storage].

> This crate used to contain a different implementation, with a different
> surface. [@yoshidan](https://github.com/yoshidan) generously donated the crate
> name to Google. Their crate continues to live as [gcloud-storage].

The client library types and functions are stable and not expected to change.
Please note that Google Cloud services do change from time to time. The client
libraries are designed to preserve backwards compatibility when the service
changes in compatible ways. For example, adding RPCs, or fields to messages
should not introduce breaking changes to the client libraries.

## Quickstart

The main types to work with this crate are the clients:

- [Storage]
- [StorageControl]

## More Information

- Read the
  [crate's documentation](https://docs.rs/google-cloud-storage/latest/google-cloud-storage)

[gcloud-storage]: https://crates.io/crates/gcloud-storage
[google cloud storage]: https://cloud.google.com/storage
[storage]: https://docs.rs/google-cloud-storage/latest/google_cloud_storage/client/struct.Storage.html
[storagecontrol]: https://docs.rs/google-cloud-storage/latest/google_cloud_storage/client/struct.StorageControl.html
