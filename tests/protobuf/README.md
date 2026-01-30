# Integration tests for the generated libraries

[Sidekick] supports multiple different specification formats, including
[Protocol Buffers] (often abbreviated as _protobuf_ or simply \_proto). This
directory contains integration tests for one client generated from a protobuf
specification.

We use the [Secret Manager] service and client in these tests as:

- Secret Manager does not have very restrictive quota requirements.
- Secret Manager uses many different field types, including maps, bytes an anys.
- Secret Manager has pagination methods, some mixins, and regional endpoints.

The one large gap is LROs. We test LROs in a separate crate, with a different
service.

[protocol buffers]: https://protobuf.dev/
[secret manager]: https://docs.cloud.google.com/secret-manager/docs
[sidekick]: https://github.com/googleapis/librarian
