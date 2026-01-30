# Integration tests for an OpenAPI-based client

[Sidekick] supports multiple different specification formats, including OpenAPI.
This directory contains integration tests for one client generated from an
OpenAPI specification.

We use the [Secret Manager] service and client in these tests as:

- Secret Manager does not have very restrictive quota requirements.
- Secret Manager uses many different field types, including maps, bytes an anys.
- Secret Manager has pagination methods, some mixins, and regional endpoints.

The one large gap is LROs, but it is unclear how LROs will work for OpenAPI in
the first place.

[secret manager]: https://docs.cloud.google.com/secret-manager/docs
[sidekick]: https://github.com/googleapis/librarian
