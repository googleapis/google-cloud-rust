# Integration tests for the generated libraries

[Sidekick] supports multiple different specification formats, including
[Discovery docs]. This directory contains integration tests for one client
generated from a discovery doc specification.

We use the [Google Compute Engine] service and client in these tests as this
service is the main motivation to support discovery docs.

This is not an ideal service to test with, as the admin operations are
relatively slow, they all require LROs, and there are complex quota
requirements.

[discovery docs]: https://developers.google.com/discovery/v1/reference/apis
[google compute engine]: https://docs.cloud.google.com/compute/docs
[sidekick]: https://github.com/googleapis/librarian
