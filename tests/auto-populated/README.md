# An integration test for auto populated fields

[Sidekick] can automatically auto-populate request fields [AIP-4235]. This crate
contains integration tests to verify this works as expected.

We use the `google-cloud-storage` client, because this is the only client which
has the necessary configuration.

[aip-4235]: https://google.aip.dev/client-libraries/4235
[sidekick]: https://github.com/googleapis/librarian
