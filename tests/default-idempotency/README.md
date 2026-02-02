# An integration test for the default idempotency

[Sidekick] automatically sets the idempotency of an operation based on the HTTP
configuration: methods that use `GET` or `PUT` are assumed to be idempotent,
other methods are assumed to be non-idempotent.

We use a number of generated clients in this test, because the implementation is
slightly different depending on the IDL (Protobuf, OpenAPI, or discovery docs)
and the protocol used in that IDL (Protobuf can use HTTP+JSON or gRPC).

- `google-cloud-secretmanager-v1`: is our canonical HTTP+JSON client.
- `google-cloud-firestore`: is a Protobuf-specified client, using gRPC as the
  transport, and with valid HTTP annotations.
- `google-cloud-storage`: is (partly) Protobuf-specified client, using gRPC as
  the transport, without valid HTTP annotations and with support for
  auto-populated request ids.

[sidekick]: https://github.com/googleapis/librarian
