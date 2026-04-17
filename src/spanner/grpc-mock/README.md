# A mockable Cloud Spanner API service implementation

This crate provides a mockable implementation of the Cloud Spanner API service over
gRPC, analogous to the `httptest` crate but specific to this service. It is used
in the client library tests, and not intended for any other use.

## On streaming RPCs

Streaming RPCs in Tonic use generics for the output (server-side) streams. To
simplify the mocks, this crate only supports `tokio::sync::mpsc::Receiver<>` as
the output type. These are easy to create in tests and good enough for that
purpose.

Streaming RPCs in Tonic use `tonic::Streaming<>` as the input (client-side)
streams. It seemed easier to reason about the mock if it always used
`Receiver<>`. If this proves to be a bad decision we can change the code.

## Usage

Create a `mocks::MockSpanner` and call `start()` to launch a (local) server
using the mock. Then connect your test to this mock server.

The types have comments with trivial examples.
