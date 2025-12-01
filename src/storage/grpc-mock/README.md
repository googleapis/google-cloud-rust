# A mockable Storage service implementation

This crate provides a mockable implementation of the Cloud Storage service over
gRPC, analogous to the `httptest` crate but specific to this service. It is used
in the client library tests, and not intended for any other use.

## On streaming RPCs

Streaming RPCs in Tonic use generics for the output (server-side) streams. To
simplify the mocks, this crate only supports `tokio::sync::mpsc::Receiver<>` as
the output type. These are easy to create in tests and good enough for that
purpose.

Streaming RPCs in Tonic use `tonic::Streaming<>` as the input (client-side)
streams. It seemed easier to reason about the mock if it always used
`Receiver<>`. If this proves to be a bad decision we can change the code it.

## On copying the protos

I (coryan) chose to copy the protos and rely on the `build.rs` script to
generate the Tonic stubs and Prost helpers. This creates a support burden: we
will need to copy the files from time-to-time to keep them up to date. We can
change `sidekick` to generate these files for us instead. I think that the
support will be low, we only need to make changes if either (1) new RPCs appear
that we want to test (most unary RPCs won't need new tests), or (2) new fields
appear that are relevant for tests (most fields are ignored in end-to-end
tests).

I also decided to save the generated files, as to avoid requiring `protoc` in
all our builds. However, any build use `--all-features`, such as the coverage
builds, will need to have this tool installed.

## Usage

Create a `mocks::MockStorage` and call `start()` to launch a (local) server
using the mock. The connect your test to this mock server.

The types have comments with trivial examples. I expect more examples as the
code grows.
