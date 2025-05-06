# Architecture Guide

This document describes the high-level architecture of the Google Cloud Rust
Client libraries. Its main audience are developers and contributors making
changes and additions to these libraries. If you want to familiarize yourself
with the code in the `google-cloud-rust` project, you are at the right place.

While we expect users of the libraries may find this document useful, this
document does not change or define the public API. You can use this document to
understand how things work, or maybe to troubleshoot problems. You should not
depend on the implementation details described here to write your application.
Only the public API is stable, the rest is subject to change without notice.

## What these libraries do

The goal of the libraries is to provide idiomatic Rust libraries to access
services in [Google Cloud](https://cloud.google.com). All services are in scope.
As of 2025-03 we have over 100 libraries, covering most Google Cloud services.
The APIs are not stable, they are **not** ready for use in production code.

What do we mean by idiomatic? We mean that Rust developers will find the APIs
familiar, or "natural", that these APIs will fit well with the rest of the Rust
ecosystem, and that very few new "concepts" are needed to understand how to use
these libraries.

More specifically, the functionality offered by these libraries include:

- Serialize and deserialize requests and responses: our customers should spend
  more time writing application code and less time dealing with message
  formatting.
- All RPCs are asynchronous and work well with [Tokio].
- It is not possible to start a RPC without providing the parameters needed to
  format the request [^required].
- Optional parameters can be provided as needed, there is no need to initialize
  parameters to their default values.
- The libraries convert pagination APIs into streams.
- The libraries convert long-running operations into an asynchronous function
  that simply returns the final outcome of the long-running operation.
  Applications that need more fine-grained control over the request can still do
  so.
- The application can define retry policies for all RPCs in a client and
  override the policies for specific requests.
- The libraries support best practices such as exponential backoff on retries
  and retry throttling.
- The libraries can be configured to log requests and responses, to help
  application developers troubleshoot their code.

## How is this code created?

Most client libraries are automatically generated from the Protobuf
specification of the service API. We create one Rust crate for each Protobuf
package. This means that some crates contain only types used by one or more
service APIs. But in general, each crate contains a client library.

## Where is the code?

The code is structured to easily distinguish automatically generated code from
hand-crafted code. The main directories are:

- `src/generated/*`: the generated libraries.
- `src/auth`: the authentication library.
- `src/wkt`: well-known types shared by all the generated and hand-crafted
  libraries. Notably, this includes some generated code in
  `src/wkt/src/generated`.
- `src/gax`: common components used to create RPCs. Notably, this is where the
  error handling code resides, as well as the implementation of the HTTP+JSON
  client.
- `src/lro`: support code for long-running operations.
- `generator/`: the code generator, also known as `sidekick`.
- `src/integration-tests`: the integration tests. These run against production
  and validate (using a small number of services) that the generator produces
  working code.
- `guide/src`: the user guide, a "Rust Book" containing several tutorials for
  `google-cloud-rust`.
- `guide/samples`: the code samples for the user guide. In general, we want the
  code samples to be at least compiled

[^required]: unfortunately some required parameters are not that easy to detect, it is
    possible to create some requests with missing required parameters.

[tokio]: https://tokio.rs
