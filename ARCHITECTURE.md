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
- All RPCs are asynchronous and work well with [Tokio]. The clients do not own
  any threads and rely on the runtime for scheduling.
- It is not possible to start a RPC without providing the parameters needed to
  format the request (with [some limitations](#what-these-libraries-do-not-do)).
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

## The structure of a client

A client is a Rust `struct` that implements the customer-facing functions to
make RPCs. Associated with each client there is a `Stub` trait.

Each function returns a "request builder". These builders provide functions to
set required and optional parameters. They also provide setters to configure the
request options (timeouts, retry policies, etc.). The application developer
calls `send()` to issue the request. This is an `async` function.

The `send()` function is a thin wrapper around a method of the `Stub` trait.
There are basically three implementations of the `Stub`:

- A `Transport` stub that serializes the request, sends it to the correct
  endpoint, receives the response (or error), deserializes either, and returns
  the response or error. The transport implements any retry loops or timeouts.
- A `Tracing` stub that is decorated for Tokio tracing and then calls the normal
  transport.
- Any mocks provided by the application for their own testing.

## Pagination

When appropriate, request builders implement `by_page()` and `by_item()`
methods. Instead of returning a single response for a request, these provide a
stream of responses for paginated APIs (think `List*()`). The paginator
basically holds the original stub and can chain requests using the page token
returned in one response to get the next page in the subsequent request.

Paginators can also become an "item paginator", where the stream returns one
element at a time, instead of one page at a time.

## LRO Pollers

When appropriate, request builders implement a `poller()` method. Instead of
returning a single response for a request, these provide a stream of responses
for long-running operations (think `Create*()` when the operation is very slow).
Each item in the stream represents the status of the long-running operation,
until its final successful or unsuccessful completion.

For applications that do not care about the intermediate state of these
long-running operations the poller can be converted into a future that
automatically polls the operation until completion. The operation status is
polled periodically, with the period controlled by a policy. Likewise, the
poller continues on recoverable errors, and a policy controls what errors should
be treated as recoverable.

## What these libraries do not do

### No client-side validation

The client libraries only validate requests to the degree it is necessary to
successfully send them. No attempt is made to verify the contents of the request
are valid, or even complete.

The libraries cannot fully validate a request before sending it. This is
obviously true and unavoidable in distributed systems: requests can fail, even
if they have required parameters, and where all the parameters are well
formatted: the caller may not have the required permissions, or the requirements
for the request may have changed since the library was released.

A counter argument is that it is desirable to create client libraries that are
"hard to use incorrectly". It would be preferable to detect missing parameters
on the client-side, or even better, to avoid the problem by including all
required parameters as part of the function signature. In practice, this does
not work well:

- Computing if all required parameters are present may require reimplementing
  server-side functionality: many services require a parameter depending on the
  presence or even the value of other parameters.
- While many Google services provide documentation hints to indicate what
  parameters are required, these hints may change over time: a required
  parameter may become optional. We think that breaking changes in such cases
  (e.g. removing the parameter from the function signature) are not the best
  experience.

Likewise, there are a few parameters that are documented as "output only" or
"immutable". Removing the setters for these parameters could be useful to avoid
mistakes. However, the number of cases where this would change the client
library is very small (maybe a dozen across thousands of functions). The
additional complexity in the implementation, and some fields that are
incorrectly documented discouraged us from pursuing the setters for these
fields.

### Localize error messages

The error messages (if any) are delivered without change from the service. If
the service localizes the messages the client will provide these messages in the
response.

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
- `src/gax`: common components shared by all clients. Notably, this is where the
  error handling code resides, as well as the retry, backoff, and polling
  policies. Most types in this crate are intended for use by application
  developers.
- `src/gax-internal`: implementation details shared by multiple client
  libraries. Types in this crate are **not** intended for application developers
  to use.
- `src/lro`: support code for long-running operations.
- `tests/integration`: the integration tests. These run against production
  and validate (using a small number of services) that the generator produces
  working code.
- `tests/endurance`: a smoke test for leaks or crashes. This test executes
  many RPCs in the generated code, the expectation is that we can complex
  over one billion requests without crashes, panics, or memory leaks.
- `tests/integration-auth`: the auth library integration tests. These run
  against production and validate (using a small number of services) that the
  different authentication flows work as expected.
- `tests/protojson-conformance`: validate our JSON parsing using the ProtoJSON
  conformance tests.
- `guide/src`: the user guide, a "Rust Book" containing several tutorials for
  `google-cloud-rust`.
- `guide/samples`: the code samples for the user guide. In general, we want the
  code samples to be at least compiled.

[tokio]: https://tokio.rs
