# Troubleshoot the Google Cloud Rust Client Library

{% block body %}

## Debug logging

The best way to troubleshoot is by enabling logging. See
[Enabling Logging][enable-logging] for more
information.

## How can I trace gRPC issues?

When working with libraries that use gRPC, you can use the underlying gRPC
environment variables to enable logging. Most Rust clients use pure-Rust gRPC
implementations like `tonic`.

### Prerequisites

Ensure your crate includes the necessary features for the gRPC transport. You
can verify your dependencies in `Cargo.toml`.

### Transport logging with gRPC

The primary method for debugging gRPC calls in Rust is using the `tracing`
subscriber filters. You can target specific gRPC crates to see underlying
transport details.

NOTE: The `tracing` crate requires that you first initialize a
[`tracing_subscriber`][tracing_subscriber].

For example, setting the `RUST_LOG` environment variable to include
`tonic=debug` or `h2=debug` will dump a lot of information regarding the gRPC
and HTTP/2 layers.

```sh
RUST_LOG=debug,tonic=debug,h2=debug cargo run --example your_program
```

## How can I diagnose proxy issues?

See [Client Configuration: Configuring a Proxy][client-configuration].

## Reporting a problem

If your issue is still not resolved, ask for help. If you have a support
contract with Google, create an issue in the
[support console][support] instead of filing on GitHub.
This will ensure a timely response.

Otherwise, file an issue on GitHub. Although there are multiple GitHub
repositories associated with the Google Cloud Libraries, we recommend filing
an issue in
[https://github.com/googleapis/google-cloud-rust][google-cloud-rust]
unless you are certain that it belongs elsewhere. The maintainers may move it to
a different repository where appropriate, but you will be notified of this using
the email associated with your GitHub account.

When filing an issue, include as much of the following information as possible.
This will enable us to help you quickly.

[client-configuration]: /configure_client.md
[google-cloud-rust]: https://github.com/googleapis/google-cloud-rust
[support]: https://cloud.google.com/support/
[tracing_subscriber]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/index.html
[enable-logging]: https://docs.cloud.google.com/rust/enable-logging