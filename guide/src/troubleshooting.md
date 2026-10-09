<!--
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Troubleshoot the Google Cloud Rust Client Library

## Debug logging

The best way to troubleshoot is by enabling logging. See
[Enabling Logging][enable-logging] for more information.

## How can I trace gRPC issues?

Clients that use gRPC use [`tonic`][tonic] for the underlying transport. The
primary method for debugging gRPC calls in Rust is using the `tracing`
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
contract with Google, create an issue in the [support console][support] instead
of filing on GitHub. This will ensure a timely response.

Otherwise, file an issue on GitHub. Although there are multiple GitHub
repositories associated with the Google Cloud Libraries, we recommend filing an
issue in [https://github.com/googleapis/google-cloud-rust][google-cloud-rust]
unless you are certain that it belongs elsewhere. The maintainers may move it to
a different repository where appropriate, but you will be notified of this using
the email associated with your GitHub account.

[client-configuration]: configure_client.md#4-configuring-a-proxy
[enable-logging]: https://docs.cloud.google.com/rust/enable-logging
[google-cloud-rust]: https://github.com/googleapis/google-cloud-rust
[support]: https://cloud.google.com/support/
[tonic]: https://docs.rs/tonic/latest/tonic/
[tracing_subscriber]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/index.html
