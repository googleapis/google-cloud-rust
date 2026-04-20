# How to configure a client

The Google Cloud Rust Client Libraries let you configure client behavior
using a configuration object passed to the client constructor. This configuration
is typically handled by a `ClientConfig` or `Config` struct provided by the
specific service crate.

## 1. Customizing the API endpoint

See [Override the default endpoint][override-endpoint].

## 2. Authentication configuration

While the client attempts to find [Application Default Credentials (ADC)][adc]
automatically, you can explicitly provide them using the `with_auth` or
`with_api_key` methods on the configuration object. See
[`Override the default authentication method`][authentication] for details and
examples.

## 3. Logging

See [Enable logging][enable-logging].

## 4. Configuring retries

See [Configuring retry policies](/configuring_retry_policies.md)

## 5. Logging

You can use the `tracing` crate to capture client logs. By initializing a
subscriber, you can debug request metadata, status codes, and events.

```rust
use tracing_subscriber;

fn main() {
    // Initialize tracing subscriber to see debug output from the client
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
}
```

## 6. Other common configuration options

To override the default authentication, including using API keys, see
[Override the default authentication method][overrride-authentication]. To
override the default endpoint, see
[Override the default endpoint][override-endpoint].

**NOTE**: To use API keys, you can use the

[adc]: https://cloud.google.com/docs/authentication/application-default-credentials
[authentication]: https://docs.cloud.google.com/rust/override-default-authentication
[enable-logging]: https://docs.cloud.google.com/rust/enable-logging
[envvars]: https://grpc.github.io/grpc/core/md_doc_environment_variables.html
[override-endpoint]: https://docs.cloud.google.com/rust/override-default-endpoint
[override-authentication]: https://docs.cloud.google.com/rust/override-default-authentication
[override-api-keys]: https://docs.cloud.google.com/rust/override-default-authentication#override_the_default_credentials_api_keys
