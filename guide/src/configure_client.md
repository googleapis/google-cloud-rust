# How to configure a client

The Google Cloud Rust Client Libraries let you configure client behavior
using a configuration object passed to the client constructor. This configuration
is typically handled by a `ClientConfig` or `Config` struct provided by the
specific service crate.

## 1. Customizing the API endpoint

See [Override the default endpoint][override-default-endpoint].

## 2. Authentication configuration

While the client attempts to find [Application Default Credentials (ADC)][adc]
automatically, you can explicitly provide them using the `with_auth` or
`with_api_key` methods on the configuration object. See
[`Override the default authentication method`][authentication] for details and
examples.

## 3. Logging

Logging is handled through the `tracing` ecosystem. You can configure a
subscriber to capture logs and traces from the client libraries.
See [Troubleshooting](/troubleshooting.md) for a comprehensive guide.

## 3. Configuring a proxy

The configuration method depends on whether you are using a gRPC or REST-based
transport.

### Proxy with gRPC

When using the gRPC transport (standard for most services), the client library
respects the [standard environment variables][envvars]. You don't need to
configure this in the Rust code itself.

Set the following environment variables in your shell or container:

```bash
export http_proxy="http://proxy.example.com:3128"
export https_proxy="http://proxy.example.com:3128"
```

**Handling self-signed certificates (gRPC):** If your proxy uses a self-signed
certificate (Deep Packet Inspection), you cannot "ignore" verification in gRPC.
You must provide the path to the proxy's CA certificate bundle.

```bash
# Point gRPC to a CA bundle that includes your proxy's certificate
export GRPC_DEFAULT_SSL_ROOTS_FILE_PATH="/path/to/roots.pem"
```

### Proxy with REST

If you're using a library that supports REST transport, you can configure the
proxy by providing a custom `reqwest` or `hyper` client to the configuration,
depending on the specific implementation of the crate.

```rust
use google_cloud_secret_manager::v1::client::{SecretManagerClient, ClientConfig};

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Configure a proxy using standard environment variables or a custom connector
    let proxy = reqwest::Proxy::all("http://user:password@proxy.example.com")?;
    let http_client = reqwest::Client::builder()
        .proxy(proxy)
        .build()?;

    let config = ClientConfig::default()
        .with_http_client(http_client);

    let client = SecretManagerClient::new(config).await?;
    Ok(())
}
```

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

The following options can be passed to the configuration builder of most
clients.

| Option | Type | Description |
| ----- | ----- | ----- |
| `credentials` | `Option<Credentials>` | Explicit credentials object for authentication. |
| `endpoint` | `String` | The address of the API remote host. Used for Regional Endpoints (e.g., `https://us-central1-pubsub.googleapis.com:443`) or Private Service Connect. |

To use API Keys [override the default credentials with API keys][override-api-keys].

[adc]: https://cloud.google.com/docs/authentication/application-default-credentials
[authentication]: https://docs.cloud.google.com/rust/override-default-authentication
[envvars]: https://grpc.github.io/grpc/core/md_doc_environment_variables.html
[override-default-endpoint]: https://docs.cloud.google.com/rust/override-default-endpoint
[override-api-keys]: https://docs.cloud.google.com/rust/override-default-authentication#override_the_default_credentials_api_keys
