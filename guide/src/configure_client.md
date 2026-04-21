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

## 4. Configuring a proxy

To configure a proxy, you can take advantage of the
[standard environment variables][envvars] supported by `reqwest`. You
don't need to configure this in the Rust code itself. Set the following
environment variables in your shell or container:

```bash
export http_proxy="http://proxy.example.com:3128"
export https_proxy="http://proxy.example.com:3128"
```

## 5. Configuring retries

See [Configuring retry policies](/configuring_retry_policies.md)

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
