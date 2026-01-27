# Integration tests for observability

This directory contains integration tests for the observability (at the moment
these are limited to tracing) features in the generated client libraries.

The tests use the `google-cloud-showcase-v1beta1` client library for testing, as
this is the first library where we enable the o11y features.

The tests also verify we can send traces from the tokio [tracing] framework to
Cloud Trace. Those tests may grow into an exporter over time.

[tracing]: https://github.com/tokio-rs/tracing
