# An integration test for locational endpoints

Locational endpoints (think `us-central1-aiplatform.googleapis.com`) need
special treatment to send the correct `Host` header. This crate contains an
integration test to verify this works against production.

We use the `google-cloud-aiplatform-v1` client library because the corresponding
service supports locational endpoints, the service will fail requests for
locational resources if the endpoint is not configured properly, and one can
issue requests without creating, deleting, and garbage collecting resources.
