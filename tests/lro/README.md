# Integration tests for LROs

This crate contains integration tests for long-running operations (LROs).

These tests use the `google-cloud-workflows-v1` client library. We use this
service because the LROs are relatively fast, and does not have troublesome
quota constraints.

We use the library to test against production (soon) and against a fake service.
The test use the fake service to exercise the error path, which sometimes is
hard to trigger in production.
