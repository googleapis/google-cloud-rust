Hey, we have a retry loop located in:

src/gax/src/retry_loop_internal.rs

This is nice and well-tested. The problem is that it is not generic enough.

It is written in terms of `gax::Error`, but we want to re-use it with a generic `std::error::Error`.

It is written in terms of a few interfaces:
- RetryThrottler in src/gax/src/retry_throttler.rs
- RetryPolicy in src/gax/src/retry_policy.rs
- BackoffPolicy in src/gax/src/backoff_policy.rs
- RetryResult in src/gax/src/retry_result.rs
- ThrottleResult in src/gax/src/throttle_result.rs

The problem is that these are all public interfaces, which have been published, and have dependents. We cannot break any public interfaces.

I want you to come up with a plan to offer a generic retry loop, written in terms of a generic error.

The plan should involve:
- making any enums/structs generic over the error enum.
- offering new traits that have a `type Error = ...` field.
  - then providing an impl for the new trait for the existing traits (specific to `gax::Error`)
- lastly refactoring the retry_loop to take an extra `E` argument

Firstly, will this plan work, and be non-breaking to existing users of the code?

If so, start planning how this should work. Describe the edits you are going to make, but don't modify any files.
