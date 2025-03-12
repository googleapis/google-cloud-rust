/// An alias of [std::result::Result] where the error is always [Error][crate::error::Error].
///
/// This is the result type used by all functions wrapping RPCs.
pub type Result<T> = std::result::Result<T, crate::error::Error>;

pub mod polling_backoff_policy;
pub mod polling_policy;
pub mod retry_policy;
pub mod retry_throttler;

/// Defines traits and helpers for HTTP client implementations.
#[cfg(feature = "unstable-sdk-client")]
#[doc(hidden)]
pub mod http_client;

pub mod backoff_policy;
pub mod exponential_backoff;
pub mod options;/// The core error types used by generated clients.
pub mod error;

pub mod loop_state;