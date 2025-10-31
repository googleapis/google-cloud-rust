# Design Doc: OIDC ID Token Verification

## tl;dr

This document describes the design for OIDC ID Token verification in our authentication library. We will introduce a `Verifier` to make it simple to validate tokens from Google and other providers. The design focuses on a flexible API and performance, using a cache for public keys.

## Objective

Our main goal is to offer a simple, secure, and efficient method for applications to verify OIDC ID tokens. This is important for service-to-service authentication, for example, with services behind Identity-Aware Proxy (IAP) or Cloud Run.

## Background

Verifying OIDC ID tokens is a critical security task. It involves fetching the correct public key, verifying the token's signature, and validating its claims. This process can be complex and easy to get wrong. By providing a `Verifier`, we offer a reliable and easy-to-use solution that handles these complexities and follows security best practices.

## Detailed Design

### API

The main API for token verification is the `Verifier` struct. It uses a builder pattern for configuration.

```rust
pub struct Verifier {
    // ...
}

impl Verifier {
    /// Creates a new Verifier with default settings.
    pub fn new() -> Self {
        // ...
    }

    /// Sets the expected audience for the token.
    /// This is a required field for validation.
    pub fn with_audience<S: Into<String>>(mut self, audience: S) -> Self {
        // ...
    }

    /// Sets the expected email claim for the token.
    /// If set, the verifier will also check if the `email_verified` claim is true.
    pub fn with_email<S: Into<String>>(mut self, email: S) -> Self {
        // ...
    }

    /// Sets a custom JWKS URL to fetch the public keys from.
    /// If not set, the URL is determined based on the token's `alg` header.
    pub fn with_jwks_url<S: Into<String>>(mut self, jwks_url: S) -> Self {
        // ...
    }

    /// Sets the allowed clock skew for validating the token's expiration time.
    /// Defaults to 10 seconds.
    pub fn with_clock_skew(mut self, clock_skew: Duration) -> Self {
        // ...
    }

    /// Verifies the ID token and returns the claims if valid.
    pub async fn verify<S: Into<String>>(&self, token: S) -> Result<HashMap<String, Value>> {
        // ...
    }
}
```

### Implementation Details

#### Verification Process

The `Verifier::verify` method follows these steps:

1.  **Decode Header**: First, we decode the JWT header to get the `kid` (Key ID) and `alg` (algorithm). The `kid` is needed to select the correct key, and the `alg` helps determine the default JWKS URL if not provided.
2.  **Fetch Public Key**: The `JwkClient` gets the public key. It checks a local cache first using the `kid`. If the key is not cached, it downloads the JWK set from the correct URL, finds the key with the matching `kid`, and caches it.
3.  **Validate Signature and Claims**: We use the `jsonwebtoken` crate to verify the token's signature. It also validates standard claims like `iss` (issuer), `aud` (audience), and `exp` (expiration time).
4.  **Validate Email**: If an email was configured in the `Verifier`, we also check that the `email` claim in the token matches and that the `email_verified` claim is `true`.

#### JWK Client

The `JwkClient` is responsible for getting the public keys.

*   **URL Resolution**: It determines the JWKS URL from the token's algorithm if a custom URL is not provided. It has default URLs for Google's `RS256` (OAuth2) and `ES256` (IAP) algorithms.
*   **Caching**: It caches the `DecodingKey`s in memory, using the `kid` as the key in a `HashMap`. This cache is protected by a `tokio::sync::RwLock` for safe concurrent access.

#### Proposed Cache Expiration

Public keys can be rotated, so we should not cache them forever. Here is a proposal for cache expiration:

1.  **Store Expiration Time**: In the cache, we will store the key and its expiration time. A simple struct can be used:
    ```rust
    struct CacheEntry {
        key: DecodingKey,
        expires_at: Instant,
    }
    ```
2.  **Set a TTL**: Each cached key will have a "time-to-live" (TTL), for example, 1 hour. The expiration time will be `now + TTL`.
3.  **Check on Access**: When we get a key from the cache, we will first check if it is expired. If it is, we will remove it and fetch a fresh one.

This ensures that the application will use updated public keys.

### Pros and Cons

#### Pros

*   **Secure and Simple**: The `Verifier` handles complex security logic, making it easier for developers to use it correctly.
*   **Good Performance**: Caching public keys avoids network requests and makes token verification faster.
*   **Flexible**: The builder pattern and the option to use a custom JWKS URL make it adaptable for different use cases, including non-Google providers.

#### Cons

*   **In-memory Cache**: The cache is not shared between processes. If an application runs on multiple instances, each will have its own cache. A distributed cache like Redis could be a solution but is not part of this design.
*   **Cache Write Lock**: When updating the cache, a write lock is required, which blocks read access. This should be a rare event, so it is not expected to be a major performance issue.