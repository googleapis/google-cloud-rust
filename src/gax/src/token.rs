/// Represents an auth token.
#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    /// The actual token string.
    ///
    /// This is the value used in `Authorization:` header.
    pub token: String,

    /// The type of the token.
    ///
    /// The most common type is `"Bearer"` but other types may appear in the
    /// future.
    pub token_type: String,

    /// The instant at which the token expires.
    ///
    /// If `None`, the token does not expire.
    ///
    /// Note that the `Instant` is not valid across processes. It is
    /// recommended to let the authentication library refresh tokens within a
    /// process instead of handling expirations yourself. If you do need to
    /// copy an expiration across processes, consider converting it to a
    /// `time::OffsetDateTime` first:
    ///
    /// ```
    /// # let expires_at = Some(std::time::Instant::now());
    /// expires_at.map(|i| time::OffsetDateTime::now_utc() + (i - std::time::Instant::now()));
    /// ```
    pub expires_at: Option<std::time::Instant>,

    /// Optional metadata associated with the token.
    ///
    /// This might include information like granted scopes or other claims.
    pub metadata: Option<std::collections::HashMap<String, String>>,
}