#[derive(Debug)]
pub struct AuthError {
    is_retryable: bool,
    source: gax::error::BoxError
}

impl AuthError {
    pub fn new(is_retryable: bool, source: gax::error::BoxError) -> Self {
        AuthError {
            is_retryable,
            source
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.is_retryable
    }
}

impl std::error::Error for AuthError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Retryable:{}, Source:{}", self.is_retryable, self.source)
    }
}

// Converts Auth Error to the gax error type
impl From<AuthError> for gax::error::Error {
    fn from(e: AuthError) -> Self {
        gax::error::Error::authentication(e)
    }
}


#[derive(Debug)]
pub struct InnerAuthError {
    message: String,
    kind: InnerAuthErrorKind
}

impl std::error::Error for InnerAuthError{}

impl std::fmt::Display for InnerAuthError{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Message: {}, Kind: {:?}", self.message, self.kind)
    }
}

impl InnerAuthError {
    pub fn new(message: String, kind: InnerAuthErrorKind) -> Self {
        InnerAuthError {
            message,
            kind
        }
    }
}

#[derive(Debug)]
pub enum InnerAuthErrorKind {
    DefaultCredentialsError, // Errors during ADC
    InvalidOptionsError, // Errors interpreting options
}

