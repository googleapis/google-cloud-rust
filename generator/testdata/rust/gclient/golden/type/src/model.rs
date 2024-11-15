#![allow(dead_code)]

/// Represents a textual expression in the Common Expression Language (CEL)
/// syntax. CEL is a C-like expression language. The syntax and semantics of CEL
/// are documented at https://github.com/google/cel-spec.
///
/// Example (Comparison):
///
///     title: "Summary size limit"
///     description: "Determines if a summary is less than 100 chars"
///     expression: "document.summary.size() < 100"
///
/// Example (Equality):
///
///     title: "Requestor is owner"
///     description: "Determines if requestor is the document owner"
///     expression: "document.owner == request.auth.claims.email"
///
/// Example (Logic):
///
///     title: "Public documents"
///     description: "Determine whether the document should be publicly visible"
///     expression: "document.type != 'private' && document.type != 'internal'"
///
/// Example (Data Manipulation):
///
///     title: "Notification string"
///     description: "Create a notification string with a timestamp."
///     expression: "'New message received at ' + string(document.create_time)"
///
/// The exact variables and functions that may be referenced within an expression
/// are determined by the service that evaluates it. See the service
/// documentation for additional information.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Expr {
    /// Textual representation of an expression in Common Expression Language
    /// syntax.
    pub expression: String,

    /// Optional. Title for the expression, i.e. a short string describing
    /// its purpose. This can be used e.g. in UIs which allow to enter the
    /// expression.
    pub title: String,

    /// Optional. Description of the expression. This is a longer text which
    /// describes the expression, e.g. when hovered over it in a UI.
    pub description: String,

    /// Optional. String indicating the location of the expression for error
    /// reporting, e.g. a file name and a position in the file.
    pub location: String,
}

impl Expr {
    /// Sets the value of `expression`.
    pub fn set_expression<T: Into<String>>(mut self, v: T) -> Self {
        self.expression = v.into();
        self
    }

    /// Sets the value of `title`.
    pub fn set_title<T: Into<String>>(mut self, v: T) -> Self {
        self.title = v.into();
        self
    }

    /// Sets the value of `description`.
    pub fn set_description<T: Into<String>>(mut self, v: T) -> Self {
        self.description = v.into();
        self
    }

    /// Sets the value of `location`.
    pub fn set_location<T: Into<String>>(mut self, v: T) -> Self {
        self.location = v.into();
        self
    }
}
