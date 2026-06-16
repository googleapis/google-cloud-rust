// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::Error;
use crate::connection::parser::parse_boolean_literal_str;
use crate::connection::{ConnectionError, Dialect};
use crate::to_value::ToValue;
use crate::types::TypeCode;
use crate::value::Value;
use std::collections::HashMap;

/// Context indicating when a connection property is allowed to be configured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Context {
    /// Property can only be specified during connection startup.
    Startup,
    /// Property can be configured both at startup and during session lifetime.
    User,
}

/// A standard trait representing a validated connection property.
pub trait ConnectionProperty: Send + Sync {
    /// Get the property name.
    fn name(&self) -> &str;
    /// Get human-readable description.
    fn description(&self) -> &str;
    /// Get configuration context constraint.
    fn context(&self) -> Context;
    /// Get default value of the property, if any.
    fn default_value(&self) -> Option<String>;
    /// Validate input string and return a normalized string representation if valid.
    fn validate_and_convert(&self, value: &str, dialect: Dialect) -> Result<String, Error>;
    /// Get the TypeCode for the property's value.
    fn type_code(&self) -> TypeCode;
    /// Parse raw string into Spanner Value.
    fn to_value(&self, value: &str, dialect: Dialect) -> Value;
    /// Check if this property is supported for the given SQL dialect.
    fn is_supported(&self, _dialect: Dialect) -> bool {
        true
    }
}

/// A connection property representing a boolean value.
pub struct BooleanProperty {
    /// The name of the property.
    pub name: &'static str,
    /// The description.
    pub description: &'static str,
    /// The default value.
    pub default: &'static str,
    /// Optional restricted dialect for this property.
    pub supported_dialect: Option<Dialect>,
}

impl BooleanProperty {
    /// Construct a new BooleanProperty.
    pub const fn new(name: &'static str, description: &'static str, default: &'static str) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: None,
        }
    }

    /// Construct a new BooleanProperty restricted to a dialect.
    pub const fn new_with_dialect(
        name: &'static str,
        description: &'static str,
        default: &'static str,
        supported_dialect: Dialect,
    ) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: Some(supported_dialect),
        }
    }

    /// Retrieve the boolean value from connection state, falling back to default.
    pub fn get_value(&self, state: &ConnectionState) -> bool {
        state
            .get(self.name)
            .and_then(|val| val.parse::<bool>().ok())
            .unwrap_or_else(|| self.default.parse::<bool>().unwrap_or(false))
    }
}

impl ConnectionProperty for BooleanProperty {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        self.description
    }
    fn context(&self) -> Context {
        Context::User
    }
    fn default_value(&self) -> Option<String> {
        Some(self.default.to_string())
    }
    fn validate_and_convert(&self, value: &str, dialect: Dialect) -> Result<String, Error> {
        match parse_boolean_literal_str(value, dialect) {
            Some(true) => Ok("true".to_string()),
            Some(false) => Ok("false".to_string()),
            None => Err(Error::deser(ConnectionError::InvalidOption(format!(
                "Invalid boolean value for {}: {}",
                self.name, value
            )))),
        }
    }
    fn type_code(&self) -> TypeCode {
        TypeCode::Bool
    }
    fn to_value(&self, value: &str, dialect: Dialect) -> Value {
        let normalized = self
            .validate_and_convert(value, dialect)
            .unwrap_or_else(|_| "false".to_string());
        let b = normalized == "true";
        b.to_value()
    }
    fn is_supported(&self, dialect: Dialect) -> bool {
        self.supported_dialect.map(|d| d == dialect).unwrap_or(true)
    }
}

/// A connection property representing an integer value.
pub struct IntegerProperty {
    /// The name of the property.
    pub name: &'static str,
    /// The description.
    pub description: &'static str,
    /// The default value.
    pub default: &'static str,
    /// Optional restricted dialect for this property.
    pub supported_dialect: Option<Dialect>,
}

impl IntegerProperty {
    /// Construct a new IntegerProperty.
    pub const fn new(name: &'static str, description: &'static str, default: &'static str) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: None,
        }
    }

    /// Construct a new IntegerProperty restricted to a dialect.
    pub const fn new_with_dialect(
        name: &'static str,
        description: &'static str,
        default: &'static str,
        supported_dialect: Dialect,
    ) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: Some(supported_dialect),
        }
    }

    /// Retrieve the i64 value from connection state, falling back to default.
    pub fn get_value(&self, state: &ConnectionState) -> i64 {
        state
            .get(self.name)
            .and_then(|val| val.parse::<i64>().ok())
            .unwrap_or_else(|| self.default.parse::<i64>().unwrap_or(0))
    }
}

impl ConnectionProperty for IntegerProperty {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        self.description
    }
    fn context(&self) -> Context {
        Context::User
    }
    fn default_value(&self) -> Option<String> {
        Some(self.default.to_string())
    }
    fn validate_and_convert(&self, value: &str, _dialect: Dialect) -> Result<String, Error> {
        let val = value.trim();
        if val.parse::<i64>().is_ok() {
            Ok(val.to_string())
        } else {
            Err(Error::deser(ConnectionError::InvalidOption(format!(
                "Invalid integer value for {}: {}",
                self.name, value
            ))))
        }
    }
    fn type_code(&self) -> TypeCode {
        TypeCode::Int64
    }
    fn to_value(&self, value: &str, _dialect: Dialect) -> Value {
        let parsed = value.trim().parse::<i64>().unwrap_or(0);
        parsed.to_value()
    }
    fn is_supported(&self, dialect: Dialect) -> bool {
        self.supported_dialect.map(|d| d == dialect).unwrap_or(true)
    }
}

/// A connection property representing a string value.
pub struct StringProperty {
    /// The name of the property.
    pub name: &'static str,
    /// The description.
    pub description: &'static str,
    /// The default value.
    pub default: Option<&'static str>,
    /// Optional restricted dialect for this property.
    pub supported_dialect: Option<Dialect>,
}

impl StringProperty {
    /// Construct a new StringProperty.
    pub const fn new(
        name: &'static str,
        description: &'static str,
        default: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: None,
        }
    }

    /// Construct a new StringProperty restricted to a dialect.
    pub const fn new_with_dialect(
        name: &'static str,
        description: &'static str,
        default: Option<&'static str>,
        supported_dialect: Dialect,
    ) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: Some(supported_dialect),
        }
    }

    /// Retrieve the String value from connection state.
    pub fn get_value(&self, state: &ConnectionState) -> Option<String> {
        state
            .get(self.name)
            .or_else(|| self.default.map(|d| d.to_string()))
    }
}

impl ConnectionProperty for StringProperty {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        self.description
    }
    fn context(&self) -> Context {
        Context::User
    }
    fn default_value(&self) -> Option<String> {
        self.default.map(|d| d.to_string())
    }
    fn validate_and_convert(&self, value: &str, _dialect: Dialect) -> Result<String, Error> {
        Ok(value.to_string())
    }
    fn type_code(&self) -> TypeCode {
        TypeCode::String
    }
    fn to_value(&self, value: &str, _dialect: Dialect) -> Value {
        value.to_value()
    }
    fn is_supported(&self, dialect: Dialect) -> bool {
        self.supported_dialect.map(|d| d == dialect).unwrap_or(true)
    }
}

/// A connection property representing a string value configured only at connection startup.
pub struct StartupStringProperty {
    /// The name of the property.
    pub name: &'static str,
    /// The description.
    pub description: &'static str,
    /// The default value.
    pub default: Option<&'static str>,
    /// Optional restricted dialect for this property.
    pub supported_dialect: Option<Dialect>,
}

impl StartupStringProperty {
    /// Construct a new StartupStringProperty.
    pub const fn new(
        name: &'static str,
        description: &'static str,
        default: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: None,
        }
    }

    /// Construct a new StartupStringProperty restricted to a dialect.
    pub const fn new_with_dialect(
        name: &'static str,
        description: &'static str,
        default: Option<&'static str>,
        supported_dialect: Dialect,
    ) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: Some(supported_dialect),
        }
    }

    /// Retrieve the String value from connection state.
    pub fn get_value(&self, state: &ConnectionState) -> Option<String> {
        state
            .get(self.name)
            .or_else(|| self.default.map(|d| d.to_string()))
    }
}

impl ConnectionProperty for StartupStringProperty {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        self.description
    }
    fn context(&self) -> Context {
        Context::Startup
    }
    fn default_value(&self) -> Option<String> {
        self.default.map(|d| d.to_string())
    }
    fn validate_and_convert(&self, value: &str, _dialect: Dialect) -> Result<String, Error> {
        Ok(value.to_string())
    }
    fn type_code(&self) -> TypeCode {
        TypeCode::String
    }
    fn to_value(&self, value: &str, _dialect: Dialect) -> Value {
        value.to_value()
    }
    fn is_supported(&self, dialect: Dialect) -> bool {
        self.supported_dialect.map(|d| d == dialect).unwrap_or(true)
    }
}

/// Macro to implement ConnectionEnum for a type.
#[macro_export]
macro_rules! impl_connection_enum {
    ($t:ty, $($variant:ident => $string:expr),+ $(,)?) => {
        impl $crate::connection::connectionstate::ConnectionEnum for $t {
            fn to_snake_case(&self) -> &'static str {
                match self {
                    $(Self::$variant => $string,)*
                }
            }
            fn from_snake_case(s: &str) -> Option<Self> {
                match s {
                    $($string => Some(Self::$variant),)*
                    _ => None,
                }
            }
            fn allowed_values() -> &'static [&'static str] {
                &[$($string,)*]
            }
        }
    };
}

/// A trait representing connection properties that can parse case-insensitive snake_case strings.
pub trait ConnectionEnum: Sized + 'static {
    /// Get the snake_case name of the enum value.
    fn to_snake_case(&self) -> &'static str;
    /// Parse snake_case string to the enum value.
    fn from_snake_case(s: &str) -> Option<Self>;
    /// Get all allowed snake_case string values.
    fn allowed_values() -> &'static [&'static str];
}

/// A connection property representing an enum value.
pub struct EnumProperty<E: ConnectionEnum> {
    /// The name of the property.
    pub name: &'static str,
    /// The description.
    pub description: &'static str,
    /// The default value.
    pub default: &'static str,
    /// Optional restricted dialect for this property.
    pub supported_dialect: Option<Dialect>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E: ConnectionEnum> EnumProperty<E> {
    /// Construct a new EnumProperty.
    pub const fn new(name: &'static str, description: &'static str, default: &'static str) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Construct a new EnumProperty restricted to a dialect.
    pub const fn new_with_dialect(
        name: &'static str,
        description: &'static str,
        default: &'static str,
        supported_dialect: Dialect,
    ) -> Self {
        Self {
            name,
            description,
            default,
            supported_dialect: Some(supported_dialect),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Retrieve the Enum value from connection state.
    pub fn get_value(&self, state: &ConnectionState) -> E {
        let val = state
            .get(self.name)
            .unwrap_or_else(|| self.default.to_string());
        E::from_snake_case(&val).unwrap_or_else(|| E::from_snake_case(self.default).unwrap())
    }
}

impl<E: ConnectionEnum + Send + Sync> ConnectionProperty for EnumProperty<E> {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        self.description
    }
    fn context(&self) -> Context {
        Context::User
    }
    fn default_value(&self) -> Option<String> {
        Some(self.default.to_string())
    }
    fn validate_and_convert(&self, value: &str, _dialect: Dialect) -> Result<String, Error> {
        let val = value.trim().to_ascii_lowercase();
        if E::allowed_values().contains(&val.as_str()) {
            Ok(val)
        } else {
            Err(Error::deser(ConnectionError::InvalidOption(format!(
                "Invalid value for {}: {}",
                self.name, value
            ))))
        }
    }
    fn type_code(&self) -> TypeCode {
        TypeCode::String
    }
    fn to_value(&self, value: &str, _dialect: Dialect) -> Value {
        value.to_value()
    }
    fn is_supported(&self, dialect: Dialect) -> bool {
        self.supported_dialect.map(|d| d == dialect).unwrap_or(true)
    }
}

/// Stateful representation of connection properties under multiple prioritized scopes.
pub struct ConnectionState {
    dialect: Dialect,
    registry: &'static HashMap<String, &'static dyn ConnectionProperty>,
    session_properties: HashMap<String, String>,
    transaction_properties: HashMap<String, String>,
    local_properties: HashMap<String, String>,
    statement_properties: HashMap<String, String>,
    in_transaction: bool,
}

impl ConnectionState {
    /// Construct a new ConnectionState for the database dialect, initializing default values.
    pub fn new(
        dialect: Dialect,
        registry: &'static HashMap<String, &'static dyn ConnectionProperty>,
    ) -> Self {
        let mut session_properties = HashMap::new();
        // Initialize defaults from the registry
        for (name, prop) in registry.iter() {
            if let Some(def_val) = prop.default_value() {
                session_properties.insert(name.clone(), def_val);
            }
        }

        Self {
            dialect,
            registry,
            session_properties,
            transaction_properties: HashMap::new(),
            local_properties: HashMap::new(),
            statement_properties: HashMap::new(),
            in_transaction: false,
        }
    }

    /// Retrieve the dialect of the connection.
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// Check if currently in an active transaction block.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Check if registry has a property.
    pub fn has_property(&self, key: &str) -> bool {
        self.registry.contains_key(key)
    }

    /// Get registered property object.
    pub fn get_property(&self, key: &str) -> Option<&'static dyn ConnectionProperty> {
        self.registry.get(key).copied()
    }

    /// Retrieve a property value by key, resolving through Statement > Local > Transaction > Session scopes.
    pub fn get(&self, key: &str) -> Option<String> {
        let key_lower = key.to_ascii_lowercase();
        self.statement_properties
            .get(&key_lower)
            .or_else(|| self.local_properties.get(&key_lower))
            .or_else(|| self.transaction_properties.get(&key_lower))
            .or_else(|| self.session_properties.get(&key_lower))
            .cloned()
    }

    /// Helper to get a boolean value from state.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(|val| val.parse::<bool>().ok())
    }

    /// Helper to get an integer value from state.
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(|val| val.parse::<i64>().ok())
    }

    /// Set a property in the target scope.
    pub fn set(
        &mut self,
        key: &str,
        value: &str,
        is_local: bool,
        is_statement: bool,
    ) -> Result<(), Error> {
        self.set_internal(key, value, is_local, is_statement, false)
    }

    /// Set a property value at connection startup.
    pub fn set_startup(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.set_internal(key, value, false, false, true)
    }

    fn set_internal(
        &mut self,
        key: &str,
        value: &str,
        is_local: bool,
        is_statement: bool,
        is_startup: bool,
    ) -> Result<(), Error> {
        let key_lower = key.to_ascii_lowercase();
        let normalized_value = if key_lower.contains('.') {
            value.to_string()
        } else {
            let prop = self.registry.get(&key_lower).ok_or_else(|| {
                Error::deser(ConnectionError::InvalidOption(format!(
                    "Unknown property: {}",
                    key
                )))
            })?;
            if !is_startup && prop.context() == Context::Startup {
                return Err(Error::deser(ConnectionError::InvalidOption(format!(
                    "Property {} can only be set at startup",
                    key
                ))));
            }
            prop.validate_and_convert(value, self.dialect)?
        };

        if is_statement {
            self.statement_properties
                .insert(key_lower, normalized_value);
        } else if is_local {
            if self.in_transaction {
                self.local_properties.insert(key_lower, normalized_value);
            } else {
                // SET LOCAL outside a transaction is a no-op
            }
        } else if self.in_transaction {
            self.transaction_properties
                .insert(key_lower, normalized_value);
        } else {
            self.session_properties.insert(key_lower, normalized_value);
        }
        Ok(())
    }

    /// Transition state to an active transaction block.
    pub fn begin(&mut self) {
        self.in_transaction = true;
    }

    /// Commit transaction block, merging transaction properties into session scope, and clearing transaction and local maps.
    pub fn commit(&mut self) {
        for (k, v) in self.transaction_properties.drain() {
            self.session_properties.insert(k, v);
        }
        self.local_properties.clear();
        self.statement_properties.clear();
        self.in_transaction = false;
    }

    /// Rollback transaction block, discarding transaction and local maps.
    pub fn rollback(&mut self) {
        self.transaction_properties.clear();
        self.local_properties.clear();
        self.statement_properties.clear();
        self.in_transaction = false;
    }

    /// Clear all statement-scoped properties. Must be called after statement execution.
    pub fn clear_statement_properties(&mut self) {
        self.statement_properties.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    static TEST_REGISTRY: LazyLock<HashMap<String, &'static dyn ConnectionProperty>> =
        LazyLock::new(|| {
            let mut m = HashMap::<String, &'static dyn ConnectionProperty>::new();
            static AUTOCOMMIT: BooleanProperty =
                BooleanProperty::new("autocommit", "Autocommit description", "true");
            static READONLY: BooleanProperty =
                BooleanProperty::new("readonly", "Readonly description", "false");
            static STATEMENT_TIMEOUT: IntegerProperty =
                IntegerProperty::new("statement_timeout", "Timeout description", "0");
            static OPTIMIZER_VERSION: StartupStringProperty = StartupStringProperty::new(
                "optimizer_version",
                "Optimizer description",
                Some("latest"),
            );

            m.insert(AUTOCOMMIT.name().to_string(), &AUTOCOMMIT);
            m.insert(READONLY.name().to_string(), &READONLY);
            m.insert(STATEMENT_TIMEOUT.name().to_string(), &STATEMENT_TIMEOUT);
            m.insert(OPTIMIZER_VERSION.name().to_string(), &OPTIMIZER_VERSION);
            m
        });

    #[test]
    fn test_initial_values_and_defaults() {
        let state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        assert_eq!(state.get("autocommit").as_deref(), Some("true"));
        assert_eq!(state.get("readonly").as_deref(), Some("false"));
        assert_eq!(state.get("statement_timeout").as_deref(), Some("0"));
        assert_eq!(state.get("transaction_tag"), None);
    }

    #[test]
    fn test_set_value_outside_transaction() {
        let mut state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        state
            .set("autocommit", "false", false, false)
            .expect("should set");
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("false"),
            "should persist outside transaction"
        );
    }

    #[test]
    fn test_set_value_in_transaction_and_commit() {
        let mut state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        state.begin();
        state
            .set("autocommit", "false", false, false)
            .expect("should set");
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("false"),
            "should be visible in transaction"
        );

        state.commit();
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("false"),
            "should persist after commit"
        );
    }

    #[test]
    fn test_set_value_in_transaction_and_rollback() {
        let mut state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        state.begin();
        state
            .set("autocommit", "false", false, false)
            .expect("should set");
        assert_eq!(state.get("autocommit").as_deref(), Some("false"));

        state.rollback();
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("true"),
            "should revert to default on rollback"
        );
    }

    #[test]
    fn test_set_local_value() {
        let mut state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        state.begin();
        state
            .set("autocommit", "false", true, false)
            .expect("should set local");
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("false"),
            "local value should override default"
        );

        state.commit();
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("true"),
            "local value should be discarded after commit"
        );

        state.begin();
        state
            .set("autocommit", "false", true, false)
            .expect("should set local again");
        state.rollback();
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("true"),
            "local value should be discarded after rollback"
        );
    }

    #[test]
    fn test_set_local_value_outside_transaction() {
        let mut state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        state
            .set("autocommit", "false", true, false)
            .expect("should run set local");
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("true"),
            "local set outside transaction must be a no-op"
        );
    }

    #[test]
    fn test_set_statement_value() {
        let mut state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        state
            .set("autocommit", "false", false, true)
            .expect("should set statement value");
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("false"),
            "statement value should override default"
        );

        state.clear_statement_properties();
        assert_eq!(
            state.get("autocommit").as_deref(),
            Some("true"),
            "statement value should be cleared"
        );
    }

    #[test]
    fn test_scoping_priorities() {
        let mut state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        // Session: autocommit = true
        state.begin();
        // Transaction: autocommit = false
        state
            .set("autocommit", "false", false, false)
            .expect("should set transaction value");
        assert_eq!(state.get("autocommit").as_deref(), Some("false"));

        // Local overrides Transaction
        state
            .set("autocommit", "true", true, false)
            .expect("should set local value");
        assert_eq!(state.get("autocommit").as_deref(), Some("true"));

        // Statement overrides Local
        state
            .set("autocommit", "false", false, true)
            .expect("should set statement value");
        assert_eq!(state.get("autocommit").as_deref(), Some("false"));

        // Clearing statement returns to Local
        state.clear_statement_properties();
        assert_eq!(state.get("autocommit").as_deref(), Some("true"));
    }

    #[test]
    fn test_postgres_extensions() {
        let mut state = ConnectionState::new(Dialect::PostgreSql, &TEST_REGISTRY);
        state
            .set("spanner.my_extension", "some-config", false, false)
            .expect("extensions bypass registry check");
        assert_eq!(
            state.get("spanner.my_extension").as_deref(),
            Some("some-config")
        );
    }

    #[test]
    fn test_unknown_properties_fail() {
        let mut state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        let res = state.set("unknown_property", "value", false, false);
        assert!(
            res.is_err(),
            "unknown properties must return validation error"
        );
    }

    #[test]
    fn test_startup_only_properties() {
        let mut state = ConnectionState::new(Dialect::GoogleSql, &TEST_REGISTRY);
        // Direct set (session scope) during execution must fail
        let res_set = state.set("optimizer_version", "2", false, false);
        assert!(res_set.is_err());
        assert!(
            res_set
                .unwrap_err()
                .to_string()
                .contains("can only be set at startup")
        );

        // Startup set must succeed
        state
            .set_startup("optimizer_version", "2")
            .expect("should succeed at startup");
        assert_eq!(state.get("optimizer_version").as_deref(), Some("2"));
    }

    #[test]
    fn test_boolean_property_googlesql() {
        let prop = BooleanProperty::new("autocommit", "Autocommit description", "true");

        // Valid inputs
        assert_eq!(
            prop.validate_and_convert("true", Dialect::GoogleSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("TRUE", Dialect::GoogleSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("  on  ", Dialect::GoogleSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("1", Dialect::GoogleSql).unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("yes", Dialect::GoogleSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("t", Dialect::GoogleSql).unwrap(),
            "true"
        );

        assert_eq!(
            prop.validate_and_convert("false", Dialect::GoogleSql)
                .unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("off", Dialect::GoogleSql)
                .unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("0", Dialect::GoogleSql).unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("no", Dialect::GoogleSql).unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("f", Dialect::GoogleSql).unwrap(),
            "false"
        );

        // Invalid inputs (no prefixes allowed in GoogleSql)
        assert!(
            prop.validate_and_convert("tru", Dialect::GoogleSql)
                .is_err()
        );
        assert!(prop.validate_and_convert("fa", Dialect::GoogleSql).is_err());
        assert!(prop.validate_and_convert("ye", Dialect::GoogleSql).is_err());
    }

    #[test]
    fn test_boolean_property_postgresql() {
        let prop = BooleanProperty::new("autocommit", "Autocommit description", "true");

        // Valid inputs including prefixes
        assert_eq!(
            prop.validate_and_convert("t", Dialect::PostgreSql).unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("tr", Dialect::PostgreSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("tru", Dialect::PostgreSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("true", Dialect::PostgreSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("y", Dialect::PostgreSql).unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("ye", Dialect::PostgreSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("yes", Dialect::PostgreSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("on", Dialect::PostgreSql)
                .unwrap(),
            "true"
        );
        assert_eq!(
            prop.validate_and_convert("1", Dialect::PostgreSql).unwrap(),
            "true"
        );

        assert_eq!(
            prop.validate_and_convert("f", Dialect::PostgreSql).unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("fa", Dialect::PostgreSql)
                .unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("fal", Dialect::PostgreSql)
                .unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("fals", Dialect::PostgreSql)
                .unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("false", Dialect::PostgreSql)
                .unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("n", Dialect::PostgreSql).unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("no", Dialect::PostgreSql)
                .unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("of", Dialect::PostgreSql)
                .unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("off", Dialect::PostgreSql)
                .unwrap(),
            "false"
        );
        assert_eq!(
            prop.validate_and_convert("0", Dialect::PostgreSql).unwrap(),
            "false"
        );

        // Invalid/Ambiguous inputs
        assert!(prop.validate_and_convert("o", Dialect::PostgreSql).is_err()); // Ambiguous (on / off)
        assert!(
            prop.validate_and_convert("invalid", Dialect::PostgreSql)
                .is_err()
        );
    }

    #[test]
    fn test_application_name_validation() {
        use crate::connection::connectionproperties::get_registry;

        // PostgreSQL dialect connection state accepts application_name
        let mut pg_state =
            ConnectionState::new(Dialect::PostgreSql, get_registry(Dialect::PostgreSql));
        assert!(pg_state.has_property("application_name"));
        pg_state
            .set("application_name", "my_app", false, false)
            .unwrap();
        assert_eq!(pg_state.get("application_name").as_deref(), Some("my_app"));

        // GoogleSQL dialect connection state rejects application_name
        let mut gsql_state =
            ConnectionState::new(Dialect::GoogleSql, get_registry(Dialect::GoogleSql));
        assert!(!gsql_state.has_property("application_name"));
        let res = gsql_state.set("application_name", "my_app", false, false);
        assert!(res.is_err());
    }
}
