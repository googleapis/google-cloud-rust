# Connection API Rules and Conventions

This directory contains the stateful Connection API implementation for Spanner.

## Visibility & Scoping

1.  **Strictly Encapsulate Internal Types**:
    - By default, modules and types in this directory (such as client caching `pool.rs`, DSN parsers, credentials loaders, etc.) must be declared crate-private (`pub(crate)`) or private.
    - Avoid exposing internal data structures, helpers, or states as public exports in `mod.rs` unless they are explicitly intended to be utilized directly by connection driver implementors.
    - Declare internal modules in `mod.rs` as `pub(crate) mod`.

## Parser Conventions

1.  **No Regex in SQL Parser**:
    - The SQL tokenizer and statement classifier in `parser.rs` must remain regular expression-free. Use stateful char/byte scanning and custom dispatch loops.
    - Trailing comment boundaries (`/* ... */`, `#`, `--`) must be parsed properly without consuming trailing comments as literals.
2.  **Connection String (DSN) Parsing**:
    - Connection strings (DSN) can be parsed using regex if helpful, but stateful character scanners are preferred when handling quoted values and nested separators (e.g. `?prop="value;with;semicolons"`).

## Connection State Scopes

1.  **Prioritized Lookup Order**:
    - Lookups must resolve properties in order: **Statement > Local > Transaction > Session**.
2.  **SET LOCAL Behavior**:
    - `SET LOCAL` is only valid within an active transaction.
    - When executed outside a transaction, it must result in a silent **no-op** (does not error, does not modify state).
3.  **PostgreSQL Dialect Extensions**:
    - Any connection property containing a dot (`.`) is considered a PostgreSQL extension property and must bypass standard registry validations.

## Code Structure, Modularity & Method Complexity

1.  **Cohesive Files**: Favor splitting code across multiple small, highly focused source files rather than creating large monolithic files.
2.  **Subdirectories**: Group related files into logical subfolders.
3.  **Clean Imports**:
    - Always place `use` import statements at the top of the file rather than using fully-qualified type names in the body of functions (e.g., use `use crate::google::spanner::v1::Type;` instead of writing `crate::google::spanner::v1::Type` inline).
    - Keep inline code clean, readable, and idiomatic.
4.  **Visibility & Scoping**: Restrict types, functions, and methods to the narrowest possible visibility scope.
5.  **Method Length**: Keep methods and functions short, focused, and single-purpose. A function should ideally not exceed 40 lines of code.
6.  **Nesting Limits**: Avoid deep nesting (e.g., loops containing matches containing multi-line blocks). If a method has more than 2 levels of control nesting, extract the inner logic into descriptive private helper methods.
7.  **No Code in mod.rs**: `mod.rs` files must not contain actual implementation code. They should only contain module declarations (`pub mod`, `pub(crate) mod`) and re-exports (`pub use`).

## Naming Conventions & Variable Names

1.  **Descriptive Naming**: Use full, descriptive words instead of abbreviations for variable, parameter, type, and function names.
2.  **Allowed Abbreviations**: Only very common and universally understood abbreviations are permitted (e.g., `db` for database).
3.  **Disallowed Abbreviations**: Avoid short-hands such as `src` (use `source`), `dst` (use `destination`), `buf` (use `buffer`), `rs` (use `result_set`), `msg` (use `message`), `cc` (use `command_complete`), `err` (use `error`), etc.

## Code Formatting & Formatting Checks

1.  **Auto-Formatting**: Always run `cargo fmt -p google-cloud-spanner` after making code changes to ensure format consistency across files.
2.  **Linting**: Run `cargo clippy -p google-cloud-spanner` and address any warnings before submitting code changes. Only run the code formatter and clippy for the Spanner client library crate.

## Self-Review & Refactoring

1.  **Complexity Review**: After writing code, perform a self-review to identify opportunities for simplification.
2.  **Code Simplification**: Refactor long or complex methods into shorter, single-responsibility functions. Reduce file size by modularizing code further if a file exceeds a few hundred lines.
3.  **Guideline Compliance Check**: Explicitly verify that all formatting, headers, naming, import structure, and error handling rules defined in this document are strictly followed.
