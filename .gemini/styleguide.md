# Gemini Code Assist Style Guide

This guide defines the standards for code generation, refactors, and code
reviews for the Google Cloud Rust SDK repository.

When reviewing or generating code, apply the following checks and references:

## General Principles

- **Follow local standards:** Ensure all changes conform to the high-level
  architecture described in [ARCHITECTURE.md](../ARCHITECTURE.md).
- **Idiomatic Rust:** Encourage modern, efficient, and readable Rust code.
- **SDK Consistency:** Maintain patterns consistent with existing crates like
  `auth` and `gax`.

## Rigor & Correctness

When reviewing or generating code, apply rigorous scrutiny:

- **Reject "Code Poetry":** Dismantle complex abstractions used for simple
  tasks. Prefer simplicity over cleverness.
- **Scrutinize Edge Cases:** Always consider failure modes (network failures,
  unexpected inputs). Do not assume a perfect world.
- **Expose Hidden Costs:** Question new dependencies or patterns that add
  significant boilerplate for minimal gain.
- **Demand Explosive Correctness:** Never swallow errors or ignore `Result`
  types. Fail loudly and explicitly when appropriate.

## Safety & Error Handling

- **Unsafe Code:** Avoid `unsafe` unless absolutely necessary. Any `unsafe`
  block must have a `// SAFETY:` comment explaining why it is safe.
- **Panics:** No `unwrap()` or `expect()` in production code or examples (use
  `?` or handle errors). No `panic!` macro calls in library code. `unwrap()` is
  acceptable in tests.
- **Error Handling:** Public functions should return `Result<T, Error>`. Use the
  `?` operator for propagation. Custom error types should imply meaningful
  distinctions for the user.

## Async & Concurrency

- **Send & Sync:** New public `async fn` and Future types MUST be `Send` and
  `Sync` unless thread-affinity is explicitly intended.
- **Blocking:** No blocking I/O (e.g., `std::fs`, `std::net`) in async
  functions. Use `tokio::task::spawn_blocking` for CPU-intensive tasks.

## API Design

- **Naming:** Follow standard Rust conventions (Types: `UpperCamelCase`,
  Functions/Variables: `snake_case`, Constants: `SCREAMING_SNAKE_CASE`). Getters
  should generally not use the `get_` prefix.
- **Builders:** Use the Builder pattern for complex configuration. Builders
  should consume `self` for fluent chaining.
- **Interoperability:** Implement `Debug` for all public types. Implement
  `Default` where appropriate.

## Implementation Style

- **Return Early:** Avoid unnecessary `else` blocks to reduce indentation and
  keep the main logic flow linear.
- **Documentation:** Document all public items with `///`. Doctests describing
  usage are highly encouraged.

## Google Cloud SDK Specifics

- **Generated Code:** Do not edit files in `src/generated` directly. Changes to
  generated code must be made via the generator or configuration.
- **Crate Organization:**
  - Authentication logic belongs in `google-cloud-auth` (`src/auth`).
  - Common logic (GAX) belongs in `google-cloud-gax` (`src/gax`).
  - Service-specific code belongs in its own crate.

## Commit Messages

- **Conventional Commits:** Follow the conventions in
  [CONTRIBUTING.md](../CONTRIBUTING.md#commit-messages).
- **Format:** `<type>(<scope>): <description>`
- **Types:** `feat`, `fix`, `docs`, `impl`, `refactor`, `cleanup`, `test`, `ci`.
- **Scope:** Crate name without the `google-cloud-` prefix.
- **Description:** Short one-line summary completing the sentence "This change
  modifies the crate to ...".

## Formatting

- Code must be formatted with `cargo fmt`.
- Avoid excessive blank lines; use line breaks only to signal context shifts.
