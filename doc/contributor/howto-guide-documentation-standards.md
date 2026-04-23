# Documentation Standards

The public API of all code (modules, structs, functions, macros) should be
documented. This standard ensures that users can easily understand the purpose
and usage of SDK components.

For general Rust documentation guidelines, see the
[Rust API Guidelines on Documentation](https://rust-lang.github.io/api-guidelines/documentation.html).

## Recommended Structure

Each item's documentation should follow this basic structure:

1. **One-liner**: A concise summary line.
1. **Newline**: A blank line should follow the one-liner to clearly separate it
   from subsequent sections.
1. **Code example**: An illustrative example (placed before the long
   description).
1. **Long description**: Detailed explanation of behavior, constraints, and
   scope.
1. **Advanced notes**: Technical details if necessary.
1. **Cross-references**: Use Markdown links (e.g., `[StructName]`) to connect
   related types and external links for deep-dive conceptual documentation.
1. **Error Handling**: Examples should use the `?` operator, not `unwrap()` or
   `expect()`.

## Style

See the [Developer Documentation Style Guide] for additional guidance on writing
documentation.

## Doc Sample Tags (`ignore`, `no_rust`, `no_run`)

When writing code blocks in documentation, you can use tags to control how
`rustdoc` and testing tools treat them.

### `no_rust`

Use the `no_rust` tag for code blocks that are not Rust code, or for Rust code
that is not expected to compile (like pseudocode). This prevents `rustdoc` from
attempting to compile and run the block as a doc test.

**When to use**:

- For example configuration files (e.g., JSON, YAML).
- For pseudocode or conceptual examples that are not valid Rust.
- For command-line output or log snippets.

**Implications**:

- The block is rendered as a code block but is ignored by the Rust compiler and
  doc test runner.

**Example**:

````rust
/// Resources are named as follows:
/// ```no_rust
/// bucket = "projects/_/buckets/my-bucket"
/// object = "my-object/with/a/folder-like/name"
/// ```
````

### `ignore`

Use the `ignore` tag for valid Rust code blocks that should not be run on
presubmit.

> [!NOTE]
> This project uses the `ignore` tag in a non-standard way. In standard Rust
> practice, `ignore` often means the test is broken or should not be run. In
> this repository, we use `ignore` as a filter to separate tests run on
> presubmit from the large volume of tests run in post-submit (to keep presubmit
> times reasonable).

**When to use**:

- When the volume of tests is too large to be included in presubmit (to keep
  presubmit times reasonable). Particularly in generated code.

**Implications**:

- The code block will be syntax-highlighted as Rust code.
- `rustdoc` will not run it during normal `cargo test` (presubmit).
- It **is** run in post-submit or periodic jobs using
  `cargo test --doc -- --ignored`.
- It will be marked as "not tested" on docs.rs.

### `no_run`

Use the `no_run` tag for valid Rust code blocks that should be compiled but not
executed.

**When to use**:

- When the code block contains statements that ARE executed (e.g., in a `main`
  function or as top-level statements) and that code requires a network
  connection or causes side effects.

> [!NOTE]
> It is NOT strictly necessary to add `no_run` to samples that are purely
> function definitions (like `async fn sample(...) { ... }`) that are never
> called, because `rustdoc` will not execute them anyway, and the overhead of
> starting the process is negligible.

**Implications**:

- The code is checked for compilation errors by `cargo test --doc`.
- It is not executed.

## Doc Test Execution in CI

The repository runs documentation tests automatically in CI to ensure examples
remain valid.

- **Workspace Doc Tests**: The primary CI workflow (via Google Cloud Build) runs
  `cargo test` on the workspace on presubmit. This automatically executes all
  compilable doc tests in library crates that are not marked with `ignore` or
  `no_rust`.
- **Ignored Doc Tests**: A separate CI job runs `cargo test --doc -- --ignored`
  (typically in post-submit) to execute tests marked with `ignore`.

### Tag Selection Summary

- Use `no_rust` for non-Rust content (YAML, JSON, logs) or Rust code that is not
  expected to compile.
- Use `no_run` for code that should be checked for compilation but cannot be
  executed (e.g., requires network or side effects).
- Use `ignore` for valid Rust code that should not be run on presubmit (e.g., to
  manage the volume of tests and keep presubmit times reasonable) but should be
  tested in post-submit.

[developer documentation style guide]: https://developers.google.com/style/
