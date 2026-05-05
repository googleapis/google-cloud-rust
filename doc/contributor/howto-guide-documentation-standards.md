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

### Feature-gated samples (`run_all_samples`)

For Rust samples that should be skipped in presubmit but still compile in a
dedicated job, prefer a feature-gated snippet over `ignore`:

````rust
/// ```
/// # #[cfg(feature = "run_all_samples")]
/// # async fn sample() -> anyhow::Result<()> {
/// // sample code
/// # Ok(())
/// # }
/// ```
````

Then run those snippets with `cargo test --doc --all-features` (or
`--features run_all_samples`) for the crate.

### `ignore`

Use `ignore` sparingly for valid Rust code blocks that are intentionally
excluded from doc-test compilation.

> [!NOTE]
> In standard Rust practice, `ignore` often means the test is broken or should
> not be run. In this repository, prefer feature-gated snippets for scalable
> sample testing; reserve `ignore` for rare cases that must remain excluded from
> doc-test compilation.

**When to use**:

- When a snippet is intentionally excluded from doc-test compilation.

**Implications**:

- The code block will be syntax-highlighted as Rust code.
- `rustdoc` will not run it during normal `cargo test` (presubmit).
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
- **Feature-Gated Samples**: Crates may define `run_all_samples`; these snippets
  are compiled by running `cargo test --doc --all-features` (or
  `--features run_all_samples`) for that crate.

### Tag Selection Summary

- Use `no_rust` for non-Rust content (YAML, JSON, logs) or Rust code that is not
  expected to compile.
- Use `no_run` for code that should be checked for compilation but cannot be
  executed (e.g., requires network or side effects).
- Prefer feature-gated snippets (`#[cfg(feature = "run_all_samples")]`) for
  runnable Rust samples that should be skipped in presubmit.
- Use `ignore` only when a snippet must remain excluded from doc-test
  compilation.

[developer documentation style guide]: https://developers.google.com/style/
