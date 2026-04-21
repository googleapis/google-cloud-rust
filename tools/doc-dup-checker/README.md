# Doc Duplicate Checker

This tool identifies instances of duplicate documentation caused by documented
re-exports (`pub use`) in the Rust workspace.

## Problem

When a crate re-exports an item and both the re-export and the target item have
documentation comments, `rustdoc` concatenates them in the generated HTML. This
often leads to redundant or confusing documentation for the user.

Example:

```rust
/// Docs for re-export.
pub use target::Item;

// in target:
/// Docs for item.
pub struct Item;
```

The resulting documentation will show both "Docs for re-export." and "Docs for
item." appended together.

## Solution

This tool leverages the Rustdoc JSON backend to detect overlapping documentation
between re-exports and their targets. It helps maintain high-quality
documentation standards programmatically.

## Usage

### Workspace Mode (Recommended)

To run the check on all handwritten, published crates in the workspace:

```bash
cargo run -p doc-dup-checker -- --workspace
```

This will automatically discover the relevant crates, generate their Rustdoc
JSON, and analyze them.

## Exceptions

The tool ignores warnings if the doc comment on the re-export contains an
intentional paragraph break (e.g., `\n\n` or ends with a newline), as this might
be a conscious choice to add context.
