# Gemini Instructions for the `guide/` Directory

This directory contains the user guide for the "Google Cloud Client Libraries
for Rust". The guide is built using
[mdBook](https://rust-lang.github.io/mdBook/).

## Directory Structure

- `src/`: Contains the Markdown files that make up the content of the guide. The
  structure is defined in `src/SUMMARY.md`.
- `samples/`: Contains a Rust crate (`user-guide-samples`) with the actual
  source code for the examples shown in the guide.

## Guidelines for Modifying the Guide

When making changes to the guide or adding new examples, please adhere to the
following rules:

1. **Code Must Compile:** Do not write large blocks of Rust code directly inside
   the Markdown files. All code examples must be valid, compilable Rust code
   located in the `guide/samples/` crate.
1. **Use Include Directives:** To insert code into the guide, use mdBook's
   `{{#include ...}}` or `{{#rustdoc_include ...}}` syntax to include specific
   portions of the files from the `samples/` directory.
   - Example: `{{#include ../samples/src/my_example.rs:my-anchor}}`
1. **Use Anchors:** Use `// ANCHOR: my-anchor` and `// ANCHOR_END: my-anchor`
   comments inside the Rust files in `guide/samples/` to define the exact lines
   that should be included in the Markdown. Note that some files use two styles
   of anchors: the `ANCHOR: ${name}` style used by mdBook (with a matching
   `ANCHOR_END: ${name}`) and also `[START ${name}]` and `[END ${name}]`
   anchors. When present, preserve both types of anchors. (For example, the
   `endpoint/default.rs` file has both types of anchors). When adding the
   `[START/END]` anchors:
   - Use underscores to separate words instead of hyphens.
   - Start the name with a `rust_` prefix (e.g., `[START rust_my_anchor]`).
   - Put both anchor styles on the same line (e.g.,
     `// [START rust_my_anchor] ANCHOR: my-anchor`).
1. **Make code readable for users:** the audience of this guide are users of the
   project.
   - Prefer simple code.
   - Use idiomatic code.
   - Use well-known crates like `anyhow` to simplify the code.
1. **Faster iteration:** When making small changes verify the code in the
   `samples/` crate compiles and is formatted. Skip linting and tests:
   - Run `cargo fmt -p user-guide-samples`
   - Run `cargo check -p user-guide-samples`
1. **Test the Samples:** Always verify that the code in the `samples/` crate
   compiles, passes linting, and formatting checks.
   - Run `cargo fmt -p user-guide-samples`
   - Run `cargo check -p user-guide-samples`
   - Run `cargo clippy -p user-guide-samples`
   - Run `cargo test -p user-guide-samples`
1. **Formatting the Markdown:** use mdformat to format the `*.md` files.
   - If needed create a virtual environment with `python3 -m venv .venv`
   - If needed install `mdformat` using `source .venv/bin/activate` and then
     `pip install -r ci/requirements.txt` in that environment.
   - Run
     `git ls-files -z -- '*.md' ':!:**/testdata/**' ':!:**/generated/**' | xargs -0 mdformat`
