# Howto-Guide: Set Up Development Environment

This guide is intended for contributors to the `google-cloud-rust` SDK. It will
walk you through the steps necessary to set up your development workstation to
compile the code, run the unit tests, and formatting miscellaneous files.

## Installing Rust

We recommend that you follow the [Getting Started][getting-started-rust] guide.
Once you have `cargo` and `rustup` installed the rest is relatively easy.

## Installing Go

The code generator is implemented in [Go](https://go.dev). Follow the
[Download and install][golang-install] guide to install golang.

## IDE Recommendations

Whatever works for you. Several team members use Visual Studio Code, but Rust
can be used with many IDEs.

## Compile the Code

Just use cargo:

```bash
cargo build
```

## Running the unit tests

```bash
cargo test
```

## Running lints and unit tests

```bash
cargo fmt && cargo clippy -- --deny warnings && cargo test
git status # Shows any diffs created by `cargo fmt`
```

## Miscellaneous Tools

We use a number of tools to format non-Rust code. The CI builds enforce
formatting, you can fix any formatting problems manually (using the CI logs),
or may prefer to install these tools locally to fix formatting problems.

Typically we do not format these files for generated code, so local runs
requires skipping the generated files.

We use `taplo` to format the hand-crafted TOML files. Install with:

```bash
cargo install taplo-cli
```

use with:

```bash
git ls-files -z -- '*.toml' ':!:**/testdata/**' ':!:src/generated/**' | xargs -0 taplo fmt
```

We use `typos` to detect typos. Install with:

```bash
cargo install typos-cli
```

We use `mdformat` to format hand-crafted markdown files. Install with:

```bash
python -m venv .venv
source .venv/bin/activate # Or whatever is the right command for you shell
pip install -r ci/requirements.txt
```

use with:

```bash
git ls-files -z -- '*.md' ':!:**/testdata/**' | xargs -0 -r -P "$(nproc)" -n 50 mdformat
```

We use `yamlfmt` to format hand-crafted YAML files (mostly GitHub Actions).
Install and use with:

```bash
go install github.com/google/yamlfmt/cmd/yamlfmt@v0.13.0
```

use with:

```bash
git ls-files -z -- '*.yaml' '*.yml' ':!:**/testdata/**' | xargs -0 yamlfmt
```

[getting-started-rust]: https://www.rust-lang.org/learn/get-started
[golang-install]: https://go.dev/doc/install
