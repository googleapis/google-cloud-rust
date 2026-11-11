# explicit_trait_params

A [Dylint] lint that requires calls to the methods of the configured traits to
spell out the trait's generic parameters, for example
`ToProto::<P>::to_proto(x)` instead of `x.to_proto()`.

If the trait parameter is inferred, a semver-compatible release of another crate
that adds a second impl for the same `Self` type makes the call ambiguous
(E0283). The lint only flags calls where that can happen: calls outside the
trait's defining crate whose receiver type is neither a generic parameter nor a
type defined in the calling crate. See the comments on `why_flag` in
`src/lib.rs` for the details.

The traits are configured in the top-level `dylint.toml`.

## Running

```sh
cargo install cargo-dylint dylint-link
cargo dylint --all -- --workspace --lib --all-features
```

This library builds with the nightly toolchain pinned in `rust-toolchain.toml`.
To run its tests:

```sh
cd tools/lints/explicit_trait_params
cargo test
```

[dylint]: https://github.com/trailofbits/dylint
