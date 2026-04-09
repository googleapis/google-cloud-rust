When you make a change to the code, you should test it. For this part of the project, you should be running:

```shell
cargo check -p google-cloud-gax
```

to make sure everything compiles. If not, fix the compiler errors.

If everything passes, then you should try running the tests.

```shell
cargo test -p google-cloud-gax
```

The tests should not require any changing. If they fail, it means we refactored wrong. Please fix the code without touching anything in the `mod test`.
