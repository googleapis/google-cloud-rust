# Google Cloud client libraries for Rust: ProtoJSON conformance

This directory contains a test to verify the client libraries follow the
[ProtoJSON] specification.

The conformance test is a Rust binary, it accepts serialized messages from its
stdin, and returns serialized messages in stdout. A driver in the
[Protocol Buffers repository] exercises the conformance test by providing
specific inputs and validating the output.

## Compiling the conformance test

In this guide we will assume you used `$HOME/rust-conformance` to build the
conformance test. Change the directories below if needed.

```shell
cd $HOME
git clone https://github.com/googleapis/google-cloud-rust rust-conformance
cd rust-conformance
cargo build -p protojson-conformance
```

## Compiling the test runner

We will also need to checkout the Protobuf code:

```shell
cd $HOME
git clone -b 31.x https://github.com/protocolbuffers/protobuf.git
cd protobuf
```

Install bazelisk:

```shell
curl -fsSL --retry 5 --retry-all-errors --retry-delay 15 https://github.com/bazelbuild/bazelisk/releases/download/v1.26.0/bazelisk-darwin-arm64 -o bazelisk
chmod 755 bazelisk
./bazelisk version
```

```shell
USE_BAZEL_VERSION=8.2.1 ./bazelisk build --repo_env=BAZEL_NO_APPLE_CPP_TOOLCHAIN=1 --enable_bzlmod  //conformance:conformance_test_runner
```

## Running

Use bazelisk to compile and run the test program:

```shell
USE_BAZEL_VERSION=8.2.1 ./bazelisk run \
    --repo_env=BAZEL_NO_APPLE_CPP_TOOLCHAIN=1 --enable_bzlmod  -- \
    //conformance:conformance_test_runner \
    --failure_list $HOME/rust-conformance/src/protojson-conformance/expected_failures.txt \
    $HOME/rust-conformance/target/debug/protojson-conformance
```

[protocol buffers repository]: https://github.com/protocolbuffers/protobuf/blob/main/conformance/README.md
[protojson]: https://protobuf.dev/programming-guides/json/
