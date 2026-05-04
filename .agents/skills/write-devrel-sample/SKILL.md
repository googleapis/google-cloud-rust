---
name: write-devrel-sample
description: Use this skill when asked to write a DevRel sample.
---

# Task

You need to write a sample using a Rust client library.

You will be given a DevRel region tag. This is a way for Google Cloud to
associate the sample code with its documentation.

## Identify the sample

First, identify which service this is for. This is almost always the first word
in the snake case of the region. For example, if the region is
`storage_list_buckets`, the service will be `storage`.

Next, identify where the examples for this service live in the codebase.
Typically, if we are writing a sample, it is for a "veneer", which will live
under `src/<service>/examples`, e.g. `src/storage/examples` above.

If this directory does not exist, stop and ask your human for help.

## Identify the sample crate

Next identify the name of the crate for the examples. Typically this is
`<service>-samples`, e.g. `storage-samples` above.

Now, in this repository, we execute our samples in our CI to make sure they
work. Typically, this code runs against production.

Before we get started, make sure you can execute the existing samples
successfully.

First, let's identify the GCP project we will use to run the samples:

```shell
PROJECT_ID=$(gcloud config get project)
```

First make sure the code compiles:

```shell
cargo check -p <service>-samples --features run-integration-tests
```

Then run the tests:

```shell
GOOGLE_CLOUD_PROJECT=${PROJECT_ID} \
    cargo test -p <service>-samples --features run-integration-tests --tests
```

When the tests pass successfully, you are done with this step. You can move on
to the next one.

### Troubleshooting failures

Some samples need extra environment configuration. Look in
`.gcb/integration.yaml` to see if an environment variable is set there. See if
you can infer or reuse its value. For example,
`GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT` should be set to
`rust-sdk-test@${PROJECT_ID}.iam.gserviceaccount.com`.

## Research prior art

### Look for the same sample written in other languages.

Do a CodeSearch for the given sample region. e.g. search for
`"[START storage_list_buckets]"`. This will show how other client libraries (in
languages other than Rust) write the code. Read up to 5 of these examples to
understand what the sample is doing.

Aside: If you are a human, you could do a Google search and try to find the
cloud.google.com docs associated with this region. For example
https://docs.cloud.google.com/storage/docs/listing-buckets is associated with
`storage_list_buckets`.

Note that other client libraries have different surfaces. We will need to adapt
the logic for the exact API exposed by Rust.

### Look for existing Rust samples for the same service.

Search the local codebase (e.g., `grep -r "key_term" src/<service>`) for key
terms from the region tag to identify the relevant Rust structs, methods, and
fields.

Look at the structure under `src/<service>/samples/src`. Read every file to get
a feel for what samples for this service look like.

Next read everything under `src/<service>/samples/tests` to see how individual
samples are invoked.

### Identify where this sample should go.

Figure out where to create a new `<sample>.rs` file. Typically, we use the
region tag for the filename, but strip any prefixes that are encoded in the
directory structure. For example, the `storage_list_buckets` sample is located
under `src/storage/examples/src/buckets/list_buckets.rs`

### Identify the most similar sample to the one you are writing

Identify which existing sample is most similar to the one you are about to
write. It is useful to identify:

- which client is used?
- which RPC is used?
- which fields in the RPC are important?

Examples:

- If the sample is to set a field in an RPC, find a sample that makes that same
  RPC.
- If the sample is to make an RPC we haven't seen, find a sample that makes a
  different RPC, with the same client.

### Look for Rust samples in the documentation.

Look at examples in the documentation (i.e. in `src/<service>/src/...`) for any
interfaces you will use in the sample. If we find examples in the documentation,
the sample you write should resemble it.

## Write the sample

### Initial set up

First create a file for this new sample. It is easiest to copy the sample that
is most similar to this one and make edits. Don't make edits just yet, though.

Register the new module in the appropriate `mod.rs` or `lib.rs` file within the
samples crate.

The sample should always include a copyright, and use a DevRel snippet region
tag (the things that looks like `// [START <snippet_region>]` and
`// [END <snippet_region>]`)

### Verify the new sample is compiled.

Add a `compile_error!("TODO : making sure the test is built")`, and then build
the samples crate as before.

```shell
cargo check -p <service>-samples --features run-integration-tests --tests
```

We should see this fail. If it does not fail, then we are not building our
sample. Make sure the new file is included somewhere.

If it does fail, you can remove the `compile_error!()` and move on.

### Verify the new sample is run.

Add a `panic!("TODO : making sure the test is run")` inside the sample function,
and then execute the samples as before.

```shell
GOOGLE_CLOUD_PROJECT=${PROJECT_ID} \
    cargo test -p <service>-samples --features run-integration-tests --tests
```

We should see this fail. If it does not fail, then we are not running our
sample. Make sure the new sample is executed by the test driver.

If it does fail, you can remove the `panic!()` and move on.

### Iterate

Next, edit the interior of the sample to fit the given DevRel snippet region.
This is where it is useful to remember what other languages did.

When you are done, test the code:

```shell
GOOGLE_CLOUD_PROJECT=${PROJECT_ID} \
    cargo test -p <service>-samples --features run-integration-tests --tests
```

If this doesn't pass, keep making edits until it works. If you fail too many
times in a row, ask for help.

When this passes, clean up the code.

- Make it concise.
- Run `cargo fmt -p <service-samples>`.
- Run `cargo clippy -p <service-samples> --all-features --all-targets`.
- Look over other things from `GEMINI.md`.

If you make any changes, test the code again.

## Report success!

Stop and report success to the human! What changes did you make to the repo?

Also, suggest any updates to this `SKILL.md` in an `EDITS.diff` that will
improve the process for next time. If the process went well, there is no need to
make any suggestions.
