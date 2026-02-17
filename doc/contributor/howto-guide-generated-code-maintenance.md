# How-To Guide: Generated Code Maintenance

This guide is intended for contributors to the `google-cloud-rust` SDK. It will
walk you through the steps necessary to generate a new library, update libraries
with new changes in the proto specifications, and refresh the generated code
when the generator changes.

## Prerequisites

The generator and its unit tests use `protoc`, the Protobuf compiler. Ensure you
have `protoc >= v23.0` installed and it is found via your `$PATH`.

```bash
protoc --version
```

If not, follow the steps in [Protocol Buffer Compiler Installation] to download
a suitable version.

Make sure your workstation has up-to-date versions of Rust and Go. Follow the
instructions in [Set Up Development Environment].

## Generate new library

### Verify `librarian` knows about the library

`librarian` has a hard-coded list of APIs. If librarian does not have the API in
its list it does not find the service config yaml file. As a result, the title,
description, and mixins (IAM, location, longrunning) may be missing.

You need to look at the [API list] for the pinned version of librarian, if the
API is not in the list:

1. Send a PR adding the API to librarian.
1. Send a PR to update the `version` field in `librarian.yaml`.

### Generate

Define the library's name. Note this should match the directory path where the
code, e.g. `google/cloud/kms/v1`:

```bash
library=... 
```

Create a new branch in your fork:

```bash
git checkout -b feat-${library}-generate-library
```

This command will generate the library, add the library to Cargo and git, and
run the necessary tests:

```bash
V=$(sed -n 's/^version: *//p' librarian.yaml)
# add library to librarian.yaml
go run github.com/googleapis/librarian/cmd/librarian@${V} add ${library}
# generate library
go run github.com/googleapis/librarian/cmd/librarian@${V} generate ${library}
```

Commit all these changes and send a PR to merge them:

```bash
git add .
git commit -m "feat(${library}): generate library"
```

## Update the code generation sources

Run:

```bash
git checkout -b chore-update-shas-circa-$(date +%Y-%m-%d)
V=$(sed -n 's/^version: *//p' librarian.yaml)
go run github.com/googleapis/librarian/cmd/librarian@${V} update discovery
go run github.com/googleapis/librarian/cmd/librarian@${V} update googleapis
go run github.com/googleapis/librarian/cmd/librarian@${V} generate --all
git commit -m"chore: update discovery and googleapis SHA circa $(date +%Y-%m-%d)" .
```

Then send a PR with whatever changed.

Alternatively you can run `librarian update --all` to update all sources at
once. Note that this includes `showcase` and `protojson-conformance`, though.

### Troubleshooting

From time to time existing libraries gain new dependencies that are unknown to
`librarian`. When this happens you may see an error like:

```
librarian: missing package "google.cloud.gkehub.rbacrolebindingactuation.v1" while generating "google.cloud.gkehub.v1", available packages:
[google.api google.cloud.common google.cloud.gkehub.configmanagement.v1 google.cloud.gkehub.multiclusteringress.v1 google.cloud.location google.iam.v1 google.logging.type google.longrunning google.protobuf google.rpc google.rpc.context google.type grafeas.v1]
```

In this case you need to:

1. Follow [Generate new library] if the new dependency is not part of the repo.
1. Follow [Add new dependency] to add the new dependency.

## Bump all version numbers

Run:

```bash
git fetch upstream
git checkout -b chore-bump-version-numbers-circa-$(date +%Y-%m-%d)
V=$(sed -n 's/^version: *//p' librarian.yaml)
go run github.com/googleapis/librarian/cmd/librarian@${V} bump --all
go run github.com/googleapis/librarian/cmd/librarian@${V} generate --all
# It is safe to commit everything because `bump` stops you from updating a
# dirty workspace.
git commit -m"chore: bump version numbers circa $(date +%Y-%m-%d)" .
```

When running on Cloudtop, you might need to set
`CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt` in order for crates.io to
accept your certs.

### Update top-level Cargo.toml

Sometimes the top-level `Cargo.toml` file may need manual updates. Changes to
GAPICs never trigger the need for such updates, or at least have never triggered
such need. But changes to core crates, including `google-cloud-auth`,
`google-cloud-gax`, `google-cloud-lro`, and `google-cloud-gax-internal` may
trigger the need for manual updates.

You can rely on the CI build to detect when such an update is needed, or you
could run some local tests first:

```bash
cargo install --locked cargo-minimal-versions
# Make sure a veneer works.
cargo minimal-versions check --package google-cloud-storage
# Make sure a GAPIC (with the locations and iam mixins) works:
cargo minimal-versions check --package google-cloud-secretmanager-v1
# Make sure a GAPIC (with the longrunning mixin) works:
cargo minimal-versions check --package google-cloud-workflows-v1
```

Alternatively, consider
[this feature request](https://github.com/googleapis/librarian/issues/3720).

## Add new dependency

The librarian configuration handles most well-known dependencies between
libraries. For example, if a library gains LROs, librarian automatically adds
the necessary directives to the library's `Cargo.toml` file.

However, some libraries have ad-hoc dependencies that require some amount of
configuration. This sub-section explains how to add these dependencies to the
librarian configuration and the Cargo.toml file.

In this guide we will assume the new dependency already is part of the
repository, if not, first follow the [Generate new library] subsection.

First, add the new dependency to the `rust.package_dependencies` section of the
library that needs them:

```patch
diff --git a/librarian.yaml b/librarian.yaml
index e088d86a5..7531ff313 100644
--- a/librarian.yaml
+++ b/librarian.yaml
@@ -779,6 +779,9 @@ libraries:
     skip_generate: true
     rust:
       package_dependencies:
+        - name: google-cloud-gkehub-rbacrolebindingactuation-v1
+          package: google-cloud-gkehub-rbacrolebindingactuation-v1
+          source: google.cloud.gkehub.rbacrolebindingactuation.v1
         - name: google-cloud-gkehub-configmanagement-v1
           package: google-cloud-gkehub-configmanagement-v1
           source: google.cloud.gkehub.configmanagement.v1
```

Then edit the `Cargo.toml` file to define this as an internal dependency:

```patch
diff --git a/Cargo.toml b/Cargo.toml
index 37b2b80e5..9a38e417e 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -471,6 +471,7 @@ google-cloud-apps-script-type-slides            = { default-features = false, ve
 google-cloud-common                             = { default-features = false, version = "1", path = "src/generated/cloud/common" }
 google-cloud-gkehub-configmanagement-v1         = { default-features = false, version = "1", path = "src/generated/cloud/gkehub/configmanagement/v1" }
 google-cloud-gkehub-multiclusteringress-v1      = { default-features = false, version = "1", path = "src/generated/cloud/gkehub/multiclusteringress/v1" }
+google-cloud-gkehub-rbacrolebindingactuation-v1 = { default-features = false, version = "1", path = "src/generated/cloud/gkehub/rbacrolebindingactuation/v1" }
 google-cloud-grafeas-v1                         = { default-features = false, version = "1", path = "src/generated/grafeas/v1" }
 google-cloud-iam-v2                             = { default-features = false, version = "1", path = "src/generated/iam/v2" }
 google-cloud-identity-accesscontextmanager-type = { default-features = false, version = "1", path = "src/generated/identity/accesscontextmanager/type" }
```

Then generate the library that gained a new dependency:

```bash
V=$(cat .librarian-version.txt)
go run github.com/googleapis/librarian/cmd/librarian@${V} generate google-cloud-gkehub-v1
```

Commit all these changes and send a PR.

## Refreshing the code

### All libraries

Run:

```bash
V=$(sed -n 's/^version: *//p' librarian.yaml)
go run github.com/googleapis/librarian/cmd/librarian@${V} generate --all 
```

Then run the unit tests and send a PR with whatever changed.

### Single library

When iterating, it can be useful to regenerate the code of a single library. Get
the library name from librarian.yaml.

Run:

```bash
V=$(sed -n 's/^version: *//p' librarian.yaml)
go run github.com/googleapis/librarian/cmd/librarian@${V} generate google-cloud-secretmanager-v1
```

## The Glorious Future

Someday `librarian` will be stable enough that we will be able to install it. At
that point we will be able to say:

```bash
V=$(sed -n 's/^version: *//p' librarian.yaml)
go install github.com/googleapis/librarian/cmd/librarian@${V}
```

And we will be able to issue shorter commands, such as:

```bash
librarian generate --all
```

## Formatting librarian.yaml

If you make manual changes to `librarian.yaml`, you should run `librarian tidy`
to automatically format and sort the file. This ensures consistency and
readability.

```bash
V=$(sed -n 's/^version: *//p' librarian.yaml)
go run github.com/googleapis/librarian/cmd/librarian@${V} tidy
```

## Special cases

### Making changes to `librarian`

Clone the `librarian` directory:

```bash
git -C .. clone git@github.com:googleapis/librarian
git -C ../librarian checkout -b fancy-rust-feature
```

Naturally you can choose to clone `librarian` into a different directory. Just
change the commands that follow.

You can make changes in the `librarian` directory as usual. To test them change
the normal commands to use the directory where your librarian changes live. For
example:

```bash
go -C ../librarian/cmd/librarian build && ../librarian/cmd/librarian/librarian -f generate --all
```

Once the changes work then send a PR in the librarian repo to make your changes.
Wait for the PR to be approved and merged.

Then finish your PR in `google-cloud-rust`.

1. Update the librarian version in `librarian.yaml`:

   ```bash
   V=$(GOPROXY=direct go list -m -f '{{.Version}}' github.com/googleapis/librarian@main)
   sed -i.bak "s;^version: .*;version: ${V};" librarian.yaml && rm librarian.yaml.bak
   ```

1. Update the generated code:

   ```bash
   V=$(sed -n 's/^version: *//p' librarian.yaml)
   go run github.com/googleapis/librarian/cmd/librarian@${V} generate --all
   ```

Use a single PR to update the librarian version and any generated code.

### Generating a library with customized directories

We may need to customize the target or source directory for some generated
libraries. For example, you may need to leave room for other crates in the same
directory.

1. Update the librarian.yaml with the correct configuration.

```
output: custom directory to generate code in
```

```
channels > path: custom path to read protos from in googleapis
```

example:

```
  - name: google-cloud-api
    version: 1.2.0
    channels:
      - path: google/api
    copyright_year: "2025"
    output: src/generated/api/types
```

2. run generate

```
bash
V=$(sed -n 's/^version: *//p' librarian.yaml)
go run github.com/googleapis/librarian/cmd/librarian@${V} generate google-cloud-apps-script-type
```

3. Add the files to `git`, compile them, and run the tests:

```bash
typos && cargo fmt && cargo build && cargo test && cargo doc
git add src/generated/cloud/api/types Cargo.toml Cargo.lock
```

4. Commit all these changes and send a PR to merge them:

```bash
git commit -m "feat(api/types): generate library"
```

### Testing library generation for an existing library

Sometimes it may be useful to re-generate an existing library, to test the
generation step, practice before generating a new library, or to test the
documentation.

We will use `websecurityscanner` as an example. Start by removing the existing
library:

```shell
sed -i.bak  '/websecurityscanner/d' Cargo.toml
rm Cargo.toml.bak
git rm -fr src/generated/cloud/websecurityscanner/
git commit -m"Remove for testing" Cargo.toml Cargo.lock src/generated/cloud/websecurityscanner/
```

Now add the library back (get the library name from librarian yaml):

```shell
go run github.com/googleapis/librarian/cmd/librarian@main generate google-cloud-websecurityscanner-v1
```

[add new dependency]: #add-new-dependency
[api list]: https://github.com/googleapis/librarian/blob/main/internal/serviceconfig/api.go
[generate new library]: #generate-new-library
[protocol buffer compiler installation]: https://protobuf.dev/installation/
[set up development environment]: /doc/contributor/howto-guide-set-up-development-environment.md
