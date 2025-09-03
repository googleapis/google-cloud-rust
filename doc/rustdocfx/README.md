# Rust DocFX YAML Generator

This tool generates DocFX YAML for Rust crates.

This tool requires cargo nightly build to generate rustdoc json file and
[docuploader](https://github.com/googleapis/docuploader) to upload the generated
docfx yaml tar file.

Example usage for all crates:

```bash
rustdocfx -project-root ./../../
```

Example usage for a single crate:

```bash
rustdocfx -project-root ./../../ google-cloud-secretmanager-v1
```

## Testing

TODO: Add golden files
