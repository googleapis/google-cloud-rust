# apiary-generator

A generator that consumes discovery documents to generate rust clients.

## Build/Install

```bash
cargo install --path .
```

## Usage

```bash
discogen -i /some/path/to/resources/test/storage-api.json -o /some/path/to/storage/src/lib.rs
```
