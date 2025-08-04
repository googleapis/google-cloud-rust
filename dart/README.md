# Dart SDK incubator

## Layout

- `examples/`: various examples of Google Cloud client usage
- `generated/`: the generated Google Cloud API packages
- `packages/`: hand-written API and support packages
- `tests/`: unit and integration tests for the generated cloud APIs

## Developing

### Testing

From `generator/`: `go test ./...`

### Regenerating the Dart packages

From `generator/`: `go run ./cmd/sidekick refreshall -project-root dart`
