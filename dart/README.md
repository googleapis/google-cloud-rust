# Dart SDK incubator

## Layout

- `examples/`: various examples of Google Cloud client usage
- `generated/`: the generated Google Cloud API packages
- `packages/`: hand-written API and support packages
- `tests/`: unit and integration tests for the generated cloud APIs

## Developing

### Regenerating the Dart packages

From `dart/`:

```bash
go run github.com/googleapis/librarian/cmd/sidekick@main  refreshall
```
