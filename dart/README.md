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
go run github.com/googleapis/librarian/cmd/sidekick@main refreshall
```

### Regenerating from a locally modified Sidekick

Clone https://github.com/googleapis/librarian as a sibling directory to this
repo, make any desired changes to Sidekick, then - from `dart/` - run:

```bash
go -C ../../librarian run ./cmd/sidekick refreshall -project-root $PWD
```

### Updating Sidekick

- make any desired changes to the Sidekick fork
- create a PR for the Sidekick changes
- rev. the Sidekick deps in `.github/workflows`; re-run Sidekick from
  that version; create a PR from the repo changes
