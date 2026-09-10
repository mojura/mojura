# Working in Mojura

This file applies to the entire repository. Mojura is a generic Go database library
with relationship indexes, a pluggable transactional backend, and Kiroku history
and replication. The module is `github.com/mojura/mojura`. The root package is not an
executable; run `examples/basic` for a complete local example. There is no server,
CLI, plugin registry, or configuration-file loader in this checkout.

## Start here

1. Read [README.md](README.md) for setup and the example.
   Read [STYLEGUIDE.md](STYLEGUIDE.md) before writing Go code. It defines style for
   new and substantially changed code, not a mandate to reformat existing files.
   Its [unit philosophy](STYLEGUIDE.md#unit-philosophy-small-and-simple) is central:
   keep functions, types, and packages small, simple, and focused on one responsibility.
2. Read [docs/development.md](docs/development.md) for the source map, verification,
   and known defects. Read [docs/usage.md](docs/usage.md) before changing values,
   indexes, filters, or transactions, and
   [docs/configuration.md](docs/configuration.md) before changing options, storage,
   encoders, history, or mirrors.
3. Inspect `git status --short` and the relevant diff before editing. Preserve
   existing user changes, including untracked files.
4. Treat executable code and the dependency versions in [go.mod](go.mod) as the
   source of current behavior. Check pinned dependency code when necessary;
   comments, old examples, TODOs, and reverted commits may describe other behavior.

## Commands

Use Go 1.25.0 or newer, as declared in `go.mod`. From the repository root:

```sh
go test ./...
go vet ./...
go build ./...
go run ./examples/basic
```

Run `gofmt -w` on changed Go files. Run `go test -race ./...` when changing shared
state, batching, transactions, cursors, replication, or lifecycle code. The pinned
Bolt dependency can abort the race run with a `checkptr` failure; report this
limitation rather than claiming a passing race check. See the development guide
for the observed failure and restricted-cache commands.

Tests use the standard `testing` package. Many existing tests share `./test_data`
and remove it during cleanup. Do not run separate test processes concurrently in
the same checkout or add `t.Parallel` to those tests. Never put real data there.
New tests should use `t.TempDir` and close their own databases before cleanup.
There is no Makefile or checked-in CI workflow. `example_test.go` is an executable
Go doc example; `examples/basic` is the complete CRUD setup smoke check.

The basic example creates a temporary directory, exercises CRUD and filtering,
closes the database, and removes its own files. It needs no external service.

## Constraints that affect implementation

- Prefer a concrete pointer type such as `*User` that embeds `Entry` by value and
  satisfies `Value`. `New` constructs a zero value and calls `GetRelationships`;
  that method must work before any application fields are populated.
- Relationship slots are positional. `GetRelationships` must always return the
  same number of slots in the order passed to `New(opts, keys...)`, including empty
  slots. Constructor validation only checks the zero value's slot count. Return
  fresh relationship slices so `Update` can compare the old and new IDs reliably.
- Relationship keys are index names; relationship IDs are string values within
  those indexes. They do not provide joins, foreign-key validation, or cascades.
  Empty relationship IDs are skipped unconditionally; `IgnoreEmptyRelationshipKeys`
  is currently unused. Schema changes require deliberate reindexing/migration.
- `MakeOpts(name, dir)` only sets those two fields. `New` calls `Opts.Validate`
  to fill defaults. Create the directory first; the default Bolt initializer does
  not do so. `RetryBatchFail` remains false unless explicitly set, despite the
  `DefaultRetryBatchFail` constant being true.
- `New(value)` allocates an ID; `Put(id, value)` is an upsert and does not advance
  the generated-ID counter. `Update` requires an existing entry. Writes mutate
  supplied values' ID/timestamps, and rollback does not undo those Go mutations.
- Multiple filters intersect; the first drives traversal order. Comparisons use
  string ordering. Broad relationship scans can return an entry more than once.
  Only the pointer filter types dispatched in `filterCursor.go` are supported.
- Use `NewFilteringOpts`, not an empty struct, for an unlimited query. A zero
  `Limit` returns no collected results; `ForEach` ignores it. `GetFirst`/`GetLast`
  require non-nil options. Read the pagination and range limitations before
  depending on `LastID`, inverse filters, or range boundaries.
- Transactions and cursors belong to their callback and must not escape it or be
  shared between goroutines. Use the provided transaction inside the callback;
  nested database calls can deadlock. Context cancellation is checked during
  operations and does not interrupt an arbitrary blocked callback.
- `Batch` has a confirmed blocking bug when `MaxBatchCalls` is reached and other
  failure-handling gaps. Prefer `Transaction` for new usage until fixed. Retryable
  callbacks must tolerate repeated execution and avoid external side effects.
- Mirrors reject public write operations, including `Batch`, `Snapshot`, and
  `Reindex`. A mirror with no source opens without synchronizing. Review snapshot
  import, delete replay, and shutdown defects before changing replication.
- Close each successfully opened database and handle the error. Constructor
  failures do not consistently clean up acquired resources. `Close` does not drain
  pending batches, and callers must coordinate shutdown with background work.

## Presenting pull request titles and descriptions

When providing a PR title and description in a response, present the title as plain
text and the description inside a fenced code block labeled `markdown`. Format the
description using [PULL_REQUEST_TEMPLATE.md](PULL_REQUEST_TEMPLATE.md). The code
fence is for presentation; submit the Markdown body without it when creating or
updating a PR.

## Scope and documentation upkeep

Keep runtime fixes distinct from documentation corrections. During a docs task,
record discovered bugs with source references in the development guide; do not
silently change public behavior. Keep examples self-contained within this module.

Keep the README, agent instructions, examples, Go doc comments, and relevant guides
aligned. Check relative links and run the documented example after changing setup
instructions. Report checks actually run, failures, and unverified behavior.

`CLAUDE.md` delegates here. Keep repository workflow and runtime constraints in this
file and detailed coding conventions in `STYLEGUIDE.md` to avoid conflicting copies.
