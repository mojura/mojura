# Developing Mojura

Read [AGENTS.md](../AGENTS.md) for workflow and [STYLEGUIDE.md](../STYLEGUIDE.md) for
coding conventions. [Usage](usage.md) and [configuration](configuration.md) describe
public behavior. This guide maps that behavior to the current source and records
limitations that must not be mistaken for working features.

## Source map

| Area | Files | Responsibilities |
| --- | --- | --- |
| Database lifecycle and public facade | [mojura.go](../mojura.go) | Construction, bucket initialization, CRUD wrappers, transactions, history import, snapshots, reindexing, close. |
| Transaction implementation | [transaction.go](../transaction.go) | Entry encoding/storage, metadata, relationship deltas, filtering, replay. |
| Value contract | [value.go](../value.go), [entry.go](../entry.go), [relationships.go](../relationships.go), [reflect.go](../reflect.go) | Generic values, metadata accessors, positional index memberships, type allocation. |
| Options | [opts.go](../opts.go), [filteringOpts.go](../filteringOpts.go) | Runtime options/defaults and query options. |
| Filters | [filters](../filters), [filter.go](../filter.go), [filterCursor.go](../filterCursor.go) | Public filter constructors and supported-type dispatch. |
| Entry cursors | [baseCursor.go](../baseCursor.go), [baseIDCursor.go](../baseIDCursor.go) | Unfiltered value/ID traversal. |
| Index cursors | [matchCursor.go](../matchCursor.go), [inverseMatchCursor.go](../inverseMatchCursor.go), [comparisonCursor.go](../comparisonCursor.go), [baseComparisonCursor.go](../baseComparisonCursor.go) | Exact, inverse, relationship comparison, and entry-ID comparison traversal. |
| Filter composition | [multiCursor.go](../multiCursor.go), [multiIDCursor.go](../multiIDCursor.go), [nopCursor.go](../nopCursor.go) | Primary traversal, secondary membership checks, empty results. |
| Cursor contracts/helpers | [cursor.go](../cursor.go), [idCursor.go](../idCursor.go), [iterators.go](../iterators.go), [forEachFn.go](../forEachFn.go), [utils.go](../utils.go) | Iteration interfaces, direction, resume tokens, IDs/timestamps. |
| Batching and context | [batcher.go](../batcher.go), [call.go](../call.go), [calls.go](../calls.go), [contextContainer.go](../contextContainer.go) | Call queues, retries, completion, cancellation checks. |
| Serialization and history | [encoder.go](../encoder.go), [jsonEncoder.go](../jsonEncoder.go), [encryptedJSONEncoder.go](../encryptedJSONEncoder.go), [action](../action), [blockwriter.go](../blockwriter.go), [meta.go](../meta.go) | Entry codecs, binary history actions, next-ID state. |
| Wrappers and logging | [readWrapper.go](../readWrapper.go), [writeWrapper.go](../writeWrapper.go), [wrappers.go](../wrappers.go), [logger.go](../logger.go), [logLine.go](../logLine.go) | Restricted method sets and formatted logs. |
| Examples and tests | [examples/basic](../examples/basic), [mojura_test.go](../mojura_test.go), cursor/encoder `*_test.go` files | Executable setup example, CRUD/index/history integration, cursor cases, encryption cases. |

The root package is a library. `action` and `filters` are library subpackages;
`examples/basic` is the executable. There is no application server, plugin system,
CLI, Makefile, configuration parser, or checked-in CI workflow.

## Verification

Use Go 1.25.0 or newer, as declared in [go.mod](../go.mod). From the root:

```sh
go test ./...
go vet ./...
go build ./...
go run ./examples/basic
```

Use `gofmt -w` on changed Go files. For concurrency, transaction, batching, cursor,
replication, or lifecycle changes, also run:

```sh
go test -race ./...
```

Do not run two test processes at once in the same checkout. Existing tests share
`./test_data`, create a local Kiroku `IOSource`, and remove the directory in cleanup.
Some cases lack complete cleanup, so a crash or an isolated test can leave files
behind. Inspect leftovers and remove only disposable test artifacts you own before
retrying. Do not put application data in that directory. The external-package
example in [example_test.go](../example_test.go) has an output assertion and runs
under `go test`. It replaces the old examples that relied on package test globals.
Use `examples/basic` for the complete CRUD integration example.

Use `t.TempDir`, `t.Cleanup`, and explicit close/error handling for new integration
tests. Close databases before removing directories. Avoid `t.Parallel` for existing
shared-directory tests. Scope regression tests to observable behavior and relevant
failure cases; a documentation/comment edit does not require mechanical tests.
Do not change public error identity or runtime behavior as part of style cleanup.

For a restricted build-cache environment:

```sh
GOCACHE=/tmp/mojura-go-build go test ./...
GOCACHE=/tmp/mojura-go-build go vet ./...
GOCACHE=/tmp/mojura-go-build go build ./...
GOCACHE=/tmp/mojura-go-build go run ./examples/basic
```

The module cache must contain dependencies or have permission/network access to
download them. If its default location is also unwritable, set
`GOMODCACHE=/tmp/mojura-go-mod` consistently on those commands; an empty cache needs
dependency downloads. Do not change `go.mod` to work around sandbox restrictions.

### Documentation audit validation

The September 2026 audit used Go 1.26.5 on macOS arm64. Standard tests, vet, and build
passed. The runnable example matched the README output, all 118 local Markdown
links resolved, and the README/usage/configuration Go snippets compiled in their
stated surrounding contexts. A Go token comparison confirmed that existing
production files changed only in comments and formatting. Focused,
temporary reproductions confirmed the query, encryption, and size-triggered batch
defects listed below; these were documentation checks, not runtime fixes or added
regression coverage.

`go test -race ./...` aborted in the pinned `github.com/gdbu/bolt` v1.4.0
`freelist.go:204`, during initial bucket creation, with:

```text
fatal error: checkptr: converted pointer straddles multiple allocations
```

This is a failed race check, not evidence of race safety. Disabling `checkptr` would
change the check and must not be reported as the ordinary race command passing.
Other platforms/toolchains and live mirror recovery were not validated in this audit.

## Known defects and limitations

These describe the current implementation. Keep fixes in separate runtime changes
with focused regression coverage, and update this list and the public guides when
they are resolved. “Reproduced” means exercised during the documentation audit;
“source inspection” means the control flow was reviewed but no dedicated end-to-end
failure test was run.

### Batching and cancellation

- **Reproduced: size-triggered batches block their caller.** In
  [batcher.go](../batcher.go), `Append` flushes at `MaxBatchCalls` and returns the
  unassigned named channel, which is nil. `Mojura.Batch` in
  [mojura.go](../mojura.go) waits on that channel. With `MaxBatchCalls = 1`, a write
  commits but the call never returns. The existing batch test only covers timer
  flushes.
- **Source inspection: batch commit errors can be lost.** `batcher.run` treats
  `failIndex == -1` as success even when the outer transaction fails after callbacks
  complete. Errors before callbacks start also use the zero-value failure index.
  The panic recovery helper in [utils.go](../utils.go), `recoverCall`, formats `err`
  rather than the recovered panic value, losing the panic detail.
- **Reproduced: declared retry default is not applied.** [opts.go](../opts.go)
  declares `DefaultRetryBatchFail = true` but `Opts.fill` never copies it, so
  `MakeOpts` followed by validation leaves retries disabled.
- **Source inspection: cancellation does not interrupt arbitrary callbacks.**
  [contextContainer.go](../contextContainer.go) leaves `done` and `cancel` channels
  nil, so the cancellation branch in `runTransaction` cannot wake. Individual
  operations check the context, but a callback blocked elsewhere can hold the
  transaction indefinitely. Batch context updates spawn waiters that are not
  reliably canceled, and transaction teardown does not close the context container.

### Queries and relationship indexes

- **Reproduced: unfiltered continuation tokens do not round-trip.**
  `appendFiltered`/`appendFilteredIDs` in [transaction.go](../transaction.go) build
  `::entryID` tokens through `joinSeekID` in [utils.go](../utils.go). Unfiltered
  cursors in [baseCursor.go](../baseCursor.go) and
  [baseIDCursor.go](../baseIDCursor.go) seek raw keys. With generated IDs and a limit
  of one, passing `::00000000` to the next query gives an empty second page. Use
  `ForEach` or the last returned raw ID for unfiltered continuation.
- **Source inspection: nil first/last options panic.** `getFirst`/`getLast` in
  [transaction.go](../transaction.go) dereference options without defaults. Pass
  `NewFilteringOpts()` to public `GetFirst`/`GetLast`. The same file shows that
  zero-limit append calls return nil rather than the input slice.
- **Reproduced: primary inverse matching is not set complement.**
  [inverseMatchCursor.go](../inverseMatchCursor.go) traverses non-excluded buckets
  without checking whether each entry also belongs to the excluded bucket. An
  entry with tags `a,b` appears in `InverseMatch("tags", "a")`; an entry with no
  tags does not. Secondary inverse filters instead test absence in the excluded
  bucket. Primary broad scans can also repeat entries with multiple memberships.
- **Reproduced: range bounds are not consistently enforced.**
  [baseComparisonCursor.go](../baseComparisonCursor.go) starts `First`/`Last` at
  database endpoints instead of range endpoints. With IDs `00000000` through
  `00000003`, `Range("", "00000001", "00000002")` includes `00000000`.
  [comparisonCursor.go](../comparisonCursor.go) checks bounds while advancing but
  not consistently at the initial position: with indexed tags `a,b,c`,
  `Range("tags", "bb", "bc")` returns the `c` entry. Its reverse upper-bound seek
  can position above a missing bound or end prematurely. Use an unbounded
  `Comparison` with an explicit predicate for reliable membership bounds.
- **Source inspection: seek and pagination assume stable positions.**
  [utils.go](../utils.go) seeks then unconditionally advances; if the resume entry
  has been deleted, this can skip the next entry. Several `SeekReverse` methods
  use forward backend seek, not a floor seek. Composite tokens split on the first
  unescaped `::`, so relationship IDs containing it are ambiguous.
- **Source inspection: schema validation happens only at construction.**
  [mojura.go](../mojura.go) validates the zero value's relationship count;
  `updateRelationships` and `setRelationships` in [transaction.go](../transaction.go)
  assume positional consistency later. Variable slot counts can panic or leave
  stale indexes. `Update` also assumes old/new relationship slices do not alias
  mutable data. Reopening with new keys does not backfill entries. Only full
  `Reindex` is implemented; the bounded-backfill feature was reverted.
- **Source inspection: explicit IDs do not advance the generated counter.**
  `Transaction.new` increments metadata; ordinary `put` does not. `New` calls the
  upsert path, so an explicit numeric ID can be overwritten when allocation reaches
  it. Padding is a minimum width and does not preserve numeric sort order forever.

### Encryption, replay, and lifecycle

- **Reproduced: short encrypted input can panic.**
  [encryptedJSONEncoder.go](../encryptedJSONEncoder.go), `decrypt`, slices input
  at the nonce length without checking it. With a valid key, unmarshaling `[]byte("{}")`
  panics before plaintext fallback. Inputs that reach fallback can be accepted as
  plaintext JSON; the encoder does not require authenticated ciphertext on reads.
- **Source inspection: snapshot import does not remove absent entries.**
  `importReader` calls `purge` in [mojura.go](../mojura.go), but `purge` deletes
  `lookups`, `meta`, and `relationships`, leaving `entries` intact. Existing entries
  absent from the snapshot remain, potentially without relationship indexes and
  with inconsistent allocation metadata.
- **Source inspection: delete replay leaves stale relationships.** `processBlock`
  in [transaction.go](../transaction.go) handles deletes with `deleteEntry` instead
  of the normal membership cleanup. Index traversal can then refer to a missing
  value. Write replay goes through `put` and `setEssetialValues` in
  [utils.go](../utils.go), refreshing `UpdatedAt` instead of preserving it exactly.
- **Source inspection: constructor/shutdown cleanup is incomplete.**
  [mojura.go](../mojura.go) does not consistently close resources acquired before
  constructor failure. `Close` marks the instance closed and closes the backend
  before its Kiroku producer/consumer; pending imports can encounter a closed
  backend. It does not drain batches. `Snapshot`/`Reindex` and import paths do not
  use the same lifecycle lock as public read/write transactions. In pinned Kiroku
  v0.18.1, `Producer.Close` also does not close its mapped metadata resource.
- **Source inspection: history and backend are not one atomic store.**
  `Mojura.transaction` finishes the Kiroku transaction inside the backend callback;
  a later backend commit failure cannot roll back an already finalized history file.
  Avoid promising crash-atomic consistency across these stores.

### Legacy declarations and missing coverage

`ErrInvalidType`, `ErrInvalidEntries`, `ErrEndOfEntries`, `ErrEmptyEncoder`, and
`ErrInvalidBlockWriter` remain declared without active return paths. Cursor
exhaustion uses `Break`. `ErrInvalidEntries` references a removed
`GetByRelationship` API. `GetRelationshipIDs` remains a deprecated interface
requirement, and `IgnoreEmptyRelationshipKeys` has no runtime readers. The `lookups`
bucket is initialized but unused by query code.

Existing tests cover CRUD, several cursor directions, relationship combinations,
basic history rebuilding, and encoder round trips. They do not establish correct
behavior for all the failures above. The `action` and `filters` packages have no
direct test files, although root tests exercise filter constructors. Replication
recovery, failure cleanup, concurrent batching/close, and boundary cases need focused
coverage when those implementations change.

## Documentation maintenance

Keep source comments, README snippets, `examples/basic`, and the corresponding
guide aligned whenever an API changes. Keep `AGENTS.md` concise enough to serve as
the starting instructions and let `CLAUDE.md` delegate to it. Preserve the shared
style preferences while applying them to Mojura's value/backend/history model.

Check Markdown relative paths and heading anchors, and run any documented example
you change. Preserve the contributor section and [LICENCE](../LICENCE). Use the
root [PULL_REQUEST_TEMPLATE.md](../PULL_REQUEST_TEMPLATE.md) and report actual test
results and limitations. Do not claim documentation review proves all runtime
behavior correct or all deployment environments tested.
