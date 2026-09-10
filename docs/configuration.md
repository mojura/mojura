# Options, storage, and replication

Mojura is configured in Go through [Opts](../opts.go). TOML tags describe fields;
this repository does not load TOML/JSON files, read environment variables, or parse
command-line flags. Its defaults and validation depend on the versions in
[go.mod](../go.mod), currently Kiroku v0.18.1, backend v0.2.1, and the Bolt adapter
v0.2.1.

## Opening a database

`MakeOpts(name, dir)` sets only the name and directory. `New[T](opts, keys...)`
validates a copy of the options, fills defaults, checks the zero value's relationship
count, opens the backend, creates buckets, and initializes history/replication.
`opts.Validate()` can be called explicitly to fill and validate your own options.

Both `Name` and `Dir` are required. Create the directory before `New`; the default
Bolt backend opens the file before Kiroku initialization and does not create its
parent directory. Relative paths are relative to the process working directory;
Mojura does not change it. Use a separate directory for each primary/mirror instance.
The [basic example](../examples/basic/main.go) uses `os.MkdirTemp` and cleans up after
closing; persistent applications can use `os.MkdirAll` on their chosen data path.

`Namespace`, when non-empty, makes `FullName()` return `namespace_name`; otherwise
it returns `Name`. That full name is used for database/history filenames and source
prefixes. Use stable filesystem-safe names. Primary and mirror names/namespaces
must match to select the same source history, but their local directories must differ.

## Mojura options and effective defaults

| Field | Default after validation | Behavior |
| --- | --- | --- |
| `IndexLength` | `8` when zero | Minimum decimal padding width for generated IDs. Not a maximum length. |
| `MaxBatchCalls` | `1024` when zero | Size trigger for Mojura batches. Reaching it currently blocks the triggering caller; see known defects. |
| `MaxBatchDuration` | `10 * time.Millisecond` when zero | Timer from the first queued call until a batch flush. |
| `RetryBatchFail` | `false` for zero-value/`MakeOpts` options | Controls retry of earlier callbacks when a later callback fails. `DefaultRetryBatchFail` is declared true but is not applied by `fill`. Set the field explicitly if using retries. |
| `IsMirror` | `false` | Selects a consumer instead of a producer and rejects public write operations. |
| `IgnoreEmptyRelationshipKeys` | `false`, unused | Does not affect runtime behavior. Empty relationship IDs are always skipped. |
| `Initializer` | `bolt.New()` | Implements `backend.Initializer`. Default adapter uses a one-second file-lock timeout and opens files with mode `0644`. |
| `Encoder` | `&JSONEncoder{}` | Encodes/decodes entry payloads. Nil selects JSON; it does not return `ErrEmptyEncoder`. |
| `Source` | `nil` | Optional `kiroku.Source` for exporting/importing history. A nil source does not configure a backup. |
| `OnImport` | `nil` | Called with the Kiroku file type and an `action.Reader` after successful import into the backend. It cannot return an error. |
| `Logger` | `NewLogger()` | Implements `Info`, `Warn`, and `Error` with printf-style arguments. The default serializes output through the standard logger. |

`Validate` fills zero values and delegates required-field validation to Kiroku.
It does not comprehensively validate negative batch sizes, durations, index widths,
or relationship schemas. Supply positive sizes/durations and valid relationship
slots. Mojura overwrites the embedded Kiroku `OnLog` and `OnError` callbacks to route
through `Logger`.

## Embedded Kiroku options

`Opts` embeds `kiroku.Options`; set promoted fields after constructing `Opts` or
provide `Options: kiroku.Options{...}` in a composite literal. Promoted fields such
as `Name` cannot be keyed directly in an `Opts{...}` literal.

These behaviors were checked against the pinned dependency's executable code,
including cases where its comments are stale:

| Field | Default / current use in Kiroku v0.18.1 |
| --- | --- |
| `Name`, `Dir`, `Namespace` | Required name/directory and optional namespace, as described above. |
| `OnLog`, `OnError` | Replaced by Mojura's logger forwarding. |
| `OnResume` | Optional consumer callback; invoked asynchronously on recovery after an error. |
| `Debugging` | False; enables additional Kiroku diagnostics. |
| `AvoidExportOnClose` | False; when true, the producer skips processing remaining export files during close. |
| `AvoidProcessOnClose` | Declared but not consulted by the pinned dependency. Its TOML tag is `avoid_merge_on_close`; do not infer behavior from the field/tag name. |
| `ConsumerFileLimit` | Non-positive values become `1000`. Consumer queue capacity setting. |
| `ConsumerConcurrencyCount` | Non-positive values become `1`. Consumer scanning concurrency. |
| `ConsumerGetNextListSize` | Non-positive values become three times `ConsumerFileLimit`. |
| `BatchDuration` | Zero becomes `10 * time.Second`. Kiroku's own batch setting; Mojura writes through Kiroku `Transaction`, so this does not tune `Mojura.Batch`. |
| `EndOfResultsDelay` | Zero becomes `10 * time.Second`. Consumer delay after exhausting source results. |
| `ErrorDelay` | Zero becomes `30 * time.Second`. Consumer retry delay after errors. |
| `RangeStart` | Zero by default; initializes the consumer history timestamp lower bound. Does not filter entry creation times. |
| `RangeEnd` | Zero means unbounded. The consumer checks source file timestamps and latest snapshots against this upper bound, despite a stale placeholder comment in the dependency. |

Inspect dependency code locally when upgrading it:

```sh
go list -m -f '{{.Dir}}' github.com/mojura/kiroku
go doc github.com/mojura/kiroku.Options
go doc github.com/mojura/backend
```

## Backend and files

The default backend is `github.com/mojura-backends/bolt`, using
`github.com/gdbu/bolt` v1.4.0. A replacement must implement the interfaces in
`github.com/mojura/backend`: initialization, read/write transactions, nested
buckets, byte-key CRUD, and ordered cursors. A generic key/value API alone is not
enough. Mojura relies on transactional rollback, lexically ordered keys, forward
seek behavior, and storing relationship entry keys with nil values.

For full name `messages`, typical local files are:

| Path relative to `Dir` | Purpose |
| --- | --- |
| `messages.bdb` | Backend database, including encoded entries, relationship indexes, and next-ID metadata. Custom initializers still receive this filename. |
| `messages.kir` | Kiroku mapped metadata. |
| `messages.<timestamp>.<type>.kir` | Kiroku temporary, chunk, or snapshot files during processing. |

Backend buckets are `entries`, `relationships`, `meta`, and `lookups`. Relationship
memberships are stored as nested `relationships / key / relationshipID / entryID`
keys. `meta/value` holds JSON with `currentIndex`. `lookups` is created/reset but
has no active lookup API in this checkout.

Without a source, the producer still runs and writes local history files, but
processed chunks/snapshots can be removed without export. Do not treat these files
as a retained backup. Configure a source for retained/exported history and verify
recovery with disposable data.

## Encoders

`JSONEncoder` delegates to `encoding/json`. A custom [Encoder](../encoder.go) must
marshal values and decode through the destination passed by Mojura, including a
pointer to a generic pointer value. Only entry payloads use this encoder; IDs,
relationship indexes, and database/history metadata do not.

`NewEncryptedJSONEncoder(key)` returns a pointer and accepts a string containing
exactly 16, 24, or 32 raw key bytes. It does not decode hexadecimal/base64 text or
derive a key from a password. `Marshal` JSON-encodes the value and uses AES-GCM with
a random nonce prepended to ciphertext and its authentication tag.

`Unmarshal` tries decryption, then falls back to interpreting the input as plaintext
JSON if decryption fails. Valid plaintext is therefore accepted; this is not a
strict encrypted-input policy. Inputs shorter than the nonce currently can panic
before fallback. See [known defects](development.md#known-defects-and-limitations).

Set `opts.Encoder` before `New` and keep it compatible with all stored/source
payloads. Switching encoders does not rewrite existing entries, rotate keys, or
encrypt index keys and metadata. Snapshot copies existing encoded bytes. When using
the encrypted encoder, the application owns key provisioning and retention.

## History, imports, and mirrors

A primary (`IsMirror: false`) with a non-nil source first runs a one-shot Kiroku
consumer, then starts a producer. With nil source it starts the producer directly.
If Kiroku has no processed-history timestamp but the database already has entries,
Mojura writes a history dump from those entries and updates its next-ID metadata.

A filesystem source can be configured with this setup fragment; handle the returned
error before opening Mojura:

```go
var err error
opts := mojura.MakeOpts("messages", localDir)
opts.Source, err = kiroku.NewIOSource(archiveDir)
```

`kiroku.NewIOSource(archiveDir)` creates `archiveDir/source`. Exported history is
grouped under `source/<FullName>/`; latest-snapshot pointers use
`source/_latestSnapshots/`. The archive directory and each instance's local data
directory serve different purposes. A custom source implements `kiroku.Source`.
Mojura does not call a source-specific `Close` method, so the application owns any
resources that source implementations acquire.

A mirror (`IsMirror: true`) with a source starts a continuing consumer. Without a
source, it still opens successfully and serves existing data without synchronizing.
Read operations are available, but `New`, `Put`, `Update`, `Delete`, `Transaction`,
`Batch`, `Snapshot`, and `Reindex` return `ErrMirrorCannotPerformWriteActions`.
Mirror construction still opens a writable backend and initializes buckets; this
mode is not the same as opening the Bolt file read-only.

`Snapshot(ctx)` records currently stored entry bytes in a Kiroku snapshot; export
is handled by Kiroku. It does not return a backup filename or guarantee that the
source export has completed when it returns. `Reindex(ctx)` rebuilds local
relationships and does not create a snapshot.

`action.Reader.ForEach` decodes history actions for `OnImport`. Action values are
encoded payload bytes, so decoding them must use the matching encoder. The
callback runs after the backend import commits; it cannot roll back that import.
Consume its reader during the callback rather than retaining it. Write/delete
actions are replayed; comment and other unhandled action types have no replay effect.

Current replication defects matter for recovery: snapshot import preserves existing
entry rows, delete replay leaves relationship memberships behind, and write replay
refreshes `UpdatedAt`. Shutdown closes the backend before the producer/consumer.
Read [the source-linked limitations](development.md#known-defects-and-limitations)
before depending on exact snapshot replacement or mirror consistency.

## Lifecycle

Close each database after its callers finish and check the returned error. A second
`Close` returns `github.com/gdbu/errors.ErrIsClosed`. Wrappers do not own their
database. Constructor failures do not consistently release resources acquired
before the failure, and `Close` does not drain pending batches. Coordinate shutdown
with transactions, batches, maintenance, and import work; the current implementation
does not guarantee safe concurrent shutdown for every path.
