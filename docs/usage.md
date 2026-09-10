# Using Mojura

Start with the [runnable example](../examples/basic/main.go) and its
[record type](../examples/basic/record.go). This guide describes the current code;
the [development guide](development.md#known-defects-and-limitations) records
defects that affect the APIs below.

## Values and relationship indexes

`Mojura[T]` stores one Go value type per database. Prefer a concrete pointer type
such as `*User`, embedding [Entry](../entry.go) by value. `Entry` supplies ID and
timestamp accessors and the deprecated `GetRelationshipIDs() string` method still
required by [Value](../value.go). Override `GetRelationships` for indexed fields;
the deprecated method is not used for indexing.

This is a type-definition fragment using an import of `github.com/mojura/mojura`:

```go
// User stores team and role memberships.
type User struct {
	mojura.Entry
	TeamID string   `json:"teamID"`
	Roles  []string `json:"roles"`
}

// GetRelationships returns team and role indexes, including empty slots.
func (u *User) GetRelationships() (relationships mojura.Relationships) {
	relationships.Append(u.TeamID)
	relationships.Append(append([]string(nil), u.Roles...)...)
	return relationships
}
```

Open that type with `mojura.New[*User](opts, "teams", "roles")`. The first slice in
`Relationships` maps to `teams`, the second to `roles`. `Append` adds one slot with
zero or more relationship IDs; it does not append IDs to the previous slot.
Always preserve the slot count and order, even for a zero value. `New` calls
`GetRelationships` on a freshly allocated value and rejects a different slot count
with `ErrInvalidNumberOfRelationships`. Later writes assume this invariant instead
of validating it; mismatches can panic or leave stale indexes.

Use non-empty, distinct index names and stable relationship IDs. Empty relationship
IDs are always skipped. There is no validation that a relationship ID names an
entry in another database, no automatic join, and no cascading delete. These are
application-managed secondary indexes, including many-to-many indexes such as roles.

Return fresh relationship slices, including copies of slice-valued fields. `Update`
captures the old relationships before running the callback. Aliasing the original
slices can hide in-place membership changes from the index delta calculation.

Changing index names, order, or `GetRelationships` is a schema change. Opening an
existing database creates missing index buckets but does not backfill them.
`Reindex(ctx)` deletes and rebuilds the relationship indexes from all stored values
in one backend write transaction. It can block other writes for the full scan and
does not rewrite entries, rebuild history, or offer bounded/resumable backfill.
Coordinate deployment and reindexing with the schema change.

## Creating, reading, and changing entries

| Method | Current behavior |
| --- | --- |
| `New(value)` | Allocates the next decimal ID, writes the entry and its indexes, and returns the supplied value. Fresh databases begin at `00000000` with the default width. |
| `Get(id)` | Decodes a value; returns `ErrEntryNotFound` for a missing ID. |
| `Exists(id)` | Returns false with no error when the ID is absent. |
| `Update(id, fn)` | Loads an existing value, calls `fn`, updates relationship deltas, and writes it. A missing ID returns `ErrEntryNotFound`. |
| `Put(id, value)` | Inserts or replaces the value at a non-empty ID. It is an upsert; use `Update` when existence is required. |
| `Delete(id)` | Removes an existing entry and its relationship memberships and returns the removed value. Missing entries return an error. |

The same operations are available on `Transaction[T]`. `Mojura` convenience methods
open their own transactions using `context.Background()`.

`New`, `Put`, and `Update` set the stored ID and `UpdatedAt` to the current Unix time
in seconds, and fill `CreatedAt` only when the supplied value has zero creation time.
`Put` does not copy `CreatedAt` from the old entry: pass a loaded value or preserve
the creation timestamp yourself. These operations mutate their Go values; rollback
does not revert those mutations. A value returned alongside an error is not proof
that the write committed.

`Put` does not advance the generated-ID counter. Mixing explicit numeric IDs with
`New` can overwrite an entry when the counter reaches that ID. `IndexLength` is a
minimum padding width, not a maximum; lexical ordering stops matching numeric
ordering once IDs outgrow the width. Choose the ID strategy before storing data.

## Filters and ordering

Import `github.com/mojura/mojura/filters`. Only `*MatchFilter`,
`*InverseMatchFilter`, and `*ComparisonFilter` are supported by the current
[dispatcher](../filterCursor.go); the empty `Filter` interface is not a custom
filter extension mechanism.

| Constructor | Meaning and limits |
| --- | --- |
| `filters.Match(key, id)` | Exact indexed membership. An unknown index key returns `ErrRelationshipNotFound`; an absent ID in an existing index gives no matches. |
| `filters.InverseMatch(key, id)` | As the first filter, traverses other ID buckets in that index. As a secondary filter, rejects entries in the excluded bucket. These differ for missing/multiple memberships; see below. |
| `filters.Comparison(key, fn)` | Calls `fn` on relationship IDs; an empty key compares entry IDs instead. Supply a non-nil callback. |
| `filters.ComparisonWithRange(key, start, end, fn)` | Adds traversal bounds to a comparison. Current boundary defects require care. |
| `filters.LessThan`, `LessThanOrEqualTo`, `GreaterThan`, `GreaterThanOrEqualTo` | String comparisons, with traversal bounds. |
| `filters.Range(key, start, end)` | Configures inclusive bounds with an always-true predicate; current cursor defects can return out-of-range entries. |

All comparisons are lexicographic string/byte comparisons, not numeric or date
parsing. For example, `"10" < "2"`. Use consistently sortable encodings for indexed
numbers and dates. Empty range endpoints mean unbounded. The MQL comment in
`filters/comparison.go` is a TODO, not an implemented query language.

Multiple filters are intended to intersect (AND). The first filter supplies the
traversal order; later filters check membership. With no filters, entries follow
backend key order. Exact-match traversal orders by entry ID. Comparison/inverse
traversal orders by relationship ID and then entry ID. An entry with multiple
matching relationship IDs can appear more than once; results are not deduplicated.

An inverse filter used first does not produce the complement of all database
entries: it omits entries without any indexed membership and can include an entry
that has both the excluded ID and another ID. As a secondary filter it checks the
excluded bucket directly, so changing filter order can change results. If you need
strict complement semantics, scan the intended universe with `ForEach` and apply
the predicate explicitly until the cursor behavior is fixed.

Range boundaries also have known defects in initial positioning and reverse
traversal. Use `Comparison` with an explicit predicate and no bounds when boundary
correctness is required. See the reproducible cases in the development guide.

## Collecting results and pagination

Create options with `mojura.NewFilteringOpts(filters...)`; it sets `Limit: -1`.

| Option | Behavior |
| --- | --- |
| `Filters` | Empty means all entries; otherwise uses the filters above. |
| `Limit` | Negative means unlimited; positive caps newly collected results; zero returns no results. Ignored by `ForEach`, `ForEachID`, `GetFirst`, and `GetLast`. |
| `Reverse` | Reverses collection/iteration. `GetFirst` always goes forward; `GetLast` always goes backward. |
| `LastID` | Resumes after a cursor position, using the same filters and direction. It is not generally a bare entry ID. |

`GetFiltered` returns values plus `lastID`; `GetFilteredIDs` returns IDs plus the same
kind of token. The IDs collection still decodes values internally; use `ForEachID`
or a transaction's `IDCursor` to traverse IDs without decoding values.
`AppendFiltered` and `AppendFilteredIDs` append to an input slice and apply `Limit`
to newly appended items. With `Limit == 0`, both currently return nil output instead
of preserving the input slice.

The collection and iteration methods accept nil options as unlimited defaults.
`GetFirst` and `GetLast` require non-nil options and return `ErrEntryNotFound` when
no match exists. Passing nil to them can panic inside the callback goroutine.

For a stable exact-match query, the following is a function-body fragment returning
an error, with `db` opened for the `User` type above:

```go
query := mojura.NewFilteringOpts(filters.Match("teams", "team_1"))
query.Limit = 50
for {
	var (
		users  []*User
		lastID string
		err    error
	)
	if users, lastID, err = db.GetFiltered(query); err != nil {
		return err
	}

	for _, user := range users {
		fmt.Println(user.ID)
	}

	if lastID == "" {
		break
	}

	query.LastID = lastID
}
```

A token is produced whenever the limit is reached, even if that was the final
entry; a subsequent page can be empty. Tokens currently encode
`relationshipID::entryID`. Preserve the returned token for filtered queries and
avoid `::` in relationship IDs used for pagination: the delimiter is not escaped.
Each call opens a new transaction, so concurrent writes/deletes can alter later
pages; deleting the resume entry can skip a result.

**Unfiltered pagination is currently broken:** it returns tokens such as
`::00000000`, but its cursor expects a raw entry ID. For an unfiltered scan, use
`ForEach`, or set `LastID` explicitly from the final returned entry's ID (the final
string for `GetFilteredIDs`) when continuing. Do not copy this workaround to
filtered queries. See [the defect details](development.md#known-defects-and-limitations).

## Iteration and transactions

`ForEach(fn, opts)` passes `(entryID, value)`; `ForEachID(fn, opts)` passes the ID.
Return `mojura.Break` directly for successful early termination. Other errors
propagate. Low-level cursor exhaustion also uses `Break`, not `ErrEndOfEntries`.

`db.Cursor(fn, filters...)` exposes a value cursor inside a read transaction.
`txn.Cursor(filters...)` and `txn.IDCursor(filters...)` expose transaction-scoped
cursors. Use `First`/`Last`, `Next`/`Prev`, and `Seek`/`SeekReverse`; handle `Break`.
Unfiltered seeks accept entry IDs; filtered seeks use the composite token format.
Several reverse seeks use the backend's forward seek internally, so do not assume
floor-seek behavior for missing IDs. Cursors cannot be implemented outside the
package because their interfaces include private methods.

For related writes, use `db.Transaction(ctx, fn)` and return every operation error
from the callback. For a read snapshot, use `db.ReadTransaction(ctx, fn)`. This is
a transaction fragment for the `User` type above:

```go
err = db.Transaction(ctx, func(txn *mojura.Transaction[*User]) error {
	var createErr error
	if _, createErr = txn.New(&User{TeamID: "team_1"}); createErr != nil {
		return createErr
	}

	_, createErr = txn.New(&User{TeamID: "team_2"})
	return createErr
})
```

Use only the supplied transaction inside its callback. Do not open nested database
transactions, call `Close` from the callback, retain transactions/cursors afterward,
or share them between goroutines. Read transactions are for read methods only.
The callback runs in a goroutine; panics in ordinary transaction callbacks are not
recovered. Cancellation is checked by individual operations and cannot interrupt
arbitrary callback code; check `ctx` yourself during work that does not access Mojura.

Backend and Kiroku history commits are separate operations. Callback errors roll
back the backend transaction, but this is not a distributed atomic-commit guarantee
across the backend, history files, and external side effects.

`Batch(ctx, fn)` groups concurrent callers into a write transaction and may repeat
callbacks when `RetryBatchFail` is enabled. It currently has a size-triggered hang
and commit-error reporting defects. Prefer `Transaction` for new usage until those
issues are fixed; see [batching defects](development.md#known-defects-and-limitations).

## Wrappers and errors

`MakeReadWrapper(db)` exposes read convenience methods. `MakeWriteWrapper(db)`
exposes `New`, `Put`, `Update`, and `Delete`. `MakeWrapper(db)` combines both by
embedding. Wrappers retain the same database pointer; they do not own a separate
connection or expose its transaction, cursor, maintenance, or close methods.
Read-only method exposure is an API design aid, not an authorization mechanism.

Check errors before using values. `Get` and a missing-entry `Update` return
`ErrEntryNotFound` directly. `Delete` adds text with `%v`, so `errors.Is` cannot
recover the underlying missing-entry sentinel from that error. Several other paths
also lose error identity. Use the actual contract of the method and avoid inferring
runtime behavior from legacy error declarations. See the development guide's list.
