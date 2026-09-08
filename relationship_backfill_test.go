package mojura

import (
	"context"
	"errors"
	"testing"

	"github.com/mojura/backend"
	"github.com/mojura/mojura/filters"
)

func TestMojuraBackfillRelationshipIsBoundedResumableAndAdditive(t *testing.T) {
	store := newBackfillTestStore(t)

	for _, item := range []struct {
		id      string
		user    string
		contact string
	}{
		{id: "z-old", user: "user-z", contact: "contact-z"},
		{id: "a-new", user: "user-a", contact: "contact-a"},
		{id: "m-middle", user: "user-m", contact: "contact-m"},
	} {
		value := makeTestStruct(item.user, item.contact, "group", item.id)
		if _, err := store.Put(item.id, &value); err != nil {
			t.Fatalf("put %s: %v", item.id, err)
		}
	}
	clearRelationshipIndex(t, store, "users")

	if _, err := store.GetFirst(NewFilteringOpts(filters.Match("contacts", "contact-m"))); err != nil {
		t.Fatalf("unrelated relationship was damaged before backfill: %v", err)
	}
	if _, err := store.GetFirst(NewFilteringOpts(filters.Match("users", "user-a"))); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("cleared relationship lookup error = %v, want %v", err, ErrEntryNotFound)
	}

	first, err := store.BackfillRelationship(context.Background(), "users", "", 1)
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	if first.Scanned != 1 || first.Indexed != 1 || first.LastID != "a-new" || first.Done {
		t.Fatalf("first batch = %#v, want one committed entry with continuation", first)
	}
	if _, err := store.GetFirst(NewFilteringOpts(filters.Match("users", "user-a"))); err != nil {
		t.Fatalf("first batch relationship lookup: %v", err)
	}
	if _, err := store.GetFirst(NewFilteringOpts(filters.Match("users", "user-m"))); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("unprocessed relationship lookup error = %v, want %v", err, ErrEntryNotFound)
	}

	second, err := store.BackfillRelationship(context.Background(), "users", first.LastID, 1)
	if err != nil {
		t.Fatalf("second batch: %v", err)
	}
	if second.LastID != "m-middle" || second.Done {
		t.Fatalf("second batch = %#v, want m-middle with continuation", second)
	}
	final, err := store.BackfillRelationship(context.Background(), "users", second.LastID, 1)
	if err != nil {
		t.Fatalf("final batch: %v", err)
	}
	if final.LastID != "z-old" || !final.Done {
		t.Fatalf("final batch = %#v, want z-old and done", final)
	}

	for _, user := range []string{"user-a", "user-m", "user-z"} {
		if _, err := store.GetFirst(NewFilteringOpts(filters.Match("users", user))); err != nil {
			t.Fatalf("lookup %s after backfill: %v", user, err)
		}
	}
	if _, err := store.GetFirst(NewFilteringOpts(filters.Match("contacts", "contact-z"))); err != nil {
		t.Fatalf("unrelated relationship was damaged after backfill: %v", err)
	}

	replay, err := store.BackfillRelationship(context.Background(), "users", "", 1)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if replay.LastID != "a-new" || replay.Indexed != 1 {
		t.Fatalf("idempotent replay = %#v, want stable first entry", replay)
	}
}

func TestMojuraBackfillRelationshipHandlesNonexistentResumeID(t *testing.T) {
	store := newBackfillTestStore(t)
	for _, id := range []string{"a", "m", "z"} {
		value := makeTestStruct("user-"+id, "contact", "group", id)
		if _, err := store.Put(id, &value); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}
	clearRelationshipIndex(t, store, "users")

	page, err := store.BackfillRelationship(context.Background(), "users", "b", 1)
	if err != nil {
		t.Fatalf("backfill after nonexistent id: %v", err)
	}
	if page.LastID != "m" || page.Scanned != 1 {
		t.Fatalf("page = %#v, want first id strictly greater than b", page)
	}
}

func TestMojuraBackfillRelationshipRejectsInvalidRequests(t *testing.T) {
	store := newBackfillTestStore(t)

	if _, err := store.BackfillRelationship(context.Background(), "", "", 1); err == nil {
		t.Fatal("empty relationship succeeded")
	}
	if _, err := store.BackfillRelationship(context.Background(), "users", "", 0); err == nil {
		t.Fatal("zero limit succeeded")
	}
	if _, err := store.BackfillRelationship(context.Background(), "missing", "", 1); !errors.Is(err, ErrRelationshipNotFound) {
		t.Fatalf("unknown relationship error = %v, want %v", err, ErrRelationshipNotFound)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.BackfillRelationship(cancelled, "users", "", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled backfill error = %v, want %v", err, context.Canceled)
	}
}

func TestMojuraBackfillRelationshipRefusesMirrorMutation(t *testing.T) {
	opts := MakeOpts("relationship-backfill-mirror", t.TempDir())
	store, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	value := makeTestStruct("user-b", "contact-b", "group", "original")
	if _, err := store.Put("b", &value); err != nil {
		t.Fatal(err)
	}
	clearRelationshipIndex(t, store, "users")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	opts.IsMirror = true
	mirror, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mirror.Close(); err != nil {
			t.Errorf("close mirror: %v", err)
		}
	})
	page, err := mirror.BackfillRelationship(context.Background(), "users", "a", 1)
	if !errors.Is(err, ErrMirrorCannotPerformWriteActions) {
		t.Errorf("mirror backfill error = %v, want %v", err, ErrMirrorCannotPerformWriteActions)
	}
	if page.LastID != "a" || page.Scanned != 0 || page.Indexed != 0 || page.Done {
		t.Errorf("refused mirror backfill advanced progress: %#v", page)
	}
	if _, err := mirror.GetFirst(NewFilteringOpts(filters.Match("users", "user-b"))); !errors.Is(err, ErrEntryNotFound) {
		t.Errorf("mirror backfill changed the missing index: %v", err)
	}
	got, err := mirror.GetFirst(NewFilteringOpts(filters.Match("contacts", "contact-b")))
	if err != nil || got.Value != "original" {
		t.Fatalf("mirror entry or unrelated index changed: value=%v err=%v", got, err)
	}
}

func TestMojuraBackfillRelationshipFinalDecodeCancellationRollsBack(t *testing.T) {
	for _, boundary := range []string{"page-limit", "end-of-store"} {
		t.Run(boundary, func(t *testing.T) {
			store := newBackfillTestStore(t)
			ids := []string{"b", "c"}
			if boundary == "page-limit" {
				ids = append(ids, "d")
			}
			for _, id := range ids {
				var tags []string
				if id != "c" {
					tags = []string{"tag-" + id}
				}
				value := makeTestStruct("user-"+id, "contact-"+id, "group", id, tags...)
				if _, err := store.Put(id, &value); err != nil {
					t.Fatal(err)
				}
			}
			clearRelationshipIndex(t, store, "tags")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			decoded := 0
			original := store.opts.Encoder
			store.opts.Encoder = backfillDecodeObserver{Encoder: original, decoded: func() {
				decoded++
				if decoded == 2 {
					cancel()
				}
			}}
			defer func() { store.opts.Encoder = original }()
			// The final entry has no tags, so no relationship write follows its decode.
			page, err := store.BackfillRelationship(ctx, "tags", "a", 2)
			store.opts.Encoder = original
			if decoded != 2 {
				t.Fatalf("decoded %d entries, want cancellation on the final entry", decoded)
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("final-decode cancellation error = %v, want %v", err, context.Canceled)
			}
			if page.LastID != "a" || page.Scanned != 0 || page.Indexed != 0 || page.Done {
				t.Errorf("cancelled batch advanced progress: %#v", page)
			}
			for _, id := range ids {
				if _, err := store.GetFirst(NewFilteringOpts(filters.Match("tags", "tag-"+id))); !errors.Is(err, ErrEntryNotFound) {
					t.Errorf("cancelled batch committed index for %q: %v", id, err)
				}
				got, err := store.GetFirst(NewFilteringOpts(filters.Match("contacts", "contact-"+id)))
				if err != nil || got.Value != id {
					t.Fatalf("entry %q or unrelated index changed: value=%v err=%v", id, got, err)
				}
			}
			retry, err := store.BackfillRelationship(context.Background(), "tags", "a", 2)
			if err != nil || retry.LastID != "c" || retry.Scanned != 2 || retry.Indexed != 1 || retry.Done != (boundary == "end-of-store") {
				t.Fatalf("retry after rollback: page=%#v err=%v", retry, err)
			}
			if _, err := store.GetFirst(NewFilteringOpts(filters.Match("tags", "tag-b"))); err != nil {
				t.Fatalf("retry did not commit index for b: %v", err)
			}
			if _, err := store.GetFirst(NewFilteringOpts(filters.Match("tags", "tag-c"))); !errors.Is(err, ErrEntryNotFound) {
				t.Fatalf("retry added a nonexistent tag: %v", err)
			}
		})
	}
}

type backfillDecodeObserver struct {
	Encoder
	decoded func()
}

func (e backfillDecodeObserver) Unmarshal(raw []byte, value any) error {
	if err := e.Encoder.Unmarshal(raw, value); err != nil {
		return err
	}
	e.decoded()
	return nil
}

func TestMojuraBackfillRelationshipRollsBackFailedBatchAndProgress(t *testing.T) {
	opts := MakeOpts("relationship-backfill-rollback", t.TempDir())
	store, err := New[*backfillTestValue](opts, "users")
	if err != nil {
		t.Fatalf("open rollback store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close rollback store: %v", err)
		}
	})
	if _, err := store.Put("a-good", &backfillTestValue{User: "user-good"}); err != nil {
		t.Fatalf("put good entry: %v", err)
	}
	if _, err := store.Put("b-bad", &backfillTestValue{User: "user-bad", Broken: true}); err != nil {
		t.Fatalf("put bad entry: %v", err)
	}
	err = store.db.Transaction(func(txn backend.Transaction) error {
		relationships := txn.GetBucket(relationshipsBktKey)
		if err := relationships.DeleteBucket([]byte("users")); err != nil {
			return err
		}
		_, err := relationships.GetOrCreateBucket([]byte("users"))
		return err
	})
	if err != nil {
		t.Fatalf("clear rollback relationship: %v", err)
	}

	page, err := store.BackfillRelationship(context.Background(), "users", "", 2)
	if !errors.Is(err, ErrInvalidNumberOfRelationships) {
		t.Fatalf("backfill error = %v, want %v", err, ErrInvalidNumberOfRelationships)
	}
	if page.Scanned != 0 || page.Indexed != 0 || page.LastID != "" || page.Done {
		t.Fatalf("failed batch leaked progress: %#v", page)
	}
	if _, err := store.GetFirst(NewFilteringOpts(filters.Match("users", "user-good"))); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("failed batch committed partial relationship: %v", err)
	}
}

type backfillTestValue struct {
	Entry
	User   string `json:"user"`
	Broken bool   `json:"broken"`
}

func newBackfillTestStore(t *testing.T) *Mojura[*testStruct] {
	t.Helper()
	opts := MakeOpts("relationship-backfill", t.TempDir())
	store, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
	if err != nil {
		t.Fatalf("open relationship backfill store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close relationship backfill store: %v", err)
		}
	})
	return store
}

func (v *backfillTestValue) GetRelationships() Relationships {
	if v.Broken {
		return nil
	}
	return Relationships{Relationship{v.User}}
}

func clearRelationshipIndex(t *testing.T, store *Mojura[*testStruct], relationship string) {
	t.Helper()
	err := store.db.Transaction(func(txn backend.Transaction) error {
		relationships := txn.GetBucket(relationshipsBktKey)
		if relationships == nil {
			return errors.New("relationships bucket is missing")
		}
		if err := relationships.DeleteBucket([]byte(relationship)); err != nil {
			return err
		}
		_, err := relationships.GetOrCreateBucket([]byte(relationship))
		return err
	})
	if err != nil {
		t.Fatalf("clear relationship %s: %v", relationship, err)
	}
}
