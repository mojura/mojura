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
