package mojura

import (
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	boltDB "github.com/gdbu/bolt"
	"github.com/gdbu/errors"
	"github.com/gdbu/stringset"
	"github.com/mojura/backend"
	"github.com/mojura/kiroku"
	"github.com/mojura/mojura/filters"
)

var c *Mojura[*testStruct]

func TestMojuraInitializedReopenDoesNotWriteBuckets(t *testing.T) {
	for _, mirror := range []bool{false, true} {
		for _, populated := range []bool{false, true} {
			t.Run(fmt.Sprintf("mirror=%t/populated=%t", mirror, populated), func(t *testing.T) {
				opts := MakeOpts("initialized-reopen", t.TempDir())
				source, err := kiroku.NewIOSource(opts.Dir)
				if err != nil {
					t.Fatal(err)
				}
				opts.Source = source
				store, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				if populated {
					value := makeTestStruct("user", "contact", "group", "preserved", "tag")
					if _, err := store.Put("existing", &value); err != nil {
						t.Fatal(err)
					}
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				filename := path.Join(opts.Dir, opts.FullName()+".bdb")
				before := initTestBoltTxID(t, filename)
				opts.IsMirror = mirror
				reopened, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = reopened.Close() })
				if populated {
					got, err := reopened.GetFirst(NewFilteringOpts(filters.Match("users", "user"), filters.Match("tags", "tag")))
					if err != nil || got.ID != "existing" || got.Value != "preserved" {
						t.Fatalf("reopen lost record or indexes: value=%v err=%v", got, err)
					}
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
				if after := initTestBoltTxID(t, filename); after != before {
					t.Fatalf("initialized reopen committed a write: Tx.ID %d -> %d", before, after)
				}
			})
		}
	}
}

func TestMojuraInitializesOnlyMissingBuckets(t *testing.T) {
	for _, missing := range []string{"fresh", "entries", "lookups", "meta", "relationships", "users"} {
		t.Run(missing, func(t *testing.T) {
			opts := MakeOpts("missing-buckets", t.TempDir())
			filename := path.Join(opts.Dir, opts.FullName()+".bdb")
			if missing != "fresh" {
				store, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			}
			db, err := boltDB.Open(filename, 0600, &boltDB.Options{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if missing != "fresh" {
				if err := db.Update(func(tx *boltDB.Tx) error {
					if missing == "users" {
						return tx.Bucket(relationshipsBktKey).DeleteBucket([]byte(missing))
					}
					return tx.DeleteBucket([]byte(missing))
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before := initTestBoltTxID(t, filename)
			opts.IsMirror = true
			store, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if err := store.db.ReadTransaction(func(tx backend.Transaction) error {
				for _, key := range [][]byte{entriesBktKey, lookupsBktKey, metaBktKey, relationshipsBktKey} {
					if tx.GetBucket(key) == nil {
						return fmt.Errorf("missing root bucket %q", key)
					}
				}
				for _, key := range store.relationships {
					if tx.GetBucket(relationshipsBktKey).GetBucket(key) == nil {
						return fmt.Errorf("missing relationship bucket %q", key)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if after := initTestBoltTxID(t, filename); after != before+1 {
				t.Fatalf("missing state requires exactly one initialization commit: Tx.ID %d -> %d", before, after)
			}
		})
	}
}

func TestMojuraSourceLessReopenPreservesRecords(t *testing.T) {
	opts := MakeOpts("source-less-reopen", t.TempDir())
	store, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	value := makeTestStruct("user", "contact", "group", "preserved", "tag")
	if _, err := store.Put("existing", &value); err != nil {
		t.Fatal(err)
	}
	meta, err := store.p.Meta()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var chunks, snapshots int
	for _, entry := range entries {
		parsed, err := kiroku.ParseFilename(entry.Name())
		if err != nil {
			continue
		}
		switch parsed.Filetype {
		case kiroku.TypeChunk:
			chunks++
		case kiroku.TypeSnapshot:
			snapshots++
		}
	}
	filename := path.Join(opts.Dir, opts.FullName()+".bdb")
	before := initTestBoltTxID(t, filename)
	reopened, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.GetFirst(NewFilteringOpts(filters.Match("users", "user"), filters.Match("tags", "tag")))
	if err != nil || got.ID != "existing" || got.Value != "preserved" {
		t.Fatalf("source-less reopen lost record/indexes: value=%v err=%v", got, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("source-less post-Put metadata=%+v, post-close chunks=%d snapshots=%d, reopen Tx.ID %d -> %d",
		meta, chunks, snapshots, before, initTestBoltTxID(t, filename))
}

func TestMojuraInitializedReopenAddsRelationshipBucket(t *testing.T) {
	opts := MakeOpts("additive-bucket", t.TempDir())
	source, err := kiroku.NewIOSource(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	opts.Source = source
	store, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	value := makeTestStruct("user", "contact", "group", "preserved", "tag")
	if _, err := store.Put("existing", &value); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	filename := path.Join(opts.Dir, opts.FullName()+".bdb")
	before := initTestBoltTxID(t, filename)
	reopened, err := New[*initAddedRelationshipValue](opts, "users", "contacts", "groups", "tags", "extra")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.db.ReadTransaction(func(tx backend.Transaction) error {
		if tx.GetBucket(relationshipsBktKey).GetBucket([]byte("extra")) == nil {
			return fmt.Errorf("additional relationship bucket was not initialized")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := reopened.GetFirst(NewFilteringOpts(filters.Match("users", "user"), filters.Match("tags", "tag")))
	if err != nil || got.ID != "existing" || got.Value != "preserved" {
		t.Fatalf("additive initialization changed existing record/indexes: value=%v err=%v", got, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if after := initTestBoltTxID(t, filename); after != before+1 {
		t.Fatalf("additive initialization requires one commit: Tx.ID %d -> %d", before, after)
	}
}

func TestMojuraBucketInitializationFailureClosesOwnedBackend(t *testing.T) {
	for _, phase := range []string{"before-read", "after-read", "missing-write"} {
		t.Run(phase, func(t *testing.T) {
			opts := MakeOpts("initialization-failure", t.TempDir())
			opts.IsMirror = true
			store, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			filename := path.Join(opts.Dir, opts.FullName()+".bdb")
			if phase == "missing-write" {
				db, err := boltDB.Open(filename, 0600, &boltDB.Options{Timeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				err = db.Update(func(tx *boltDB.Tx) error { return tx.DeleteBucket(metaBktKey) })
				closeErr := db.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("remove metadata: update=%v close=%v", err, closeErr)
				}
			}
			before := initTestBoltTxID(t, filename)
			refusal := stderrors.New("backend initialization refused")
			probe := &initFailureBackend{phase: phase, refusal: refusal}
			opts.Initializer = initFailureInitializer{probe}
			t.Cleanup(func() {
				if probe.Backend != nil && probe.closes == 0 {
					_ = probe.Backend.Close()
				}
			})
			opened, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
			if opened != nil {
				_ = opened.Close()
				t.Fatalf("backend refusal was accepted: err=%v", err)
			}
			if !stderrors.Is(err, refusal) || probe.closes != 1 {
				t.Fatalf("initialization refusal/ownership lost: err=%v closes=%d", err, probe.closes)
			}
			wantWrites := 0
			if phase == "missing-write" {
				wantWrites = 1
			}
			if probe.writes != wantWrites {
				t.Fatalf("write attempts=%d, want %d", probe.writes, wantWrites)
			}
			if after := initTestBoltTxID(t, filename); after != before {
				t.Fatalf("failed initialization committed: Tx.ID %d -> %d", before, after)
			}
		})
	}
}

type initFailureInitializer struct{ probe *initFailureBackend }

func (i initFailureInitializer) New(filename string) (backend.Backend, error) {
	var err error
	i.probe.Backend, err = defaultOpts.Initializer.New(filename)
	return i.probe, err
}

type initFailureBackend struct {
	backend.Backend
	phase          string
	refusal        error
	writes, closes int
}

func (b *initFailureBackend) ReadTransaction(fn func(backend.Transaction) error) error {
	if b.phase == "before-read" {
		return b.refusal
	}
	if err := b.Backend.ReadTransaction(fn); err != nil {
		return err
	}
	if b.phase == "after-read" {
		return b.refusal
	}
	return nil
}

func (b *initFailureBackend) Transaction(fn func(backend.Transaction) error) error {
	b.writes++
	return b.Backend.Transaction(func(tx backend.Transaction) error {
		if err := fn(tx); err != nil {
			return err
		}
		if b.phase == "missing-write" {
			return b.refusal
		}
		return nil
	})
}

func (b *initFailureBackend) Close() error {
	b.closes++
	return b.Backend.Close()
}

type initAddedRelationshipValue struct{ testStruct }

func (v *initAddedRelationshipValue) GetRelationships() Relationships {
	return append(v.testStruct.GetRelationships(), Relationship{})
}

func initTestBoltTxID(t *testing.T, filename string) int {
	t.Helper()
	db, err := boltDB.Open(filename, 0600, &boltDB.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var id int
	if err := db.View(func(tx *boltDB.Tx) error { id = tx.ID(); return nil }); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestFixtureStoresRemainIsolated(t *testing.T) {
	survivor, err := testInit(t)
	if err != nil {
		t.Fatal(err)
	}
	var retiredDir string
	t.Run("independent", func(t *testing.T) {
		retired, err := testInit(t)
		if err != nil {
			t.Fatal(err)
		}
		retiredDir = retired.opts.Dir
		if retiredDir == survivor.opts.Dir {
			t.Fatalf("independent fixtures share directory %q", retiredDir)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		results := make(chan error, 2)
		for i, store := range []*Mojura[*testStruct]{survivor, retired} {
			go func() {
				value := makeTestStruct("user", "contact", "group", fmt.Sprintf("owner-%d", i))
				results <- store.Transaction(ctx, func(tx *Transaction[*testStruct]) error {
					created, err := tx.New(&value)
					if err != nil {
						return err
					}
					if created.ID != "00000000" {
						return fmt.Errorf("fixture %d first ID=%q, want 00000000", i, created.ID)
					}
					got, err := tx.Get(created.ID)
					if err != nil {
						return err
					}
					return value.compare(got)
				})
			}()
		}
		for range 2 {
			if err := <-results; err != nil {
				t.Error(err)
			}
		}
	})
	if t.Failed() {
		return
	}
	if _, err := os.Stat(retiredDir); !os.IsNotExist(err) {
		t.Fatalf("retired fixture directory survived cleanup: %v", err)
	}
	got, err := survivor.Get("00000000")
	if err != nil {
		t.Fatalf("surviving fixture lost its record: %v", err)
	}
	if got.Value != "owner-0" {
		t.Fatalf("surviving fixture record=%q, want owner-0", got.Value)
	}
}

func TestMojuraTransactionCancellation(t *testing.T) {
	for _, mode := range []string{"read", "write", "import"} {
		for _, phase := range []string{"before", "after", "callback-error"} {
			t.Run(mode+"/"+phase, func(t *testing.T) {
				store := newBackfillTestStore(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if phase == "before" {
					cancel()
				}
				called := false
				callbackErr := stderrors.New("callback refused")
				fn := func(tx *Transaction[*testStruct]) error {
					called = true
					if mode != "read" && phase != "before" {
						value := makeTestStruct("user", "contact", "group", "cancelled")
						if _, err := tx.Put("cancelled", &value); err != nil {
							return err
						}
					}
					cancel()
					if phase == "callback-error" {
						return callbackErr
					}
					return nil
				}
				var err error
				switch mode {
				case "read":
					err = store.ReadTransaction(ctx, fn)
				case "write":
					err = store.Transaction(ctx, fn)
				case "import":
					err = store.importTransaction(ctx, fn)
				}
				want := error(context.Canceled)
				if phase == "callback-error" {
					want = callbackErr
				}
				if !stderrors.Is(err, want) || called != (phase != "before") {
					t.Fatalf("err=%v want=%v called=%v", err, want, called)
				}
				if _, err := store.Get("cancelled"); !stderrors.Is(err, ErrEntryNotFound) {
					t.Fatalf("cancelled write committed: %v", err)
				}
			})
		}
	}
}

func TestMojuraReadCancellationJoinsCallback(t *testing.T) {
	store := newBackfillTestStore(t)
	value := makeTestStruct("user", "contact", "group", "held")
	if _, err := store.Put("held", &value); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	callbackReturned := make(chan struct{})
	store.db = callbackLifetimeBackend{Backend: store.db, returned: callbackReturned}
	done := make(chan error, 1)
	go func() {
		done <- store.ReadTransaction(ctx, func(tx *Transaction[*testStruct]) error {
			defer close(callbackReturned)
			bkt, err := tx.getEntriesBucket()
			if err != nil {
				return err
			}
			close(entered)
			<-release
			// This view must remain usable until the callback returns, even after cancellation.
			if len(bkt.Get([]byte("held"))) == 0 {
				return fmt.Errorf("read view closed before callback returned")
			}
			return nil
		})
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		close(release)
		t.Fatalf("transaction returned before held callback: %v", err)
	default:
	}
	close(release)
	if err := <-done; !stderrors.Is(err, context.Canceled) {
		t.Fatalf("held read cancellation: %v", err)
	}
}

type callbackLifetimeBackend struct {
	backend.Backend
	returned <-chan struct{}
}

func (b callbackLifetimeBackend) ReadTransaction(fn func(backend.Transaction) error) error {
	return b.Backend.ReadTransaction(func(tx backend.Transaction) error {
		err := fn(tx)
		select {
		case <-b.returned:
			return err
		default:
			return fmt.Errorf("backend view closing before user callback returned")
		}
	})
}

type transactionFailureBackend struct {
	backend.Backend
	phase    string
	err      error
	attempts *int
}

func (b transactionFailureBackend) Transaction(fn func(backend.Transaction) error) error {
	*b.attempts++
	if b.phase == "begin" {
		return b.err
	}
	err := b.Backend.Transaction(func(tx backend.Transaction) error {
		if err := fn(tx); err != nil {
			if b.phase == "callback-replaced" {
				return b.err
			}
			if b.phase == "callback-joined" {
				return stderrors.Join(err, b.err)
			}
			return err
		}
		if b.phase == "rollback" {
			return b.err
		}
		return nil
	})
	if err == nil && b.phase == "committed" {
		return b.err
	}
	return err
}

func TestMojuraBatchReturnsOuterErrorsWithoutRetry(t *testing.T) {
	for _, phase := range []string{"begin", "rollback", "committed", "callback-replaced", "callback-joined"} {
		t.Run(phase, func(t *testing.T) {
			store := newBackfillTestStore(t)
			store.opts.RetryBatchFail = true
			store.opts.MaxBatchDuration = time.Hour
			failure := stderrors.New("outer transaction failed")
			callbackFailure := stderrors.New("callback failed before outer error")
			callbackFails := phase == "callback-replaced" || phase == "callback-joined"
			attempts := 0
			store.db = transactionFailureBackend{Backend: store.db, phase: phase, err: failure, attempts: &attempts}
			var called [3]int
			var results [3]chan error
			for i := range results {
				results[i] = store.b.Append(context.Background(), func(tx *Transaction[*testStruct]) error {
					called[i]++
					if callbackFails && i == 1 {
						return callbackFailure
					}
					value := makeTestStruct("user", "contact", "group", phase)
					_, err := tx.Put(fmt.Sprintf("outer-%d", i), &value)
					return err
				})
			}
			store.b.Run()
			if attempts != 1 {
				t.Errorf("outer failure retried %d transactions, want 1", attempts)
			}
			for i, result := range results {
				if err := <-result; !stderrors.Is(err, failure) {
					t.Errorf("call %d acknowledged outer failure: %v", i, err)
				}
				wantCalls := 1
				if phase == "begin" || (callbackFails && i == 2) {
					wantCalls = 0
				}
				if called[i] != wantCalls {
					t.Errorf("call %d ran %d times, want %d", i, called[i], wantCalls)
				}
				_, err := store.Get(fmt.Sprintf("outer-%d", i))
				if phase == "committed" && err != nil {
					t.Errorf("ambiguous commit did not retain committed data: %v", err)
				} else if phase != "committed" && !stderrors.Is(err, ErrEntryNotFound) {
					t.Errorf("failed transaction retained record: %v", err)
				}
			}
			store.opts.MaxBatchCalls = 1
			err := store.Batch(context.Background(), func(*Transaction[*testStruct]) error {
				if callbackFails {
					return callbackFailure
				}
				return nil
			})
			if !stderrors.Is(err, failure) || attempts != 2 {
				t.Fatalf("public Batch err=%v attempts=%d, want outer failure and one additional attempt", err, attempts)
			}
		})
	}
}

type batchSliceError []string

func (err batchSliceError) Error() string { return strings.Join(err, ": ") }

func TestMojuraBatchPreservesNoncomparableCallbackError(t *testing.T) {
	store := newBackfillTestStore(t)
	store.opts.MaxBatchCalls = 1
	want := batchSliceError{"original", "callback error"}
	err := store.Batch(context.Background(), func(*Transaction[*testStruct]) error { return want })
	got, ok := err.(batchSliceError)
	if !ok || len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("callback error replaced: %#v", err)
	}
}

func TestMojuraBatchCancellationAndCallbackRetry(t *testing.T) {
	for _, phase := range []string{"before", "after", "callback-error"} {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retry=%t", phase, retry), func(t *testing.T) {
				store := newBackfillTestStore(t)
				store.opts.RetryBatchFail = retry
				store.opts.MaxBatchDuration = time.Hour
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if phase == "before" {
					cancel()
				}
				callbackErr := stderrors.New("batch callback refused")
				var called [3]int
				var results [3]chan error
				for i := range results {
					callCtx := context.Background()
					if i == 1 {
						callCtx = ctx
					}
					results[i] = store.b.Append(callCtx, func(tx *Transaction[*testStruct]) error {
						called[i]++
						value := makeTestStruct("user", "contact", "group", phase)
						if _, err := tx.Put(fmt.Sprintf("batch-%d", i), &value); err != nil {
							return err
						}
						if i == 1 {
							cancel()
							if phase == "callback-error" {
								return callbackErr
							}
						}
						return nil
					})
				}
				store.b.Run()
				for i, result := range results {
					err := <-result
					want := error(nil)
					if i == 1 {
						want = context.Canceled
						if phase == "callback-error" {
							want = callbackErr
						}
					}
					if i == 0 && !retry {
						if err == nil {
							t.Error("failed group acknowledged unretried prefix")
						}
					} else if !stderrors.Is(err, want) {
						t.Errorf("call %d err=%v want=%v", i, err, want)
					}
					wantCalls := 1
					if i == 0 && retry {
						wantCalls = 2
					} else if i == 1 && phase == "before" {
						wantCalls = 0
					}
					if called[i] != wantCalls {
						t.Errorf("call %d count=%d want=%d", i, called[i], wantCalls)
					}
					_, readErr := store.Get(fmt.Sprintf("batch-%d", i))
					if i == 1 || (i == 0 && !retry) {
						if !stderrors.Is(readErr, ErrEntryNotFound) {
							t.Errorf("failed call %d retained data: %v", i, readErr)
						}
					} else if readErr != nil {
						t.Errorf("successful call %d missing: %v", i, readErr)
					}
				}
			})
		}
	}
}

func TestMojuraBatchLimitReturnsResult(t *testing.T) {
	store := newBackfillTestStore(t)
	store.opts.MaxBatchCalls = 1
	called := 0
	fn := func(*Transaction[*testStruct]) error { called++; return nil }
	result := store.b.Append(context.Background(), fn)
	if result == nil {
		t.Fatal("batch-limit flush returned nil channel; public Batch would wait forever")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := store.Batch(context.Background(), fn); err != nil {
		t.Fatal(err)
	}
	if called != 2 {
		t.Fatalf("callback count=%d want 2", called)
	}
}

type observedDoneContext struct {
	context.Context
	observed atomic.Int64
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.observed.Add(1)
	return c.Context.Done()
}

func TestMojuraContextUpdatesDoNotStartWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		observed := &observedDoneContext{Context: ctx}
		cc := newContextContainer(context.Background())
		for range 8 {
			cc.update(observed)
		}
		synctest.Wait()
		if got := observed.observed.Load(); got != 0 {
			t.Errorf("context updates started %d asynchronous waiters", got)
		}
		cancel()
		synctest.Wait()
	})
}

func TestNew(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		testTeardown(c, t)
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		return
	}
}

func TestMojura_New(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if len(created.ID) == 0 {
		t.Fatal("invalid entry id, expected non-empty value")
	}
}

func TestMojura_New_with_database_build(t *testing.T) {
	testDir := t.TempDir()
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInitInDir(t, testDir); err != nil {
		t.Fatal(err)
	}

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var e *testStruct
	if e, err = c.Get(created.ID); err != nil {
		t.Fatalf("error getting: %v", err)
	}

	if err = foobar.compare(e); err != nil {
		t.Fatal(err)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

	filename := path.Join(testDir, "test.bdb")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	filename = path.Join(testDir, "test.kir")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	if c, err = testInitInDir(t, testDir); err != nil {
		t.Fatalf("error initializing: %v", err)
	}
	defer testTeardown(c, t)

	if e, err = c.Get(created.ID); err != nil {
		t.Fatalf("error getting after rebuild: %v", err)
	}

	if err = foobar.compare(e); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_New_with_history_build(t *testing.T) {
	testDir := t.TempDir()
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInitInDir(t, testDir); err != nil {
		t.Fatal(err)
	}

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

	filename := path.Join(testDir, "test.kir")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	if c, err = testInitInDir(t, testDir); err != nil {
		t.Fatalf("error initializing: %v", err)
	}
	defer testTeardown(c, t)

	var e *testStruct
	if e, err = c.Get(created.ID); err != nil {
		t.Fatalf("error getting: %v", err)
	}

	if err = foobar.compare(e); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_New_with_history_and_database_build(t *testing.T) {
	testDir := t.TempDir()
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInitInDir(t, testDir); err != nil {
		t.Fatal(err)
	}

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

	filename := path.Join(testDir, "test.kir")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	sourcepath := path.Join(testDir, "source")
	if err = os.RemoveAll(sourcepath); err != nil {
		t.Fatal(err)
	}

	if c, err = testInitInDir(t, testDir); err != nil {
		t.Fatalf("error initializing: %v", err)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

	filename = path.Join(testDir, "test.bdb")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	filename = path.Join(testDir, "test.kir")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	if c, err = testInitInDir(t, testDir); err != nil {
		t.Fatalf("error initializing: %v", err)
	}
	defer testTeardown(c, t)

	var e *testStruct
	if e, err = c.Get(created.ID); err != nil {
		t.Fatalf("error getting: %v", err)
	}

	if err = foobar.compare(e); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_Put(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if created, err = c.Put("test", &foobar); err != nil {
		t.Fatal(err)
	}

	if len(created.ID) == 0 {
		t.Fatal("invalid entry id, expected non-empty value")
	}

	byGroup := filters.Match("groups", "group_1")
	opts := NewFilteringOpts(byGroup)

	var results []*testStruct
	if results, _, err = c.GetFiltered(opts); err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("invalid results, expectected count of %d and received count of %d", 1, len(results))
	}

	foobar.GroupID = "group_2"

	if _, err = c.Put("test", &foobar); err != nil {
		t.Fatal(err)
	}

	if results, _, err = c.GetFiltered(opts); err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Fatalf("invalid results, expectected count of %d and received count of %d", 0, len(results))
	}

	byGroup = filters.Match("groups", "group_2")
	opts = NewFilteringOpts(byGroup)

	if results, _, err = c.GetFiltered(opts); err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("invalid results, expectected count of %d and received count of %d", 1, len(results))
	}
}

func TestMojura_New_indexing(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if created.ID != "00000000" {
		t.Fatalf("invalid created ID, expected <%s> and received <%s>", "00000000", created.ID)
	}

	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if created.ID != "00000001" {
		t.Fatalf("invalid created ID, expected <%s> and received <%s>", "00000001", created.ID)
	}

	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if created.ID != "00000002" {
		t.Fatalf("invalid created ID, expected <%s> and received <%s>", "00000002", created.ID)
	}

	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if created.ID != "00000003" {
		t.Fatalf("invalid created ID, expected <%s> and received <%s>", "00000003", created.ID)
	}

}

func TestMojura_Get(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if created.ID == "" {
		t.Fatal("invalid created ID, empty ID received")
	}

	foobar.ID = created.ID

	if err = testCheck(&foobar, created); err != nil {
		t.Fatal(err)
	}

	var fb *testStruct
	if fb, err = c.Get(created.ID); err != nil {
		t.Fatal(err)
	}

	if err = testCheck(&foobar, fb); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_Get_context(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	type testcase struct {
		iterations int
		timeout    time.Duration
		err        error
	}

	tcs := []testcase{
		{iterations: 1, timeout: time.Millisecond * 190, err: nil},
		{iterations: 1, timeout: time.Millisecond * 210, err: context.DeadlineExceeded},
		{iterations: 5, timeout: time.Millisecond * 100, err: context.DeadlineExceeded},
		{iterations: 10, timeout: time.Millisecond * 180, err: context.DeadlineExceeded},
		{iterations: 5, timeout: time.Millisecond * 35, err: nil},
		{iterations: 10, timeout: time.Millisecond * 15, err: nil},
		{iterations: 3, timeout: time.Millisecond * 500, err: context.DeadlineExceeded},
	}

	for _, tc := range tcs {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
		defer cancel()
		if err = c.ReadTransaction(ctx, func(txn *Transaction[*testStruct]) (err error) {
			for i := 0; i < tc.iterations; i++ {
				time.Sleep(tc.timeout)
				if _, err = txn.Get(created.ID); err != nil {
					return
				}
			}

			return
		}); err != tc.err {
			t.Fatalf("invalid error, expected %v and received %v [test case %+v]", tc.err, err, tc)
		}
	}
}

func TestMojura_GetFiltered_many_to_many(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	entries := []*testStruct{
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "foo", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "baz"),
	}

	type testcase struct {
		tag           string
		expectedCount int
	}

	runCases := func(cases []testcase) (err error) {
		for _, tc := range cases {
			filter := filters.Match("tags", tc.tag)
			o := NewFilteringOpts(filter)
			var entries []*testStruct
			if entries, _, err = c.GetFiltered(o); err != nil {
				return
			}

			if len(entries) != tc.expectedCount {
				err = fmt.Errorf("invalid number of entries, expected %d and received %d for tag of \"%s\"", tc.expectedCount, len(entries), tc.tag)
			}
		}

		return
	}

	createCases := []testcase{
		{
			tag:           "foo",
			expectedCount: 1,
		},
		{
			tag:           "bar",
			expectedCount: 2,
		},
		{
			tag:           "baz",
			expectedCount: 1,
		},
		{
			tag:           "beam",
			expectedCount: 0,
		},
		{
			tag:           "boom",
			expectedCount: 0,
		},
	}

	updateCases := []testcase{
		{
			tag:           "foo",
			expectedCount: 0,
		},
		{
			tag:           "bar",
			expectedCount: 0,
		},
		{
			tag:           "baz",
			expectedCount: 0,
		},
		{
			tag:           "beam",
			expectedCount: 0,
		},
		{
			tag:           "boom",
			expectedCount: 3,
		},
	}

	deleteCases := []testcase{
		{
			tag:           "foo",
			expectedCount: 0,
		},
		{
			tag:           "bar",
			expectedCount: 0,
		},
		{
			tag:           "baz",
			expectedCount: 0,
		},
		{
			tag:           "beam",
			expectedCount: 0,
		},
		{
			tag:           "boom",
			expectedCount: 0,
		},
	}

	for i, entry := range entries {
		if entries[i], err = c.New(entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(createCases); err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		entry.Tags = []string{"boom"}
		if _, err = c.Put(entry.ID, entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(updateCases); err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		if _, err = c.Delete(entry.ID); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(deleteCases); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_GetFilteredIDs_many_to_many(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	entries := []*testStruct{
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "foo", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "baz"),
	}

	type testcase struct {
		tag           string
		expectedCount int
	}

	runCases := func(cases []testcase) (err error) {
		for _, tc := range cases {
			filter := filters.Match("tags", tc.tag)
			o := NewFilteringOpts(filter)
			var ids []string
			if ids, _, err = c.GetFilteredIDs(o); err != nil {
				return
			}

			if len(ids) != tc.expectedCount {
				err = fmt.Errorf("invalid number of entries, expected %d and received %d for tag of \"%s\"", tc.expectedCount, len(entries), tc.tag)
			}
		}

		return
	}

	createCases := []testcase{
		{
			tag:           "foo",
			expectedCount: 1,
		},
		{
			tag:           "bar",
			expectedCount: 2,
		},
		{
			tag:           "baz",
			expectedCount: 1,
		},
		{
			tag:           "beam",
			expectedCount: 0,
		},
		{
			tag:           "boom",
			expectedCount: 0,
		},
	}

	updateCases := []testcase{
		{
			tag:           "foo",
			expectedCount: 0,
		},
		{
			tag:           "bar",
			expectedCount: 0,
		},
		{
			tag:           "baz",
			expectedCount: 0,
		},
		{
			tag:           "beam",
			expectedCount: 0,
		},
		{
			tag:           "boom",
			expectedCount: 3,
		},
	}

	deleteCases := []testcase{
		{
			tag:           "foo",
			expectedCount: 0,
		},
		{
			tag:           "bar",
			expectedCount: 0,
		},
		{
			tag:           "baz",
			expectedCount: 0,
		},
		{
			tag:           "beam",
			expectedCount: 0,
		},
		{
			tag:           "boom",
			expectedCount: 0,
		},
	}

	for i, entry := range entries {
		if entries[i], err = c.New(entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(createCases); err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		entry.Tags = []string{"boom"}
		if _, err = c.Put(entry.ID, entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(updateCases); err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		if _, err = c.Delete(entry.ID); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(deleteCases); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_GetFiltered_seek(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	entries := []*testStruct{
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "foo", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "baz"),
	}

	for i, entry := range entries {
		if entries[i], err = c.New(entry); err != nil {
			t.Fatal(err)
		}
	}

	filter := filters.Match("users", "user_1")

	var o FilteringOpts
	o.Filters = append(o.Filters, filter)
	o.Limit = 1

	var filtered []*testStruct
	if filtered, o.LastID, err = c.GetFiltered(&o); err != nil {
		t.Fatal(err)
	}

	target := filtered[0]
	if target.ID != entries[0].ID {
		t.Fatalf("invalid ID, expected <%s> and received <%s>", entries[0].ID, target.ID)
	}

	if filtered, o.LastID, err = c.GetFiltered(&o); err != nil {
		t.Fatal(err)
	}

	target = filtered[0]
	if target.ID != entries[1].ID {
		t.Fatalf("invalid ID, expected <%s> and received <%s>", entries[0].ID, target.ID)
	}

	if filtered, o.LastID, err = c.GetFiltered(&o); err != nil {
		t.Fatal(err)
	}

	target = filtered[0]

	if target.ID != entries[2].ID {
		t.Fatalf("invalid ID, expected <%s> and received <%s>", entries[0].ID, target.ID)
	}

	if filtered, o.LastID, err = c.GetFiltered(&o); err != nil {
		t.Fatal(err)
	}

	if len(filtered) != 0 {
		t.Fatalf("invalid filtered length, expected %d and received %d <%v>", 0, len(filtered), filtered)
	}
}

func TestMojura_GetFilteredIDs_seek(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	entries := []*testStruct{
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "foo", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "baz"),
	}

	for i, entry := range entries {
		if entries[i], err = c.New(entry); err != nil {
			t.Fatal(err)
		}
	}

	filter := filters.Match("users", "user_1")

	var o FilteringOpts
	o.Filters = append(o.Filters, filter)
	o.Limit = 1

	var filtered []string
	if filtered, o.LastID, err = c.GetFilteredIDs(&o); err != nil {
		t.Fatal(err)
	}

	targetID := filtered[0]
	if targetID != entries[0].ID {
		t.Fatalf("invalid ID, expected <%s> and received <%s>", entries[0].ID, targetID)
	}

	if filtered, o.LastID, err = c.GetFilteredIDs(&o); err != nil {
		t.Fatal(err)
	}

	targetID = filtered[0]
	if targetID != entries[1].ID {
		t.Fatalf("invalid ID, expected <%s> and received <%s>", entries[0].ID, targetID)
	}

	if filtered, o.LastID, err = c.GetFilteredIDs(&o); err != nil {
		t.Fatal(err)
	}

	targetID = filtered[0]

	if targetID != entries[2].ID {
		t.Fatalf("invalid ID, expected <%s> and received <%s>", entries[0].ID, targetID)
	}

	if filtered, o.LastID, err = c.GetFilteredIDs(&o); err != nil {
		t.Fatal(err)
	}

	if len(filtered) != 0 {
		t.Fatalf("invalid filtered length, expected %d and received %d <%v>", 0, len(filtered), filtered)
	}
}

func TestMojura_AppendFiltered(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	entries := []testStruct{
		makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "foo", "bar", "baz"),
		makeTestStruct("user_1", "contact_1", "group_2", "FOO FOO", "bar"),
		makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "baz"),
	}

	type testcase struct {
		tag           string
		group         string
		expectedCount int
	}

	runCases := func(cases []testcase) (err error) {
		for _, tc := range cases {
			var entries []*testStruct
			filter := filters.Match("tags", tc.tag)
			o := NewFilteringOpts(filter)
			if entries, _, err = c.AppendFiltered(entries, o); err != nil {
				return
			}

			filter = filters.Match("groups", tc.group)
			o = NewFilteringOpts(filter)
			if entries, _, err = c.AppendFiltered(entries, o); err != nil {
				return
			}

			if len(entries) != tc.expectedCount {
				err = fmt.Errorf("invalid number of entries, expected %d and received %d for tag of <%s> and group of <%s>", tc.expectedCount, len(entries), tc.tag, tc.group)
			}
		}

		return
	}

	createCases := []testcase{
		{
			tag:           "foo",
			group:         "group_1",
			expectedCount: 3,
		},
		{
			tag:           "bar",
			group:         "group_1",
			expectedCount: 4,
		},

		{
			tag:           "baz",
			group:         "group_1",
			expectedCount: 4,
		},
		{
			tag:           "foo",
			group:         "group_2",
			expectedCount: 2,
		},
		{
			tag:           "bar",
			group:         "group_2",
			expectedCount: 3,
		},

		{
			tag:           "baz",
			group:         "group_2",
			expectedCount: 3,
		},
	}

	for _, entry := range entries {
		if _, err = c.New(&entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(createCases); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_AppendFilteredIDs(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	entries := []testStruct{
		makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "foo", "bar", "baz"),
		makeTestStruct("user_1", "contact_1", "group_2", "FOO FOO", "bar"),
		makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "baz"),
	}

	type testcase struct {
		tag           string
		group         string
		expectedCount int
	}

	runCases := func(cases []testcase) (err error) {
		for _, tc := range cases {
			var ids []string
			filter := filters.Match("tags", tc.tag)
			o := NewFilteringOpts(filter)
			if ids, _, err = c.AppendFilteredIDs(ids, o); err != nil {
				return
			}

			filter = filters.Match("groups", tc.group)
			o = NewFilteringOpts(filter)
			if ids, _, err = c.AppendFilteredIDs(ids, o); err != nil {
				return
			}

			if len(ids) != tc.expectedCount {
				err = fmt.Errorf("invalid number of entries, expected %d and received %d for tag of <%s> and group of <%s>", tc.expectedCount, len(ids), tc.tag, tc.group)
			}
		}

		return
	}

	createCases := []testcase{
		{
			tag:           "foo",
			group:         "group_1",
			expectedCount: 3,
		},
		{
			tag:           "bar",
			group:         "group_1",
			expectedCount: 4,
		},

		{
			tag:           "baz",
			group:         "group_1",
			expectedCount: 4,
		},
		{
			tag:           "foo",
			group:         "group_2",
			expectedCount: 2,
		},
		{
			tag:           "bar",
			group:         "group_2",
			expectedCount: 3,
		},

		{
			tag:           "baz",
			group:         "group_2",
			expectedCount: 3,
		},
	}

	for _, entry := range entries {
		if _, err = c.New(&entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(createCases); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_Update(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if created, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	foobar.ID = created.ID
	foobar.Value = "FOO FOO"

	var updated *testStruct
	if updated, err = c.Update(created.ID, func(e *testStruct) (err error) {
		*e = foobar
		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = testCheck(&foobar, updated); err != nil {
		t.Fatal(err)
	}

	var fb *testStruct
	if fb, err = c.Get(created.ID); err != nil {
		t.Fatal(err)
	}

	if err = testCheck(&foobar, fb); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_ForEach(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var cnt int
	if err = c.ForEach(func(key string, v *testStruct) (err error) {
		// We are not checking ID correctness in this test
		foobar.ID = v.ID

		if err = testCheck(&foobar, v); err != nil {
			t.Fatal(err)
		}

		cnt++
		return
	}, nil); err != nil {
		t.Fatal(err)
	}

	if cnt != 2 {
		t.Fatalf("invalid number of entries, expected %d and received %d", 2, cnt)
	}
}

func TestMojura_ForEach_with_filter(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	foobar.UserID = "user_2"
	foobar.ContactID = "contact_3"

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var cnt int
	fn := func(key string, v *testStruct) (err error) {
		// We are not checking ID correctness in this test
		foobar.ID = v.ID

		if err = testCheck(&foobar, v); err != nil {
			t.Fatal(err)
		}

		cnt++
		return
	}

	var o FilteringOpts
	filter := filters.Match("contacts", foobar.ContactID)
	o.Filters = append(o.Filters, filter)
	if err = c.ForEach(fn, &o); err != nil {
		t.Fatal(err)
	}

	if cnt != 1 {
		t.Fatalf("invalid number of entries, expected %d and received %d", 1, cnt)
	}
}

func TestMojura_ForEach_with_multiple_filters(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	user1 := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")
	user2 := makeTestStruct("user_2", "contact_1", "group_1", "bunny bar bar")
	user3 := makeTestStruct("user_3", "contact_2", "group_1", "baz")
	user4 := makeTestStruct("user_4", "contact_2", "group_1", "yep")

	if _, err = c.New(&user1); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&user2); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&user3); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&user4); err != nil {
		t.Fatal(err)
	}

	type testcase struct {
		filters     []Filter
		expectedIDs []string
	}

	tcs := []testcase{
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
			},
			expectedIDs: []string{"00000000", "00000001"},
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
			},
			expectedIDs: []string{"00000002", "00000003"},
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
				filters.Match("groups", "group_1"),
			},
			expectedIDs: []string{"00000000", "00000001"},
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
				filters.Match("groups", "group_1"),
			},
			expectedIDs: []string{"00000002", "00000003"},
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
				filters.Match("users", "user_1"),
			},
			expectedIDs: []string{"00000000"},
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
				filters.Match("users", "user_2"),
			},
			expectedIDs: []string{},
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
				filters.Match("users", "user_1"),
				filters.Match("groups", "group_1"),
			},
			expectedIDs: []string{"00000000"},
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
				filters.Match("users", "user_2"),
				filters.Match("groups", "group_1"),
			},
			expectedIDs: []string{},
		},
		{
			filters: []Filter{
				filters.Match("groups", "group_1"),
				filters.Comparison("contacts", func(relationshipID string) (ok bool, err error) {
					ok = string(relationshipID) != "contact_1"
					return
				}),
			},
			expectedIDs: []string{"00000002", "00000003"},
		},
		{
			filters: []Filter{
				filters.Match("groups", "group_1"),
				filters.Comparison("contacts", func(relationshipID string) (ok bool, err error) {
					ok = string(relationshipID) != "contact_2"
					return
				}),
			},
			expectedIDs: []string{"00000000", "00000001"},
		},
	}

	for i, tc := range tcs {
		ss := stringset.MakeMap()
		fn := func(key string, v *testStruct) (err error) {
			ss.Set(key)
			return
		}

		var o FilteringOpts
		o.Filters = tc.filters

		if err = c.ForEach(fn, &o); err != nil {
			t.Fatal(err)
		}

		for j, expectedID := range tc.expectedIDs {
			if !ss.Has(expectedID) {
				t.Fatalf("expected ID of %s was not found, testcase #%d and expected ID #%d", expectedID, i, j)
			}
		}
	}
}

func TestMojura_GetFirst_with_multiple_filters(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	user1 := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")
	user2 := makeTestStruct("user_2", "contact_1", "group_1", "bunny bar bar")
	user3 := makeTestStruct("user_3", "contact_2", "group_1", "baz")
	user4 := makeTestStruct("user_4", "contact_2", "group_1", "yep")

	if _, err = c.New(&user1); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&user2); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&user3); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&user4); err != nil {
		t.Fatal(err)
	}

	type testcase struct {
		filters    []Filter
		expectedID string
		err        error
	}

	tcs := []testcase{
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
			},
			expectedID: "00000000",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
			},
			expectedID: "00000002",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
				filters.Match("groups", "group_1"),
			},
			expectedID: "00000000",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
				filters.Match("groups", "group_1"),
			},
			expectedID: "00000002",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
				filters.Match("users", "user_1"),
			},
			expectedID: "00000000",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
				filters.Match("users", "user_2"),
			},
			expectedID: "",
			err:        ErrEntryNotFound,
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
				filters.Match("users", "user_1"),
				filters.Match("groups", "group_1"),
			},
			expectedID: "00000000",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
				filters.Match("users", "user_2"),
				filters.Match("groups", "group_1"),
			},
			expectedID: "",
			err:        ErrEntryNotFound,
		},
		{
			filters: []Filter{
				filters.Match("groups", "group_1"),
				filters.Comparison("contacts", func(relationshipID string) (ok bool, err error) {
					ok = string(relationshipID) != "contact_1"
					return
				}),
			},
			expectedID: "00000002",
		},
		{
			filters: []Filter{
				filters.Match("groups", "group_1"),
				filters.Comparison("contacts", func(relationshipID string) (ok bool, err error) {
					ok = string(relationshipID) != "contact_2"
					return
				}),
			},
			expectedID: "00000000",
		},
	}

	for i, tc := range tcs {
		ss := stringset.MakeMap()
		fn := func(key string, v *testStruct) (err error) {
			ss.Set(key)
			return
		}

		var o FilteringOpts
		o.Filters = tc.filters

		if err = c.ForEach(fn, &o); err != nil {
			t.Fatal(err)
		}

		var match *testStruct
		if match, err = c.GetFirst(&o); err != tc.err {
			t.Fatalf("invalid error, expected <%v> and received <%v> (test #%d)", tc.err, err, i)
		}

		if match.GetID() != tc.expectedID {
			t.Fatalf("invalid ID, expected <%s> and recieved <%s> (test #%d)", tc.expectedID, match.GetID(), i)
		}
	}
}

func TestMojura_GetLast_with_multiple_filters(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	user1 := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")
	user2 := makeTestStruct("user_2", "contact_1", "group_1", "bunny bar bar")
	user3 := makeTestStruct("user_3", "contact_2", "group_1", "baz")
	user4 := makeTestStruct("user_4", "contact_2", "group_1", "yep")

	if _, err = c.New(&user1); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&user2); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&user3); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&user4); err != nil {
		t.Fatal(err)
	}

	type testcase struct {
		filters    []Filter
		expectedID string
		err        error
	}

	tcs := []testcase{
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
			},
			expectedID: "00000001",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
			},
			expectedID: "00000003",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
				filters.Match("groups", "group_1"),
			},
			expectedID: "00000001",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
				filters.Match("groups", "group_1"),
			},
			expectedID: "00000003",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
				filters.Match("users", "user_1"),
			},
			expectedID: "00000000",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
				filters.Match("users", "user_2"),
			},
			expectedID: "",
			err:        ErrEntryNotFound,
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_1"),
				filters.Match("users", "user_1"),
				filters.Match("groups", "group_1"),
			},
			expectedID: "00000000",
		},
		{
			filters: []Filter{
				filters.Match("contacts", "contact_2"),
				filters.Match("users", "user_2"),
				filters.Match("groups", "group_1"),
			},
			expectedID: "",
			err:        ErrEntryNotFound,
		},
		{
			filters: []Filter{
				filters.Match("groups", "group_1"),
				filters.Comparison("contacts", func(relationshipID string) (ok bool, err error) {
					ok = string(relationshipID) != "contact_1"
					return
				}),
			},
			expectedID: "00000003",
		},
		{
			filters: []Filter{
				filters.Match("groups", "group_1"),
				filters.Comparison("contacts", func(relationshipID string) (ok bool, err error) {
					ok = string(relationshipID) != "contact_2"
					return
				}),
			},
			expectedID: "00000001",
		},
	}

	for i, tc := range tcs {
		ss := stringset.MakeMap()
		fn := func(key string, v *testStruct) (err error) {
			ss.Set(key)
			return
		}

		var o FilteringOpts
		o.Filters = tc.filters

		if err = c.ForEach(fn, &o); err != nil {
			t.Fatal(err)
		}

		var match *testStruct
		if match, err = c.GetLast(&o); err != tc.err {
			t.Fatalf("invalid error, expected <%v> and received <%v> (test #%d)", tc.err, err, i)
		}

		if match.GetID() != tc.expectedID {
			t.Fatalf("invalid ID, expected <%s> and recieved <%s> (test #%d)", tc.expectedID, match.GetID(), i)
		}
	}
}

func TestMojura_Cursor(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var cnt int
	if err = c.Cursor(func(cursor Cursor[*testStruct]) (err error) {
		var val *testStruct
		for val, err = cursor.Seek(""); err == nil; val, err = cursor.Next() {
			// We are not checking ID correctness in this test
			foobar.ID = val.ID

			if err = testCheck(&foobar, val); err != nil {
				break
			}

			cnt++
		}

		if err == ErrEndOfEntries {
			err = nil
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	if cnt != 2 {
		t.Fatalf("invalid number of entries, expected %d and received %d", 2, cnt)
	}
}

func TestMojura_Cursor_First(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Cursor(func(cursor Cursor[*testStruct]) (err error) {
		var val *testStruct
		if val, err = cursor.First(); err != nil {
			return
		}

		if val.ID != "00000000" {
			return fmt.Errorf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000000", val.ID)
		}

		foobar.ID = val.ID

		if err = testCheck(&foobar, val); err != nil {
			t.Fatal(err)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_Cursor_Last(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Cursor(func(cursor Cursor[*testStruct]) (err error) {
		var val *testStruct
		if val, err = cursor.Last(); err != nil {
			return
		}

		if val.ID != "00000001" {
			return fmt.Errorf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000001", val.ID)
		}

		foobar.ID = val.ID

		if err = testCheck(&foobar, val); err != nil {
			t.Fatal(err)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_Cursor_Seek(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Cursor(func(cursor Cursor[*testStruct]) (err error) {
		var val *testStruct
		if val, err = cursor.Seek("00000001"); err != nil {
			return
		}

		if val.ID != "00000001" {
			return fmt.Errorf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000001", val.ID)
		}

		foobar.ID = val.ID

		if err = testCheck(&foobar, val); err != nil {
			t.Fatal(err)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_Batch(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var created *testStruct
	if err = c.Batch(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		created, err = txn.New(&foobar)
		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = c.Batch(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		foobar.Value = "foo bar baz"
		_, err = txn.Put(created.ID, &foobar)
		return
	}); err != nil {
		t.Fatal(err)
	}

	var val *testStruct
	if val, err = c.Get(created.ID); err != nil {
		t.Fatal(err)
	}

	if val.Value != "foo bar baz" {
		t.Fatalf("invalid value for Value, expected \"%s\" and received \"%s\"", foobar.Value, val.Value)
	}
}

func TestMojura_index_increment_persist(t *testing.T) {
	testDir := t.TempDir()
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInitInDir(t, testDir); err != nil {
		testTeardown(c, t)
		t.Fatal(err)
	}

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	if err = c.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		_, err = txn.New(&foobar)
		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("error closing Mojura: %v", err)
	}

	if c, err = testInitInDir(t, testDir); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	var created *testStruct
	if err = c.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		created, err = txn.New(&foobar)
		return
	}); err != nil {
		t.Fatal(err)
	}

	if created.ID != "00000001" {
		t.Fatalf("unexpected ID, expected %s and recieved %s", "00000001", created.ID)
	}
}

func TestMojura_Reindex(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(t); err != nil {
		testTeardown(c, t)
		t.Fatal(err)
	}

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	opts := NewFilteringOpts(filters.Match("users", "user_1"))

	var before *testStruct
	if before, err = c.GetFirst(opts); err != nil {
		t.Fatal(err)
	}

	if err = c.Reindex(context.Background()); err != nil {
		t.Fatal(err)
	}

	var after *testStruct
	// Ensure relationship works after reindex
	if after, err = c.GetFirst(opts); err != nil {
		t.Fatal(err)
	}

	if before.ID != after.ID {
		t.Fatalf("invalid ID, expected <%s> and received <%s>", before.ID, after.ID)
	}
}

func BenchmarkMojura_New_2(b *testing.B) {
	benchmarkMojuraNew(b, 2)
}

func BenchmarkMojura_New_4(b *testing.B) {
	benchmarkMojuraNew(b, 4)
}

func BenchmarkMojura_New_8(b *testing.B) {
	benchmarkMojuraNew(b, 8)
}

func BenchmarkMojura_New_16(b *testing.B) {
	benchmarkMojuraNew(b, 16)
}

func BenchmarkMojura_New_32(b *testing.B) {
	benchmarkMojuraNew(b, 32)
}

func BenchmarkMojura_New_64(b *testing.B) {
	benchmarkMojuraNew(b, 64)
}

func BenchmarkMojura_Batch_2(b *testing.B) {
	benchmarkMojuraBatch(b, 2)
}

func BenchmarkMojura_Batch_4(b *testing.B) {
	benchmarkMojuraBatch(b, 4)
}

func BenchmarkMojura_Batch_8(b *testing.B) {
	benchmarkMojuraBatch(b, 8)
}

func BenchmarkMojura_Batch_16(b *testing.B) {
	benchmarkMojuraBatch(b, 16)
}

func BenchmarkMojura_Batch_32(b *testing.B) {
	benchmarkMojuraBatch(b, 32)
}

func BenchmarkMojura_Batch_64(b *testing.B) {
	benchmarkMojuraBatch(b, 64)
}

func benchmarkMojuraNew(b *testing.B, threads int) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(b); err != nil {
		b.Fatal(err)
	}
	defer testTeardown(c, b)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	b.SetParallelism(threads)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err = c.New(&foobar); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.ReportAllocs()
}

func benchmarkMojuraBatch(b *testing.B, threads int) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(b); err != nil {
		b.Fatal(err)
	}
	defer testTeardown(c, b)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	b.SetParallelism(threads)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err = c.Batch(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
				_, err = txn.New(&foobar)
				return
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.ReportAllocs()
}

func ExampleNew() {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	opts := MakeOpts("example", "./data")

	if c, err = New[*testStruct](opts, "users", "contacts", "groups"); err != nil {
		return
	}

	fmt.Printf("Mojura! %v\n", c)
}

func ExampleMojura_New() {
	var ts testStruct
	ts.UserID = "user_1"
	ts.Value = "Foo bar"

	var (
		created *testStruct
		err     error
	)

	if created, err = c.New(&ts); err != nil {
		return
	}

	fmt.Printf("New entry! %+v\n", created)
}

func ExampleMojura_Get() {
	var (
		ts  *testStruct
		err error
	)

	if ts, err = c.Get("00000000"); err != nil {
		return
	}

	fmt.Printf("Retrieved entry! %+v\n", ts)
}

func ExampleMojura_GetFiltered() {
	var (
		tss    []*testStruct
		lastID string
		err    error
	)

	filter := filters.Match("users", "user_1")
	opts := NewFilteringOpts(filter)

	if tss, lastID, err = c.GetFiltered(opts); err != nil {
		return
	}

	fmt.Printf("Retrieved entries! %+v with a lastID of <%s>\n", tss, lastID)
}

func ExampleMojura_ForEach() {
	var err error
	opts := NewFilteringOpts()
	if err = c.ForEach(func(entryID string, val *testStruct) (err error) {
		fmt.Printf("Iterating entry (%s)! %+v\n", entryID, val)
		return
	}, opts); err != nil {
		return
	}
}

func ExampleMojura_ForEach_with_filter() {
	var err error
	filter := filters.Match("users", "user_1")
	opts := NewFilteringOpts(filter)
	if err = c.ForEach(func(entryID string, val *testStruct) (err error) {
		fmt.Printf("Iterating entry (%s)! %+v\n", entryID, val)
		return
	}, opts); err != nil {
		return
	}
}

func ExampleMojura_Update() {
	var err error
	var updated *testStruct
	if updated, err = c.Update("00000000", func(ts *testStruct) (err error) {
		// Let's update the Value field to "New foo value"
		ts.Value = "New foo value"
		return
	}); err != nil {
		return
	}

	fmt.Printf("Edited entry %+v!\n", updated)
}

func ExampleMojura_Put() {
	var (
		ts  testStruct
		err error
	)

	// We will pretend the test struct is already populated

	// Let's update the Value field to "New foo value"
	ts.Value = "New foo value"

	var updated *testStruct
	if updated, err = c.Put("00000000", &ts); err != nil {
		return
	}

	fmt.Printf("Updated entry %+v!\n", updated)
}

func ExampleMojura_Delete() {
	var (
		removed *testStruct
		err     error
	)

	if removed, err = c.Delete("00000000"); err != nil {
		return
	}

	fmt.Printf("Removed entry %+v!\n", removed)
}

func testInit(t testing.TB) (*Mojura[*testStruct], error) {
	t.Helper()
	return testInitInDir(t, t.TempDir())
}

func testInitInDir(t testing.TB, testDir string) (c *Mojura[*testStruct], err error) {
	t.Helper()
	if err = os.MkdirAll(testDir, 0744); err != nil {
		return
	}

	opts := MakeOpts("test", testDir)
	if opts.Source, err = kiroku.NewIOSource(testDir); err != nil {
		return
	}

	c, err = New[*testStruct](opts, "users", "contacts", "groups", "tags")
	if err == nil {
		t.Cleanup(func() {
			if err := c.Close(); err != nil && !stderrors.Is(err, errors.ErrIsClosed) {
				t.Error(err)
			}
		})
	}
	return
}

func testTeardown(c *Mojura[*testStruct], t testing.TB) {
	t.Helper()
	if c != nil {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func testCheck(a, b *testStruct) (err error) {
	if a.ID != b.ID {
		return fmt.Errorf("invalid id, expected %s and received %s", a.ID, b.ID)
	}

	if a.UserID != b.UserID {
		return fmt.Errorf("invalid user id, expected %s and received %s", a.UserID, b.UserID)
	}

	if a.ContactID != b.ContactID {
		return fmt.Errorf("invalid contact id, expected %s and received %s", a.ContactID, b.ContactID)
	}

	if a.Value != b.Value {
		return fmt.Errorf("invalid Value, expected %s and received %s", a.Value, b.Value)
	}

	return
}

func newTestStruct(userID, contactID, groupID, value string, tags ...string) *testStruct {
	t := makeTestStruct(userID, contactID, groupID, value, tags...)
	return &t
}

func makeTestStruct(userID, contactID, groupID, value string, tags ...string) (t testStruct) {
	t.UserID = userID
	t.ContactID = contactID
	t.GroupID = groupID
	t.Value = value
	t.Tags = tags
	return
}

type testStruct struct {
	Entry

	UserID    string   `json:"userID"`
	ContactID string   `json:"contactID"`
	GroupID   string   `json:"groupID"`
	Tags      []string `json:"tags"`

	Value string `json:"value"`
}

func (t *testStruct) GetID() (id string) {
	if t == nil {
		return
	}

	return t.ID
}

func (t *testStruct) GetRelationships() (r Relationships) {
	r.Append(t.UserID)
	r.Append(t.ContactID)
	r.Append(t.GroupID)
	r.Append(t.Tags...)
	return
}

func (t *testStruct) compare(v *testStruct) (err error) {
	var errs errors.ErrorList
	if v.UserID != t.UserID {
		err = fmt.Errorf("invalid user ID, expected <%s> and received <%s>", t.UserID, v.UserID)
		errs.Push(err)
	}

	if v.ContactID != t.ContactID {
		err = fmt.Errorf("invalid contact ID, expected <%s> and received <%s>", t.ContactID, v.ContactID)
		errs.Push(err)
	}

	if v.GroupID != t.GroupID {
		err = fmt.Errorf("invalid group ID, expected <%s> and received <%s>", t.GroupID, v.UserID)
		errs.Push(err)
	}

	return errs.Err()
}
