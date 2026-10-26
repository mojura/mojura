package mojura

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/mojura/kiroku"
	"github.com/mojura/mojura/filters"
)

// openTempMojura opens a Mojura in a directory of tb's own. Its Close is
// registered after the directory, so it runs first, and its error fails tb.
func openTempMojura(tb testing.TB) *Mojura[*testStruct] {
	tb.Helper()
	dir := tb.TempDir()
	opts := MakeOpts("test", dir)
	source, err := kiroku.NewIOSource(dir)
	if err != nil {
		tb.Fatal(err)
	}

	opts.Source = source
	c, err := New[*testStruct](opts, "users", "contacts", "groups", "tags")
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := c.Close(); err != nil {
			tb.Error(err)
		}
	})
	return c
}

// seedTagged writes n entries; tags(i) names the tags entry i holds.
func seedTagged(tb testing.TB, c *Mojura[*testStruct], n int, tags func(i int) []string) {
	tb.Helper()
	if err := c.Transaction(context.Background(), func(txn *Transaction[*testStruct]) error {
		for i := 0; i < n; i++ {
			if _, err := txn.New(newTestStruct("user_1", "contact_1", "group_1", "value", tags(i)...)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
}

func tag(names ...string) []Filter {
	fs := make([]Filter, len(names))
	for i, name := range names {
		fs[i] = filters.Match("tags", name)
	}
	return fs
}

// multiFilterIDs is the multi-filter cursor's walk of fs, the oracle an
// intersection must equal.
func multiFilterIDs(txn *Transaction[*testStruct], fs []Filter) ([]string, error) {
	cursor, err := txn.IDCursor(fs...)
	if err != nil {
		return nil, err
	}
	var ids []string
	id, err := cursor.First()
	for err == nil {
		ids = append(ids, id)
		id, err = cursor.Next()
	}
	if err != Break {
		return nil, err
	}
	return ids, nil
}

func intersectedIDs(txn *Transaction[*testStruct], fs []Filter) ([]string, error) {
	var ids []string
	err := txn.ForEachIntersectedID(func(id string) error {
		ids = append(ids, id)
		return nil
	}, fs...)
	return ids, err
}

func TestAnIntersectionValidatesMatchFilters(t *testing.T) {
	var nilMatch *filters.MatchFilter
	for _, test := range []struct {
		name       string
		filters    []Filter
		wantError  error
		wantVisits int
	}{
		{"nil interface", []Filter{nil}, ErrIntersectionNeedsMatchFilters, 0},
		{"typed nil", []Filter{nilMatch}, ErrIntersectionNeedsMatchFilters, 0},
		{"typed nil after valid", []Filter{filters.Match("tags", "a"), nilMatch}, ErrIntersectionNeedsMatchFilters, 0},
		{"valid match", []Filter{filters.Match("tags", "a")}, nil, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := openTempMojura(t)
			seedTagged(t, c, 1, func(int) []string { return []string{"a"} })
			var visits int
			err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) (result error) {
				defer func() {
					if recovered := recover(); recovered != nil {
						result = fmt.Errorf("intersection panicked instead of returning a filter error: %v", recovered)
					}
				}()

				return txn.ForEachIntersectedID(func(string) error {
					visits++
					return nil
				}, test.filters...)
			})
			if !errors.Is(err, test.wantError) || visits != test.wantVisits {
				t.Fatalf("intersection returned %v after %d visits; want %v after %d", err, visits, test.wantError, test.wantVisits)
			}
		})
	}
}

// An intersection of Match filters yields exactly the IDs the multi-filter
// cursor yields, in ascending order, whatever the sets' shapes; anything but
// one or more Match filters is refused (INT-MOJURA-TICKET-5, AC-1).
func TestAnIntersectionYieldsTheMultiFilterCursorsIDs(t *testing.T) {
	c := openTempMojura(t)
	seedTagged(t, c, 600, func(i int) (tags []string) {
		if i%2 == 0 {
			tags = append(tags, "even")
		} else {
			tags = append(tags, "odd")
		}
		if i%3 == 0 {
			tags = append(tags, "third")
		}
		if i%5 == 0 {
			tags = append(tags, "fifth")
		}
		if i == 7 || i == 300 || i == 599 {
			tags = append(tags, "rare")
		}
		return
	})
	for _, tc := range []struct {
		name string
		fs   []Filter
		none bool
	}{
		{name: "one Match", fs: tag("third")},
		{name: "an absent relationship", fs: tag("even", "nowhere"), none: true},
		{name: "overlapping", fs: tag("even", "third")},
		{name: "interleaved and disjoint", fs: tag("even", "odd"), none: true},
		{name: "partial overlap, two, the shortest last", fs: tag("third", "rare")},
		{name: "partial overlap, three, the shortest last", fs: tag("even", "fifth", "rare")},
		{name: "three overlapping", fs: tag("even", "third", "fifth")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
				want, err := multiFilterIDs(txn, tc.fs)
				if err != nil {
					return err
				}
				got, err := intersectedIDs(txn, tc.fs)
				if err != nil {
					return err
				}
				if !slices.Equal(got, want) || (len(got) == 0) != tc.none || !slices.IsSorted(got) {
					return fmt.Errorf("the intersection yielded %d ids %v, the multi-filter cursor %d %v", len(got), got, len(want), want)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
	for name, fs := range map[string][]Filter{
		"no filters":      nil,
		"a Comparison":    {filters.Match("tags", "even"), filters.Comparison("tags", func(string) (bool, error) { return true, nil })},
		"an InverseMatch": {filters.Match("tags", "even"), filters.InverseMatch("tags", "odd")},
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
				_, err := intersectedIDs(txn, fs)
				return err
			})
			if !errors.Is(err, ErrIntersectionNeedsMatchFilters) {
				t.Fatalf("an intersection of %s returned %v, want ErrIntersectionNeedsMatchFilters", name, err)
			}
		})
	}
}

func TestAnIntersectionYieldsAContainedSet(t *testing.T) {
	c := openTempMojura(t)
	err := c.Transaction(context.Background(), func(txn *Transaction[*testStruct]) error {
		for _, id := range []string{"a", "b", "c", "d"} {
			tags := []string{"outer"}
			if id == "b" || id == "c" {
				tags = append(tags, "inner")
			}

			if _, putErr := txn.Put(id, newTestStruct("user", "contact", "group", "value", tags...)); putErr != nil {
				return putErr
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, names := range [][]string{{"outer", "inner"}, {"inner", "outer"}} {
		t.Run(strings.Join(names, "+"), func(t *testing.T) {
			readErr := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
				ids, intersectionErr := intersectedIDs(txn, tag(names...))
				if intersectionErr != nil {
					return intersectionErr
				}

				if !slices.Equal(ids, []string{"b", "c"}) {
					return fmt.Errorf("contained intersection yielded %v; want [b c]", ids)
				}

				return nil
			})
			if readErr != nil {
				t.Fatal(readErr)
			}
		})
	}
}

// A visitor's Break ends the walk with nil, and the transaction serves reads
// after it (AC-2).
func TestAnIntersectionEndsOnItsVisitorsBreak(t *testing.T) {
	c := openTempMojura(t)
	seedTagged(t, c, 50, func(int) []string { return []string{"a", "b"} })
	if err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
		var ids []string
		if err := txn.ForEachIntersectedID(func(id string) error {
			ids = append(ids, id)
			return Break
		}, tag("a", "b")...); err != nil {
			return fmt.Errorf("a walk its visitor broke returned %v, want nil", err)
		}
		if len(ids) != 1 {
			return fmt.Errorf("a walk its visitor broke at the first id yielded %d ids", len(ids))
		}
		if _, err := txn.Get(ids[0]); err != nil {
			return fmt.Errorf("the transaction did not serve a read after the walk: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// A visitor's error other than Break is returned as is (AC-3).
func TestAnIntersectionReturnsItsVisitorsError(t *testing.T) {
	c := openTempMojura(t)
	seedTagged(t, c, 50, func(int) []string { return []string{"a", "b"} })
	visitor := errors.New("the visitor failed")
	err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
		return txn.ForEachIntersectedID(func(string) error { return visitor }, tag("a", "b")...)
	})
	if err != visitor {
		t.Fatalf("a walk whose visitor failed returned %v, want the visitor's error as is", err)
	}
}

// A context cancelled between yielded ids ends the walk with
// context.Canceled, and no id after it is yielded (AC-4).
func TestAnIntersectionEndsWithItsContext(t *testing.T) {
	c := openTempMojura(t)
	seedTagged(t, c, 50, func(int) []string { return []string{"a", "b"} })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	yielded := 0
	err := c.ReadTransaction(ctx, func(txn *Transaction[*testStruct]) error {
		return txn.ForEachIntersectedID(func(string) error {
			yielded++
			cancel()
			return nil
		}, tag("a", "b")...)
	})
	if !errors.Is(err, context.Canceled) || yielded != 1 {
		t.Fatalf("a walk cancelled at its first id returned %v after %d ids, want context.Canceled after 1", err, yielded)
	}
}

// On interleaved, disjoint sets an intersection yields nothing and allocates
// the same at 1,000 and at 10,000 ids each: it allocates for the ids it
// yields, not for the ids it passes (AC-5).
func TestAnIntersectionAllocatesOnlyForTheIDsItYields(t *testing.T) {
	c := openTempMojura(t)
	seedTagged(t, c, 20000, func(i int) (tags []string) {
		if i%2 == 0 {
			tags = append(tags, "even-large")
		} else {
			tags = append(tags, "odd-large")
		}
		if i < 2000 {
			if i%2 == 0 {
				tags = append(tags, "even-small")
			} else {
				tags = append(tags, "odd-small")
			}
		}
		return
	})
	allocs := func(fs []Filter) (n float64) {
		if err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
			n = testing.AllocsPerRun(5, func() {
				if err := txn.ForEachIntersectedID(func(string) error { return nil }, fs...); err != nil {
					t.Error(err)
				}
			})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	small, large := allocs(tag("even-small", "odd-small")), allocs(tag("even-large", "odd-large"))
	if small != large {
		t.Fatalf("a disjoint walk allocated %v times over 1,000 ids each and %v over 10,000", small, large)
	}
}

// movesCursor counts an intersection cursor's steps and seeks.
type movesCursor struct {
	filterCursor
	steps, seeks *int
}

func (c movesCursor) Next() ([]byte, error) {
	*c.steps++
	return c.filterCursor.Next()
}

func (c movesCursor) SeekForward(key, id []byte) ([]byte, error) {
	*c.seeks++
	return c.filterCursor.SeekForward(key, id)
}

// A lagging cursor steps once before it seeks: on interleaved sets a step
// reaches the target and no seek is made, and on a selective pair the large
// set is crossed by seeks, a few per ID of the small one (AC-6's cost model).
// The walk's own cursors are counted, handed to it directly.
func TestAnIntersectionStepsBeforeItSeeks(t *testing.T) {
	c := openTempMojura(t)
	seedTagged(t, c, 4000, func(i int) []string {
		tags := []string{"odd"}
		if i%2 == 0 {
			tags = []string{"even"}
		}

		if i%1000 == 7 {
			tags = append(tags, "rare")
		}

		return tags
	})
	walk := func(names ...string) (steps, seeks int) {
		if err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
			cursors := make([]filterCursor, len(names))
			for i, name := range names {
				cursor, err := newMatchCursor(txn, filters.Match("tags", name))
				if err != nil {
					return err
				}

				cursors[i] = movesCursor{cursor, &steps, &seeks}
			}

			return walkIntersection(cursors, func(string) error { return nil })
		}); err != nil {
			t.Fatal(err)
		}

		return steps, seeks
	}
	if steps, seeks := walk("even", "odd"); seeks != 0 || steps > 4000 {
		t.Fatalf("interleaved sets of 2,000 each took %d steps and %d seeks, want no seek and at most 4,000 steps", steps, seeks)
	}

	if steps, seeks := walk("odd", "rare"); seeks == 0 || steps+seeks > 4*4+4 {
		t.Fatalf("a selective pair, 2,000 against 4, took %d steps and %d seeks, want a few seeks per rare ID", steps, seeks)
	}
}

// variableLengthID is entry i's ID: one byte longer every 50 entries, and in
// i's order.
func variableLengthID(i int) string {
	return strings.Repeat("a", i/50) + fmt.Sprintf("%06d", i)
}

// On interleaved, disjoint sets whose IDs grow longer along the walk, an
// intersection visits nothing, and beyond its cursors' own setup it
// allocates the same at 1,000 and at 10,000 IDs a set: an ID it passes never
// grows what the walk holds. Setup is measured apart because a cursor's first
// key allocates with the bucket's depth, which grows with the set (codex, F1
// of round 2 of INT-MOJURA-TICKET-5).
func TestAVariableLengthWalkAllocatesOnlyForTheIDsItYields(t *testing.T) {
	walkAllocs := func(n int) float64 {
		c := openTempMojura(t)
		if err := c.Transaction(context.Background(), func(txn *Transaction[*testStruct]) error {
			for i := 0; i < 2*n; i++ {
				parity := "even"
				if i%2 == 1 {
					parity = "odd"
				}

				if _, err := txn.Put(variableLengthID(i), newTestStruct("user_1", "contact_1", "group_1", "value", parity)); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		var (
			visits      int
			walk, setup float64
		)
		if err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
			walk = testing.AllocsPerRun(5, func() {
				if err := txn.ForEachIntersectedID(func(string) error { visits++; return nil }, tag("even", "odd")...); err != nil {
					t.Error(err)
				}
			})
			setup = testing.AllocsPerRun(5, func() {
				for _, name := range []string{"even", "odd"} {
					cursor, err := newMatchCursor(txn, filters.Match("tags", name))
					if err != nil {
						t.Error(err)
						return
					}

					if _, err := cursor.First(); err != nil {
						t.Error(err)
					}
				}
			})
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if visits != 0 {
			t.Fatalf("a disjoint walk over %d IDs a set visited %d IDs", n, visits)
		}

		return walk - setup
	}
	small, large := walkAllocs(1000), walkAllocs(10000)
	if small != large {
		t.Fatalf("beyond its cursors' setup, a disjoint walk of growing IDs allocated %v times over 1,000 IDs a set and %v over 10,000", small, large)
	}
}
