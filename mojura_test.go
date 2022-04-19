package mojura

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/gdbu/stringset"
	"github.com/hatchify/errors"
	"github.com/mojura/mojura/filters"
)

const (
	testDir = "./test_data"
)

var c *Mojura[*testStruct]

func TestNew(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(); err != nil {
		testTeardown(c, t)
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	if err = c.Close(); err != nil {
		return
	}
}

func TestMojura_New(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if len(entryID) == 0 {
		t.Fatal("invalid entry id, expected non-empty value")
	}
}

func TestMojura_New_with_database_build(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

	filename := path.Join(testDir, "test.bdb")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	if c, err = testInit(); err != nil {
		t.Fatalf("error initializing: %v", err)
	}
	defer testTeardown(c, t)

	var e *testStruct
	if e, err = c.Get(entryID); err != nil {
		t.Fatalf("error getting: %v", err)
	}

	if err = foobar.compare(e); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_New_with_history_build(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

	filename := path.Join(testDir, "test.moj")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	if c, err = testInit(); err != nil {
		t.Fatalf("error initializing: %v", err)
	}
	defer testTeardown(c, t)

	var e *testStruct
	if e, err = c.Get(entryID); err != nil {
		t.Fatalf("error getting: %v", err)
	}

	if err = foobar.compare(e); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_New_with_history_and_database_build(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

	filename := path.Join(testDir, "test.moj")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	if c, err = testInit(); err != nil {
		t.Fatalf("error initializing: %v", err)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

	filename = path.Join(testDir, "test.bdb")
	if err = os.Remove(filename); err != nil {
		t.Fatal(err)
	}

	if c, err = testInit(); err != nil {
		t.Fatalf("error initializing: %v", err)
	}
	defer testTeardown(c, t)

	var e *testStruct
	if e, err = c.Get(entryID); err != nil {
		t.Fatalf("error getting: %v", err)
	}

	if err = foobar.compare(e); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_Get(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var fb *testStruct
	if fb, err = c.Get(entryID); err != nil {
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

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
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
				if _, err = txn.Get(entryID); err != nil {
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

	if c, err = testInit(); err != nil {
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

	for _, entry := range entries {
		if entry.ID, err = c.New(entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(createCases); err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		entry.Tags = []string{"boom"}
		if err = c.Edit(entry.ID, entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(updateCases); err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		if err = c.Remove(entry.ID); err != nil {
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

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	entries := []*testStruct{
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "foo", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "baz"),
	}

	for _, entry := range entries {
		if entry.ID, err = c.New(entry); err != nil {
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

	filtered = filtered[:0]

	if filtered, o.LastID, err = c.GetFiltered(&o); err != nil {
		t.Fatal(err)
	}

	target = filtered[0]
	if target.ID != entries[1].ID {
		t.Fatalf("invalid ID, expected <%s> and received <%s>", entries[0].ID, target.ID)
	}

	filtered = filtered[:0]
	if filtered, o.LastID, err = c.GetFiltered(&o); err != nil {
		t.Fatal(err)
	}

	target = filtered[0]

	if target.ID != entries[2].ID {
		t.Fatalf("invalid ID, expected <%s> and received <%s>", entries[0].ID, target.ID)
	}

	filtered = filtered[:0]
	if filtered, o.LastID, err = c.GetFiltered(&o); err != nil {
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

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	entries := []*testStruct{
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "foo", "bar", "baz"),
		newTestStruct("user_1", "contact_1", "group_2", "FOO FOO", "bar"),
		newTestStruct("user_1", "contact_1", "group_1", "FOO FOO", "baz"),
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
		if entry.ID, err = c.New(entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = runCases(createCases); err != nil {
		t.Fatal(err)
	}
}

func TestMojura_Edit(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	foobar.Value = "FOO FOO"

	if err = c.Edit(entryID, &foobar); err != nil {
		t.Fatal(err)
	}

	var fb *testStruct
	if fb, err = c.Get(entryID); err != nil {
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

	if c, err = testInit(); err != nil {
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

	if c, err = testInit(); err != nil {
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

	var o IteratingOpts
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

	if c, err = testInit(); err != nil {
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
		ss := stringset.New()
		fn := func(key string, v *testStruct) (err error) {
			ss.Set(key)
			return
		}

		var o IteratingOpts
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

	if c, err = testInit(); err != nil {
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
		ss := stringset.New()
		fn := func(key string, v *testStruct) (err error) {
			ss.Set(key)
			return
		}

		var o IteratingOpts
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

	if c, err = testInit(); err != nil {
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
		ss := stringset.New()
		fn := func(key string, v *testStruct) (err error) {
			ss.Set(key)
			return
		}

		var o IteratingOpts
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

	if c, err = testInit(); err != nil {
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
		var val Value
		for val, err = cursor.Seek(""); err == nil; val, err = cursor.Next() {
			fb := val.(*testStruct)

			// We are not checking ID correctness in this test
			foobar.ID = fb.ID

			if err = testCheck(&foobar, fb); err != nil {
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

	if c, err = testInit(); err != nil {
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
		var val Value
		if val, err = cursor.First(); err != nil {
			return
		}

		fb := val.(*testStruct)

		if fb.ID != "00000000" {
			return fmt.Errorf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000000", fb.ID)
		}

		foobar.ID = fb.ID

		if err = testCheck(&foobar, fb); err != nil {
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

	if c, err = testInit(); err != nil {
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
		var val Value
		if val, err = cursor.Last(); err != nil {
			return
		}

		fb := val.(*testStruct)

		if fb.ID != "00000001" {
			return fmt.Errorf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000001", fb.ID)
		}

		foobar.ID = fb.ID

		if err = testCheck(&foobar, fb); err != nil {
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

	if c, err = testInit(); err != nil {
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
		var val Value
		if val, err = cursor.Seek("00000001"); err != nil {
			return
		}

		fb := val.(*testStruct)

		if fb.ID != "00000001" {
			return fmt.Errorf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000001", fb.ID)
		}

		foobar.ID = fb.ID

		if err = testCheck(&foobar, fb); err != nil {
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

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	var entryID string
	if err = c.Batch(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		entryID, err = txn.New(&foobar)
		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = c.Batch(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		foobar.Value = "foo bar baz"
		err = txn.Edit(entryID, &foobar)
		return
	}); err != nil {
		t.Fatal(err)
	}

	var val *testStruct
	if val, err = c.Get(entryID); err != nil {
		t.Fatal(err)
	}

	if val.Value != "foo bar baz" {
		t.Fatalf("invalid value for Value, expected \"%s\" and received \"%s\"", foobar.Value, val.Value)
	}
}

func TestMojura_index_increment_persist(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(); err != nil {
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

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c, t)

	var entryID string
	if err = c.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		entryID, err = txn.New(&foobar)
		return
	}); err != nil {
		t.Fatal(err)
	}

	if entryID != "00000001" {
		t.Fatalf("unexpected ID, expected %s and recieved %s", "00000001", entryID)
	}
}

func TestMojura_Reindex(t *testing.T) {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	if c, err = testInit(); err != nil {
		testTeardown(c, t)
		t.Fatal(err)
	}

	foobar := makeTestStruct("user_1", "contact_1", "group_1", "FOO FOO")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	opts := NewIteratingOpts(filters.Match("users", "user_1"))

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

	if c, err = testInit(); err != nil {
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

	if c, err = testInit(); err != nil {
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

	if c, err = New(opts, newEmptyTestStruct, "users", "contacts", "groups"); err != nil {
		return
	}

	fmt.Printf("Mojura! %v\n", c)
}

func ExampleMojura_New() {
	var ts testStruct
	ts.UserID = "user_1"
	ts.Value = "Foo bar"

	var (
		entryID string
		err     error
	)

	if entryID, err = c.New(&ts); err != nil {
		return
	}

	fmt.Printf("New entry! %s\n", entryID)
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
	filter := filters.Match("users", "user_1")
	opts := NewIteratingOpts(filter)
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
	opts := NewIteratingOpts(filter)
	if err = c.ForEach(func(entryID string, val *testStruct) (err error) {
		fmt.Printf("Iterating entry (%s)! %+v\n", entryID, val)
		return
	}, opts); err != nil {
		return
	}
}

func ExampleMojura_Edit() {
	var (
		ts  *testStruct
		err error
	)

	// We will pretend the test struct is already populated

	// Let's update the Value field to "New foo value"
	ts.Value = "New foo value"

	if err = c.Edit("00000000", ts); err != nil {
		return
	}

	fmt.Printf("Edited entry %s!\n", "00000000")
}

func ExampleMojura_Remove() {
	var err error
	if err = c.Remove("00000000"); err != nil {
		return
	}

	fmt.Printf("Removed entry %s!\n", "00000000")
}

func testInit() (c *Mojura[*testStruct], err error) {
	if err = os.MkdirAll(testDir, 0744); err != nil {
		return
	}

	opts := MakeOpts("test", testDir)

	return New(opts, newEmptyTestStruct, "users", "contacts", "groups", "tags")
}

func testTeardown(c *Mojura[*testStruct], t interface{ Fatal(...interface{}) }) {
	var errs errors.ErrorList
	if c != nil {
		errs.Push(c.Close())
	}

	errs.Push(os.RemoveAll(testDir))

	if err := errs.Err(); err != nil {
		t.Fatal(err)
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

func newEmptyTestStruct() *testStruct {
	var t testStruct
	return &t
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
