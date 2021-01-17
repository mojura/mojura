package mojura

import (
	"context"
	"fmt"
	"testing"

	"github.com/mojura/mojura/filters"
)

func Test_matchCursor_SeekForward(t *testing.T) {
	var (
		m   *Mojura
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		relationshipKey string
		relationshipID  string
		seekID          string

		expected expected
	}

	a := newTestStruct("user_0", "contact_0", "group_3", "1")
	b := newTestStruct("user_1", "contact_2", "group_2", "2")
	c := newTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			relationshipKey: "users",
			relationshipID:  "user_0",
			seekID:          "00000000",
			expected:        expected{expectedID: "00000000"},
		},
		{
			relationshipKey: "contacts",
			relationshipID:  "contact_2",
			seekID:          "00000001",
			expected:        expected{expectedID: "00000001"},
		},
		{
			relationshipKey: "groups",
			relationshipID:  "group_1",
			seekID:          "00000002",
			expected:        expected{expectedID: "00000002"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		if _, err = txn.New(a); err != nil {
			return
		}

		if _, err = txn.New(b); err != nil {
			return
		}

		if _, err = txn.New(c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur filterCursor
			f := filters.Match(tc.relationshipKey, tc.relationshipID)
			if cur, err = newMatchCursor(txn, f); err != nil {
				return
			}

			exp := tc.expected

			var idBytes []byte
			if idBytes, err = cur.SeekForward([]byte(tc.relationshipID), []byte(tc.seekID)); err != exp.expectedErr {
				err = fmt.Errorf("invalid error, expected <%v> and received <%v> (test case #%d)", exp.expectedErr, err, i)
				return
			}

			if id := string(idBytes); id != exp.expectedID {
				err = fmt.Errorf("invalid ID, expected <%s> and received <%s> (test case #%d)", exp.expectedID, id, i)
				return
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func Test_matchCursor_SeekReverse(t *testing.T) {
	var (
		m   *Mojura
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		relationshipKey string
		relationshipID  string
		seekID          string

		expected expected
	}

	a := newTestStruct("user_0", "contact_0", "group_3", "1")
	b := newTestStruct("user_1", "contact_2", "group_2", "2")
	c := newTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			relationshipKey: "users",
			relationshipID:  "user_2",
			seekID:          "00000002",
			expected:        expected{expectedID: "00000002"},
		},
		{
			relationshipKey: "contacts",
			relationshipID:  "contact_2",
			seekID:          "00000001",
			expected:        expected{expectedID: "00000001"},
		},
		{
			relationshipKey: "groups",
			relationshipID:  "group_1",
			seekID:          "00000002",
			expected:        expected{expectedID: "00000002"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		if _, err = txn.New(a); err != nil {
			return
		}

		if _, err = txn.New(b); err != nil {
			return
		}

		if _, err = txn.New(c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur filterCursor
			f := filters.Match(tc.relationshipKey, tc.relationshipID)
			if cur, err = newMatchCursor(txn, f); err != nil {
				return
			}

			exp := tc.expected

			var idBytes []byte
			if idBytes, err = cur.SeekReverse([]byte(tc.relationshipID), []byte(tc.seekID)); err != exp.expectedErr {
				err = fmt.Errorf("invalid error, expected <%v> and received <%v> (test case #%d)", exp.expectedErr, err, i)
				return
			}

			if id := string(idBytes); id != exp.expectedID {
				err = fmt.Errorf("invalid ID, expected <%s> and received <%s> (test case #%d)", exp.expectedID, id, i)
				return
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func Test_matchCursor_First(t *testing.T) {
	var (
		m   *Mojura
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		relationshipKey string
		relationshipID  string
		expected        expected
	}

	a := newTestStruct("user_0", "contact_0", "group_3", "1")
	b := newTestStruct("user_1", "contact_2", "group_2", "2")
	c := newTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			relationshipKey: "users",
			relationshipID:  "user_1",
			expected:        expected{expectedID: "00000001"},
		},
		{
			relationshipKey: "contacts",
			relationshipID:  "contact_2",
			expected:        expected{expectedID: "00000001"},
		},
		{
			relationshipKey: "groups",
			relationshipID:  "group_1",
			expected:        expected{expectedID: "00000002"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		if _, err = txn.New(a); err != nil {
			return
		}

		if _, err = txn.New(b); err != nil {
			return
		}

		if _, err = txn.New(c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur filterCursor
			f := filters.Match(tc.relationshipKey, tc.relationshipID)
			if cur, err = newMatchCursor(txn, f); err != nil {
				return
			}

			exp := tc.expected

			var idBytes []byte
			if idBytes, err = cur.First(); err != exp.expectedErr {
				err = fmt.Errorf("invalid error, expected <%v> and received <%v> (test case #%d)", exp.expectedErr, err, i)
				return
			}

			if id := string(idBytes); id != exp.expectedID {
				err = fmt.Errorf("invalid ID, expected <%s> and received <%s> (test case #%d)", exp.expectedID, id, i)
				return
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func Test_matchCursor_Next(t *testing.T) {
	var (
		m   *Mojura
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		relationshipKey string
		relationshipID  string
		expected        []expected
	}

	a := newTestStruct("user_0", "contact_0", "group_3", "1")
	b := newTestStruct("user_1", "contact_2", "group_2", "2")
	c := newTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			relationshipKey: "users",
			relationshipID:  "user_2",
			expected: []expected{
				{expectedID: "00000002"},
				{expectedErr: Break},
			},
		},
		{
			relationshipKey: "contacts",
			relationshipID:  "contact_2",
			expected: []expected{
				{expectedID: "00000001"},
				{expectedID: "00000002"},
				{expectedErr: Break},
			},
		},
		{
			relationshipKey: "groups",
			relationshipID:  "group_2",
			expected: []expected{
				{expectedID: "00000001"},
				{expectedErr: Break},
			},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		if _, err = txn.New(a); err != nil {
			return
		}

		if _, err = txn.New(b); err != nil {
			return
		}

		if _, err = txn.New(c); err != nil {
			return
		}

		for _, tc := range tcs {
			var cur filterCursor
			f := filters.Match(tc.relationshipKey, tc.relationshipID)
			if cur, err = newMatchCursor(txn, f); err != nil {
				return
			}

			for i, exp := range tc.expected {
				fn := cur.Next
				if i == 0 {
					fn = cur.First
				}

				var idBytes []byte
				if idBytes, err = fn(); err != exp.expectedErr {
					err = fmt.Errorf("invalid error, expected <%v> and received <%v> (test case #%d)", exp.expectedErr, err, i)
					return
				}

				if id := string(idBytes); id != exp.expectedID {
					err = fmt.Errorf("invalid ID, expected <%s> and received <%s> (test case #%d)", exp.expectedID, id, i)
					return
				}
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func Test_matchCursor_Prev(t *testing.T) {
	var (
		m   *Mojura
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		relationshipKey string
		relationshipID  string
		expected        []expected
	}

	a := newTestStruct("user_0", "contact_0", "group_3", "1")
	b := newTestStruct("user_1", "contact_2", "group_2", "2")
	c := newTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			relationshipKey: "users",
			relationshipID:  "user_0",
			expected: []expected{
				{expectedID: "00000000"},
				{expectedErr: Break},
			},
		},
		{
			relationshipKey: "contacts",
			relationshipID:  "contact_2",
			expected: []expected{
				{expectedID: "00000002"},
				{expectedID: "00000001"},
				{expectedErr: Break},
			},
		},
		{
			relationshipKey: "groups",
			relationshipID:  "group_2",
			expected: []expected{
				{expectedID: "00000001"},
				{expectedErr: Break},
			},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		if _, err = txn.New(a); err != nil {
			return
		}

		if _, err = txn.New(b); err != nil {
			return
		}

		if _, err = txn.New(c); err != nil {
			return
		}

		for _, tc := range tcs {
			var cur filterCursor
			f := filters.Match(tc.relationshipKey, tc.relationshipID)
			if cur, err = newMatchCursor(txn, f); err != nil {
				return
			}

			for i, exp := range tc.expected {
				fn := cur.Prev
				if i == 0 {
					fn = cur.Last
				}

				var idBytes []byte
				if idBytes, err = fn(); err != exp.expectedErr {
					err = fmt.Errorf("invalid error, expected <%v> and received <%v> (test case #%d)", exp.expectedErr, err, i)
					return
				}

				if id := string(idBytes); id != exp.expectedID {
					err = fmt.Errorf("invalid ID, expected <%s> and received <%s> (test case #%d)", exp.expectedID, id, i)
					return
				}
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func Test_matchCursor_Last(t *testing.T) {
	var (
		m   *Mojura
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		relationshipKey string
		relationshipID  string
		expected        expected
	}

	a := newTestStruct("user_0", "contact_0", "group_3", "1")
	b := newTestStruct("user_1", "contact_2", "group_2", "2")
	c := newTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			relationshipKey: "users",
			relationshipID:  "user_0",
			expected:        expected{expectedID: "00000000"},
		},
		{
			relationshipKey: "contacts",
			relationshipID:  "contact_2",
			expected:        expected{expectedID: "00000002"},
		},
		{
			relationshipKey: "groups",
			relationshipID:  "group_2",
			expected:        expected{expectedID: "00000001"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		if _, err = txn.New(a); err != nil {
			return
		}

		if _, err = txn.New(b); err != nil {
			return
		}

		if _, err = txn.New(c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur filterCursor
			f := filters.Match(tc.relationshipKey, tc.relationshipID)
			if cur, err = newMatchCursor(txn, f); err != nil {
				return
			}

			exp := tc.expected

			var idBytes []byte
			if idBytes, err = cur.Last(); err != exp.expectedErr {
				err = fmt.Errorf("invalid error, expected <%v> and received <%v> (test case #%d)", exp.expectedErr, err, i)
				return
			}

			if id := string(idBytes); id != exp.expectedID {
				err = fmt.Errorf("invalid ID, expected <%s> and received <%s> (test case #%d)", exp.expectedID, id, i)
				return
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func Test_matchCursor_HasForward(t *testing.T) {
	fn := func(c filterCursor, entryID []byte) (value bool, err error) {
		return c.HasForward(entryID)
	}

	testMatchCursorHas(t, fn)
}

func Test_matchCursor_HasReverse(t *testing.T) {
	fn := func(c filterCursor, entryID []byte) (value bool, err error) {
		return c.HasReverse(entryID)
	}

	testMatchCursorHas(t, fn)
}

func testMatchCursorHas(t *testing.T, fn func(c filterCursor, entryID []byte) (value bool, err error)) {
	type expected struct {
		value bool
		err   error
	}

	type testcase struct {
		relationshipKey string
		relationshipID  string
		entryID         string
		expected        expected
	}

	tcs := []testcase{
		{
			relationshipKey: "users",
			relationshipID:  "user_1",
			entryID:         "00000001",
			expected:        expected{value: true},
		},
		{
			relationshipKey: "contacts",
			relationshipID:  "contact_2",
			entryID:         "00000000",
			expected:        expected{value: false},
		},
		{
			relationshipKey: "groups",
			relationshipID:  "group_1",
			entryID:         "00000002",
			expected:        expected{value: true},
		},
	}

	testMatchCursor(t, func(txn *Transaction) (err error) {
		for i, tc := range tcs {
			var cur filterCursor
			f := filters.Match(tc.relationshipKey, tc.relationshipID)
			if cur, err = newMatchCursor(txn, f); err != nil {
				return
			}

			exp := tc.expected

			var ok bool
			if ok, err = fn(cur, []byte(tc.entryID)); err != exp.err {
				err = fmt.Errorf("invalid error, expected <%v> and received <%v> (test case #%d)", exp.err, err, i)
				return
			}

			if ok != exp.value {
				err = fmt.Errorf("invalid value, expected <%v> and received <%v> (test case #%d)", exp.value, ok, i)
				return
			}
		}

		return
	})
}

func testMatchCursor(t *testing.T, fn func(*Transaction) error) {
	var (
		m   *Mojura
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m)

	a := newTestStruct("user_0", "contact_0", "group_3", "1")
	b := newTestStruct("user_1", "contact_2", "group_2", "2")
	c := newTestStruct("user_2", "contact_2", "group_1", "3")

	if err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		if _, err = txn.New(a); err != nil {
			return
		}

		if _, err = txn.New(b); err != nil {
			return
		}

		if _, err = txn.New(c); err != nil {
			return
		}

		return fn(txn)
	}); err != nil {
		t.Fatal(err)
	}
}
