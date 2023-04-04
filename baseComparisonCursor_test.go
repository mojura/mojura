package mojura

import (
	"context"
	"fmt"
	"testing"

	"github.com/mojura/mojura/filters"
)

func Test_baseComparisonCursor_SeekForward(t *testing.T) {
	var (
		m   *Mojura[*testStruct]
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m, t)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		relationshipID string
		seekID         string

		isMatch  filters.ComparisonFn
		expected expected
	}

	a := makeTestStruct("user_0", "contact_0", "group_3", "1")
	b := makeTestStruct("user_1", "contact_2", "group_2", "2")
	c := makeTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			relationshipID: "user_0",
			seekID:         "00000000",
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000000"
				return
			},
			expected: expected{expectedID: "00000001"},
		},
		{
			relationshipID: "user_0",
			seekID:         "00000000",
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: expected{expectedID: "00000000"},
		},
		{
			relationshipID: "user_0",
			seekID:         "00000001",
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000000"
				return
			},
			expected: expected{expectedID: "00000001"},
		},
		{
			relationshipID: "user_0",
			seekID:         "00000001",
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: expected{expectedID: "00000002"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		if _, err = txn.New(&a); err != nil {
			return
		}

		if _, err = txn.New(&b); err != nil {
			return
		}

		if _, err = txn.New(&c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur *baseComparisonCursor[*testStruct]
			f := filters.Comparison("", tc.isMatch)
			if cur, err = newBaseComparisonCursor(txn, f); err != nil {
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

func Test_baseComparisonCursor_SeekReverse(t *testing.T) {
	var (
		m   *Mojura[*testStruct]
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m, t)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		seekID string

		isMatch  filters.ComparisonFn
		expected expected
	}

	a := makeTestStruct("user_0", "contact_0", "group_3", "1")
	b := makeTestStruct("user_1", "contact_2", "group_2", "2")
	c := makeTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			seekID: "00000002",
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000002"
				return
			},
			expected: expected{expectedID: "00000001"},
		},
		{
			seekID: "00000002",
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: expected{expectedID: "00000002"},
		},
		{
			seekID: "00000001",
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000002"
				return
			},
			expected: expected{expectedID: "00000001"},
		},
		{
			seekID: "00000001",
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: expected{expectedID: "00000000"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		if _, err = txn.New(&a); err != nil {
			return
		}

		if _, err = txn.New(&b); err != nil {
			return
		}

		if _, err = txn.New(&c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur *baseComparisonCursor[*testStruct]
			f := filters.Comparison("", tc.isMatch)
			if cur, err = newBaseComparisonCursor(txn, f); err != nil {
				return
			}

			exp := tc.expected

			var idBytes []byte
			if idBytes, err = cur.SeekReverse(nil, []byte(tc.seekID)); err != exp.expectedErr {
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

func Test_baseComparisonCursor_First(t *testing.T) {
	var (
		m   *Mojura[*testStruct]
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m, t)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		isMatch  filters.ComparisonFn
		expected expected
	}

	a := makeTestStruct("user_0", "contact_0", "group_3", "1")
	b := makeTestStruct("user_1", "contact_2", "group_2", "2")
	c := makeTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000000"
				return
			},
			expected: expected{expectedID: "00000001"},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: expected{expectedID: "00000000"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		if _, err = txn.New(&a); err != nil {
			return
		}

		if _, err = txn.New(&b); err != nil {
			return
		}

		if _, err = txn.New(&c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur *baseComparisonCursor[*testStruct]
			f := filters.Comparison("", tc.isMatch)
			if cur, err = newBaseComparisonCursor(txn, f); err != nil {
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

func Test_baseComparisonCursor_First_with_deletion(t *testing.T) {
	var (
		m   *Mojura[*testStruct]
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m, t)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		isMatch  filters.ComparisonFn
		expected expected
	}

	a := makeTestStruct("user_0", "contact_0", "group_3", "1")
	b := makeTestStruct("user_1", "contact_2", "group_2", "2")
	c := makeTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000000"
				return
			},
			expected: expected{expectedID: "00000001"},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: expected{expectedID: "00000002"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		if _, err = txn.New(&a); err != nil {
			return
		}

		if _, err = txn.New(&b); err != nil {
			return
		}

		if _, err = txn.New(&c); err != nil {
			return
		}

		if _, err = txn.Delete("00000000"); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur *baseComparisonCursor[*testStruct]
			f := filters.Comparison("", tc.isMatch)
			if cur, err = newBaseComparisonCursor(txn, f); err != nil {
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

func Test_baseComparisonCursor_Next(t *testing.T) {
	var (
		m   *Mojura[*testStruct]
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m, t)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		isMatch  filters.ComparisonFn
		expected []expected
	}

	a := makeTestStruct("user_0", "contact_0", "group_3", "1")
	b := makeTestStruct("user_1", "contact_2", "group_2", "2")
	c := makeTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != ""
				return
			},
			expected: []expected{
				{expectedID: "00000000"},
				{expectedID: "00000001"},
				{expectedID: "00000002"},
				{expectedErr: Break},
			},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000000"
				return
			},
			expected: []expected{
				{expectedID: "00000001"},
				{expectedID: "00000002"},
				{expectedErr: Break},
			},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: []expected{
				{expectedID: "00000000"},
				{expectedID: "00000002"},
				{expectedErr: Break},
			},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000002"
				return
			},
			expected: []expected{
				{expectedID: "00000000"},
				{expectedID: "00000001"},
				{expectedErr: Break},
			},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000000" && relationshipID != "00000002"
				return
			},
			expected: []expected{
				{expectedID: "00000001"},
				{expectedErr: Break},
			},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		if _, err = txn.New(&a); err != nil {
			return
		}

		if _, err = txn.New(&b); err != nil {
			return
		}

		if _, err = txn.New(&c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur *baseComparisonCursor[*testStruct]
			f := filters.Comparison("", tc.isMatch)
			if cur, err = newBaseComparisonCursor(txn, f); err != nil {
				return
			}

			for j, exp := range tc.expected {
				fn := cur.Next
				if j == 0 {
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

func Test_baseComparisonCursor_Prev(t *testing.T) {
	var (
		m   *Mojura[*testStruct]
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m, t)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		isMatch  filters.ComparisonFn
		expected []expected
	}

	a := makeTestStruct("user_0", "contact_0", "group_3", "1")
	b := makeTestStruct("user_1", "contact_2", "group_2", "2")
	c := makeTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000002"
				return
			},
			expected: []expected{
				{expectedID: "00000001"},
				{expectedID: "00000000"},
				{expectedErr: Break},
			},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: []expected{
				{expectedID: "00000002"},
				{expectedID: "00000000"},
				{expectedErr: Break},
			},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000000"
				return
			},
			expected: []expected{
				{expectedID: "00000002"},
				{expectedID: "00000001"},
				{expectedErr: Break},
			},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		if _, err = txn.New(&a); err != nil {
			return
		}

		if _, err = txn.New(&b); err != nil {
			return
		}

		if _, err = txn.New(&c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur *baseComparisonCursor[*testStruct]
			f := filters.Comparison("", tc.isMatch)
			if cur, err = newBaseComparisonCursor(txn, f); err != nil {
				return
			}

			for j, exp := range tc.expected {
				fn := cur.Prev
				if j == 0 {
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

func Test_baseComparisonCursor_Last(t *testing.T) {
	var (
		m   *Mojura[*testStruct]
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m, t)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		isMatch  filters.ComparisonFn
		expected expected
	}

	a := makeTestStruct("user_0", "contact_0", "group_3", "1")
	b := makeTestStruct("user_1", "contact_2", "group_2", "2")
	c := makeTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000002"
				return
			},
			expected: expected{expectedID: "00000001"},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: expected{expectedID: "00000002"},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000000"
				return
			},
			expected: expected{expectedID: "00000002"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		if _, err = txn.New(&a); err != nil {
			return
		}

		if _, err = txn.New(&b); err != nil {
			return
		}

		if _, err = txn.New(&c); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur *baseComparisonCursor[*testStruct]
			f := filters.Comparison("", tc.isMatch)
			if cur, err = newBaseComparisonCursor(txn, f); err != nil {
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

func Test_baseComparisonCursor_Last_with_deletion(t *testing.T) {
	var (
		m   *Mojura[*testStruct]
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m, t)

	type expected struct {
		expectedID  string
		expectedErr error
	}

	type testcase struct {
		isMatch  filters.ComparisonFn
		expected expected
	}

	a := makeTestStruct("user_0", "contact_0", "group_3", "1")
	b := makeTestStruct("user_1", "contact_2", "group_2", "2")
	c := makeTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000002"
				return
			},
			expected: expected{expectedID: "00000000"},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000001"
				return
			},
			expected: expected{expectedID: "00000002"},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID != "00000000"
				return
			},
			expected: expected{expectedID: "00000002"},
		},
	}

	if err = m.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		if _, err = txn.New(&a); err != nil {
			return
		}

		if _, err = txn.New(&b); err != nil {
			return
		}

		if _, err = txn.New(&c); err != nil {
			return
		}

		if _, err = txn.Delete("00000001"); err != nil {
			return
		}

		for i, tc := range tcs {
			var cur *baseComparisonCursor[*testStruct]
			f := filters.Comparison("", tc.isMatch)
			if cur, err = newBaseComparisonCursor(txn, f); err != nil {
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

func Test_baseComparisonCursor_HasForward(t *testing.T) {
	fn := func(c *baseComparisonCursor[*testStruct], entryID []byte) (value bool, err error) {
		return c.HasForward(entryID)
	}

	testBaseComparisonCursorHas(t, fn)
}

func Test_baseComparisonCursor_HasReverse(t *testing.T) {
	fn := func(c *baseComparisonCursor[*testStruct], entryID []byte) (value bool, err error) {
		return c.HasReverse(entryID)
	}

	testBaseComparisonCursorHas(t, fn)
}

func testBaseComparisonCursorHas(t *testing.T, fn func(c *baseComparisonCursor[*testStruct], entryID []byte) (value bool, err error)) {
	type expected struct {
		value bool
		err   error
	}

	type testcase struct {
		isMatch  filters.ComparisonFn
		entryID  string
		expected expected
	}

	tcs := []testcase{
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID == "00000001"
				return
			},
			entryID:  "00000001",
			expected: expected{value: true},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID == "00000001"
				return
			},
			entryID:  "00000001",
			expected: expected{value: true},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID == "00000001"
				return
			},
			entryID:  "00000002",
			expected: expected{value: false},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID == "00000000"
				return
			},
			entryID:  "00000000",
			expected: expected{value: true},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID == "00000001"
				return
			},
			entryID:  "00000001",
			expected: expected{value: true},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID == "00000002"
				return
			},
			entryID:  "00000002",
			expected: expected{value: true},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID == "00000002"
				return
			},
			entryID:  "00000002",
			expected: expected{value: true},
		},
		{
			isMatch: func(relationshipID string) (ok bool, err error) {
				ok = relationshipID == "00000000"
				return
			},
			entryID:  "00000001",
			expected: expected{value: false},
		},
	}

	testBaseComparisonCursor(t, func(txn *Transaction[*testStruct]) (err error) {
		for i, tc := range tcs {
			var cur *baseComparisonCursor[*testStruct]
			f := filters.Comparison("", tc.isMatch)
			if cur, err = newBaseComparisonCursor(txn, f); err != nil {
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

func testBaseComparisonCursor(t *testing.T, fn func(*Transaction[*testStruct]) error) {
	var (
		m   *Mojura[*testStruct]
		err error
	)

	if m, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(m, t)

	a := makeTestStruct("user_0", "contact_0", "group_3", "1")
	b := makeTestStruct("user_1", "contact_2", "group_2", "2")
	c := makeTestStruct("user_2", "contact_2", "group_1", "3")

	if err = m.Transaction(context.Background(), func(txn *Transaction[*testStruct]) (err error) {
		if _, err = txn.New(&a); err != nil {
			return
		}

		if _, err = txn.New(&b); err != nil {
			return
		}

		if _, err = txn.New(&c); err != nil {
			return
		}

		return fn(txn)
	}); err != nil {
		t.Fatal(err)
	}
}
