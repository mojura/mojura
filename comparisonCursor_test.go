package mojura

import (
	"context"
	"fmt"
	"testing"
)

func Test_comparisonCursor_Next(t *testing.T) {
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
		isMatch         comparisonFn
		expected        []expected
	}

	a := newTestStruct("user_0", "contact_0", "group_3", "1")
	b := newTestStruct("user_1", "contact_2", "group_2", "2")
	c := newTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			relationshipKey: "users",
			isMatch: func(relationshipID []byte) (ok bool, err error) {
				ok = string(relationshipID) != "user_2"
				return
			},
			expected: []expected{
				{expectedID: "00000000"},
				{expectedID: "00000001"},
				{expectedErr: Break},
			},
		},
		{
			relationshipKey: "contacts",
			isMatch: func(relationshipID []byte) (ok bool, err error) {
				ok = string(relationshipID) != "contact_2"
				return
			},
			expected: []expected{
				{expectedID: "00000000"},
				{expectedErr: Break},
			},
		},
		{
			relationshipKey: "groups",
			isMatch: func(relationshipID []byte) (ok bool, err error) {
				ok = string(relationshipID) != "group_2"
				return
			},
			expected: []expected{
				{expectedID: "00000002"},
				{expectedID: "00000000"},
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

		for i, tc := range tcs {
			var cur *comparisonCursor
			if cur, err = newComparisonCursor(txn, []byte(tc.relationshipKey), tc.isMatch); err != nil {
				return
			}

			for _, exp := range tc.expected {
				var idBytes []byte
				if idBytes, err = cur.Next(); err != exp.expectedErr {
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

func Test_comparisonCursor_Prev(t *testing.T) {
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
		isMatch         comparisonFn
		expected        []expected
	}

	a := newTestStruct("user_0", "contact_0", "group_3", "1")
	b := newTestStruct("user_1", "contact_2", "group_2", "2")
	c := newTestStruct("user_2", "contact_2", "group_1", "3")

	tcs := []testcase{
		{
			relationshipKey: "users",
			isMatch: func(relationshipID []byte) (ok bool, err error) {
				ok = string(relationshipID) != "user_2"
				return
			},
			expected: []expected{
				{expectedID: "00000001"},
				{expectedID: "00000000"},
				{expectedErr: Break},
			},
		},
		{
			relationshipKey: "contacts",
			isMatch: func(relationshipID []byte) (ok bool, err error) {
				ok = string(relationshipID) != "contact_2"
				return
			},
			expected: []expected{
				{expectedID: "00000000"},
				{expectedErr: Break},
			},
		},
		{
			relationshipKey: "groups",
			isMatch: func(relationshipID []byte) (ok bool, err error) {
				ok = string(relationshipID) != "group_2"
				return
			},
			expected: []expected{
				{expectedID: "00000000"},
				{expectedID: "00000002"},
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

		for i, tc := range tcs {
			var cur *comparisonCursor
			if cur, err = newComparisonCursor(txn, []byte(tc.relationshipKey), tc.isMatch); err != nil {
				return
			}

			for _, exp := range tc.expected {
				var idBytes []byte
				if idBytes, err = cur.Prev(); err != exp.expectedErr {
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
