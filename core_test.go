package dbl

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

const (
	testDir = "./test_data"
)

var c *Core

func TestNew(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}

	if err = testTeardown(c); err != nil {
		t.Fatal(err)
	}
}

func TestCore_New(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if len(entryID) == 0 {
		t.Fatal("invalid entry id, expected non-empty value")
	}
}

func TestCore_Get(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var fb testStruct
	if err = c.Get(entryID, &fb); err != nil {
		t.Fatal(err)
	}

	if err = testCheck(&foobar, &fb); err != nil {
		t.Fatal(err)
	}
}

func TestCore_Get_context(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

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
		{iterations: 1, timeout: time.Millisecond * 200, err: ErrTransactionTimedOut},
		{iterations: 5, timeout: time.Millisecond * 100, err: nil},
		{iterations: 10, timeout: time.Millisecond * 180, err: nil},
		{iterations: 3, timeout: time.Millisecond * 500, err: ErrTransactionTimedOut},
	}

	for _, tc := range tcs {
		ctx := NewTouchContext(context.Background(), time.Millisecond*200)
		if err = c.ReadTransaction(ctx, func(txn *Transaction) (err error) {
			var fb testStruct
			for i := 0; i < tc.iterations; i++ {
				time.Sleep(tc.timeout)

				if err = txn.Get(entryID, &fb); err != nil {
					return
				}
			}

			return
		}); err != tc.err {
			t.Fatalf("invalid error, expected %v and received %v [test case %+v]", tc.err, err, tc)
		}
	}
}

func TestCore_GetByRelationship_users(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var foobars []*testStruct
	if err = c.GetByRelationship("users", "user_1", &foobars); err != nil {
		t.Fatal(err)
	}

	for _, fb := range foobars {
		if err = testCheck(&foobar, fb); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCore_GetByRelationship_contacts(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var foobars []*testStruct
	if err = c.GetByRelationship("contacts", "contact_1", &foobars); err != nil {
		t.Fatal(err)
	}

	for _, fb := range foobars {
		if err = testCheck(&foobar, fb); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCore_GetByRelationship_invalid(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var foobars []*testBadType
	if err = c.GetByRelationship("contacts", "contact_1", &foobars); err != ErrInvalidType {
		t.Fatalf("invalid error, expected %v and received %v", ErrInvalidType, err)
	}
}

func TestCore_GetByRelationship_update(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	foobar.UserID = "user_3"

	if err = c.Edit(entryID, &foobar); err != nil {
		t.Fatal(err)
	}

	var foobars []*testStruct
	if err = c.GetByRelationship("users", "user_1", &foobars); err != nil {
		t.Fatal(err)
	}

	if len(foobars) != 0 {
		t.Fatalf("invalid number of entries, expected %d and received %d", 0, len(foobars))
	}

	if err = c.GetByRelationship("users", "user_3", &foobars); err != nil {
		t.Fatal(err)
	}

	if len(foobars) != 1 {
		t.Fatalf("invalid number of entries, expected %d and received %d", 1, len(foobars))
	}

	for _, fb := range foobars {
		if err = testCheck(&foobar, fb); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCore_GetFirstByRelationship(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var fb testStruct
	if err = c.GetFirstByRelationship("contacts", foobar.ContactID, &fb); err != nil {
		t.Fatal(err)
	}

	if fb.ID != "00000000" {
		t.Fatalf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000001", fb.ID)
	}

	foobar.ID = fb.ID

	if err = testCheck(&foobar, &fb); err != nil {
		t.Fatal(err)
	}

	return
}

func TestCore_GetLastByRelationship(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var fb testStruct
	if err = c.GetLastByRelationship("contacts", foobar.ContactID, &fb); err != nil {
		t.Fatal(err)
	}

	if fb.ID != "00000001" {
		t.Fatalf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000001", fb.ID)
	}

	foobar.ID = fb.ID

	if err = testCheck(&foobar, &fb); err != nil {
		t.Fatal(err)
	}

	return
}

func TestCore_Edit(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	var entryID string
	if entryID, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	foobar.Foo = "HELLO"

	if err = c.Edit(entryID, &foobar); err != nil {
		t.Fatal(err)
	}

	var fb testStruct
	if err = c.Get(entryID, &fb); err != nil {
		t.Fatal(err)
	}

	if err = testCheck(&foobar, &fb); err != nil {
		t.Fatal(err)
	}
}

func TestCore_ForEach(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var cnt int
	if err = c.ForEach("", func(key string, v Value) (err error) {
		fb := v.(*testStruct)
		// We are not checking ID correctness in this test
		foobar.ID = fb.ID

		if err = testCheck(&foobar, fb); err != nil {
			t.Fatal(err)
		}

		cnt++
		return
	}); err != nil {
		t.Fatal(err)
	}

	if cnt != 2 {
		t.Fatalf("invalid number of entries, expected %d and received %d", 2, cnt)
	}

	return
}

func TestCore_ForEach_with_filter(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	foobar.UserID = "user_2"
	foobar.ContactID = "contact_3"

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var cnt int
	fn := func(key string, v Value) (err error) {
		fb := v.(*testStruct)
		// We are not checking ID correctness in this test
		foobar.ID = fb.ID

		if err = testCheck(&foobar, fb); err != nil {
			t.Fatal(err)
		}

		cnt++
		return
	}

	if err = c.ForEach("", fn, MakeFilter("contacts", foobar.ContactID, false)); err != nil {
		t.Fatal(err)
	}

	if cnt != 1 {
		t.Fatalf("invalid number of entries, expected %d and received %d", 1, cnt)
	}

	return
}

func TestCore_Cursor(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var cnt int
	if err = c.Cursor(func(cursor *Cursor) (err error) {
		var fb testStruct
		for err = cursor.Seek("", &fb); err == nil; err = cursor.Next(&fb) {
			// We are not checking ID correctness in this test
			foobar.ID = fb.ID

			if err = testCheck(&foobar, &fb); err != nil {
				break
			}

			cnt++
			fb = testStruct{}
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

	return
}

func TestCore_Cursor_First(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Cursor(func(cursor *Cursor) (err error) {
		var fb testStruct
		if err = cursor.First(&fb); err != nil {
			return
		}

		if fb.ID != "00000000" {
			return fmt.Errorf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000000", fb.ID)
		}

		foobar.ID = fb.ID

		if err = testCheck(&foobar, &fb); err != nil {
			t.Fatal(err)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	return
}

func TestCore_Cursor_Last(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Cursor(func(cursor *Cursor) (err error) {
		var fb testStruct
		if err = cursor.Last(&fb); err != nil {
			return
		}

		if fb.ID != "00000001" {
			return fmt.Errorf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000001", fb.ID)
		}

		foobar.ID = fb.ID

		if err = testCheck(&foobar, &fb); err != nil {
			t.Fatal(err)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	return
}

func TestCore_Cursor_Seek(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	if err = c.Cursor(func(cursor *Cursor) (err error) {
		var fb testStruct
		if err = cursor.Seek("00000001", &fb); err != nil {
			return
		}

		if fb.ID != "00000001" {
			return fmt.Errorf("invalid ID, expected \"%s\" and recieved \"%s\"", "00000001", fb.ID)
		}

		foobar.ID = fb.ID

		if err = testCheck(&foobar, &fb); err != nil {
			t.Fatal(err)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	return
}

func TestCore_CursorRelationship(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	foobar.UserID = "user_2"
	foobar.ContactID = "contact_3"

	if _, err = c.New(&foobar); err != nil {
		t.Fatal(err)
	}

	var cnt int
	if err = c.CursorRelationship("contacts", foobar.ContactID, func(cursor *Cursor) (err error) {
		var fb testStruct
		for err = cursor.Seek("", &fb); err == nil; err = cursor.Next(&fb) {
			// We are not checking ID correctness in this test
			foobar.ID = fb.ID

			if err = testCheck(&foobar, &fb); err != nil {
				t.Fatal(err)
			}

			cnt++
			fb = testStruct{}
		}

		if err == ErrEndOfEntries {
			err = nil
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	if cnt != 1 {
		t.Fatalf("invalid number of entries, expected %d and received %d", 1, cnt)
	}

	return
}

func TestCore_Lookups(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	if err = c.SetLookup("test_lookup", "test_0", "foo"); err != nil {
		t.Fatal(err)
	}

	if err = c.SetLookup("test_lookup", "test_0", "bar"); err != nil {
		t.Fatal(err)
	}

	var keys []string
	if keys, err = c.GetLookup("test_lookup", "test_0"); err != nil {
		t.Fatal(err)
	}

	if len(keys) != 2 {
		t.Fatalf("invalid number of keys, expected %d and received %d (%+v)", 2, len(keys), keys)
	}

	for i, key := range keys {
		var expected string
		switch i {
		case 0:
			expected = "bar"
		case 1:
			expected = "foo"
		}

		if expected != key {
			t.Fatalf("invalid key, expected %s and recieved %s", expected, key)
		}
	}

	if err = c.RemoveLookup("test_lookup", "test_0", "foo"); err != nil {
		t.Fatal(err)
	}

	if keys, err = c.GetLookup("test_lookup", "test_0"); err != nil {
		t.Fatal(err)
	}

	if len(keys) != 1 {
		t.Fatalf("invalid number of keys, expected %d and received %d (%+v)", 1, len(keys), keys)
	}

	if keys[0] != "bar" {
		t.Fatalf("invalid key, expected %s and recieved %s", "bar", keys[0])
	}
}

func TestCore_Batch(t *testing.T) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	var entryID string
	if err = c.Batch(func(txn *Transaction) (err error) {
		entryID, err = txn.New(&foobar)
		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = c.Batch(func(txn *Transaction) (err error) {
		foobar.Foo = "foo"
		foobar.Bar = "bar"
		err = txn.Edit(entryID, &foobar)
		return
	}); err != nil {
		t.Fatal(err)
	}

	var val testStruct
	if err = c.Get(entryID, &val); err != nil {
		t.Fatal(err)
	}

	if val.Foo != "foo" {
		t.Fatalf("invalid value for Foo, expected \"%s\" and received \"%s\"", foobar.Foo, val.Foo)
	}

	if val.Bar != "bar" {
		t.Fatalf("invalid value for Bar, expected \"%s\" and received \"%s\"", foobar.Bar, val.Bar)
	}

	return
}

func BenchmarkCore_New_2(b *testing.B) {
	benchmarkCoreNew(b, 2)
	return
}

func BenchmarkCore_New_4(b *testing.B) {
	benchmarkCoreNew(b, 4)
	return
}

func BenchmarkCore_New_8(b *testing.B) {
	benchmarkCoreNew(b, 8)
	return
}

func BenchmarkCore_New_16(b *testing.B) {
	benchmarkCoreNew(b, 16)
	return
}

func BenchmarkCore_New_32(b *testing.B) {
	benchmarkCoreNew(b, 32)
	return
}

func BenchmarkCore_New_64(b *testing.B) {
	benchmarkCoreNew(b, 64)
	return
}

func BenchmarkCore_Batch_2(b *testing.B) {
	benchmarkCoreBatch(b, 2)
	return
}

func BenchmarkCore_Batch_4(b *testing.B) {
	benchmarkCoreBatch(b, 4)
	return
}

func BenchmarkCore_Batch_8(b *testing.B) {
	benchmarkCoreBatch(b, 8)
	return
}

func BenchmarkCore_Batch_16(b *testing.B) {
	benchmarkCoreBatch(b, 16)
	return
}

func BenchmarkCore_Batch_32(b *testing.B) {
	benchmarkCoreBatch(b, 32)
	return
}

func BenchmarkCore_Batch_64(b *testing.B) {
	benchmarkCoreBatch(b, 64)
	return
}

func benchmarkCoreNew(b *testing.B, threads int) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		b.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	b.SetParallelism(threads)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err = c.New(&foobar); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.ReportAllocs()
	return
}

func benchmarkCoreBatch(b *testing.B, threads int) {
	var (
		c   *Core
		err error
	)

	if c, err = testInit(); err != nil {
		b.Fatal(err)
	}
	defer testTeardown(c)

	foobar := newTestStruct("user_1", "contact_1", "FOO FOO", "bunny bar bar")

	b.SetParallelism(threads)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err = c.Batch(func(txn *Transaction) (err error) {
				_, err = txn.New(&foobar)
				return
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.ReportAllocs()
	return
}

func ExampleNew() {
	var (
		c   *Core
		err error
	)

	if c, err = New("example", "./data", &testStruct{}, "users", "contacts"); err != nil {
		return
	}

	fmt.Printf("Core! %v\n", c)
}

func ExampleCore_New() {
	var ts testStruct
	ts.Foo = "Foo foo"
	ts.Bar = "Bar bar"

	var (
		entryID string
		err     error
	)

	if entryID, err = c.New(&ts); err != nil {
		return
	}

	fmt.Printf("New entry! %s\n", entryID)
}

func ExampleCore_Get() {
	var (
		ts  testStruct
		err error
	)

	if err = c.Get("00000000", &ts); err != nil {
		return
	}

	fmt.Printf("Retrieved entry! %+v\n", ts)
}

func ExampleCore_GetByRelationship() {
	var (
		tss []*testStruct
		err error
	)

	if err = c.GetByRelationship("users", "user_1", &tss); err != nil {
		return
	}

	for i, ts := range tss {
		fmt.Printf("Retrieved entry #%d! %+v\n", i, ts)
	}
}

func ExampleCore_ForEach() {
	var err error
	if err = c.ForEach("", func(key string, val Value) (err error) {
		fmt.Printf("Iterating entry (%s)! %+v\n", key, val)
		return
	}); err != nil {
		return
	}
}

func ExampleCore_ForEach_with_filter() {
	var err error
	fn := func(key string, val Value) (err error) {
		fmt.Printf("Iterating entry (%s)! %+v\n", key, val)
		return
	}

	if err = c.ForEach("", fn, MakeFilter("users", "user_1", false)); err != nil {
		return
	}
}

func ExampleCore_Edit() {
	var (
		ts  *testStruct
		err error
	)

	// We will pretend the test struct is already populated

	// Let's update the Foo field to "New foo value"
	ts.Foo = "New foo value"

	if err = c.Edit("00000000", ts); err != nil {
		return
	}

	fmt.Printf("Edited entry %s!\n", "00000000")
}

func ExampleCore_Remove() {
	var err error
	if err = c.Remove("00000000"); err != nil {
		return
	}

	fmt.Printf("Removed entry %s!\n", "00000000")
}

func testInit() (c *Core, err error) {
	if err = os.MkdirAll(testDir, 0744); err != nil {
		return
	}

	return New("test", testDir, &testStruct{}, "users", "contacts")
}

func testTeardown(c *Core) (err error) {
	if err = c.Close(); err != nil {
		return
	}

	return os.RemoveAll(testDir)
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

	if a.Foo != b.Foo {
		return fmt.Errorf("invalid foo, expected %s and received %s", a.Foo, b.Foo)
	}

	if a.Bar != b.Bar {
		return fmt.Errorf("invalid bar, expected %s and received %s", a.Bar, b.Bar)
	}

	return
}

func newTestStruct(userID, contactID, foo, bar string) (t testStruct) {
	t.UserID = userID
	t.ContactID = contactID
	t.Foo = foo
	t.Bar = bar
	return
}

type testStruct struct {
	ID        string `json:"id"`
	UserID    string `json:"userID"`
	ContactID string `json:"contactID"`

	Foo string `json:"foo"`
	Bar string `json:"bar"`

	UpdatedAt int64 `json:"updatedAt"`
	CreatedAt int64 `json:"createdAt"`
}

func (t *testStruct) SetID(id string) {
	t.ID = id
}

func (t *testStruct) GetUpdatedAt() (updatedAt int64) {
	return t.UpdatedAt
}

func (t *testStruct) GetCreatedAt() (createdAt int64) {
	return t.CreatedAt
}

func (t *testStruct) GetID() (id string) {
	return t.ID
}

func (t *testStruct) GetRelationshipIDs() (ids []string) {
	ids = append(ids, t.UserID)
	ids = append(ids, t.ContactID)
	return
}

func (t *testStruct) SetUpdatedAt(updatedAt int64) {
	t.UpdatedAt = updatedAt
}

func (t *testStruct) SetCreatedAt(createdAt int64) {
	t.CreatedAt = createdAt
}

type testBadType struct {
	Foo string
	Bar string
}
