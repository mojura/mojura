package core

import (
	"fmt"
	"os"
	"testing"
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
	if entryID, err = c.New(&foobar, foobar.UserID, foobar.ContactID); err != nil {
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
	if entryID, err = c.New(&foobar, foobar.UserID, foobar.ContactID); err != nil {
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

	if _, err = c.New(&foobar, foobar.UserID, foobar.ContactID); err != nil {
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

	if _, err = c.New(&foobar, foobar.UserID, foobar.ContactID); err != nil {
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

	if _, err = c.New(&foobar, foobar.UserID, foobar.ContactID); err != nil {
		t.Fatal(err)
	}

	var foobars []*testBadType
	if err = c.GetByRelationship("contacts", "contact_1", &foobars); err != ErrInvalidType {
		t.Fatalf("invalid error, expected %v and received %v", ErrInvalidType, err)
	}
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
	if entryID, err = c.New(&foobar, foobar.UserID, foobar.ContactID); err != nil {
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

	if entryID, err = c.New(&ts, "user_1", "contact_3"); err != nil {
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
