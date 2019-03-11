package core

import (
	"os"
	"testing"
)

const (
	testDir = "./test_data"
)

func TestCore(t *testing.T) {
	var err error
	if err = os.MkdirAll(testDir, 0744); err != nil {
		t.Fatal(err)
	}
	// defer os.RemoveAll(testDir)

	var c *Core
	if c, err = New("test", testDir, testGenerator, "userID", "contactID"); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var foobar testStruct
	foobar.UserID = "user_1"
	foobar.ContactID = "contact_1"
	foobar.Foo = "FOO FOO"
	foobar.Bar = "bunny bar bar"

	var entryID string
	if entryID, err = c.New(&foobar, foobar.UserID, foobar.ContactID); err != nil {
		t.Fatal(err)
	}

	if len(entryID) == 0 {
		t.Fatal("invalid entry id, expected non-empty value")
	}
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

func (t *testStruct) SetUpdatedAt(updatedAt int64) {
	t.UpdatedAt = updatedAt
}

func (t *testStruct) SetCreatedAt(createdAt int64) {
	t.CreatedAt = createdAt
}

func testGenerator() Value {
	return &testStruct{}
}
