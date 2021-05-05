package scratch

import (
	"testing"

	"github.com/mojura/mojura"
)

func TestTransaction(t *testing.T) {
	var (
		s   *Scratch
		err error
	)

	if s, err = New("test", "./test_data", &testStruct{}); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}()
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
	mojura.Entry

	UserID    string   `json:"userID"`
	ContactID string   `json:"contactID"`
	GroupID   string   `json:"groupID"`
	Tags      []string `json:"tags"`

	Value string `json:"value"`
}

func (t *testStruct) GetRelationships() (r mojura.Relationships) {
	r.Append(t.UserID)
	r.Append(t.ContactID)
	r.Append(t.GroupID)
	r.Append(t.Tags...)
	return
}

type testBadType struct {
	Foo string
	Bar string
}
