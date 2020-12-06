package mojura

import (
	"bytes"
	"testing"
)

func Test_stripLeadingZeros(t *testing.T) {
	type testcase struct {
		value    []byte
		expected []byte
	}

	tcs := []testcase{
		{
			value:    []byte("00000000"),
			expected: []byte(""),
		},
		{
			value:    []byte("00000001"),
			expected: []byte("1"),
		},
		{
			value:    []byte("00001337"),
			expected: []byte("1337"),
		},
		{
			value:    []byte("01234567"),
			expected: []byte("1234567"),
		},
	}

	for _, tc := range tcs {
		var out []byte
		if out = stripLeadingZeros(tc.value); bytes.Compare(tc.expected, out) != 0 {
			t.Fatalf("invalid value, expected %s and received %s", string(tc.expected), out)
		}
	}
}
