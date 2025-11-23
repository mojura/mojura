package mojura_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/mojura/mojura"
)

func TestEncryptedJSONEncoder_Marshal_Unmarshal_RoundTrip(t *testing.T) {
	type testvalue struct {
		Foo int    `json:"foo"`
		Bar string `json:"bar"`
	}

	type testcase struct {
		name    string
		key     string
		value   testvalue
		wantErr bool
	}

	tests := []testcase{
		{
			name:  "AES-128 roundtrip",
			key:   "0123456789abcdef", // 16 bytes
			value: testvalue{Foo: 42, Bar: "hello"},
		},
		{
			name:  "AES-192 roundtrip",
			key:   "0123456789abcdefghijkl", // 24 bytes
			value: testvalue{Foo: -7, Bar: "world"},
		},
		{
			name:  "AES-256 roundtrip",
			key:   "0123456789abcdefghijklmnop", // 32 bytes
			value: testvalue{Foo: 0, Bar: "!"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := mojura.MakeEncryptedJSONEncoder(tt.key)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}

			got, gotErr := e.Marshal(tt.value)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("EncryptedJSONEncoder.Marshal() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("EncryptedJSONEncoder.Marshal() succeeded unexpectedly")
			}

			var result testvalue
			if err = e.Unmarshal(got, &result); err != nil {
				t.Errorf("EncryptedJSONEncoder.Unmarshal() failed: %v", err)
			}

			if tt.value != result {
				t.Errorf("EncryptedJSONEncoder.Unmarshal() = %v, want %v", result, tt.value)
			}
		})
	}
}

func TestMakeEncryptedJSONEncoder_KeyValidation(t *testing.T) {
	type testcase struct {
		name    string
		key     string
		wantErr bool
	}

	tests := []testcase{
		{name: "too short (15)", key: "0123456789abcdef"[:15], wantErr: true},
		{name: "valid 16", key: "0123456789abcdef"},
		{name: "valid 24", key: "0123456789abcdefghijklmn"},
		{name: "valid 32", key: "0123456789abcdefghijklmnopqrstuv"},
		{name: "too long (33)", key: "0123456789abcdefghijklmnopqrstuvw", wantErr: true},
		{name: "empty", key: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mojura.MakeEncryptedJSONEncoder(tt.key)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MakeEncryptedJSONEncoder() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestEncryptedJSONEncoder_BackCompat_PlainJSON(t *testing.T) {
	type testvalue struct {
		Foo int    `json:"foo"`
		Bar string `json:"bar"`
	}

	// Create a valid encoder (key length is correct),
	// but feed Unmarshal with *plain JSON* to exercise the fallback path.
	e, err := mojura.MakeEncryptedJSONEncoder("0123456789abcdef")
	if err != nil {
		t.Fatalf("could not construct encoder: %v", err)
	}

	want := testvalue{Foo: 77, Bar: "plain"}
	plain, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("json.Marshal() failed: %v", err)
	}

	var got testvalue
	if err := e.Unmarshal(plain, &got); err != nil {
		t.Fatalf("Unmarshal() plain JSON failed: %v", err)
	}

	if got != want {
		t.Errorf("Unmarshal() plain JSON = %v, want %v", got, want)
	}
}

func TestEncryptedJSONEncoder_WrongKey_Fails(t *testing.T) {
	type testvalue struct {
		Foo int    `json:"foo"`
		Bar string `json:"bar"`
	}

	e1, err := mojura.MakeEncryptedJSONEncoder("0123456789abcdef")
	if err != nil {
		t.Fatalf("could not construct encoder e1: %v", err)
	}
	e2, err := mojura.MakeEncryptedJSONEncoder("abcdef0123456789")
	if err != nil {
		t.Fatalf("could not construct encoder e2: %v", err)
	}

	in := testvalue{Foo: 1, Bar: "x"}
	ct, err := e1.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal() with e1 failed: %v", err)
	}

	var out testvalue
	if err := e2.Unmarshal(ct, &out); err == nil {
		t.Fatal("Unmarshal() with wrong key succeeded unexpectedly")
	}
}

func TestEncryptedJSONEncoder_NonceRandomized(t *testing.T) {
	type testvalue struct {
		Foo int    `json:"foo"`
		Bar string `json:"bar"`
	}

	e, err := mojura.MakeEncryptedJSONEncoder("0123456789abcdef")
	if err != nil {
		t.Fatalf("could not construct encoder: %v", err)
	}

	v := testvalue{Foo: 99, Bar: "nonce"}
	a, err := e.Marshal(v)
	if err != nil {
		t.Fatalf("first Marshal() failed: %v", err)
	}
	b, err := e.Marshal(v)
	if err != nil {
		t.Fatalf("second Marshal() failed: %v", err)
	}

	if bytes.Equal(a, b) {
		t.Errorf("ciphertexts equal for same plaintext; expected different due to random nonce")
	}
}

func TestEncryptedJSONEncoder_TamperDetect(t *testing.T) {
	type testvalue struct {
		Foo int    `json:"foo"`
		Bar string `json:"bar"`
	}

	e, err := mojura.MakeEncryptedJSONEncoder("0123456789abcdef")
	if err != nil {
		t.Fatalf("could not construct encoder: %v", err)
	}

	orig := testvalue{Foo: 7, Bar: "auth"}
	ct, err := e.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}

	if len(ct) == 0 {
		t.Fatal("empty ciphertext")
	}

	// Flip a byte in the ciphertext (avoid the nonce region by flipping near the end).
	ctTampered := append([]byte(nil), ct...)
	ctTampered[len(ctTampered)-1] ^= 0xFF

	var out testvalue
	if err := e.Unmarshal(ctTampered, &out); err == nil {
		t.Fatal("Unmarshal() succeeded on tampered ciphertext; expected authentication failure")
	}
}
