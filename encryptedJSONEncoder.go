package mojura

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

var _ Encoder = &EncryptedJSONEncoder{} // compile-time check that EncryptedJSONEncoder satisfies Encoder

// MakeEncryptedJSONEncoder constructs an EncryptedJSONEncoder by value.
//
// The provided key must be exactly 16, 24, or 32 bytes long — corresponding
// to AES-128, AES-192, or AES-256 respectively. Any other key length will
// cause this function to return an error, ensuring that encryption behavior
// is always explicit and predictable.
func NewEncryptedJSONEncoder(key string) (out *EncryptedJSONEncoder, err error) {
	var enc EncryptedJSONEncoder
	// Validate key length against the three AES block cipher key sizes.
	switch len(key) {
	case 16, 24, 32:
		// Convert the string key to a byte slice and assign it.
		// AES requires raw bytes, so this conversion is expected and safe.
		enc.key = []byte(key)
		return &enc, nil
	default:
		// Reject invalid key lengths to prevent implicit resizing or insecure padding.
		// Fail fast to avoid unpredictable encryption behavior.
		return &enc, fmt.Errorf("invalid AES key length, %d is not accepted: must be 16, 24, or 32 bytes", len(key))
	}
}

// EncryptedJSONEncoderJSONEncoder represents an encrypted JSON encoder
type EncryptedJSONEncoder struct {
	// key is the encryption and decryption key
	// Must be 16, 24, or 32 bytes long for AES-128/192/256
	key []byte
}

// Marshal is an encoding helper method
func (e *EncryptedJSONEncoder) Marshal(value any) (bs []byte, err error) {
	// Marshal the Go value into JSON first
	var marshalled []byte
	if marshalled, err = json.Marshal(value); err != nil {
		return
	}

	// Encrypt the JSON bytes and return them
	return e.encrypt(marshalled)
}

// Unmarshal is a decoding helper method
func (e *EncryptedJSONEncoder) Unmarshal(bs []byte, val any) (err error) {
	// Declare local vars for decrypted data and collected errors
	var (
		jsonBytes []byte
		errs      []error
	)

	// Try to decrypt the provided bytes
	// If decryption fails, fall back to treating bs as raw JSON
	if jsonBytes, err = e.decrypt(bs); err != nil {
		errs = append(errs, err)
		jsonBytes = bs
	}

	// Attempt to unmarshal the decrypted (or raw) JSON bytes into val
	if err = json.Unmarshal(jsonBytes, val); err != nil {
		errs = append(errs, err)
		// Return a joined error if any step failed
		return errors.Join(errs...)
	}

	return nil
}

// encrypt encrypts the provided plaintext bytes using AES-GCM
func (e *EncryptedJSONEncoder) encrypt(in []byte) (out []byte, err error) {
	// Create a new AES block cipher using the provided key
	var b cipher.Block
	if b, err = aes.NewCipher(e.key); err != nil {
		return
	}

	// Create a GCM AEAD cipher for authenticated encryption
	var aead cipher.AEAD
	if aead, err = cipher.NewGCM(b); err != nil {
		return
	}

	// Generate a random nonce of the correct size for this AEAD instance
	nonce := make([]byte, aead.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return
	}

	// Encrypt (Seal) the input data with AES-GCM
	// The output includes both ciphertext and authentication tag
	data := aead.Seal(nil, nonce, in, nil)

	// Concatenate nonce and ciphertext into a single slice for storage
	out = append(out, nonce...)
	out = append(out, data...)
	return
}

// decrypt decrypts bytes produced by encrypt()
func (e *EncryptedJSONEncoder) decrypt(in []byte) (out []byte, err error) {
	// Create an AES block cipher using the same key used for encryption
	var b cipher.Block
	if b, err = aes.NewCipher(e.key); err != nil {
		return nil, err
	}

	// Create a GCM AEAD cipher for authenticated decryption
	var aead cipher.AEAD
	if aead, err = cipher.NewGCM(b); err != nil {
		return nil, err
	}

	// Split the input into nonce and ciphertext
	split := aead.NonceSize()

	// Decrypt and verify the data using AES-GCM
	// If authentication fails, an error is returned
	return aead.Open(nil, in[:split], in[split:], nil)
}
