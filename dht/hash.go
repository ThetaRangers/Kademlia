package dht

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
)

const (
	HashSize   = 160
	BucketSize = HashSize / 8
)

type Hash []byte

func (h Hash) Redacted() interface{} {
	if len(h) == 0 {
		return h
	}

	return hex.EncodeToString(h)[:16]
}

func NewHash(val []byte) Hash {
	h := sha1.New()

	h.Write(val)

	return h.Sum(nil)[:BucketSize]
}

func NewRandomHash() Hash {
	res := make([]byte, BucketSize)

	_, err := rand.Read(res)
	if err != nil {
		return nil
	}

	return res[:BucketSize]
}
