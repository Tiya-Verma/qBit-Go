package magnet

import (
	"encoding/base32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// knownHash is a fixed 20-byte info hash used across multiple test cases.
var knownHash = [20]byte{
	0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11,
	0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
	0xab, 0xcd, 0xef, 0x01,
}

// knownHex is the 40-char lowercase hex encoding of knownHash.
const knownHex = "aabbccddeeff00112233445566778899abcdef01"

// knownBase32 is the 32-char base32 encoding of knownHash (uppercase, no padding).
var knownBase32 = base32.StdEncoding.EncodeToString(knownHash[:])

func TestParse_ValidHexHash(t *testing.T) {
	uri := "magnet:?xt=urn:btih:" + knownHex
	m, err := Parse(uri)
	require.NoError(t, err)
	assert.Equal(t, knownHash, m.InfoHash)
}

func TestParse_ValidBase32Hash(t *testing.T) {
	// base32 hash must be 32 chars (160 bits / 5 bits per char).
	require.Len(t, knownBase32, 32, "test invariant: base32 should be 32 chars")
	uri := "magnet:?xt=urn:btih:" + knownBase32
	m, err := Parse(uri)
	require.NoError(t, err)
	assert.Equal(t, knownHash, m.InfoHash, "base32 and hex should decode to the same info hash")
}

func TestParse_DisplayName(t *testing.T) {
	uri := "magnet:?xt=urn:btih:" + knownHex + "&dn=Ubuntu+22.04"
	m, err := Parse(uri)
	require.NoError(t, err)
	assert.Equal(t, "Ubuntu 22.04", m.Name)
}

func TestParse_MultipleTrackers(t *testing.T) {
	uri := "magnet:?xt=urn:btih:" + knownHex + "&tr=http://a.example.com/announce&tr=http://b.example.com/announce"
	m, err := Parse(uri)
	require.NoError(t, err)
	require.Len(t, m.Trackers, 2)
	assert.Equal(t, "http://a.example.com/announce", m.Trackers[0])
	assert.Equal(t, "http://b.example.com/announce", m.Trackers[1])
}

func TestParse_Errors(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{
			name: "not a magnet URI",
			uri:  "https://example.com/file.torrent",
		},
		{
			name: "missing urn:btih",
			uri:  "magnet:?xt=urn:sha1:aabbccddeeff00112233445566778899abcdef01",
		},
		{
			name: "invalid hash length — too short",
			uri:  "magnet:?xt=urn:btih:aabbccdd11",
		},
		{
			name: "bad hex encoding",
			uri:  "magnet:?xt=urn:btih:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.uri)
			assert.Error(t, err, "expected an error for %q", tc.uri)
		})
	}
}

func TestParse_EmptyTrackers(t *testing.T) {
	// No &tr= params → Trackers should be nil/empty.
	uri := "magnet:?xt=urn:btih:" + knownHex
	m, err := Parse(uri)
	require.NoError(t, err)
	assert.Empty(t, m.Trackers)
}

func TestParse_EmptyName(t *testing.T) {
	// No &dn= param → Name should be empty string.
	uri := "magnet:?xt=urn:btih:" + knownHex
	m, err := Parse(uri)
	require.NoError(t, err)
	assert.Empty(t, m.Name)
}
