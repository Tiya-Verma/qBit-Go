package bencode_test

import (
	"crypto/sha1"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiyaverma/qbit-go/internal/bencode"
)

func TestFindInfoBytes(t *testing.T) {
	// Construct a minimal .torrent-like bencoded dict:
	// d8:announce15:http://test.com4:infod4:name4:test12:piece lengthi262144e6:pieces20:xxxxxxxxxxxxxxxxxxxx6:lengthi1048576eee
	infoRaw := "d4:name4:test12:piece lengthi262144e6:pieces20:" + string(make([]byte, 20)) + "6:lengthi1048576ee"
	torrentRaw := "d8:announce15:http://test.com4:info" + infoRaw + "e"

	got, err := bencode.FindInfoBytes([]byte(torrentRaw))
	require.NoError(t, err)
	assert.Equal(t, []byte(infoRaw), got)

	// The SHA1 of those raw bytes is what torrents use as the info hash.
	h := sha1.Sum(got)
	assert.NotEqual(t, [20]byte{}, h, "info hash must be non-zero")
}

func TestFindInfoBytesNotFound(t *testing.T) {
	_, err := bencode.FindInfoBytes([]byte("d3:foo3:bare"))
	assert.ErrorContains(t, err, "info")
}

func TestFindInfoBytesNotDict(t *testing.T) {
	_, err := bencode.FindInfoBytes([]byte("li1ee"))
	assert.Error(t, err)
}
