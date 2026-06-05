package torrent_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiyaverma/qbit-go/internal/torrent"
)

// buildMinimalTorrent constructs the raw bencoded bytes of a valid minimal .torrent.
func buildMinimalTorrent() []byte {
	pieces := make([]byte, 20) // one piece hash
	// bencoded info dict
	info := "d6:lengthi1048576e4:name8:testfile12:piece lengthi262144e6:pieces20:" + string(pieces) + "e"
	// bencoded outer dict
	raw := "d8:announce26:http://tracker.example.com4:info" + info + "e"
	return []byte(raw)
}

func TestParseFileInfoHashNonZero(t *testing.T) {
	data := buildMinimalTorrent()
	tf, err := torrent.ParseFile(data)
	require.NoError(t, err)

	var zero [20]byte
	assert.NotEqual(t, zero, tf.InfoHash, "info hash must be computed from real bytes, not zeros")
}

func TestParseFileFields(t *testing.T) {
	data := buildMinimalTorrent()
	tf, err := torrent.ParseFile(data)
	require.NoError(t, err)

	assert.Equal(t, "http://tracker.example.com", tf.Announce)
	assert.Equal(t, "testfile", tf.Name)
	assert.Equal(t, 262144, tf.PieceLength)
	assert.Equal(t, int64(1048576), tf.Length)
	assert.Len(t, tf.PieceHashes, 1)
}

func TestParseFilePieceCount(t *testing.T) {
	data := buildMinimalTorrent()
	tf, err := torrent.ParseFile(data)
	require.NoError(t, err)
	assert.Equal(t, 1, tf.PieceCount())
}

func TestParseFileMissingInfo(t *testing.T) {
	_, err := torrent.ParseFile([]byte("d8:announce15:http://test.come"))
	assert.Error(t, err)
}
