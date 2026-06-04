package peer_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiyaverma/qbit-go/internal/peer"
)

func TestMessageRoundtrip(t *testing.T) {
	msg := peer.FormatHave(42)
	data := msg.Serialize()

	got, err := peer.Read(bytes.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, peer.MsgHave, got.ID)

	idx, err := peer.ParseHave(got)
	require.NoError(t, err)
	assert.Equal(t, 42, idx)
}

func TestRequestMessage(t *testing.T) {
	msg := peer.FormatRequest(5, 16384, 16384)
	idx, begin, length, err := peer.ParseRequest(msg)
	require.NoError(t, err)
	assert.Equal(t, 5, idx)
	assert.Equal(t, 16384, begin)
	assert.Equal(t, 16384, length)
}

func TestKeepalive(t *testing.T) {
	keepalive := make([]byte, 4) // zero-length message
	got, err := peer.Read(bytes.NewReader(keepalive))
	require.NoError(t, err)
	assert.Nil(t, got, "keepalive should return nil message")
}
