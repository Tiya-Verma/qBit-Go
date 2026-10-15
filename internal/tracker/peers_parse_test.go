package tracker

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCompactPeers6(t *testing.T) {
	ip := net.ParseIP("2001:db8::1")
	buf := make([]byte, 18)
	copy(buf[0:16], ip.To16())
	binary.BigEndian.PutUint16(buf[16:18], 51413)

	peers, err := parseCompactPeers6(buf)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	assert.True(t, peers[0].IP.Equal(ip))
	assert.Equal(t, 51413, peers[0].Port)

	_, err = parseCompactPeers6([]byte{1, 2, 3})
	assert.Error(t, err, "a truncated list should be rejected")
}

// TestParseDictPeers covers the non-compact response shape. A tracker
// answering in this form previously yielded zero peers.
func TestParseDictPeers(t *testing.T) {
	peers := parseDictPeers([]interface{}{
		map[string]interface{}{"ip": "192.0.2.10", "port": int64(6881)},
		map[string]interface{}{"ip": "2001:db8::2", "port": int64(6969)},
		map[string]interface{}{"ip": "not-an-ip", "port": int64(6881)}, // skipped
		map[string]interface{}{"ip": "192.0.2.11", "port": int64(0)},   // skipped
		"garbage", // skipped
	})
	require.Len(t, peers, 2)
	assert.Equal(t, "192.0.2.10:6881", peers[0].String())
	assert.Equal(t, 6969, peers[1].Port)
}
