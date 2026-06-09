package tracker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var emptyParams = AnnounceParams{}

// ---- New ----

func TestNew_HTTPScheme(t *testing.T) {
	c, err := New("http://tracker.example.com/announce", emptyParams)
	require.NoError(t, err)
	assert.NotNil(t, c)
	c.Close()
}

func TestNew_HTTPSScheme(t *testing.T) {
	c, err := New("https://tracker.example.com/announce", emptyParams)
	require.NoError(t, err)
	assert.NotNil(t, c)
	c.Close()
}

func TestNew_UDPScheme(t *testing.T) {
	// newUDPClient resolves the host and binds a local UDP socket.
	// Use a numeric host so DNS is not involved.
	c, err := New("udp://127.0.0.1:6881/announce", emptyParams)
	require.NoError(t, err)
	assert.NotNil(t, c)
	c.Close()
}

func TestNew_UnsupportedScheme(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"websocket scheme", "ws://tracker.example.com/announce"},
		{"ftp scheme", "ftp://tracker.example.com/"},
		{"unknown scheme", "xyz://tracker.example.com/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.url, emptyParams)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrUnsupportedTrackerScheme),
				"expected ErrUnsupportedTrackerScheme, got %v", err)
		})
	}
}

// ---- NewMultiTracker ----

func TestNewMultiTracker_EmptyList(t *testing.T) {
	mt := NewMultiTracker(nil, emptyParams)
	require.NotNil(t, mt)
	assert.Len(t, mt.tiers, 0, "empty announce-list should produce zero tiers")

	// Announce on an empty MultiTracker must return ErrAllTrackersFailed.
	_, err := mt.Announce("started")
	assert.True(t, errors.Is(err, ErrAllTrackersFailed))
}

func TestNewMultiTracker_OneValidHTTP(t *testing.T) {
	announceList := [][]string{
		{"http://tracker.example.com/announce"},
	}
	mt := NewMultiTracker(announceList, emptyParams)
	require.NotNil(t, mt)
	assert.Len(t, mt.tiers, 1, "one valid URL should produce one tier")
	assert.Len(t, mt.tiers[0], 1, "tier should contain one client")
	mt.Close()
}

func TestNewMultiTracker_InvalidSchemeSkipped(t *testing.T) {
	// A URL with an unsupported scheme must be silently skipped.
	// If no valid clients remain, no tier is added.
	announceList := [][]string{
		{"ws://bad.example.com/announce"},
	}
	mt := NewMultiTracker(announceList, emptyParams)
	require.NotNil(t, mt)
	assert.Len(t, mt.tiers, 0, "unsupported scheme should result in zero tiers")
}

func TestNewMultiTracker_MultipleTiers(t *testing.T) {
	announceList := [][]string{
		{"http://tier1a.example.com/announce", "http://tier1b.example.com/announce"},
		{"udp://127.0.0.1:6882/announce"},
		{"ws://bad.example.com/announce"}, // invalid — should be skipped entirely
	}
	mt := NewMultiTracker(announceList, emptyParams)
	require.NotNil(t, mt)
	// Only the first two tiers have valid clients.
	assert.Len(t, mt.tiers, 2, "should have exactly 2 valid tiers")
	assert.Len(t, mt.tiers[0], 2, "first tier should have 2 clients")
	assert.Len(t, mt.tiers[1], 1, "second tier should have 1 client")
	mt.Close()
}
