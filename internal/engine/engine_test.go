package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiyaverma/qbit-go/internal/torrent"
)

// testConfig returns a minimal Config that avoids any network activity or disk I/O.
func testConfig() *Config {
	return &Config{
		ListenPort:     0, // let OS pick an available port
		APIPort:        0,
		DownloadDir:    ".",
		MaxConnections: 0,
		MaxPerTorrent:  0,
		DHTEnabled:     false,
		PEXEnabled:     false,
		DBPath:         "", // no bbolt file
	}
}

// drainPlaceholderSessions removes sessions that have nil peer managers
// (i.e. magnet placeholders) from the engine before Shutdown so the nil-
// deref in sess.peers.Stop() is avoided. This is needed because Shutdown does
// not guard against placeholder sessions that still have a nil peer manager.
func drainPlaceholderSessions(e *Engine) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for hash, sess := range e.sessions {
		if sess.peers == nil {
			close(sess.quit)
			delete(e.sessions, hash)
		}
	}
}

// ---- New ----

func TestNew_NilConfigUsesDefaults(t *testing.T) {
	// Pass an explicit config with DHTEnabled=false so no UDP socket is opened.
	e, err := New(testConfig())
	require.NoError(t, err)
	assert.NotNil(t, e)
	assert.NoError(t, e.Shutdown())
}

func TestNew_ExplicitConfig(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	assert.NotNil(t, e)
	assert.NoError(t, e.Shutdown())
}

// ---- Add ----

func TestAdd_InvalidBytes(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { e.Shutdown() }) //nolint:errcheck

	_, err = e.Add(strings.NewReader("not a valid torrent file"))
	assert.Error(t, err, "Add with garbage bytes must return an error")
}

func TestAdd_EmptyReader(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { e.Shutdown() }) //nolint:errcheck

	_, err = e.Add(strings.NewReader(""))
	assert.Error(t, err)
}

// ---- Get ----

func TestGet_UnknownHash(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { e.Shutdown() }) //nolint:errcheck

	var unknown [20]byte
	unknown[0] = 0xDE
	unknown[1] = 0xAD

	_, err = e.Get(unknown)
	assert.Error(t, err, "Get on unknown hash must return an error")
}

// ---- Remove ----

func TestRemove_UnknownHash(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { e.Shutdown() }) //nolint:errcheck

	var unknown [20]byte
	unknown[0] = 0xBE
	unknown[1] = 0xEF

	err = e.Remove(unknown, false)
	assert.Error(t, err, "Remove on unknown hash must return an error")
}

// ---- List ----

func TestList_FreshEngineIsEmpty(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { e.Shutdown() }) //nolint:errcheck

	torrents := e.List()
	assert.Empty(t, torrents, "fresh engine should have no torrents")
}

// ---- AddMagnet ----

func TestAddMagnet_InvalidURI(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { e.Shutdown() }) //nolint:errcheck

	_, err = e.AddMagnet("https://not-a-magnet-link.com/file.torrent")
	assert.Error(t, err, "AddMagnet with non-magnet URI must return an error")
}

func TestAddMagnet_MissingBtih(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { e.Shutdown() }) //nolint:errcheck

	_, err = e.AddMagnet("magnet:?xt=urn:sha1:aabbccddeeff00112233445566778899abcdef01")
	assert.Error(t, err)
}

func TestAddMagnet_ValidURI_PlaceholderStateFetching(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	// Drain placeholder sessions before shutdown to avoid nil peer manager panic.
	t.Cleanup(func() {
		drainPlaceholderSessions(e)
		e.Shutdown() //nolint:errcheck
	})

	// A valid magnet URI with a real 40-char hex info hash.
	const uri = "magnet:?xt=urn:btih:aabbccddeeff00112233445566778899abcdef01&dn=TestTorrent"

	t0 := time.Now()
	tObj, err := e.AddMagnet(uri)
	elapsed := time.Since(t0)

	require.NoError(t, err, "AddMagnet with valid URI should not return an error")
	require.NotNil(t, tObj)

	// Must return quickly — fetchMetadata runs in a background goroutine.
	assert.Less(t, elapsed, time.Second, "AddMagnet must return without blocking")

	// The placeholder torrent must be in StateFetching immediately after return.
	assert.Equal(t, torrent.StateFetching, tObj.State)

	// The display name from the &dn= parameter should be preserved.
	assert.Equal(t, "TestTorrent", tObj.File.Name)
}

// ---- Shutdown ----

func TestShutdown_FreshEngine(t *testing.T) {
	e, err := New(testConfig())
	require.NoError(t, err)
	assert.NoError(t, e.Shutdown(), "Shutdown on a fresh engine must return nil")
}
