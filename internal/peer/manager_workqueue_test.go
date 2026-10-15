package peer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tiyaverma/qbit-go/internal/scheduler"
	"github.com/tiyaverma/qbit-go/internal/torrent"
)

// testTorrentFile builds a TorrentFile with n pieces and nothing else.
func testTorrentFile(n int) *torrent.TorrentFile {
	return &torrent.TorrentFile{PieceHashes: make([][20]byte, n)}
}

// TestWorkQueueCapacity_CoversEveryPiece is the invariant that makes requeueing
// safe: the channel must be able to hold every piece at once.
func TestWorkQueueCapacity_CoversEveryPiece(t *testing.T) {
	// A torrent larger than the old fixed 256-slot buffer.
	assert.Equal(t, 24736, workQueueCapacity(testTorrentFile(24736)))
	// Small torrents keep a sensible floor.
	assert.Equal(t, 256, workQueueCapacity(testTorrentFile(10)))
	assert.Equal(t, 256, workQueueCapacity(nil))
}

// TestRequeue_DoesNotBlockWhenQueueIsFull reproduces the deadlock that stalled
// downloads: a peer is the only consumer of m.work, so if it blocks while
// putting a piece back, it stops draining the queue and nothing recovers.
// Before the fix this test hangs and fails on the timeout.
func TestRequeue_DoesNotBlockWhenQueueIsFull(t *testing.T) {
	m := &Manager{
		work: make(chan *scheduler.PieceWork, 2),
		quit: make(chan struct{}),
	}
	t.Cleanup(func() { close(m.quit) })

	// Saturate the queue.
	m.work <- &scheduler.PieceWork{Index: 1}
	m.work <- &scheduler.PieceWork{Index: 2}
	require.Len(t, m.work, 2, "queue should be full")

	// Requeueing into a full queue must return immediately.
	done := make(chan struct{})
	go func() {
		m.requeue(&scheduler.PieceWork{Index: 3})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("requeue blocked on a full work queue — peers would deadlock")
	}

	// The piece must not be silently dropped; it lands once there is room.
	<-m.work
	select {
	case w := <-m.work:
		assert.Contains(t, []int{2, 3}, w.Index)
	case <-time.After(2 * time.Second):
		t.Fatal("requeued piece was lost")
	}
}
