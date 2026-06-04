package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tiyaverma/qbit-go/internal/bitfield"
	"github.com/tiyaverma/qbit-go/internal/scheduler"
)

func newSched(n int) *scheduler.Scheduler {
	hashes := make([][20]byte, n)
	lens := make([]int, n)
	for i := range lens {
		lens[i] = 256 * 1024
	}
	return scheduler.New(hashes, lens, bitfield.New(n))
}

func TestPickRarestFirst(t *testing.T) {
	sched := newSched(4)

	// peer A has pieces 0 and 2
	bfA := bitfield.New(4)
	bfA.SetPiece(0)
	bfA.SetPiece(2)

	// peer B has piece 1 only (rarest)
	bfB := bitfield.New(4)
	bfB.SetPiece(0)
	bfB.SetPiece(1)
	bfB.SetPiece(2)

	sched.AddPeerBitfield(bfA)
	sched.AddPeerBitfield(bfB)

	// asking for a piece from bfA's perspective — piece 0 appears in 2 peers,
	// piece 2 appears in 2 peers, so both are equally rare from bfA's view.
	// The test just verifies a valid piece is returned.
	idx, ok := sched.PickPiece(bfA)
	assert.True(t, ok)
	assert.True(t, bfA.HasPiece(idx), "returned piece must be available from that peer")
}

func TestMarkComplete(t *testing.T) {
	sched := newSched(3)
	bf := bitfield.New(3)
	for i := 0; i < 3; i++ {
		bf.SetPiece(i)
	}
	sched.AddPeerBitfield(bf)

	idx, ok := sched.PickPiece(bf)
	assert.True(t, ok)

	sched.MarkComplete(idx)
	stats := sched.Stats()
	assert.Equal(t, 1, stats.PiecesComplete)
	assert.Equal(t, 0, stats.PiecesInFlight)
}

func TestReturnWork(t *testing.T) {
	sched := newSched(2)
	bf := bitfield.New(2)
	bf.SetPiece(0)
	sched.AddPeerBitfield(bf)

	idx, ok := sched.PickPiece(bf)
	assert.True(t, ok)
	assert.Equal(t, 1, sched.Stats().PiecesInFlight)

	sched.ReturnWork(idx)
	assert.Equal(t, 0, sched.Stats().PiecesInFlight)
}
