package scheduler

import (
	"math"
	"sync"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
)

// Mode controls which piece selection algorithm is active.
type Mode int

const (
	ModeRarestFirst Mode = iota
	ModeSequential
	ModeEndGame
)

// PieceWork is a unit of work sent to a peer goroutine.
type PieceWork struct {
	Index  int
	Hash   [20]byte
	Length int
}

// PieceResult carries verified piece data back from a peer.
type PieceResult struct {
	Index int
	Data  []byte
}

// Priority controls download order within multi-file torrents.
type Priority int

const (
	PrioritySkip   Priority = 0
	PriorityLow    Priority = 1
	PriorityNormal Priority = 2
	PriorityHigh   Priority = 3
)

// Stats is exposed to the API and WebSocket feed.
type Stats struct {
	PiecesTotal    int
	PiecesComplete int
	PiecesInFlight int
	Mode           Mode
}

// Scheduler selects which piece to download next.
type Scheduler struct {
	pieceCount  int
	pieceHashes [][20]byte
	pieceLens   []int
	priorities  []Priority

	mu        sync.Mutex
	frequency []int
	have      bitfield.Bitfield
	inFlight  map[int]bool
	mode      Mode
}

// New creates a Scheduler for a torrent.
func New(pieceHashes [][20]byte, pieceLens []int, have bitfield.Bitfield) *Scheduler {
	n := len(pieceHashes)
	priorities := make([]Priority, n)
	for i := range priorities {
		priorities[i] = PriorityNormal
	}
	return &Scheduler{
		pieceCount:  n,
		pieceHashes: pieceHashes,
		pieceLens:   pieceLens,
		priorities:  priorities,
		frequency:   make([]int, n),
		have:        have.Clone(),
		inFlight:    make(map[int]bool),
		mode:        ModeRarestFirst,
	}
}

// SetMode switches the piece selection algorithm.
func (s *Scheduler) SetMode(m Mode) {
	s.mu.Lock()
	s.mode = m
	if m == ModeEndGame {
		s.inFlight = make(map[int]bool)
	}
	s.mu.Unlock()
}

// AddPeerBitfield increments frequency counts for each piece a peer has.
func (s *Scheduler) AddPeerBitfield(bf bitfield.Bitfield) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < s.pieceCount; i++ {
		if bf.HasPiece(i) {
			s.frequency[i]++
		}
	}
}

// RemovePeerBitfield decrements frequency counts when a peer disconnects.
func (s *Scheduler) RemovePeerBitfield(bf bitfield.Bitfield) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < s.pieceCount; i++ {
		if bf.HasPiece(i) && s.frequency[i] > 0 {
			s.frequency[i]--
		}
	}
}

// PickPiece selects the next piece to request from a peer.
// Returns (index, true) on success or (0, false) if no eligible piece exists.
func (s *Scheduler) PickPiece(peerBitfield bitfield.Bitfield) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.mode {
	case ModeSequential:
		return s.pickSequential(peerBitfield)
	default:
		return s.pickRarest(peerBitfield)
	}
}

func (s *Scheduler) pickRarest(peerBF bitfield.Bitfield) (int, bool) {
	rarest := -1
	rarestFreq := math.MaxInt

	for i := 0; i < s.pieceCount; i++ {
		if s.have.HasPiece(i) {
			continue
		}
		if s.inFlight[i] {
			continue
		}
		if !peerBF.HasPiece(i) {
			continue
		}
		if s.priorities[i] == PrioritySkip {
			continue
		}
		if s.frequency[i] < rarestFreq {
			rarestFreq = s.frequency[i]
			rarest = i
		}
	}

	if rarest == -1 {
		return 0, false
	}
	s.inFlight[rarest] = true
	return rarest, true
}

func (s *Scheduler) pickSequential(peerBF bitfield.Bitfield) (int, bool) {
	for i := 0; i < s.pieceCount; i++ {
		if s.have.HasPiece(i) {
			continue
		}
		if s.inFlight[i] {
			continue
		}
		if !peerBF.HasPiece(i) {
			continue
		}
		if s.priorities[i] == PrioritySkip {
			continue
		}
		s.inFlight[i] = true
		return i, true
	}
	return 0, false
}

// MarkComplete records that a piece has been fully downloaded and verified.
func (s *Scheduler) MarkComplete(index int) {
	s.mu.Lock()
	s.have.SetPiece(index)
	delete(s.inFlight, index)
	s.mu.Unlock()
}

// ReturnWork puts a piece back on the queue (e.g., after a peer disconnect).
func (s *Scheduler) ReturnWork(index int) {
	s.mu.Lock()
	delete(s.inFlight, index)
	s.mu.Unlock()
}

// IsEndGame reports whether fewer than 20 pieces remain.
func (s *Scheduler) IsEndGame() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	remaining := 0
	for i := 0; i < s.pieceCount; i++ {
		if !s.have.HasPiece(i) && s.priorities[i] != PrioritySkip {
			remaining++
		}
	}
	return remaining < 20
}

// SetPriority updates the download priority for a piece.
func (s *Scheduler) SetPriority(index int, p Priority) {
	s.mu.Lock()
	if index >= 0 && index < s.pieceCount {
		s.priorities[index] = p
	}
	s.mu.Unlock()
}

// Stats returns a snapshot of current scheduler state.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	complete := s.have.CountComplete()
	return Stats{
		PiecesTotal:    s.pieceCount,
		PiecesComplete: complete,
		PiecesInFlight: len(s.inFlight),
		Mode:           s.mode,
	}
}

// WorkQueue builds a channel pre-populated with all outstanding PieceWork.
func (s *Scheduler) WorkQueue() chan *PieceWork {
	s.mu.Lock()
	defer s.mu.Unlock()

	var work []*PieceWork
	for i := 0; i < s.pieceCount; i++ {
		if s.have.HasPiece(i) || s.priorities[i] == PrioritySkip {
			continue
		}
		work = append(work, &PieceWork{
			Index:  i,
			Hash:   s.pieceHashes[i],
			Length: s.pieceLens[i],
		})
	}
	ch := make(chan *PieceWork, len(work))
	for _, w := range work {
		ch <- w
	}
	return ch
}
