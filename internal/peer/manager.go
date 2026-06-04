package peer

import (
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
	"github.com/tiyaverma/qbit-go/internal/ratelimit"
	"github.com/tiyaverma/qbit-go/internal/scheduler"
	"github.com/tiyaverma/qbit-go/internal/torrent"
)

// ManagerStats is returned by Stats().
type ManagerStats struct {
	Connected    int
	Choked       int
	Downloading  int
	Downloaded   int64
	Uploaded     int64
	DownloadRate int64
	UploadRate   int64
}

// Manager maintains the peer connection pool for one torrent.
type Manager struct {
	tf       *torrent.TorrentFile
	ourID    [20]byte
	bf       bitfield.Bitfield
	sched    *scheduler.Scheduler
	limiter  *ratelimit.Limiter
	maxConns int

	mu    sync.RWMutex
	conns map[string]*Conn // addr → Conn

	peers   chan []net.TCPAddr
	work    chan *scheduler.PieceWork
	results chan *scheduler.PieceResult
	quit    chan struct{}
}

// NewManager constructs a Manager. workQueue and results must be set up by the caller.
func NewManager(
	tf *torrent.TorrentFile,
	ourID [20]byte,
	bf bitfield.Bitfield,
	sched *scheduler.Scheduler,
	limiter *ratelimit.Limiter,
	maxConns int,
) *Manager {
	return &Manager{
		tf:       tf,
		ourID:    ourID,
		bf:       bf,
		sched:    sched,
		limiter:  limiter,
		maxConns: maxConns,
		conns:    make(map[string]*Conn),
		peers:    make(chan []net.TCPAddr, 32),
		work:     make(chan *scheduler.PieceWork, 256),
		results:  make(chan *scheduler.PieceResult, 64),
		quit:     make(chan struct{}),
	}
}

// WorkQueue returns the channel that should be loaded with PieceWork.
func (m *Manager) WorkQueue() chan<- *scheduler.PieceWork { return m.work }

// Results returns the channel that delivers completed PieceResults.
func (m *Manager) Results() <-chan *scheduler.PieceResult { return m.results }

// AddPeers feeds a batch of peer addresses into the manager.
func (m *Manager) AddPeers(peers []net.TCPAddr) {
	select {
	case m.peers <- peers:
	case <-m.quit:
	}
}

// BroadcastHave sends a Have message for pieceIndex to all connected peers.
func (m *Manager) BroadcastHave(index int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	msg := FormatHave(index)
	for _, c := range m.conns {
		c.Send(msg)
	}
}

// Run is the manager's main loop; call it in a goroutine.
func (m *Manager) Run() {
	chokeTicker10 := time.NewTicker(10 * time.Second)
	chokeTicker30 := time.NewTicker(30 * time.Second)
	defer chokeTicker10.Stop()
	defer chokeTicker30.Stop()

	for {
		select {
		case batch := <-m.peers:
			for _, addr := range batch {
				m.maybeConnect(addr)
			}
		case <-chokeTicker10.C:
			m.regularUnchoke()
		case <-chokeTicker30.C:
			m.optimisticUnchoke()
		case <-m.quit:
			return
		}
	}
}

// Stop signals the manager and all connections to shut down.
func (m *Manager) Stop() {
	select {
	case <-m.quit:
	default:
		close(m.quit)
	}
	m.mu.Lock()
	for _, c := range m.conns {
		c.Close()
	}
	m.mu.Unlock()
}

// Stats returns a snapshot of connection pool state.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	choked := 0
	for _, c := range m.conns {
		if c.Choked {
			choked++
		}
	}
	return ManagerStats{
		Connected: len(m.conns),
		Choked:    choked,
	}
}

func (m *Manager) maybeConnect(addr net.TCPAddr) {
	m.mu.RLock()
	_, exists := m.conns[addr.String()]
	count := len(m.conns)
	m.mu.RUnlock()

	if exists || count >= m.maxConns {
		return
	}

	go func() {
		c, err := Dial(addr, m.tf.InfoHash, m.ourID)
		if err != nil {
			log.Printf("manager: dial %s: %v", addr.String(), err)
			return
		}

		m.mu.Lock()
		if len(m.conns) >= m.maxConns {
			m.mu.Unlock()
			c.Close()
			return
		}
		m.conns[addr.String()] = c
		m.mu.Unlock()

		m.sched.AddPeerBitfield(c.Bitfield)
		go m.downloadFromPeer(c)
	}()
}

func (m *Manager) downloadFromPeer(c *Conn) {
	defer func() {
		m.mu.Lock()
		delete(m.conns, c.Addr.String())
		m.mu.Unlock()
		m.sched.RemovePeerBitfield(c.Bitfield)
		c.Close()
	}()

	for {
		// send Interested + wait for Unchoke
		c.Send(&Message{ID: MsgInterested})
		c.Interested = true

		work, ok := <-m.work
		if !ok {
			return
		}

		data, err := c.Download(work)
		if err != nil {
			log.Printf("manager: download piece %d from %s: %v", work.Index, c.Addr.String(), err)
			m.work <- work // return to queue
			return
		}

		select {
		case m.results <- &scheduler.PieceResult{Index: work.Index, Data: data}:
		case <-m.quit:
			return
		}
	}
}

func (m *Manager) regularUnchoke() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Simplified: unchoke all connected peers
	// Production: rank by upload rate and unchoke top N
	for _, c := range m.conns {
		c.Send(&Message{ID: MsgUnchoke})
		c.Choked = false
	}
}

func (m *Manager) optimisticUnchoke() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Pick one random choked peer to unchoke
	choked := make([]*Conn, 0)
	for _, c := range m.conns {
		if c.Choked {
			choked = append(choked, c)
		}
	}
	if len(choked) == 0 {
		return
	}
	pick := choked[rand.Intn(len(choked))]
	pick.Send(&Message{ID: MsgUnchoke})
	pick.Choked = false
}
