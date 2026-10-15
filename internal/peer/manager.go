package peer

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
	"github.com/tiyaverma/qbit-go/internal/ratelimit"
	"github.com/tiyaverma/qbit-go/internal/scheduler"
	"github.com/tiyaverma/qbit-go/internal/torrent"
)

// BlockReader is implemented by storage.Manager and used by the upload handler.
type BlockReader interface {
	ReadBlock(index, begin, length int) ([]byte, error)
}

// ManagerStats is returned by Stats().
type ManagerStats struct {
	Connected int
	Choked    int
	// Uploaded is the current tit-for-tat window only: the choking algorithm
	// resets each peer's counter every round, so it cannot be used as a total.
	Uploaded int64
	// TotalDownloaded and TotalUploaded are cumulative wire bytes for this
	// torrent, never reset. Transfer rates are derived from these.
	TotalDownloaded int64
	TotalUploaded   int64
	// DialFailures counts peers we could not reach. A torrent with many
	// failures and no connections is unreachable, not merely idle.
	DialFailures int64
}

// connEntry tracks a connection plus per-peer upload stats.
type connEntry struct {
	conn     *Conn
	uploaded atomic.Int64 // bytes uploaded to this peer in the current window
}

// Manager maintains the peer connection pool for one torrent.
type Manager struct {
	tf         *torrent.TorrentFile
	ourID      [20]byte
	listenPort int
	bf         bitfield.Bitfield
	sched      *scheduler.Scheduler
	storage    BlockReader
	limiter    *ratelimit.Limiter
	maxConns   int

	mu    sync.RWMutex
	conns map[string]*connEntry // addr → entry

	totalDownloaded atomic.Int64
	totalUploaded   atomic.Int64
	dialFailures    atomic.Int64

	peers   chan []net.TCPAddr
	work    chan *scheduler.PieceWork
	results chan *scheduler.PieceResult
	quit    chan struct{}
}

// NewManager constructs a Manager.
func NewManager(
	tf *torrent.TorrentFile,
	ourID [20]byte,
	listenPort int,
	bf bitfield.Bitfield,
	sched *scheduler.Scheduler,
	stor BlockReader,
	limiter *ratelimit.Limiter,
	maxConns int,
) *Manager {
	return &Manager{
		tf:         tf,
		ourID:      ourID,
		listenPort: listenPort,
		bf:         bf,
		sched:      sched,
		storage:    stor,
		limiter:    limiter,
		maxConns:   maxConns,
		conns:      make(map[string]*connEntry),
		peers:      make(chan []net.TCPAddr, 32),
		work:       make(chan *scheduler.PieceWork, workQueueCapacity(tf)),
		results:    make(chan *scheduler.PieceResult, 64),
		quit:       make(chan struct{}),
	}
}

// WorkQueue returns the channel for incoming PieceWork.
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
	for _, e := range m.conns {
		e.conn.Send(msg)
	}
}

// AcceptInbound takes an already-accepted net.Conn, completes the handshake,
// and registers it in the pool (used for inbound seeding connections).
func (m *Manager) AcceptInbound(c net.Conn) {
	remotePeerID, err := Handshake(c, m.tf.InfoHash, m.ourID)
	if err != nil {
		c.Close()
		return
	}
	addr, _ := c.RemoteAddr().(*net.TCPAddr)
	if addr == nil {
		addr = &net.TCPAddr{}
	}
	conn := newConn(c, m.tf.InfoHash, remotePeerID, *addr)
	conn.BytesIn = &m.totalDownloaded

	m.mu.Lock()
	if len(m.conns) >= m.maxConns {
		m.mu.Unlock()
		conn.Close()
		return
	}
	entry := &connEntry{conn: conn}
	m.conns[addr.String()] = entry
	m.mu.Unlock()

	conn.Send(FormatBitfield(m.bf))
	go m.tryStartPEX(conn)
	go m.serveUpload(entry)
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
			log.Printf("manager: got batch of %d peers from tracker", len(batch))
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
	for _, e := range m.conns {
		e.conn.Close()
	}
	m.mu.Unlock()
}

// Stats returns a snapshot of connection pool state.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	choked := 0
	var uploaded int64
	for _, e := range m.conns {
		if e.conn.Choked {
			choked++
		}
		uploaded += e.uploaded.Load()
	}
	return ManagerStats{
		TotalDownloaded: m.totalDownloaded.Load(),
		TotalUploaded:   m.totalUploaded.Load(),
		DialFailures:    m.dialFailures.Load(),
		Connected:       len(m.conns),
		Choked:          choked,
		Uploaded:        uploaded,
	}
}

func (m *Manager) maybeConnect(addr net.TCPAddr) {
	// Skip loopback and connections to our own listen port (self-connections).
	if addr.IP.IsLoopback() || addr.Port == m.listenPort && isOwnIP(addr.IP) {
		return
	}

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
			m.dialFailures.Add(1)
			return
		}

		m.mu.Lock()
		if len(m.conns) >= m.maxConns {
			m.mu.Unlock()
			c.Close()
			return
		}
		c.BytesIn = &m.totalDownloaded
		entry := &connEntry{conn: c}
		m.conns[addr.String()] = entry
		m.mu.Unlock()

		log.Printf("peer %s: connected, sending bitfield (%d bytes)", c.Addr.String(), len(m.bf))
		c.Send(FormatBitfield(m.bf))
		m.sched.AddPeerBitfield(c.Bitfield)

		// Do not run tryStartPEX here — it reads from c.conn concurrently with
		// downloadFromPeer, causing a race where the Unchoke message gets stolen.
		go m.downloadFromPeer(entry)
	}()
}

// workQueueCapacity sizes the work channel to hold every piece of the torrent.
// This is what makes requeueing safe: a peer that pulls a piece and puts it
// back can never find the channel full, because the pieces buffered plus the
// pieces currently held by peers never exceed the total piece count.
func workQueueCapacity(tf *torrent.TorrentFile) int {
	const min = 256
	if tf == nil || len(tf.PieceHashes) < min {
		return min
	}
	return len(tf.PieceHashes)
}

// requeue returns a piece to the work queue without ever blocking the caller.
//
// This must not block: the peer goroutines are the only consumers of m.work,
// so a peer that blocks while sending to it stops draining it, and the queue
// can never recover. A buffered send is expected to succeed given the channel
// is sized to the piece count; the goroutine fallback keeps that from being a
// correctness requirement.
func (m *Manager) requeue(w *scheduler.PieceWork) {
	select {
	case m.work <- w:
	case <-m.quit:
	default:
		go func() {
			select {
			case m.work <- w:
			case <-m.quit:
			}
		}()
	}
}

func (m *Manager) downloadFromPeer(e *connEntry) {
	c := e.conn
	defer func() {
		m.mu.Lock()
		delete(m.conns, c.Addr.String())
		m.mu.Unlock()
		m.sched.RemovePeerBitfield(c.Bitfield)
		c.Close()
	}()

	log.Printf("peer %s: sending INTERESTED", c.Addr.String())
	c.Send(&Message{ID: MsgInterested})
	c.Interested = true

	// Wait for the peer's bitfield + unchoke before requesting any piece.
	// Otherwise we may request pieces they don't have, which causes RSTs.
	if err := m.awaitReady(c); err != nil {
		log.Printf("peer %s: awaitReady: %v", c.Addr.String(), err)
		return
	}

	misses := 0
	for {
		select {
		case work, ok := <-m.work:
			if !ok {
				return
			}
			// Skip pieces this peer doesn't have; requeue for someone else.
			if len(c.Bitfield) > 0 && !c.Bitfield.HasPiece(work.Index) {
				m.requeue(work)
				// Once we have cycled the entire queue without finding a piece
				// this peer can serve, stop spinning on it. Without this, a
				// peer holding few pieces burns a core rejecting work as fast
				// as the channel can hand it over.
				misses++
				if misses > len(m.work) {
					misses = 0
					select {
					case <-time.After(500 * time.Millisecond):
					case <-m.quit:
						return
					}
				}
				continue
			}
			misses = 0
			data, err := c.Download(work)
			if err != nil {
				log.Printf("manager: download piece %d from %s: %v", work.Index, c.Addr.String(), err)
				m.requeue(work)
				return
			}
			select {
			case m.results <- &scheduler.PieceResult{Index: work.Index, Data: data}:
			case <-m.quit:
				return
			}
		case <-m.quit:
			return
		}
	}
}

// awaitReady blocks until the peer has sent its bitfield AND unchoked us, or
// the deadline expires. Reads any interim messages (Have, Bitfield, Choke,
// Unchoke, Extended) and applies them. Returns once the peer is in a state
// where requesting pieces is safe.
func (m *Manager) awaitReady(c *Conn) error {
	start := time.Now()
	deadline := start.Add(2 * time.Minute)

	// Only a Bitfield establishes which pieces the peer actually holds. A Have
	// announces exactly one piece, so accepting a single Have as "ready" left
	// us believing a full seed owned one piece: we downloaded that piece and
	// then rejected every other one. Haves still populate the bitfield, but a
	// peer that sends them without a bitfield only becomes ready after a short
	// grace period, since BEP 3 requires the bitfield right after the
	// handshake if it is sent at all.
	const bitfieldGrace = 5 * time.Second
	sawBitfield := false
	haves := 0

	for {
		ready := sawBitfield || (haves > 0 && time.Since(start) >= bitfieldGrace)
		if !c.Choked && ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for bitfield+unchoke (choked=%v, sawBitfield=%v, haves=%d)", c.Choked, sawBitfield, haves)
		}
		// Short read deadline so readiness is re-evaluated while the peer is
		// quiet, rather than blocking here for the full two minutes.
		c.conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
		msg, err := Read(c.conn)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue // re-evaluate readiness, then keep waiting
			}
			return err
		}
		if msg == nil {
			continue // keepalive
		}
		switch msg.ID {
		case MsgBitfield:
			c.Bitfield = ParseBitfield(msg)
			sawBitfield = true
		case MsgHave:
			if idx, err := ParseHave(msg); err == nil {
				if c.Bitfield == nil {
					c.Bitfield = make([]byte, (len(m.tf.PieceHashes)+7)/8)
				}
				c.Bitfield.SetPiece(idx)
				haves++
			}
		case MsgUnchoke:
			log.Printf("peer %s: UNCHOKE (awaitReady)", c.Addr.String())
			c.Choked = false
		case MsgChoke:
			log.Printf("peer %s: CHOKE (awaitReady)", c.Addr.String())
			c.Choked = true
		}
	}
}

// serveUpload handles a seeding (upload-only) connection, responding to Requests.
func (m *Manager) serveUpload(e *connEntry) {
	c := e.conn
	defer func() {
		m.mu.Lock()
		delete(m.conns, c.Addr.String())
		m.mu.Unlock()
		c.Close()
	}()

	// Unchoke the peer so they can request blocks from us.
	c.Send(&Message{ID: MsgUnchoke})

	for {
		msg, err := Read(c.conn)
		if err != nil {
			return
		}
		if msg == nil {
			continue // keepalive
		}
		switch msg.ID {
		case MsgRequest:
			if c.Choked {
				continue
			}
			idx, begin, length, err := ParseRequest(msg)
			if err != nil || m.storage == nil {
				continue
			}
			block, err := m.storage.ReadBlock(idx, begin, length)
			if err != nil {
				continue
			}
			c.Send(FormatPiece(idx, begin, block))
			e.uploaded.Add(int64(len(block))) // per-peer window, reset each round
			m.totalUploaded.Add(int64(len(block)))
		case MsgBitfield:
			c.Bitfield = ParseBitfield(msg)
		case MsgHave:
			if idx, err := ParseHave(msg); err == nil {
				c.Bitfield.SetPiece(idx)
			}
		case MsgInterested:
			c.Interested = true
		case MsgNotInterested:
			c.Interested = false
		case MsgExtended:
			if len(msg.Payload) >= 2 && msg.Payload[0] == extHandshakeMsgID {
				_, peerPEXID := parseExtHandshakeIDs(msg.Payload[1:])
				if peerPEXID != 0 {
					// Peer sent their ext handshake; we respond with ours and start PEX.
					c.Send(&Message{
						ID:      MsgExtended,
						Payload: append([]byte{extHandshakeMsgID}, buildOurExtHandshake()...),
					})
					go runPEX(c, &pexState{
						peerPEXExtID: peerPEXID,
						lastKnown:    make(map[string]net.TCPAddr),
					}, m.getConnectedAddrs)
				}
			} else if len(msg.Payload) >= 2 && msg.Payload[0] == ourPEXExtID {
				peers, err := ParsePEXPeers(msg.Payload[1:])
				if err == nil && len(peers) > 0 {
					m.AddPeers(peers)
				}
			}
		}
	}
}

// tryStartPEX sends our extension handshake to a freshly connected peer and,
// if the peer replies supporting ut_pex, starts the periodic PEX goroutine.
func (m *Manager) tryStartPEX(c *Conn) {
	c.Send(&Message{
		ID:      MsgExtended,
		Payload: append([]byte{extHandshakeMsgID}, buildOurExtHandshake()...),
	})
	// The peer's response is handled in the read loop (serveUpload / downloadFromPeer).
	// For download connections the MsgExtended case in Download() will surface it.
	// For PEX to work on outbound download connections, we watch for the response here.
	for {
		select {
		case <-c.quit:
			return
		default:
		}
		msg, err := Read(c.conn)
		if err != nil {
			return
		}
		if msg == nil {
			continue
		}
		if msg.ID == MsgExtended && len(msg.Payload) >= 2 && msg.Payload[0] == extHandshakeMsgID {
			_, peerPEXID := parseExtHandshakeIDs(msg.Payload[1:])
			if peerPEXID != 0 {
				go runPEX(c, &pexState{
					peerPEXExtID: peerPEXID,
					lastKnown:    make(map[string]net.TCPAddr),
				}, m.getConnectedAddrs)
			}
			return
		}
		// If the peer sends a PEX message instead of a handshake first, handle it.
		if msg.ID == MsgExtended && len(msg.Payload) >= 2 && msg.Payload[0] == ourPEXExtID {
			peers, err := ParsePEXPeers(msg.Payload[1:])
			if err == nil && len(peers) > 0 {
				m.AddPeers(peers)
			}
			return
		}
	}
}

// isOwnIP returns true if ip matches any local interface address.
func isOwnIP(ip net.IP) bool {
	ifaces, err := net.Interfaces()
	if err != nil {
		return false
	}
	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			var localIP net.IP
			switch v := a.(type) {
			case *net.IPNet:
				localIP = v.IP
			case *net.IPAddr:
				localIP = v.IP
			}
			if localIP != nil && localIP.Equal(ip) {
				return true
			}
		}
	}
	return false
}

// getConnectedAddrs returns the TCP addresses of all currently connected peers.
func (m *Manager) getConnectedAddrs() []net.TCPAddr {
	m.mu.RLock()
	defer m.mu.RUnlock()
	addrs := make([]net.TCPAddr, 0, len(m.conns))
	for _, e := range m.conns {
		addrs = append(addrs, e.conn.Addr)
	}
	return addrs
}

// regularUnchoke implements tit-for-tat: unchoke the top 3 peers by upload rate,
// choke the rest. Upload rate is measured over the last 10-second window.
func (m *Manager) regularUnchoke() {
	m.mu.Lock()
	defer m.mu.Unlock()

	type scored struct {
		entry    *connEntry
		uploaded int64
	}
	peers := make([]scored, 0, len(m.conns))
	for _, e := range m.conns {
		peers = append(peers, scored{e, e.uploaded.Swap(0)})
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].uploaded > peers[j].uploaded
	})

	const maxUnchoked = 3
	for i, p := range peers {
		if i < maxUnchoked {
			p.entry.conn.Send(&Message{ID: MsgUnchoke})
			p.entry.conn.Choked = false
		} else {
			p.entry.conn.Send(&Message{ID: MsgChoke})
			p.entry.conn.Choked = true
		}
	}
}

func (m *Manager) optimisticUnchoke() {
	m.mu.Lock()
	defer m.mu.Unlock()
	choked := make([]*connEntry, 0)
	for _, e := range m.conns {
		if e.conn.Choked {
			choked = append(choked, e)
		}
	}
	if len(choked) == 0 {
		return
	}
	pick := choked[rand.Intn(len(choked))]
	pick.conn.Send(&Message{ID: MsgUnchoke})
	pick.conn.Choked = false
}
