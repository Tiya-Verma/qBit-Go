package engine

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
	"github.com/tiyaverma/qbit-go/internal/dht"
	"github.com/tiyaverma/qbit-go/internal/magnet"
	"github.com/tiyaverma/qbit-go/internal/peer"
	"github.com/tiyaverma/qbit-go/internal/ratelimit"
	"github.com/tiyaverma/qbit-go/internal/scheduler"
	"github.com/tiyaverma/qbit-go/internal/storage"
	"github.com/tiyaverma/qbit-go/internal/torrent"
	"github.com/tiyaverma/qbit-go/internal/tracker"
)

// EngineStats aggregates metrics across all active torrents.
type EngineStats struct {
	ActiveTorrents  int   `json:"activeTorrents"`
	SeedingTorrents int   `json:"seedingTorrents"`
	TotalDownSpeed  int64 `json:"totalDownSpeed"`
	TotalUpSpeed    int64 `json:"totalUpSpeed"`
	TotalDownloaded int64 `json:"totalDownloaded"`
	TotalUploaded   int64 `json:"totalUploaded"`
	DHTPeers        int   `json:"dhtPeers"`
	ListenPort      int   `json:"listenPort"`
}

// session holds all runtime objects for a single active torrent.
type session struct {
	t       *torrent.Torrent
	storage *storage.Manager
	sched   *scheduler.Scheduler
	peers   *peer.Manager
	tracker *tracker.MultiTracker
	quit    chan struct{}
}

// Engine is the top-level coordinator that owns all active torrents.
type Engine struct {
	mu       sync.RWMutex
	sessions map[[20]byte]*session

	db      *bolt.DB
	dht     *dht.Node
	limiter *ratelimit.Limiter
	cfg     *Config
	peerID  [20]byte
}

// New creates an Engine with the given Config.
func New(cfg *Config) (*Engine, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	var peerID [20]byte
	copy(peerID[:8], "-QB0100-")
	rand.Read(peerID[8:]) //nolint:errcheck

	e := &Engine{
		sessions: make(map[[20]byte]*session),
		limiter:  ratelimit.New(cfg.GlobalDownSpeed, cfg.GlobalUpSpeed),
		cfg:      cfg,
		peerID:   peerID,
	}

	if cfg.DBPath != "" {
		db, err := openDB(cfg.DBPath)
		if err != nil {
			return nil, fmt.Errorf("engine: open db: %w", err)
		}
		e.db = db
	}

	if cfg.DHTEnabled {
		node, err := dht.New(cfg.ListenPort)
		if err != nil {
			return nil, fmt.Errorf("engine: start DHT: %w", err)
		}
		if err := node.Bootstrap(); err != nil {
			return nil, fmt.Errorf("engine: DHT bootstrap: %w", err)
		}
		e.dht = node
	}

	return e, nil
}

// Add parses a .torrent file from r and starts downloading.
func (e *Engine) Add(r io.Reader) (*torrent.Torrent, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	tf, err := torrent.ParseFile(data)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	if _, exists := e.sessions[tf.InfoHash]; exists {
		e.mu.Unlock()
		return nil, fmt.Errorf("engine: torrent already added")
	}
	e.mu.Unlock()

	addedAt := time.Now()
	t, err := e.startSession(tf, data, bitfield.New(tf.PieceCount()), e.cfg.DownloadDir, addedAt)
	if err != nil {
		return nil, err
	}
	e.dbPutTorrent(tf.InfoHash, data, e.cfg.DownloadDir, addedAt)
	return t, nil
}

// AddMagnet parses a magnet URI and starts fetching metadata via DHT + BEP 9.
// Returns immediately with a placeholder Torrent in StateFetching; the torrent
// transitions to StateDownloading once the info dict has been retrieved.
func (e *Engine) AddMagnet(uri string) (*torrent.Torrent, error) {
	m, err := magnet.Parse(uri)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	if _, exists := e.sessions[m.InfoHash]; exists {
		e.mu.Unlock()
		return nil, fmt.Errorf("engine: torrent already added")
	}
	t := &torrent.Torrent{
		File:    torrent.TorrentFile{InfoHash: m.InfoHash, Name: m.Name},
		State:   torrent.StateFetching,
		AddedAt: time.Now(),
	}
	placeholder := &session{t: t, quit: make(chan struct{})}
	e.sessions[m.InfoHash] = placeholder
	e.mu.Unlock()

	go e.fetchMetadata(placeholder, m)
	return t, nil
}

// fetchMetadata runs in a goroutine: continuously discovers peers and tries
// FetchMetadata against them in parallel until one succeeds or the budget
// expires. Then promotes the placeholder session to a full download session.
func (e *Engine) fetchMetadata(placeholder *session, m *magnet.Magnet) {
	const totalBudget = 5 * time.Minute
	const maxWorkers = 20
	const rediscoverInterval = 20 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), totalBudget)
	defer cancel()
	go func() {
		select {
		case <-placeholder.quit:
			cancel()
		case <-ctx.Done():
		}
	}()

	peerCh := make(chan net.TCPAddr, 256)
	resultCh := make(chan []byte, 1)

	var triedMu sync.Mutex
	tried := make(map[string]bool)

	// Discovery loop: re-query DHT + trackers periodically and feed new peers
	// into peerCh. Closes peerCh when ctx is done so workers can exit.
	go func() {
		defer close(peerCh)
		for {
			peers := e.peersForMagnet(m)
			newCount := 0
			for _, p := range peers {
				key := p.String()
				triedMu.Lock()
				if tried[key] {
					triedMu.Unlock()
					continue
				}
				tried[key] = true
				triedMu.Unlock()
				newCount++
				select {
				case peerCh <- p:
				case <-ctx.Done():
					return
				}
			}
			log.Printf("engine: magnet %x: discovered %d new peer(s) (total tried: %d)", m.InfoHash, newCount, len(tried))
			select {
			case <-time.After(rediscoverInterval):
			case <-ctx.Done():
				return
			}
		}
	}()

	// Workers: pull peers off the channel and try FetchMetadata until one wins.
	var wg sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range peerCh {
				if ctx.Err() != nil {
					return
				}
				data, err := peer.FetchMetadata(addr, m.InfoHash, e.peerID)
				if err != nil {
					log.Printf("engine: magnet %x: metadata from %s: %v", m.InfoHash, addr.String(), err)
					continue
				}
				select {
				case resultCh <- data:
					cancel()
				default:
				}
				return
			}
		}()
	}

	var infoBytes []byte
	select {
	case infoBytes = <-resultCh:
		log.Printf("engine: magnet %x: metadata fetched (%d bytes)", m.InfoHash, len(infoBytes))
	case <-ctx.Done():
		log.Printf("engine: magnet %x: metadata fetch timed out after %s", m.InfoHash, totalBudget)
		placeholder.t.State = torrent.StateError
		return
	}

	tf, err := torrent.ParseInfoDict(infoBytes)
	if err != nil {
		log.Printf("engine: magnet %x: parse info dict: %v", m.InfoHash, err)
		placeholder.t.State = torrent.StateError
		return
	}
	if len(m.Trackers) > 0 {
		tf.AnnounceList = [][]string{m.Trackers}
	}

	// Check the torrent hasn't been removed while we were fetching.
	e.mu.RLock()
	current, ok := e.sessions[m.InfoHash]
	e.mu.RUnlock()
	if !ok || current != placeholder {
		return
	}

	// Remove placeholder so startSession can insert the real session.
	e.mu.Lock()
	delete(e.sessions, m.InfoHash)
	e.mu.Unlock()

	addedAt := placeholder.t.AddedAt
	bf := bitfield.New(tf.PieceCount())
	_, err = e.startSessionReusing(placeholder.t, tf, infoBytes, bf, e.cfg.DownloadDir, addedAt)
	if err != nil {
		log.Printf("engine: magnet %x: start session: %v", m.InfoHash, err)
		// Re-insert placeholder in error state so clients can observe the failure.
		placeholder.t.State = torrent.StateError
		e.mu.Lock()
		e.sessions[m.InfoHash] = placeholder
		e.mu.Unlock()
		return
	}

	wrapped := torrent.WrapInfoDict(infoBytes, tf.AnnounceList)
	e.dbPutTorrent(tf.InfoHash, wrapped, e.cfg.DownloadDir, addedAt)
}

// peersForMagnet collects peer addresses via DHT and the magnet's tracker URLs
// concurrently. DHT iterative lookups are slow (cold routing tables), so we run
// it in parallel with the tracker announce instead of sequentially.
func (e *Engine) peersForMagnet(m *magnet.Magnet) []net.TCPAddr {
	var (
		mu       sync.Mutex
		peers    []net.TCPAddr
		wg       sync.WaitGroup
	)

	if e.dht != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			udpPeers, err := e.dht.FindPeers(m.InfoHash)
			if err != nil {
				return
			}
			mu.Lock()
			for _, p := range udpPeers {
				peers = append(peers, net.TCPAddr{IP: p.IP, Port: p.Port})
			}
			mu.Unlock()
		}()
	}

	if len(m.Trackers) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			params := tracker.AnnounceParams{
				InfoHash: m.InfoHash,
				PeerID:   e.peerID,
				Port:     uint16(e.cfg.ListenPort),
				Left:     1 << 40, // unknown size; signal we're downloading, not seeding
			}
			mt := tracker.NewMultiTracker([][]string{m.Trackers}, params)
			defer mt.Close()
			resp, err := mt.Announce("started")
			if err != nil || resp == nil {
				return
			}
			mu.Lock()
			peers = append(peers, resp.Peers...)
			mu.Unlock()
		}()
	}

	wg.Wait()
	return peers
}

// startSessionReusing is like startSession but updates an existing *Torrent in-place
// instead of allocating a new one. Used by the magnet flow to preserve the pointer
// returned from AddMagnet.
func (e *Engine) startSessionReusing(t *torrent.Torrent, tf *torrent.TorrentFile, _ []byte, bf bitfield.Bitfield, dir string, addedAt time.Time) (*torrent.Torrent, error) {
	if dir == "" {
		dir = e.cfg.DownloadDir
	}

	stor := storage.NewManager(tf, dir)
	if err := stor.Init(); err != nil {
		return nil, fmt.Errorf("engine: init storage: %w", err)
	}

	pieceLens := make([]int, tf.PieceCount())
	for i := range pieceLens {
		pieceLens[i] = tf.PieceSize(i)
	}
	sched := scheduler.New(tf.PieceHashes, pieceLens, bf)

	params := tracker.AnnounceParams{
		InfoHash: tf.InfoHash,
		PeerID:   e.peerID,
		Port:     uint16(e.cfg.ListenPort),
		Left:     tf.TotalLength(),
	}
	mt := tracker.NewMultiTracker(tf.AnnounceList, params)
	mgr := peer.NewManager(tf, e.peerID, e.cfg.ListenPort, bf, sched, stor, e.limiter, e.cfg.MaxPerTorrent)

	// Update the existing Torrent object in-place.
	t.File = *tf
	t.Bitfield = bf
	t.DownloadDir = dir
	t.AddedAt = addedAt

	sess := &session{
		t:       t,
		storage: stor,
		sched:   sched,
		peers:   mgr,
		tracker: mt,
		quit:    make(chan struct{}),
	}

	e.mu.Lock()
	e.sessions[tf.InfoHash] = sess
	e.mu.Unlock()

	go e.runSession(sess)
	return t, nil
}

// Remove stops and optionally deletes a torrent's files.
func (e *Engine) Remove(infoHash [20]byte, deleteFiles bool) error {
	e.mu.Lock()
	sess, ok := e.sessions[infoHash]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("engine: torrent not found")
	}
	delete(e.sessions, infoHash)
	e.mu.Unlock()

	close(sess.quit)
	if sess.peers != nil {
		sess.peers.Stop()
	}
	if sess.storage != nil {
		sess.storage.Close()
	}
	e.dbDeleteTorrent(infoHash)
	return nil
}

// Pause suspends activity for a torrent without removing it.
func (e *Engine) Pause(infoHash [20]byte) error {
	e.mu.RLock()
	sess, ok := e.sessions[infoHash]
	e.mu.RUnlock()
	if !ok {
		return fmt.Errorf("engine: torrent not found")
	}
	sess.t.State = torrent.StatePaused
	sess.peers.Stop()
	return nil
}

// Resume restarts a paused torrent.
func (e *Engine) Resume(infoHash [20]byte) error {
	e.mu.RLock()
	sess, ok := e.sessions[infoHash]
	e.mu.RUnlock()
	if !ok {
		return fmt.Errorf("engine: torrent not found")
	}
	if sess.t.State != torrent.StatePaused {
		return nil
	}
	sess.t.State = torrent.StateDownloading
	return nil
}

// Get returns the Torrent for infoHash or an error if not found.
func (e *Engine) Get(infoHash [20]byte) (*torrent.Torrent, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	sess, ok := e.sessions[infoHash]
	if !ok {
		return nil, fmt.Errorf("engine: torrent not found")
	}
	return sess.t, nil
}

// List returns all active torrents.
func (e *Engine) List() []*torrent.Torrent {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*torrent.Torrent, 0, len(e.sessions))
	for _, sess := range e.sessions {
		out = append(out, sess.t)
	}
	return out
}

// Stats returns aggregated stats across all torrents.
func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s := EngineStats{ListenPort: e.cfg.ListenPort}
	if e.dht != nil {
		s.DHTPeers = e.dht.Stats().NodeCount
	}
	for _, sess := range e.sessions {
		switch sess.t.State {
		case torrent.StateDownloading:
			s.ActiveTorrents++
		case torrent.StateSeeding:
			s.SeedingTorrents++
		}
		s.TotalDownSpeed += sess.t.Stats.DownloadSpeed
		s.TotalUpSpeed += sess.t.Stats.UploadSpeed
		s.TotalDownloaded += sess.t.Stats.Downloaded
		s.TotalUploaded += sess.t.Stats.Uploaded
	}
	return s
}

// Shutdown gracefully stops all torrents and closes shared resources.
func (e *Engine) Shutdown() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, sess := range e.sessions {
		e.dbSaveFastResume(sess)
		if sess.peers != nil {
			sess.peers.Stop()
		}
		if sess.storage != nil {
			sess.storage.Close()
		}
	}
	if e.dht != nil {
		e.dht.Close()
	}
	if e.db != nil {
		e.db.Close()
	}
	return nil
}

func (e *Engine) startSession(tf *torrent.TorrentFile, _ []byte, bf bitfield.Bitfield, dir string, addedAt time.Time) (*torrent.Torrent, error) {
	if dir == "" {
		dir = e.cfg.DownloadDir
	}
	if addedAt.IsZero() {
		addedAt = time.Now()
	}

	stor := storage.NewManager(tf, dir)
	if err := stor.Init(); err != nil {
		return nil, fmt.Errorf("engine: init storage: %w", err)
	}

	pieceLens := make([]int, tf.PieceCount())
	for i := range pieceLens {
		pieceLens[i] = tf.PieceSize(i)
	}
	sched := scheduler.New(tf.PieceHashes, pieceLens, bf)

	params := tracker.AnnounceParams{
		InfoHash: tf.InfoHash,
		PeerID:   e.peerID,
		Port:     uint16(e.cfg.ListenPort),
		Left:     tf.TotalLength(),
	}
	mt := tracker.NewMultiTracker(tf.AnnounceList, params)

	mgr := peer.NewManager(tf, e.peerID, e.cfg.ListenPort, bf, sched, stor, e.limiter, e.cfg.MaxPerTorrent)

	t := torrent.New(*tf, bf, dir)
	sess := &session{
		t:       t,
		storage: stor,
		sched:   sched,
		peers:   mgr,
		tracker: mt,
		quit:    make(chan struct{}),
	}

	e.mu.Lock()
	e.sessions[tf.InfoHash] = sess
	e.mu.Unlock()

	go e.runSession(sess)
	return t, nil
}

func (e *Engine) runSession(sess *session) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess.t.State = torrent.StateDownloading
	work := sess.sched.WorkQueue()

	// Start peer manager (handles dialing and choking).
	go sess.peers.Run()

	// Listen for inbound peer connections on the configured port.
	go e.acceptInbound(ctx, sess)

	// Feed tracker-discovered peers to the manager.
	peersCh := make(chan []net.TCPAddr, 8)
	go sess.tracker.AnnounceLoop(ctx, peersCh)
	go func() {
		for {
			select {
			case batch := <-peersCh:
				sess.peers.AddPeers(batch)
			case <-sess.quit:
				return
			}
		}
	}()

	// Feed outstanding piece work to the peer manager.
	go func() {
		for {
			select {
			case w, ok := <-work:
				if !ok {
					return
				}
				select {
				case sess.peers.WorkQueue() <- w:
				case <-sess.quit:
					return
				}
			case <-sess.quit:
				return
			}
		}
	}()

	// Sample byte counters every second to compute rolling download/upload speeds.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		prevDown := atomic.LoadInt64(&sess.t.Stats.Downloaded)
		prevUp := atomic.LoadInt64(&sess.t.Stats.Uploaded)
		for {
			select {
			case <-ticker.C:
				curDown := atomic.LoadInt64(&sess.t.Stats.Downloaded)
				curUp := atomic.LoadInt64(&sess.t.Stats.Uploaded)
				atomic.StoreInt64(&sess.t.Stats.DownloadSpeed, curDown-prevDown)
				atomic.StoreInt64(&sess.t.Stats.UploadSpeed, curUp-prevUp)
				prevDown = curDown
				prevUp = curUp
			case <-sess.quit:
				return
			}
		}
	}()

	fastResumeTicker := time.NewTicker(30 * time.Second)
	defer fastResumeTicker.Stop()

	// Collect verified piece results and write to disk.
	for {
		select {
		case result := <-sess.peers.Results():
			if err := sess.storage.WritePiece(result.Index, result.Data); err != nil {
				// Hash mismatch: return piece to work queue.
				sess.sched.ReturnWork(result.Index)
				continue
			}
			sess.sched.MarkComplete(result.Index)
			// Scheduler keeps its own clone of the bitfield, so update the
			// Torrent's bitfield separately — that's what Progress() reads.
			sess.t.Bitfield.SetPiece(result.Index)
			atomic.AddInt64(&sess.t.Stats.Downloaded, int64(len(result.Data)))
			sess.peers.BroadcastHave(result.Index)
			log.Printf("torrent: piece %d complete (%d bytes), %.2f%% done",
				result.Index, len(result.Data), sess.t.Progress()*100)
			stats := sess.sched.Stats()
			if stats.PiecesComplete == stats.PiecesTotal {
				e.dbSaveFastResume(sess)
				sess.t.State = torrent.StateSeeding
				cancel()
				return
			}
		case <-fastResumeTicker.C:
			e.dbSaveFastResume(sess)
		case <-sess.quit:
			cancel()
			return
		}
	}
}

// acceptInbound listens for inbound peer connections and hands them to the manager.
func (e *Engine) acceptInbound(ctx context.Context, sess *session) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", e.cfg.ListenPort))
	if err != nil {
		return
	}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go sess.peers.AcceptInbound(conn)
	}
}
