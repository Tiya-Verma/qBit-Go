package engine

import (
	"crypto/rand"
	"fmt"
	"io"
	"sync"

	"github.com/tiyaverma/qbit-go/internal/bencode"
	"github.com/tiyaverma/qbit-go/internal/bitfield"
	"github.com/tiyaverma/qbit-go/internal/dht"
	"github.com/tiyaverma/qbit-go/internal/peer"
	"github.com/tiyaverma/qbit-go/internal/ratelimit"
	"github.com/tiyaverma/qbit-go/internal/scheduler"
	"github.com/tiyaverma/qbit-go/internal/storage"
	"github.com/tiyaverma/qbit-go/internal/torrent"
	"github.com/tiyaverma/qbit-go/internal/tracker"
)

// EngineStats aggregates metrics across all active torrents.
type EngineStats struct {
	ActiveTorrents  int
	SeedingTorrents int
	TotalDownSpeed  int64
	TotalUpSpeed    int64
	TotalDownloaded int64
	TotalUploaded   int64
	DHTPeers        int
	ListenPort      int
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
	raw, err := bencode.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("engine: bencode decode: %w", err)
	}
	dict, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("engine: not a bencoded dict")
	}
	tf, err := torrent.ParseFile(dict)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	if _, exists := e.sessions[tf.InfoHash]; exists {
		e.mu.Unlock()
		return nil, fmt.Errorf("engine: torrent already added")
	}
	e.mu.Unlock()

	return e.startSession(tf)
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
	sess.peers.Stop()
	sess.storage.Close()
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
		sess.peers.Stop()
		sess.storage.Close()
	}
	if e.dht != nil {
		e.dht.Close()
	}
	return nil
}

func (e *Engine) startSession(tf *torrent.TorrentFile) (*torrent.Torrent, error) {
	bf := bitfield.New(tf.PieceCount())

	stor := storage.NewManager(tf, e.cfg.DownloadDir)
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

	mgr := peer.NewManager(tf, e.peerID, bf, sched, e.limiter, e.cfg.MaxPerTorrent)

	t := torrent.New(*tf, bf, e.cfg.DownloadDir)
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
	sess.t.State = torrent.StateDownloading
	work := sess.sched.WorkQueue()

	go sess.peers.Run()

	// feed work to peers
	go func() {
		for {
			select {
			case w, ok := <-work:
				if !ok {
					return
				}
				sess.peers.WorkQueue() <- w
			case <-sess.quit:
				return
			}
		}
	}()

	// collect results
	for {
		select {
		case result := <-sess.peers.Results():
			if err := sess.storage.WritePiece(result.Index, result.Data); err != nil {
				continue
			}
			sess.sched.MarkComplete(result.Index)
			sess.peers.BroadcastHave(result.Index)
			if sess.sched.Stats().PiecesComplete == sess.sched.Stats().PiecesTotal {
				sess.t.State = torrent.StateSeeding
				return
			}
		case <-sess.quit:
			return
		}
	}
}
