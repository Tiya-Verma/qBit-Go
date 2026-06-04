# Architecture

This document describes the system design of qbit-go. Each section maps to a package under `internal/`.

---

## Core Principle

Every active torrent is an independent state machine. The Engine owns a registry of torrents and routes API calls to them. Torrents own their peers, scheduler, and storage. Nothing is shared between torrents except the global rate limiter and the DHT node.

---

## Component Map

```
cmd/qbit/main.go
│
│  Reads config, constructs components, starts HTTP server
│
├── engine.Engine
│     │
│     │  Registry of active torrents. Handles add/remove/pause/resume.
│     │  Each torrent runs in its own goroutine tree.
│     │
│     ├── torrent.Torrent  (one per active torrent)
│     │     │
│     │     ├── tracker.Client        HTTP + UDP tracker announce loop
│     │     │
│     │     ├── peer.Manager          Maintains peer connection pool
│     │     │     └── peer.Conn ×N   One goroutine pair per peer
│     │     │           ├── readLoop  Reads messages off TCP
│     │     │           └── writeLoop Sends queued messages
│     │     │
│     │     ├── scheduler.Scheduler   Decides which piece to request next
│     │     │
│     │     └── storage.Manager       Writes/reads pieces, verifies SHA1
│     │
│     └── dht.Node                    Shared Kademlia DHT (trackerless lookup)
│
├── ratelimit.Limiter                 Global + per-torrent token buckets
│
└── api.Server                        REST + WebSocket server
```

---

## Data Flow: Adding a Torrent

```
User POST /api/torrents { .torrent file or magnet }
        │
        ▼
api.Server.AddTorrent()
        │
        ▼
engine.Engine.Add(torrentFile)
        │
        ├── Parse .torrent → torrent.TorrentFile { InfoHash, Pieces, Files, TrackerURL }
        │
        ├── Load .fastresume if exists → restore piece bitfield
        │
        ├── Construct torrent.Torrent{ scheduler, storage, peerManager }
        │
        └── go torrent.Run()  ←── starts the goroutine tree
                │
                ├── go tracker.AnnounceLoop()   → feeds peers into peer.Manager
                │
                ├── go peer.Manager.Run()        → dials peers, spawns Conn goroutines
                │
                └── go scheduler.DispatchLoop()  → pulls work, sends to peers
```

---

## Data Flow: Downloading a Piece

```
scheduler.Scheduler
    │  picks piece index P (rarest-first)
    │  assigns to peer.Conn C
    ▼
peer.Conn.writeLoop
    │  sends Request{index: P, begin: 0, length: 16KB} ×N (pipelined)
    ▼
[network]
    ▼
peer.Conn.readLoop
    │  receives Piece{index: P, begin: 0, data: []byte}
    │  accumulates blocks until full piece received
    ▼
storage.Manager.WritePiece(index P, data)
    │  SHA1(data) == torrent.Pieces[P] ?
    │    YES → write to file, mark piece complete in bitfield
    │    NO  → discard, put P back on work queue (peer sent corrupt data)
    ▼
peer.Manager.BroadcastHave(P)
    │  sends Have{index: P} to all connected peers
    ▼
scheduler.Scheduler marks P complete
    │  updates piece frequency map
    │  if all pieces done → torrent transitions to SEEDING state
```

---

## Key Data Structures

### torrent.TorrentFile
```go
type TorrentFile struct {
    Announce     string      // tracker URL
    InfoHash     [20]byte    // SHA1 of info dict — torrent identity
    PieceHashes  [][20]byte  // one SHA1 per piece
    PieceLength  int         // bytes per piece (except last)
    Length       int         // total bytes (single-file)
    Files        []File      // populated for multi-file torrents
    Name         string      // suggested file/folder name
}
```

### torrent.Torrent (state machine)
```go
type State int

const (
    StateChecking   State = iota // verifying existing data on disk
    StateDownloading             // actively downloading
    StateSeeding                 // upload only, download complete
    StatePaused                  // user-paused
    StateStopped                 // no activity, not paused
    StateError                   // unrecoverable error
)

type Torrent struct {
    File       TorrentFile
    State      State
    Bitfield   bitfield.Bitfield   // which pieces we have
    Stats      Stats               // downloaded, uploaded, speed

    peers      *peer.Manager
    scheduler  *scheduler.Scheduler
    storage    *storage.Manager
    tracker    *tracker.Client

    workQueue  chan *scheduler.PieceWork
    results    chan *scheduler.PieceResult
    quit       chan struct{}
}
```

### peer.Conn
```go
type Conn struct {
    InfoHash  [20]byte
    PeerID    [20]byte
    Conn      net.Conn

    Choked     bool        // are we choked by them?
    Interested bool        // are we interested in them?
    Bitfield   bitfield.Bitfield

    inbound  chan Message  // messages from readLoop
    outbound chan Message  // messages to writeLoop
    quit     chan struct{}
}
```

### scheduler.PieceWork / PieceResult
```go
type PieceWork struct {
    Index  int        // piece index
    Hash   [20]byte   // expected SHA1
    Length int        // bytes in this piece
}

type PieceResult struct {
    Index int
    Data  []byte
}
```

---

## Concurrency Model

Each `peer.Conn` spawns exactly **two goroutines**:

```
readLoop  — blocks on conn.Read(), parses messages, sends to inbound chan
writeLoop — blocks on outbound chan, serializes messages, writes to conn
```

The `scheduler.DispatchLoop` is a single goroutine that:
- pulls `PieceWork` from the work queue channel
- finds the best peer for that piece (has it, is unchoked)
- sends `Request` messages via the peer's outbound channel

The `storage.Manager` runs a single **writer goroutine** that serializes all disk writes. Piece verification happens inline in this goroutine before writing.

```
N peer readLoops  →  results chan  →  storage writer goroutine
                                              │
                                     file.WriteAt(offset, data)
```

This design avoids locking entirely in the hot path. The only shared state is:
- `Torrent.Bitfield` — protected by a `sync.RWMutex`
- `Torrent.Stats` — updated via `atomic` operations
- `scheduler.pieceFrequency` map — owned by the single dispatch goroutine

---

## Error Handling Philosophy

**Peer errors are expected and never fatal.** If a peer connection dies, the readLoop closes, the PieceWork goes back on the queue, and the torrent continues with remaining peers.

**Storage errors are fatal per-torrent.** A disk write failure transitions the torrent to `StateError` and notifies the API.

**Tracker errors are retried with exponential backoff.** A failed announce is retried after 30s, 60s, 120s, up to a max of 1800s.

---

## Configuration

```go
type Config struct {
    ListenPort      int           // TCP port for peer connections
    APIPort         int           // HTTP API port
    DownloadDir     string        // default download directory
    MaxConnections  int           // global peer connection cap (default: 200)
    MaxPerTorrent   int           // per-torrent peer cap (default: 50)
    GlobalDownSpeed int64         // bytes/sec, 0 = unlimited
    GlobalUpSpeed   int64         // bytes/sec, 0 = unlimited
    DHTEnabled      bool
    PEXEnabled      bool
    EncryptionMode  EncryptionMode // Disabled / Preferred / Required
}
```

---

## State Persistence

On clean shutdown (and every 30 seconds), each torrent writes a `.fastresume` file:

```go
type FastResume struct {
    InfoHash    [20]byte
    Bitfield    []byte    // which pieces are complete
    DownloadDir string
    AddedAt     time.Time
    Files       []FileState  // per-file priority/skip
}
```

On startup, the engine scans for `.fastresume` files and restores torrents without re-verifying already-downloaded pieces — unless the user requests a forced recheck.
