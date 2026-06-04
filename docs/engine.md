# Engine

`internal/engine`

The Engine is the top-level coordinator. It owns the registry of all active torrents and is the single entry point for all operations the API layer performs.

---

## Responsibilities

- Maintain a map of `InfoHash → *Torrent`
- Translate API calls (Add, Remove, Pause, Resume) into torrent state transitions
- Own the shared `dht.Node` and `ratelimit.Limiter`
- Aggregate stats across all torrents for the dashboard

---

## Interface

```go
type Engine struct {
    torrents map[[20]byte]*torrent.Torrent
    mu       sync.RWMutex

    dht      *dht.Node
    limiter  *ratelimit.Limiter
    config   *Config
    db       *bbolt.DB   // persistent torrent metadata
}

func (e *Engine) Add(r io.Reader) (*torrent.Torrent, error)
func (e *Engine) AddMagnet(uri string) (*torrent.Torrent, error)
func (e *Engine) Remove(infoHash [20]byte, deleteFiles bool) error
func (e *Engine) Pause(infoHash [20]byte) error
func (e *Engine) Resume(infoHash [20]byte) error
func (e *Engine) Get(infoHash [20]byte) (*torrent.Torrent, error)
func (e *Engine) List() []*torrent.Torrent
func (e *Engine) Stats() EngineStats
func (e *Engine) Shutdown() error
```

---

## Add Flow

```
Engine.Add(reader)
    │
    ├── bencode.Decode(reader) → raw map
    ├── torrent.ParseFile(raw) → TorrentFile
    ├── Check: already exists? → return error
    │
    ├── storage.Manager.Init(TorrentFile, downloadDir)
    │     └── creates file stubs at correct sizes (no zeroing)
    │
    ├── FastResume.Load(infoHash)?
    │     YES → restore Bitfield, skip verification
    │     NO  → Bitfield all zeros, queue full verification if files exist
    │
    ├── Construct Torrent{ scheduler, storage, peerManager, tracker }
    │
    ├── db.Put(infoHash, serializedTorrent)  ← persist so we survive restarts
    │
    └── go torrent.Run()
```

## AddMagnet Flow

Magnet links contain the info hash but NOT the `.torrent` metadata. You need to fetch it first.

```
Engine.AddMagnet("magnet:?xt=urn:btih:<infohash>&dn=<name>&tr=<tracker>")
    │
    ├── Parse magnet URI → extract InfoHash, trackers, display name
    │
    ├── Create a placeholder Torrent in StateFetching
    │
    ├── Attempt tracker announce with info hash
    │     → get peer list even without metadata
    │
    ├── Connect to peers, use Extension Protocol (BEP 9) to request
    │   ut_metadata — peers send us the .torrent info dict in pieces
    │
    ├── Reassemble info dict, verify SHA1 == InfoHash
    │
    └── Proceed as normal Add flow
```

This is why DHT matters for magnet links — if trackers fail, DHT finds peers who have the metadata.

---

## Torrent State Machine

```
              Add()
                │
                ▼
         ┌─────────────┐
         │  CHECKING   │ ← verifying existing data on disk
         └──────┬──────┘
                │ verification complete
                ▼
         ┌─────────────┐    Pause()    ┌────────────┐
         │ DOWNLOADING │ ────────────► │   PAUSED   │
         └──────┬──────┘               └─────┬──────┘
                │                            │ Resume()
                │ all pieces verified         │
                ▼                            ▼
         ┌─────────────┐    Pause()    ┌────────────┐
         │   SEEDING   │ ────────────► │   PAUSED   │
         └──────┬──────┘               └────────────┘
                │ Remove()
                ▼
         ┌─────────────┐
         │   STOPPED   │
         └─────────────┘

Any state → ERROR on unrecoverable storage failure
```

---

## Stats Aggregation

The engine polls each torrent's atomic stats counters every second and computes:

```go
type EngineStats struct {
    ActiveTorrents    int
    SeedingTorrents   int
    TotalDownSpeed    int64   // bytes/sec
    TotalUpSpeed      int64   // bytes/sec
    TotalDownloaded   int64   // bytes, session
    TotalUploaded     int64   // bytes, session
    DHTPeers          int
    ListenPort        int
}
```

These are pushed to connected WebSocket clients every second.

---

## Persistence (bbolt)

The engine uses `bbolt` (an embedded key-value store) so torrents survive restarts without re-adding them.

**Bucket: `torrents`**
- Key: `[20]byte` info hash
- Value: `msgpack`-encoded `TorrentRecord{ TorrentFile, DownloadDir, AddedAt, Labels }`

**Bucket: `fastresume`**
- Key: `[20]byte` info hash
- Value: `msgpack`-encoded `FastResume{ Bitfield, FileStates }`

On startup:
```go
func (e *Engine) RestoreFromDB() error {
    // iterate torrents bucket
    // for each: construct Torrent, load fastresume, go torrent.Run()
}
```

---

## Shutdown

```go
func (e *Engine) Shutdown() error {
    e.mu.Lock()
    defer e.mu.Unlock()

    for _, t := range e.torrents {
        t.SaveFastResume()   // flush bitfield to disk
        t.Stop()             // close quit channel, wait for goroutines
    }

    e.dht.Close()
    e.db.Close()
    return nil
}
```

Graceful shutdown is important — an incomplete piece in flight is discarded (verified on next start) but the bitfield correctly reflects what was fully written.
