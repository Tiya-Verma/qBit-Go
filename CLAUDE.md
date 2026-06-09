# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Status

Feature-complete. All planned functionality is implemented and tested:
- Bencode parser with `FindInfoBytes` for correct info hash computation
- Full peer wire protocol (BEP 3): download, upload, choking/unchoking (tit-for-tat)
- Piece scheduler: rarest-first, sequential, end-game modes
- Storage: multi-file, SHA1 verification, fast resume
- Tracker: HTTP + UDP (BEP 15), multi-tracker tier fallback (BEP 12)
- DHT: Kademlia (BEP 5) with `announce_peer` and serving inbound queries
- Magnet links: BEP 9 (`ut_metadata`) + BEP 10 extension protocol
- Peer Exchange: BEP 11 (`ut_pex`) with periodic diffs
- Persistence: bbolt (`torrents` + `fastresume` buckets), restored on startup
- React + Tailwind web UI, embedded in the Go binary via `//go:embed`

## Build the frontend

The React SPA must be built before compiling Go (the embed requires `internal/api/ui/` to exist):

```bash
cd web && npm install && npm run build
```

This outputs to `../internal/api/ui/` which is picked up by `//go:embed all:ui` in `internal/api/server.go`.

## Commands

```bash
# Run
go run ./cmd/qbit --port 8080 --download-dir ~/Downloads

# Build
go build ./cmd/qbit

# Test all
go test ./...

# Test a single package
go test ./internal/scheduler/...

# Test a single test
go test ./internal/peer/... -run TestHandshake

# Lint
go vet ./...

# Docker
docker compose up
```

## Architecture

Every active torrent is an independent state machine. The Engine owns a registry of torrents and routes API calls to them. Nothing is shared between torrents except the global rate limiter and the DHT node.

### Package Map

```
cmd/qbit/main.go              — wires config, constructs all components, starts HTTP server

internal/
  engine/     — registry of active torrents (map[InfoHash]*Torrent); owns dht.Node and ratelimit.Limiter
  torrent/    — TorrentFile struct + bencode parsing; Torrent state machine (Checking→Downloading→Seeding→Paused/Error)
  peer/       — TCP handshake, wire protocol (BEP 3), choking algorithm, PEX (BEP 11)
  tracker/    — HTTP and UDP tracker clients (BEP 15); MultiTracker with tier fallback (BEP 12)
  dht/        — Kademlia DHT node (BEP 5); used for trackerless/magnet-link peer discovery
  scheduler/  — piece selection: rarest-first (default), sequential, end-game (<20 pieces left)
  storage/    — file I/O, piece→file region mapping, SHA1 verification, bitfield, fastresume
  ratelimit/  — token bucket limiter, per-torrent and global
  api/        — chi-based REST API + gorilla/websocket broadcaster; serves React build as static files
  bencode/    — encode/decode for .torrent files and DHT messages

web/          — React + Tailwind frontend (built artifact served by Go)
```

### Concurrency Model

Each `peer.Conn` runs exactly two goroutines: a `readLoop` (blocks on `conn.Read`) and a `writeLoop` (blocks on the outbound channel). The `scheduler.DispatchLoop` is a single goroutine — it owns the `pieceFrequency` map with no locks needed. The `storage.Manager` serializes all disk writes through a single writer goroutine; reads are concurrent.

The only shared mutable state across goroutines:
- `Torrent.Bitfield` — protected by `sync.RWMutex`
- `Torrent.Stats` — updated via `atomic` operations

### Key Data Flows

**Adding a torrent:** `POST /api/v1/torrents` → `engine.Add()` → parse bencode → load fastresume (or queue verify) → construct `Torrent{scheduler, storage, peerManager, tracker}` → persist to bbolt → `go torrent.Run()`.

**Downloading a piece:** `scheduler` picks rarest piece → sends `Request` messages via peer's outbound channel (5 in-flight / 16 KiB blocks) → `peer.readLoop` accumulates blocks → `storage.Manager` SHA1-verifies and writes → `BroadcastHave` to all peers → scheduler marks complete.

**Magnet links:** extract InfoHash from URI → create placeholder torrent in `StateFetching` → use DHT + BEP 9 (`ut_metadata`) to fetch the info dict from peers → verify SHA1 → proceed as normal add.

### Error Handling

- **Peer errors** are never fatal: failed connections return `PieceWork` to the queue.
- **Storage errors** are fatal per-torrent: transitions to `StateError`.
- **Tracker errors** retry with exponential backoff (30s → 60s → 120s → max 1800s); falls back to DHT on full failure.

### Persistence

bbolt buckets:
- `torrents` — `InfoHash → JSON(TorrentRecord{Data, DownloadDir, AddedAt})` for restart restoration
- `fastresume` — `InfoHash → raw bitfield bytes` written every 30s and on shutdown

`engine.RestoreFromDB()` is called at startup (after `engine.New()`) to reload all previously added torrents.

### API

All REST endpoints are under `/api/v1`. The WebSocket endpoint (`GET /api/v1/ws`) pushes `stats` and `torrent_update` frames every second, plus event-driven frames (`torrent_added`, `torrent_removed`, `torrent_state_change`). The React SPA is served as static files from the same server.

### Tech Stack

| Component | Library |
|---|---|
| Router | `chi` |
| WebSocket | `gorilla/websocket` |
| KV store | `bbolt` |
| Serialization | `encoding/json` (persistence), `bencode` (protocol) |
| Testing | `testify`, table-driven |
| Frontend | React + Tailwind CSS |
