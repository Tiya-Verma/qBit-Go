# qbit-go

A BitTorrent client written in Go, inspired by qBittorrent. Built from scratch as a systems engineering project covering peer-to-peer networking, concurrent protocol implementation, distributed hash tables, and file I/O.

---

## Features

### Core Protocol
- `.torrent` file parsing via custom bencode parser
- Magnet link resolution via DHT
- HTTP and UDP tracker support
- Full peer wire protocol (BEP 3)
- SHA1 piece integrity verification

### Peer Management
- Concurrent peer connections via goroutines
- Choking/unchoking algorithm (optimistic unchoke every 30s)
- Pipelined block requests (5 in-flight per peer)
- PEX — Peer Exchange (BEP 11)

### Piece Scheduling
- Rarest-first piece selection (default)
- Sequential mode for streaming use cases
- End-game mode: broadcast remaining pieces to all peers

### Network
- DHT node (Kademlia-based, BEP 5) for trackerless torrents
- uTP transport support (BEP 29) — congestion-friendly UDP
- Token bucket rate limiter (per-torrent and global)

### Storage
- Multi-file torrent support with cross-piece boundary handling
- Piece buffer with write coalescing
- Fast resume via `.fastresume` state files

### API & UI
- REST API for all client operations
- Minimal web UI (React, served by the Go backend)
- WebSocket feed for real-time torrent stats

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.22+ |
| Web framework | `net/http` + `chi` router |
| WebSockets | `gorilla/websocket` |
| Storage | `bbolt` (embedded KV for torrent metadata) |
| Testing | `testify`, table-driven Go tests |
| Frontend | React + Tailwind CSS |
| Containerization | Docker + Docker Compose |

---

## Project Structure

```
qbit-go/
├── cmd/
│   └── qbit/
│       └── main.go              # Entry point — wires everything together
│
├── internal/
│   ├── bencode/                 # Bencode encode/decode
│   ├── torrent/                 # .torrent parsing, TorrentFile struct
│   ├── engine/                  # Central engine — manages all active torrents
│   ├── peer/                    # TCP connection, handshake, wire protocol
│   ├── tracker/
│   │   ├── http.go              # HTTP tracker client
│   │   └── udp.go               # UDP tracker client (BEP 15)
│   ├── dht/                     # Kademlia DHT node (BEP 5)
│   ├── scheduler/               # Piece selection algorithms
│   ├── storage/                 # File I/O, piece buffering, SHA1 verify
│   ├── ratelimit/               # Token bucket rate limiter
│   └── api/                     # REST handlers + WebSocket broadcaster
│
├── web/                         # React frontend (built, served by Go)
│
├── docs/                        # Architecture deep-dives
│   ├── engine.md
│   ├── peer_protocol.md
│   ├── scheduler.md
│   ├── tracker.md
│   ├── dht.md
│   ├── api.md
│   └── storage.md
│
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        REST API + WebSocket                      │
│                        (internal/api)                            │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Engine                                  │
│              (manages N Torrent instances)                       │
│   Add / Remove / Pause / Resume / Stats                          │
└───┬──────────────┬────────────────────┬───────────────┬─────────┘
    │              │                    │               │
    ▼              ▼                    ▼               ▼
Torrent 1      Torrent 2           Torrent N         DHT Node
    │
    ├── Tracker Client (HTTP + UDP)
    ├── Peer Manager
    │     ├── Peer Conn 1 ──┐
    │     ├── Peer Conn 2   ├── Wire Protocol goroutines
    │     └── Peer Conn N ──┘
    ├── Piece Scheduler
    └── Storage Manager
          └── File I/O + SHA1 verify
```

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full system design.

---

## Getting Started

```bash
git clone https://github.com/yourname/qbit-go
cd qbit-go

# Run with Docker
docker compose up

# Or run directly
go run ./cmd/qbit --port 8080 --download-dir ~/Downloads

# Add a torrent via CLI
curl -X POST http://localhost:8080/api/torrents \
  -F "torrent=@ubuntu.torrent"

# Or via magnet link
curl -X POST http://localhost:8080/api/torrents \
  -d '{"magnet": "magnet:?xt=urn:btih:..."}'
```

---

## BEPs Implemented

| BEP | Description |
|---|---|
| BEP 3 | The BitTorrent Protocol Specification |
| BEP 5 | DHT Protocol (Kademlia) |
| BEP 11 | Peer Exchange (PEX) |
| BEP 15 | UDP Tracker Protocol |
| BEP 23 | Tracker Returns Compact Peer Lists |
| BEP 29 | uTP — Micro Transport Protocol |

---

## Learning Resources

- [Jesse Li's BitTorrent in Go](https://blog.jse.li/posts/torrent/) — starting point
- [BEP Index](https://www.bittorrent.org/beps/bep_0000.html) — official protocol specs
- [libtorrent blog](https://blog.libtorrent.org/) — deep dives on real client engineering
- [Kademlia paper](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf) — DHT theory
