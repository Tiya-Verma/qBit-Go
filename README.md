# qBit-Go

qBit-Go is a BitTorrent client written from scratch in Go with no torrent libraries, just the specs. It implements the full stack a real client needs: a bencode parser, the peer wire protocol, HTTP and UDP trackers, a Kademlia DHT for trackerless and magnet-link discovery, rarest-first piece scheduling, and a storage layer that SHA1-verifies every piece before it touches disk. A REST + WebSocket API sits on top, and the React web UI is embedded directly into the Go binary, so the whole client ships as one executable with no runtime dependencies.

It was built as a systems engineering project: peer-to-peer networking, concurrent protocol implementation, distributed hash tables, and file I/O, assembled into something you can actually download with.

## Technologies Used

- [Go](https://go.dev) 1.26
- [chi](https://github.com/go-chi/chi) — HTTP router
- [gorilla/websocket](https://github.com/gorilla/websocket) — real-time stats feed
- [bbolt](https://github.com/etcd-io/bbolt) — embedded key-value store for torrent state
- [React](https://react.dev) + [Vite](https://vite.dev)
- [Tailwind CSS](https://tailwindcss.com)
- [testify](https://github.com/stretchr/testify) — table-driven tests
- [Docker](https://www.docker.com) + Docker Compose

## Demo

<!--
To embed the demo video: open this README on GitHub, click the pencil (Edit),
and drag the video file into the editor right below this comment. GitHub uploads
it and inserts a https://github.com/user-attachments/... link that plays inline.
Supported: .mp4, .mov, .webm — up to 10 MB (100 MB for GitHub Pro).
-->

_Demo video coming soon._

## Features

**Core protocol**
- `.torrent` parsing via a custom bencode parser, with `FindInfoBytes` for byte-exact info hashes
- Magnet links: info dict fetched from peers over `ut_metadata` (BEP 9) on the extension protocol (BEP 10)
- Full peer wire protocol (BEP 3) — download *and* upload
- SHA1 verification of every piece before it is written

**Peers**
- Each connection runs its own read/write goroutine pair
- Tit-for-tat choking with optimistic unchoke
- Pipelined block requests (5 in flight, 16 KiB blocks)
- Peer Exchange (BEP 11) with periodic diffs

**Piece scheduling**
- Rarest-first selection by default
- Sequential mode for streaming
- End-game mode once fewer than 20 pieces remain

**Discovery**
- HTTP trackers and UDP trackers (BEP 15)
- Multi-tracker tier fallback (BEP 12)
- Kademlia DHT (BEP 5) — including `announce_peer` and serving inbound queries

**Storage & persistence**
- Multi-file torrents with cross-piece file-boundary mapping
- Serialized writer goroutine, concurrent reads
- Fast resume: bitfields checkpointed to bbolt every 30s and on shutdown
- Torrents are restored automatically on restart

**Interface**
- REST API under `/api/v1`
- WebSocket feed pushing stats and per-torrent updates every second
- React + Tailwind UI served by the same binary
- Token bucket rate limiting, per-torrent and global

## Getting Started

```bash
git clone https://github.com/Tiya-Verma/qBit-Go.git
cd qBit-Go
```

The React SPA must be built before compiling Go — the binary embeds it via `//go:embed`:

```bash
cd web && npm install && npm run build && cd ..
```

Then run:

```bash
go run ./cmd/qbit --port 8080 --download-dir ~/Downloads
```

Open http://localhost:8080.

Or with Docker:

```bash
docker compose up
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--port` | `8080` | HTTP API / web UI port |
| `--listen-port` | `6881` | TCP port for incoming peer connections |
| `--download-dir` | `.` | Where completed files land |
| `--dht` | `true` | Enable the DHT node |
| `--max-connections` | `200` | Global peer connection cap |

## API

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/torrents` | List all torrents |
| `POST` | `/api/v1/torrents` | Add a `.torrent` upload or a magnet link |
| `GET` | `/api/v1/torrents/{hash}` | Torrent detail |
| `DELETE` | `/api/v1/torrents/{hash}` | Remove a torrent |
| `POST` | `/api/v1/torrents/{hash}/pause` | Pause |
| `POST` | `/api/v1/torrents/{hash}/resume` | Resume |
| `GET` | `/api/v1/stats` | Global stats |
| `GET` | `/api/v1/settings` | Read settings |
| `PATCH` | `/api/v1/settings` | Update settings |
| `GET` | `/api/v1/ws` | WebSocket stats feed |

```bash
# Add a .torrent file
curl -X POST http://localhost:8080/api/v1/torrents \
  -F "torrent=@ubuntu.torrent"

# Add a magnet link
curl -X POST http://localhost:8080/api/v1/torrents \
  -H "Content-Type: application/json" \
  -d '{"magnet": "magnet:?xt=urn:btih:..."}'
```

## Architecture

Every active torrent is an independent state machine. The Engine owns a registry of them and routes API calls; nothing is shared between torrents except the global rate limiter and the DHT node.

```
┌─────────────────────────────────────────────────────────────────┐
│                      REST API + WebSocket                        │
│                        (internal/api)                            │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Engine                                  │
│                (manages N Torrent instances)                     │
│          Add / Remove / Pause / Resume / Stats                   │
└───┬──────────────┬────────────────────┬───────────────┬─────────┘
    │              │                    │               │
    ▼              ▼                    ▼               ▼
Torrent 1      Torrent 2           Torrent N         DHT Node
    │
    ├── Tracker Client (HTTP + UDP, tier fallback)
    ├── Peer Manager
    │     ├── Peer Conn 1 ──┐
    │     ├── Peer Conn 2   ├── read/write goroutine pairs
    │     └── Peer Conn N ──┘
    ├── Piece Scheduler  (rarest-first / sequential / end-game)
    └── Storage Manager
          └── File I/O + SHA1 verify + fast resume
```

```
cmd/qbit/main.go              — wires config, constructs components, starts the server

internal/
  engine/     — registry of active torrents; owns the DHT node and rate limiter
  torrent/    — .torrent parsing; Torrent state machine
  peer/       — handshake, wire protocol, choking, PEX, extension protocol
  tracker/    — HTTP + UDP clients, multi-tracker tier fallback
  dht/        — Kademlia DHT node
  scheduler/  — piece selection
  storage/    — file I/O, piece→file mapping, SHA1 verification, bitfields
  ratelimit/  — token bucket limiter
  api/        — REST handlers, WebSocket broadcaster, embedded UI
  bencode/    — encode/decode for .torrent files and DHT messages
  magnet/     — magnet URI parsing

web/          — React + Tailwind frontend, built into the Go binary
docs/         — per-package deep dives
```

Full write-up in [`ARCHITECTURE.md`](ARCHITECTURE.md), with per-package notes in [`docs/`](docs/).

## Testing

```bash
go test ./...          # everything
go test ./internal/... -race
go vet ./...
```

Table-driven tests cover bencode round-trips and info-hash extraction, magnet parsing, the peer message codec and work queue, tracker response parsing (compact and dictionary peer lists), bitfield operations, scheduler selection, DHT routing and the engine's persistence layer.

## BEPs Implemented

| BEP | Description |
|---|---|
| [3](https://www.bittorrent.org/beps/bep_0003.html) | The BitTorrent Protocol Specification |
| [5](https://www.bittorrent.org/beps/bep_0005.html) | DHT Protocol (Kademlia) |
| [9](https://www.bittorrent.org/beps/bep_0009.html) | Metadata exchange (`ut_metadata`) for magnet links |
| [10](https://www.bittorrent.org/beps/bep_0010.html) | Extension Protocol |
| [11](https://www.bittorrent.org/beps/bep_0011.html) | Peer Exchange (PEX) |
| [12](https://www.bittorrent.org/beps/bep_0012.html) | Multitracker Metadata Extension |
| [15](https://www.bittorrent.org/beps/bep_0015.html) | UDP Tracker Protocol |
| [23](https://www.bittorrent.org/beps/bep_0023.html) | Tracker Returns Compact Peer Lists |

## References

- [Jesse Li — Building a BitTorrent client from the ground up in Go](https://blog.jse.li/posts/torrent/)
- [BEP Index](https://www.bittorrent.org/beps/bep_0000.html) — official protocol specs
- [libtorrent blog](https://blog.libtorrent.org/) — real-client engineering notes
- [Kademlia paper](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf)
