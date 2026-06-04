# API

`internal/api`

A REST + WebSocket API served on a configurable port (default `8080`). The web UI is a React SPA served from the same server as static files. All endpoints are prefixed with `/api/v1`.

---

## Server Setup

```go
func NewServer(engine *engine.Engine, cfg *Config) *Server {
    r := chi.NewRouter()
    r.Use(middleware.Logger)
    r.Use(middleware.Recoverer)
    r.Use(middleware.RealIP)

    // Serve React build
    r.Handle("/*", http.FileServer(http.FS(webFS)))

    // API routes
    r.Route("/api/v1", func(r chi.Router) {
        r.Get("/torrents",          s.listTorrents)
        r.Post("/torrents",         s.addTorrent)
        r.Get("/torrents/{hash}",   s.getTorrent)
        r.Delete("/torrents/{hash}",s.removeTorrent)
        r.Post("/torrents/{hash}/pause",  s.pauseTorrent)
        r.Post("/torrents/{hash}/resume", s.resumeTorrent)
        r.Get("/torrents/{hash}/files",   s.listFiles)
        r.Patch("/torrents/{hash}/files", s.updateFilePriorities)
        r.Get("/torrents/{hash}/peers",   s.listPeers)
        r.Get("/stats",             s.globalStats)
        r.Get("/ws",                s.websocket)     // WebSocket upgrade
        r.Get("/settings",          s.getSettings)
        r.Patch("/settings",        s.updateSettings)
    })

    return &Server{engine: engine, router: r}
}
```

---

## Endpoints

### POST /api/v1/torrents

Add a torrent. Accepts multipart form with `.torrent` file or JSON with magnet URI.

**Request (multipart):**
```
Content-Type: multipart/form-data
Field: torrent  → .torrent file
Field: savePath → optional download directory
```

**Request (JSON):**
```json
{
  "magnet": "magnet:?xt=urn:btih:...",
  "savePath": "/downloads"
}
```

**Response 201:**
```json
{
  "hash": "a1b2c3d4e5f6...",
  "name": "ubuntu-24.04.iso",
  "state": "checking",
  "size": 1073741824
}
```

---

### GET /api/v1/torrents

List all torrents with summary stats.

**Response 200:**
```json
[
  {
    "hash": "a1b2c3...",
    "name": "ubuntu-24.04.iso",
    "state": "downloading",
    "size": 1073741824,
    "downloaded": 536870912,
    "uploaded": 10485760,
    "progress": 0.50,
    "downloadSpeed": 5242880,
    "uploadSpeed": 524288,
    "peers": 24,
    "seeds": 41,
    "eta": 102,
    "addedAt": "2025-06-01T10:00:00Z"
  }
]
```

---

### GET /api/v1/torrents/{hash}

Full detail for one torrent including tracker and piece info.

**Response 200:**
```json
{
  "hash": "a1b2c3...",
  "name": "ubuntu-24.04.iso",
  "state": "downloading",
  "savePath": "/downloads",
  "size": 1073741824,
  "pieceSize": 262144,
  "pieceCount": 4096,
  "piecesComplete": 2048,
  "progress": 0.50,
  "downloadSpeed": 5242880,
  "uploadSpeed": 524288,
  "downloaded": 536870912,
  "uploaded": 10485760,
  "ratio": 0.02,
  "peers": 24,
  "seeds": 41,
  "eta": 102,
  "trackers": [
    {
      "url": "udp://tracker.opentrackr.org:1337/announce",
      "status": "working",
      "peers": 65,
      "nextAnnounce": 1800
    }
  ],
  "addedAt": "2025-06-01T10:00:00Z"
}
```

---

### DELETE /api/v1/torrents/{hash}

Remove a torrent.

**Query params:**
- `deleteFiles=true` → also delete downloaded files

**Response 204:** No content.

---

### POST /api/v1/torrents/{hash}/pause

Pause an active torrent (disconnects all peers, stops announces).

**Response 200:**
```json
{ "state": "paused" }
```

---

### GET /api/v1/torrents/{hash}/files

List files within a multi-file torrent with per-file progress and priority.

**Response 200:**
```json
[
  {
    "index": 0,
    "name": "ubuntu-24.04-desktop-amd64.iso",
    "size": 1073741824,
    "progress": 0.50,
    "priority": "normal"   // skip | low | normal | high
  }
]
```

---

### PATCH /api/v1/torrents/{hash}/files

Update file priorities (controls which files get downloaded).

**Request:**
```json
[
  { "index": 0, "priority": "high" },
  { "index": 1, "priority": "skip" }
]
```

**Response 200:** Updated file list.

---

### GET /api/v1/torrents/{hash}/peers

List connected peers for a torrent.

**Response 200:**
```json
[
  {
    "ip": "192.168.1.100",
    "port": 51234,
    "client": "qBittorrent/4.6.2",
    "progress": 0.95,
    "downloadSpeed": 1048576,
    "uploadSpeed": 0,
    "flags": "dI"           // d=downloading from, I=interested
  }
]
```

---

### GET /api/v1/stats

Global client statistics.

**Response 200:**
```json
{
  "activeTorrents": 3,
  "seedingTorrents": 1,
  "totalDownSpeed": 10485760,
  "totalUpSpeed": 1048576,
  "sessionDownloaded": 5368709120,
  "sessionUploaded": 536870912,
  "dhtNodes": 847,
  "listenPort": 6881
}
```

---

### PATCH /api/v1/settings

Update client settings live (no restart required).

**Request:**
```json
{
  "globalDownSpeed": 10485760,
  "globalUpSpeed": 1048576,
  "maxConnections": 200,
  "dhtEnabled": true
}
```

---

## WebSocket Feed

`GET /api/v1/ws` (upgrade to WebSocket)

Pushes real-time updates to the web UI. The client subscribes and receives JSON frames every second:

```go
type WSMessage struct {
    Type    string          `json:"type"`
    Payload json.RawMessage `json:"payload"`
}
```

**Message types:**

| Type | Payload | Frequency |
|---|---|---|
| `stats` | GlobalStats | 1/sec |
| `torrent_update` | TorrentSummary[] | 1/sec |
| `torrent_added` | TorrentDetail | on event |
| `torrent_removed` | `{"hash": "..."}` | on event |
| `torrent_state_change` | `{"hash": "...", "state": "..."}` | on event |

```go
func (s *Server) websocket(w http.ResponseWriter, r *http.Request) {
    conn, _ := s.upgrader.Upgrade(w, r, nil)
    defer conn.Close()

    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            stats   := s.engine.Stats()
            torrents := s.engine.List()
            conn.WriteJSON(WSMessage{Type: "stats",          Payload: marshalJSON(stats)})
            conn.WriteJSON(WSMessage{Type: "torrent_update", Payload: marshalJSON(torrents)})

        case event := <-s.eventBus:
            conn.WriteJSON(event)

        case <-r.Context().Done():
            return
        }
    }
}
```

---

## Error Responses

All errors follow a consistent format:

```json
{
  "error": "torrent not found",
  "code": "NOT_FOUND"
}
```

| HTTP Status | Code | Meaning |
|---|---|---|
| 400 | `BAD_REQUEST` | Malformed request or invalid .torrent |
| 404 | `NOT_FOUND` | Torrent hash not in registry |
| 409 | `ALREADY_EXISTS` | Torrent with this hash already added |
| 422 | `UNPROCESSABLE` | Valid JSON but invalid field values |
| 500 | `INTERNAL_ERROR` | Storage failure or unexpected panic |
