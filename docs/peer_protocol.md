# Peer Wire Protocol

`internal/peer`

This package handles everything from the moment you have an IP:port to the moment piece data lands in the results channel. It implements BEP 3 (the core protocol) plus BEP 11 (PEX).

---

## Handshake

Before any messages are exchanged, both sides perform a fixed-format handshake over TCP:

```
Byte layout (68 bytes total):
┌──────────────────────────────────────────────────────────────┐
│ 0x13 (1 byte)  │  "BitTorrent protocol" (19 bytes)           │
├──────────────────────────────────────────────────────────────┤
│ Reserved (8 bytes) — extension flags                         │
│   Byte 5, bit 4 = 1  →  Extension Protocol (BEP 10) enabled  │
│   Byte 7, bit 0 = 1  →  DHT enabled                          │
├──────────────────────────────────────────────────────────────┤
│ InfoHash (20 bytes)                                          │
├──────────────────────────────────────────────────────────────┤
│ PeerID   (20 bytes)                                          │
└──────────────────────────────────────────────────────────────┘
```

**Validation:** after receiving the peer's handshake, verify `peer.InfoHash == our.InfoHash`. Mismatch → close immediately.

**PeerID format (qbit-go):** `-QB0100-` + 12 random bytes. The prefix identifies the client and version in swarm analytics.

```go
func Handshake(conn net.Conn, infoHash, peerID [20]byte) error {
    buf := buildHandshake(infoHash, peerID)
    conn.SetDeadline(time.Now().Add(10 * time.Second))
    _, err := conn.Write(buf)
    if err != nil { return err }

    resp, err := readHandshake(conn)
    if err != nil { return err }

    if resp.InfoHash != infoHash {
        return ErrInfoHashMismatch
    }
    conn.SetDeadline(time.Time{}) // clear deadline for normal operation
    return nil
}
```

---

## Message Format

After the handshake, all messages share a common framing:

```
┌──────────────────────────────────┐
│  Length prefix  (4 bytes, big-endian uint32)                 │
│  Message ID     (1 byte)  — absent if length == 0 (keepalive)│
│  Payload        (length-1 bytes)                             │
└──────────────────────────────────┘
```

**Keepalive:** length = 0, no ID, no payload. Send every 2 minutes if no other message is sent.

```go
type MessageID uint8

const (
    MsgChoke         MessageID = 0
    MsgUnchoke       MessageID = 1
    MsgInterested    MessageID = 2
    MsgNotInterested MessageID = 3
    MsgHave          MessageID = 4   // payload: piece index (4 bytes)
    MsgBitfield      MessageID = 5   // payload: bitfield
    MsgRequest       MessageID = 6   // payload: index, begin, length (12 bytes)
    MsgPiece         MessageID = 7   // payload: index, begin, data
    MsgCancel        MessageID = 8   // payload: index, begin, length (12 bytes)
    MsgPort          MessageID = 9   // DHT port (BEP 5)
)
```

---

## Conn Lifecycle

```
dial / accept TCP connection
        │
        ▼
Handshake()
        │ success
        ▼
Send Bitfield (our current piece possession)
        │
        ▼
┌───────────────────────────────┐
│  readLoop goroutine           │  Reads from TCP, parses messages,
│  (blocks on conn.Read)        │  sends to inbound chan
└───────────────────────────────┘
┌───────────────────────────────┐
│  writeLoop goroutine          │  Reads from outbound chan,
│  (blocks on outbound chan)    │  serializes and writes to TCP
└───────────────────────────────┘
        │
        │  Normal flow:
        │  1. Receive Bitfield from peer
        │  2. If peer has pieces we need → send Interested
        │  3. Receive Unchoke
        │  4. Send Request ×N (pipelined)
        │  5. Receive Piece ×N → accumulate blocks
        │  6. Full piece received → send to results chan
        │  7. Send Have to all other peers
        │
        │  On any error → close conn, both goroutines exit
        ▼
peer.Manager notified → piece back on work queue if incomplete
```

---

## Pipelining

Without pipelining, each round trip wastes latency:

```
Without pipelining (bad):
Client: REQUEST piece 0
Server: PIECE piece 0     (wait for full RTT before next request)
Client: REQUEST piece 1
Server: PIECE piece 1
...

With pipelining (good):
Client: REQUEST piece 0
Client: REQUEST piece 1
Client: REQUEST piece 2   (5 in-flight simultaneously)
Server: PIECE piece 0
Server: PIECE piece 1
Server: PIECE piece 2
```

We keep **5 requests in flight** per peer at all times. When a block arrives, we immediately send the next request.

Block size is fixed at **16 KiB** (16,384 bytes). Pieces are split into blocks; a 256 KiB piece needs 16 requests.

```go
const MaxPipelineDepth = 5
const BlockSize = 16 * 1024  // 16 KiB
```

---

## Choking Algorithm

BitTorrent uses tit-for-tat: peers who upload to us get unchoked (can request from us). This incentivizes contribution.

**Every 10 seconds:**
- Rank all peers by their upload rate to us (in last 20 seconds)
- Unchoke the top 3 (or 4 in seeding mode, ranked by download rate to them)
- Choke everyone else

**Every 30 seconds (optimistic unchoke):**
- Pick one random choked peer and unchoke them regardless of rate
- This gives new peers a chance to join and discover fast peers

```go
func (m *Manager) runChokingAlgorithm() {
    ticker10 := time.NewTicker(10 * time.Second)
    ticker30 := time.NewTicker(30 * time.Second)
    for {
        select {
        case <-ticker10.C:
            m.regularUnchoke()
        case <-ticker30.C:
            m.optimisticUnchoke()
        case <-m.quit:
            return
        }
    }
}
```

---

## Peer Exchange (PEX) — BEP 11

After the handshake, if both sides indicated Extension Protocol support (reserved byte 5, bit 4), they negotiate extensions via `MsgExtended` (ID 20).

The first extended message is a handshake exchanging supported extension IDs:

```
Extension handshake payload (bencoded):
{
  "m": {
    "ut_pex": 1,       // we support peer exchange
    "ut_metadata": 2   // we support metadata exchange (for magnet links)
  },
  "v": "qbit-go 0.1.0"
}
```

Every 60 seconds, connected peers send each other lists of:
- **added:** peers they've connected to recently
- **dropped:** peers they've disconnected from recently

This lets the swarm grow without relying solely on the tracker.

```go
type PEXMessage struct {
    Added      []net.TCPAddr
    AddedFlags []byte        // bit 0x01 = prefers encryption
    Dropped    []net.TCPAddr
}
```

---

## Connection Limits and Peer Scoring

The manager maintains a connection pool capped at `config.MaxPerTorrent` (default 50).

When choosing which peers to dial from the tracker/DHT/PEX list, peers are scored:

| Factor | Weight |
|---|---|
| Has rare pieces we need | +10 |
| Previously connected, good rate | +5 |
| Same AS / geographic region | +2 |
| Previously timed out or choked repeatedly | -10 |
| Already at max connections | reject |

This is a simplified version of what production clients do. Even a basic version produces measurably better download speeds than random peer selection.

---

## Package Interface

```go
// Manager owns all peer connections for one torrent
type Manager struct {}

func NewManager(torrent *torrent.TorrentFile, bitfield bitfield.Bitfield,
                workQueue <-chan *scheduler.PieceWork,
                results chan<- *scheduler.PieceResult,
                limiter *ratelimit.Limiter) *Manager

func (m *Manager) AddPeers(peers []net.TCPAddr)
func (m *Manager) Run()                    // blocking — call in goroutine
func (m *Manager) Stop()
func (m *Manager) BroadcastHave(index int) // notify all peers of new piece
func (m *Manager) Stats() ManagerStats     // connected count, rates
```
