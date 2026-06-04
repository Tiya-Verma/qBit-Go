# Piece Scheduler

`internal/scheduler`

The scheduler decides **which piece to request from which peer**. This sounds simple but the algorithm directly determines download speed, swarm health, and user experience for partial downloads.

---

## The Problem

You have:
- 500 pieces remaining
- 30 connected peers, each with a different subset of pieces
- A single work queue feeding all peer goroutines

Naively picking pieces at random works but is measurably worse than the alternatives. The scheduler implements three modes:

| Mode | Use case |
|---|---|
| Rarest-first | Default. Best overall download speed and swarm health. |
| Sequential | User wants to watch/play the file before it finishes. |
| End-game | Activated automatically when <20 pieces remain. |

---

## Mode 1: Rarest-First

**Principle:** Always download the piece that the fewest peers have. This maximizes piece diversity across the swarm — if everyone downloads popular pieces first, rare pieces might disappear when their only seeder goes offline.

**Data structure:** a frequency map tracking how many connected peers have each piece.

```go
type Scheduler struct {
    pieceCount   int
    pieceHashes  [][20]byte

    mu           sync.Mutex
    frequency    []int              // frequency[i] = how many peers have piece i
    have         bitfield.Bitfield  // pieces we have
    inFlight     map[int]bool       // pieces currently being downloaded

    mode         Mode
}
```

**When a peer connects** and sends their Bitfield:
```go
func (s *Scheduler) AddPeerBitfield(bf bitfield.Bitfield) {
    s.mu.Lock()
    defer s.mu.Unlock()
    for i := 0; i < s.pieceCount; i++ {
        if bf.HasPiece(i) {
            s.frequency[i]++
        }
    }
}
```

**When a peer disconnects:**
```go
func (s *Scheduler) RemovePeerBitfield(bf bitfield.Bitfield) {
    // decrement frequency for each piece they had
}
```

**Piece selection:**
```go
func (s *Scheduler) PickPiece(peerBitfield bitfield.Bitfield) (int, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()

    rarest := -1
    rarestFreq := math.MaxInt

    for i := 0; i < s.pieceCount; i++ {
        if s.have.HasPiece(i)    { continue }  // already have it
        if s.inFlight[i]         { continue }  // being downloaded
        if !peerBitfield.HasPiece(i) { continue }  // this peer doesn't have it

        if s.frequency[i] < rarestFreq {
            rarestFreq = s.frequency[i]
            rarest = i
        }
    }

    if rarest == -1 { return 0, false }

    s.inFlight[rarest] = true
    return rarest, true
}
```

**Time complexity:** O(n) per pick where n = piece count. For a typical torrent (500-2000 pieces) this is negligible. For very large torrents, a min-heap indexed by frequency reduces this to O(log n).

---

## Mode 2: Sequential

The user wants to stream or preview the file before it finishes. Pick pieces in order from index 0 upward.

**The tradeoff:** sequential downloading is bad for swarm health (everyone wants piece 0 simultaneously, flooding seeders of early pieces) but necessary for streaming use cases.

```go
func (s *Scheduler) pickSequential(peerBitfield bitfield.Bitfield) (int, bool) {
    for i := 0; i < s.pieceCount; i++ {
        if s.have.HasPiece(i)       { continue }
        if s.inFlight[i]            { continue }
        if !peerBitfield.HasPiece(i){ continue }
        s.inFlight[i] = true
        return i, true
    }
    return 0, false
}
```

**Hybrid mode (practical improvement):** download the first 5% of pieces sequentially (for playback start), then switch to rarest-first for the remainder. Most video players buffer about 2% before playback starts.

---

## Mode 3: End-Game

When **less than 20 pieces remain**, the normal scheduler creates a bottleneck: you're waiting for a single slow peer to deliver your last few pieces. End-game mode sends duplicate requests for remaining pieces to **all peers** who have them.

```
Normal mode (last 3 pieces):
  Peer A has piece 497 → assigned to Peer A only
  Peer A is slow / congested → you wait

End-game mode:
  All peers who have piece 497 → all get Request{497}
  Whoever replies first wins → Cancel{497} sent to the rest
```

This typically reduces the final wait from 10-30 seconds to under 1 second.

```go
func (s *Scheduler) EnterEndGame() {
    s.mu.Lock()
    s.mode = ModeEndGame
    // clear inFlight so all remaining pieces get re-broadcast
    s.inFlight = make(map[int]bool)
    s.mu.Unlock()
}

func (s *Scheduler) IsEndGame() bool {
    return s.piecesRemaining() < 20
}
```

The peer manager handles end-game by sending each remaining `PieceWork` to **all** peers who have it rather than one, and tracking which response arrives first.

---

## Priority Queues (Multi-File Torrents)

When a torrent contains multiple files and the user selects which ones to download, pieces are assigned priorities:

```go
type Priority int

const (
    PrioritySkip   Priority = 0  // don't download
    PriorityLow    Priority = 1
    PriorityNormal Priority = 2
    PriorityHigh   Priority = 3
)
```

The scheduler maintains a priority per piece (derived from the files it belongs to). When picking the next piece:

1. Filter: skip pieces with `PrioritySkip`
2. Among eligible pieces, prefer higher priority
3. Within the same priority, apply rarest-first

**Cross-boundary pieces** (a piece that contains bytes from two different files) inherit the **higher** of the two files' priorities.

---

## Dispatch Loop

The scheduler's main loop runs as a single goroutine:

```go
func (s *Scheduler) Run(
    peers    <-chan peer.ConnWithBitfield,  // new peers joining
    departed <-chan peer.ConnWithBitfield,  // peers leaving
    requests chan<- PieceRequest,           // send work to peer goroutines
    results  <-chan PieceResult,            // completed pieces
) {
    for {
        select {
        case p := <-peers:
            s.AddPeerBitfield(p.Bitfield)
            s.tryDispatch(p.Conn, requests)

        case p := <-departed:
            s.RemovePeerBitfield(p.Bitfield)

        case r := <-results:
            s.MarkComplete(r.Index)
            if s.IsEndGame() && !s.inEndGame {
                s.EnterEndGame()
            }
            // re-dispatch if this peer has other pieces we need
            s.tryDispatch(r.Conn, requests)

        case <-s.quit:
            return
        }
    }
}
```

One goroutine. No locks needed on the frequency map because only this goroutine touches it.

---

## Metrics

The scheduler exposes stats used by the API and WebSocket feed:

```go
type Stats struct {
    PiecesTotal     int
    PiecesComplete  int
    PiecesInFlight  int
    Mode            Mode      // Rarest / Sequential / EndGame
    ETA             time.Duration
    DownloadSpeed   int64     // bytes/sec (rolling 20s average)
}
```

ETA calculation:
```
bytesRemaining = (piecesTotal - piecesComplete) * pieceLength
ETA = bytesRemaining / downloadSpeed
```
