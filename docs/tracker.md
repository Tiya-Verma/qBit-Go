# Tracker Protocol

`internal/tracker`

Trackers are HTTP or UDP servers that maintain lists of peers for each torrent. You announce your presence and receive a list of peers in return. This is the bootstrapping mechanism — without peers, you can't download.

---

## HTTP Tracker (BEP 3)

A standard HTTP GET request to the tracker's announce URL.

### Request Parameters

```
GET /announce?
  info_hash=%xx%xx...   (20 bytes, URL-encoded)
  peer_id=-QB0100-xxxx  (20 bytes)
  port=6881             (our listening port)
  uploaded=0            (bytes uploaded this session)
  downloaded=0          (bytes downloaded this session)
  left=1073741824       (bytes remaining)
  compact=1             (request compact peer format — BEP 23)
  event=started         (started | stopped | completed | empty)
  numwant=50            (how many peers we want)
```

**Events:**
- `started` — first announce for this torrent
- `stopped` — we're shutting down / removing the torrent
- `completed` — we just finished downloading (transitions to seeding)
- *(omit event)* — regular re-announce

### Response (bencoded)

```
{
  "interval": 1800,          // re-announce every N seconds
  "min interval": 900,       // minimum re-announce interval
  "complete": 42,            // seeders in swarm
  "incomplete": 17,          // leechers in swarm
  "peers": <compact binary>  // 6 bytes × N peers
}
```

**Compact peer format (BEP 23):** 6 bytes per peer — 4 bytes IPv4 address + 2 bytes port, big-endian. Always request compact; non-compact (list of dicts) is deprecated and wastes bandwidth.

```go
func parseCompactPeers(data []byte) ([]net.TCPAddr, error) {
    if len(data)%6 != 0 {
        return nil, ErrInvalidPeerData
    }
    peers := make([]net.TCPAddr, len(data)/6)
    for i := range peers {
        b := data[i*6 : i*6+6]
        ip   := net.IP(b[0:4])
        port := binary.BigEndian.Uint16(b[4:6])
        peers[i] = net.TCPAddr{IP: ip, Port: int(port)}
    }
    return peers, nil
}
```

### Announce Loop

```go
func (c *HTTPClient) AnnounceLoop(ctx context.Context, peers chan<- []net.TCPAddr) {
    event := "started"
    for {
        resp, err := c.Announce(event)
        if err != nil {
            // exponential backoff: 30s → 60s → 120s → ... → 1800s
            c.backoff.Wait()
            continue
        }
        event = ""  // subsequent announces omit event
        peers <- resp.Peers

        select {
        case <-time.After(time.Duration(resp.Interval) * time.Second):
        case <-ctx.Done():
            c.Announce("stopped")
            return
        }
    }
}
```

---

## UDP Tracker (BEP 15)

Most modern trackers use UDP. It's faster (no TCP handshake overhead) and cheaper to run. The protocol uses a connection ID mechanism to prevent IP spoofing.

### Flow

```
1. CONNECT REQUEST
   Client → Tracker:
   ┌────────────────────────────────────┐
   │ connection_id  = 0x41727101980     │ (magic constant)
   │ action         = 0  (connect)      │
   │ transaction_id = random uint32     │
   └────────────────────────────────────┘

2. CONNECT RESPONSE
   Tracker → Client:
   ┌────────────────────────────────────┐
   │ action         = 0  (connect)      │
   │ transaction_id = (matches request) │
   │ connection_id  = <new ID>          │ ← use this for next 2 minutes
   └────────────────────────────────────┘

3. ANNOUNCE REQUEST
   Client → Tracker:
   ┌────────────────────────────────────┐
   │ connection_id  = <from step 2>     │
   │ action         = 1  (announce)     │
   │ transaction_id = random uint32     │
   │ info_hash      = [20]byte          │
   │ peer_id        = [20]byte          │
   │ downloaded     = int64             │
   │ left           = int64             │
   │ uploaded       = int64             │
   │ event          = int32             │
   │ ip             = 0  (use sender)   │
   │ key            = random uint32     │
   │ num_want       = -1  (default)     │
   │ port           = uint16            │
   └────────────────────────────────────┘

4. ANNOUNCE RESPONSE
   Tracker → Client:
   ┌────────────────────────────────────┐
   │ action         = 1  (announce)     │
   │ transaction_id = (matches)         │
   │ interval       = uint32            │
   │ leechers       = uint32            │
   │ seeders        = uint32            │
   │ peers          = 6 bytes × N       │
   └────────────────────────────────────┘
```

### Retransmission

UDP packets can be lost. BEP 15 specifies a retry schedule:

```
Attempt 0: send, wait 15 seconds
Attempt 1: send, wait 30 seconds
Attempt 2: send, wait 60 seconds
Attempt 3: send, wait 120 seconds
...up to 8 attempts, then give up
```

```go
func (c *UDPClient) sendWithRetry(req []byte) ([]byte, error) {
    for n := 0; n < 8; n++ {
        c.conn.SetDeadline(time.Now().Add(15 * (1 << n) * time.Second))
        c.conn.Write(req)
        resp := make([]byte, 2048)
        nr, err := c.conn.Read(resp)
        if err == nil {
            return resp[:nr], nil
        }
        // timeout → retry
    }
    return nil, ErrTrackerUnreachable
}
```

---

## Multi-Tracker Support

Many `.torrent` files include a `announce-list` (BEP 12): a list of tiers, each tier a list of tracker URLs. The algorithm:

```
For each tier (in order):
    Shuffle URLs within the tier
    Try each URL until one succeeds
    If successful, move that URL to the front of the tier

If tier 0 fails entirely → try tier 1
If all tiers fail → fall back to DHT
```

```go
type MultiTracker struct {
    tiers [][]TrackerClient  // tiers[0] is highest priority
}

func (m *MultiTracker) Announce(event string) (*AnnounceResponse, error) {
    for _, tier := range m.tiers {
        rand.Shuffle(len(tier), func(i, j int) { tier[i], tier[j] = tier[j], tier[i] })
        for i, tracker := range tier {
            resp, err := tracker.Announce(event)
            if err == nil {
                if i != 0 { tier[0], tier[i] = tier[i], tier[0] } // promote
                return resp, nil
            }
        }
    }
    return nil, ErrAllTrackersFailed
}
```

---

## Client Interface

```go
type Client interface {
    Announce(event string) (*AnnounceResponse, error)
    Close() error
}

type AnnounceResponse struct {
    Interval   int
    Seeders    int
    Leechers   int
    Peers      []net.TCPAddr
}

// Factory — picks HTTP or UDP based on URL scheme
func NewClient(announceURL string, params AnnounceParams) (Client, error) {
    u, err := url.Parse(announceURL)
    switch u.Scheme {
    case "http", "https":
        return NewHTTPClient(u, params)
    case "udp":
        return NewUDPClient(u, params)
    default:
        return nil, ErrUnsupportedTrackerScheme
    }
}
```
