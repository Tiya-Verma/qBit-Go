# DHT — Distributed Hash Table

`internal/dht`

The DHT (BEP 5) lets you find peers for a torrent **without a tracker**. It's what makes magnet links work. Every DHT-enabled client is a node in a distributed key-value network. The key is an info hash; the value is a list of peers downloading that torrent.

The algorithm is **Kademlia** — the same DHT used by Ethereum, IPFS, and most peer-to-peer systems. Understanding it will come up in distributed systems interviews.

---

## Core Concept: XOR Distance

In Kademlia, "distance" between two nodes (or between a node and a key) is defined as the **bitwise XOR** of their 160-bit IDs:

```
distance(A, B) = A XOR B
```

This is not geographic or network distance. It's a mathematical metric that has a useful property: for any node A and target key K, exactly one other node is "closer" in each bit position. This creates a structured routing topology.

```go
type NodeID [20]byte

func distance(a, b NodeID) NodeID {
    var d NodeID
    for i := range d {
        d[i] = a[i] ^ b[i]
    }
    return d
}

func (d NodeID) Less(other NodeID) bool {
    for i := range d {
        if d[i] < other[i] { return true }
        if d[i] > other[i] { return false }
    }
    return false
}
```

---

## Routing Table: K-Buckets

Each node maintains a routing table of **160 buckets** (one per bit of the node ID). Each bucket holds up to **K=8** nodes whose IDs share a common prefix of that length with ours.

```
Bucket 0: nodes whose ID differs from ours in the first bit
Bucket 1: nodes differing in bits 1-2
Bucket 2: nodes differing in bits 2-4
...
Bucket 159: the bucket for nodes very close to us
```

When a bucket is full and a new node is discovered:
- Ping the **least recently seen** node in the bucket
- If it responds → keep it, discard the new node (prefer known good nodes)
- If it doesn't → evict it, add the new node

```go
type RoutingTable struct {
    self    NodeID
    buckets [160]*KBucket
}

type KBucket struct {
    nodes []*Node       // up to K=8 nodes, LRU ordered
    mu    sync.Mutex
}

func (rt *RoutingTable) Add(n *Node) {
    bucket := rt.bucketFor(n.ID)
    bucket.Add(n, rt.ping)
}

func (rt *RoutingTable) ClosestN(target NodeID, n int) []*Node {
    // return n closest nodes to target across all buckets
}
```

---

## The Four RPC Messages

Kademlia defines four operations, all sent over UDP:

### 1. ping
Are you alive?
```
Request:  { "t": txID, "y": "q", "q": "ping", "a": {"id": ourID} }
Response: { "t": txID, "y": "r", "r": {"id": theirID} }
```

### 2. find_node
Find nodes close to a target ID.
```
Request:  { "q": "find_node", "a": {"id": ourID, "target": targetID} }
Response: { "r": {"id": theirID, "nodes": <compact node info>} }
```

Compact node info: 26 bytes per node — 20 bytes NodeID + 4 bytes IP + 2 bytes port.

### 3. get_peers
Find peers for a torrent info hash.
```
Request:  { "q": "get_peers", "a": {"id": ourID, "info_hash": infoHash} }

Response A (has peers):
  { "r": {"id": theirID, "token": <token>, "values": [<compact peer>...]} }

Response B (doesn't have peers, but knows closer nodes):
  { "r": {"id": theirID, "token": <token>, "nodes": <compact node info>} }
```

The **token** is required for the next step (announce_peer). It's a short secret that proves you recently talked to this node.

### 4. announce_peer
Tell a node "I am downloading this torrent at this port."
```
Request:  { "q": "announce_peer", "a": {
              "id": ourID,
              "info_hash": infoHash,
              "port": 6881,
              "token": <token from get_peers response>
            }}
Response: { "r": {"id": theirID} }
```

---

## Peer Lookup Algorithm

This is the core of Kademlia. To find peers for `infoHash`:

```
1. Start with the K closest nodes to infoHash from our routing table

2. Send get_peers to all of them simultaneously

3. For each response:
   - If it contains "values" (peers) → add to results, continue
   - If it contains "nodes" → add them to the candidate set

4. From the candidate set, pick the K closest nodes to infoHash
   that we haven't queried yet

5. Send get_peers to those nodes (alpha=3 concurrent queries at a time)

6. Repeat steps 3-5 until:
   - We've queried all K closest nodes and got no new candidates, OR
   - We have enough peers (configurable threshold)

7. announce_peer to the K closest nodes who responded
   (so they'll return us to future queries)
```

The **alpha=3 concurrent queries** is a Kademlia tuning parameter — enough parallelism to be fast, not so much that we flood the network.

```go
func (n *Node) FindPeers(infoHash [20]byte) ([]net.UDPAddr, error) {
    closest := n.table.ClosestN(NodeID(infoHash), K)
    queried := make(map[NodeID]bool)
    candidates := NewSortedByDistance(NodeID(infoHash))
    candidates.AddAll(closest)

    var peers []net.UDPAddr
    sem := make(chan struct{}, alpha) // alpha = 3

    for !candidates.Empty() {
        node := candidates.PopClosestUnqueried(queried)
        if node == nil { break }

        sem <- struct{}{}
        go func(node *Node) {
            defer func() { <-sem }()
            resp, err := n.rpc.GetPeers(node, infoHash)
            if err != nil { return }
            if resp.Peers != nil {
                peers = append(peers, resp.Peers...)
            }
            if resp.Nodes != nil {
                candidates.AddAll(resp.Nodes)
            }
        }(node)
    }
    return peers, nil
}
```

---

## Bootstrap

On startup, the DHT node has an empty routing table. To join the network, it contacts hardcoded **bootstrap nodes** (maintained by the BitTorrent Foundation):

```go
var bootstrapNodes = []string{
    "dht.transmissionbt.com:6881",
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "dht.aelitis.com:6881",
}
```

Bootstrap procedure:
1. Send `find_node` to bootstrap nodes with our own ID as the target
2. Each response gives us real DHT nodes close to us
3. Add those to our routing table and repeat
4. After ~3 rounds, our routing table is populated enough to be useful

---

## Node Persistence

The routing table is saved to disk every 15 minutes and on shutdown. On startup, we restore it and verify nodes are still alive via ping before using them. This avoids the bootstrap delay on restarts.

---

## Interface

```go
type Node struct{}

func NewNode(id NodeID, port int) *Node
func (n *Node) Bootstrap(nodes []string) error
func (n *Node) FindPeers(infoHash [20]byte) ([]net.UDPAddr, error)
func (n *Node) Announce(infoHash [20]byte, port int) error
func (n *Node) Close() error
func (n *Node) Stats() DHTStats
```

```go
type DHTStats struct {
    NodeCount    int     // nodes in routing table
    GoodNodes    int     // nodes that responded recently
    Torrents     int     // distinct info hashes we've seen
}
```
