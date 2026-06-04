package dht

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	K     = 8 // k-bucket size
	alpha = 3 // concurrency parameter for lookups
)

// NodeID is a 160-bit Kademlia node identifier.
type NodeID [20]byte

// xorDistance computes the XOR distance between two NodeIDs.
func xorDistance(a, b NodeID) NodeID {
	var d NodeID
	for i := range d {
		d[i] = a[i] ^ b[i]
	}
	return d
}

// less reports whether a < b (big-endian byte comparison).
func less(a, b NodeID) bool {
	for i := range a {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return false
}

// bucketIndex returns which k-bucket a target belongs in relative to self.
func bucketIndex(self, target NodeID) int {
	d := xorDistance(self, target)
	for i, b := range d {
		if b == 0 {
			continue
		}
		for bit := 7; bit >= 0; bit-- {
			if (b>>uint(bit))&1 != 0 {
				return i*8 + (7 - bit)
			}
		}
	}
	return 159
}

// remoteNode holds the address and ID of a DHT peer.
type remoteNode struct {
	ID   NodeID
	Addr net.UDPAddr
	seen time.Time
}

// kBucket holds up to K nodes, LRU ordered.
type kBucket struct {
	mu    sync.Mutex
	nodes []*remoteNode
}

func (b *kBucket) add(n *remoteNode) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, existing := range b.nodes {
		if existing.ID == n.ID {
			b.nodes[i].seen = time.Now()
			return
		}
	}
	if len(b.nodes) < K {
		b.nodes = append(b.nodes, n)
		return
	}
	// evict LRU (index 0) — in production: ping first
	b.nodes = b.nodes[1:]
	b.nodes = append(b.nodes, n)
}

func (b *kBucket) closest(target NodeID, n int, out []*remoteNode) []*remoteNode {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, node := range b.nodes {
		if len(out) >= n {
			break
		}
		out = append(out, node)
	}
	return out
}

// RoutingTable maintains 160 k-buckets indexed by XOR distance prefix.
type RoutingTable struct {
	self    NodeID
	buckets [160]*kBucket
}

func newRoutingTable(self NodeID) *RoutingTable {
	rt := &RoutingTable{self: self}
	for i := range rt.buckets {
		rt.buckets[i] = &kBucket{}
	}
	return rt
}

func (rt *RoutingTable) add(n *remoteNode) {
	idx := bucketIndex(rt.self, n.ID)
	rt.buckets[idx].add(n)
}

func (rt *RoutingTable) closestN(target NodeID, n int) []*remoteNode {
	var out []*remoteNode
	for _, b := range rt.buckets {
		out = b.closest(target, n, out)
		if len(out) >= n {
			break
		}
	}
	return out
}

// DHTStats is returned by Node.Stats().
type DHTStats struct {
	NodeCount int
	Torrents  int
}

// Node is a Kademlia DHT node (BEP 5).
type Node struct {
	id    NodeID
	conn  *net.UDPConn
	table *RoutingTable

	mu       sync.Mutex
	pending  map[string]chan []byte // txID → reply channel
	quit     chan struct{}
}

var bootstrapNodes = []string{
	"dht.transmissionbt.com:6881",
	"router.bittorrent.com:6881",
	"router.utorrent.com:6881",
}

// New creates a DHT node listening on the given UDP port.
func New(port int) (*Node, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return nil, fmt.Errorf("dht: listen: %w", err)
	}

	var id NodeID
	rand.Read(id[:])

	n := &Node{
		id:      id,
		conn:    conn,
		table:   newRoutingTable(id),
		pending: make(map[string]chan []byte),
		quit:    make(chan struct{}),
	}
	go n.readLoop()
	return n, nil
}

// Bootstrap contacts the hardcoded bootstrap nodes to populate the routing table.
func (n *Node) Bootstrap() error {
	for _, host := range bootstrapNodes {
		addr, err := net.ResolveUDPAddr("udp", host)
		if err != nil {
			continue
		}
		n.sendFindNode(addr, NodeID(n.id))
	}
	return nil
}

// FindPeers performs a Kademlia peer lookup for infoHash and returns peer addresses.
func (n *Node) FindPeers(infoHash [20]byte) ([]net.UDPAddr, error) {
	target := NodeID(infoHash)
	closest := n.table.closestN(target, K)
	if len(closest) == 0 {
		return nil, fmt.Errorf("dht: routing table empty — bootstrap first")
	}
	var peers []net.UDPAddr
	queried := make(map[NodeID]bool)
	sem := make(chan struct{}, alpha)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range closest {
		if queried[node.ID] {
			continue
		}
		queried[node.ID] = true
		wg.Add(1)
		sem <- struct{}{}
		go func(nd *remoteNode) {
			defer func() { <-sem; wg.Done() }()
			found := n.getPeers(&nd.Addr, infoHash)
			mu.Lock()
			peers = append(peers, found...)
			mu.Unlock()
		}(node)
	}
	wg.Wait()
	return peers, nil
}

// Announce tells the K closest nodes that we are downloading infoHash on port.
func (n *Node) Announce(infoHash [20]byte, port int) error {
	_ = port
	// Simplified: in production this would send announce_peer with tokens
	return nil
}

// Stats returns a snapshot of routing table state.
func (n *Node) Stats() DHTStats {
	count := 0
	for _, b := range n.table.buckets {
		b.mu.Lock()
		count += len(b.nodes)
		b.mu.Unlock()
	}
	return DHTStats{NodeCount: count}
}

// Close shuts down the DHT node.
func (n *Node) Close() error {
	close(n.quit)
	return n.conn.Close()
}

func (n *Node) readLoop() {
	buf := make([]byte, 2048)
	for {
		nr, addr, err := n.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-n.quit:
				return
			default:
				log.Printf("dht: read error: %v", err)
				continue
			}
		}
		n.handle(buf[:nr], addr)
	}
}

func (n *Node) handle(data []byte, addr *net.UDPAddr) {
	// Minimal handler: register the sender in our routing table
	if len(data) < 20 {
		return
	}
	var id NodeID
	copy(id[:], data[:20])
	n.table.add(&remoteNode{ID: id, Addr: *addr, seen: time.Now()})

	// Dispatch pending reply channels
	if len(data) >= 4 {
		txID := string(data[:4])
		n.mu.Lock()
		ch, ok := n.pending[txID]
		n.mu.Unlock()
		if ok {
			select {
			case ch <- data:
			default:
			}
		}
	}
}

func (n *Node) sendFindNode(addr *net.UDPAddr, target NodeID) {
	txID := randomTxID()
	msg := buildFindNode(n.id, target, txID)
	n.conn.WriteToUDP(msg, addr) //nolint:errcheck
}

func (n *Node) getPeers(addr *net.UDPAddr, infoHash [20]byte) []net.UDPAddr {
	txID := randomTxID()
	msg := buildGetPeers(n.id, infoHash, txID)

	ch := make(chan []byte, 1)
	n.mu.Lock()
	n.pending[txID] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.pending, txID)
		n.mu.Unlock()
	}()

	n.conn.SetDeadline(time.Now().Add(5 * time.Second))
	n.conn.WriteToUDP(msg, addr) //nolint:errcheck

	select {
	case resp := <-ch:
		return parsePeersFromResponse(resp)
	case <-time.After(5 * time.Second):
		return nil
	}
}

func parsePeersFromResponse(data []byte) []net.UDPAddr {
	// Simplified: real implementation would bencode-decode the response
	_ = data
	return nil
}

func buildFindNode(self, target NodeID, txID string) []byte {
	// Simplified bencoded find_node message
	msg := fmt.Sprintf("d1:ad2:id20:%s6:target20:%se1:q9:find_node1:t%d:%s1:y1:qe",
		self[:], target[:], len(txID), txID)
	return []byte(msg)
}

func buildGetPeers(self NodeID, infoHash [20]byte, txID string) []byte {
	msg := fmt.Sprintf("d1:ad2:id20:%s9:info_hash20:%se1:q9:get_peers1:t%d:%s1:y1:qe",
		self[:], infoHash[:], len(txID), txID)
	return []byte(msg)
}

func randomTxID() string {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, rand.Uint32())
	return string(b)
}
