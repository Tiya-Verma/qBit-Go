package dht

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/tiyaverma/qbit-go/internal/bencode"
)

const (
	K        = 8
	alpha    = 3
	tokenTTL = 10 * time.Minute
)

// NodeID is a 160-bit Kademlia node identifier.
type NodeID [20]byte

func xorDistance(a, b NodeID) NodeID {
	var d NodeID
	for i := range d {
		d[i] = a[i] ^ b[i]
	}
	return d
}

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

type remoteNode struct {
	ID   NodeID
	Addr net.UDPAddr
	seen time.Time
}

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

// tokenEntry stores a token received from a responding node.
type tokenEntry struct {
	token  []byte
	expiry time.Time
}

// Node is a Kademlia DHT node (BEP 5).
type Node struct {
	id    NodeID
	conn  *net.UDPConn
	table *RoutingTable

	mu      sync.Mutex
	pending map[string]chan []byte // txID → reply channel
	tokens  map[string]tokenEntry  // addr.String() → token received from that node
	quit    chan struct{}
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
		tokens:  make(map[string]tokenEntry),
		quit:    make(chan struct{}),
	}
	go n.readLoop()
	return n, nil
}

// Bootstrap contacts hardcoded bootstrap nodes to populate the routing table.
// It blocks until all bootstrap queries have completed or timed out.
func (n *Node) Bootstrap() error {
	var wg sync.WaitGroup
	for _, host := range bootstrapNodes {
		addr, err := net.ResolveUDPAddr("udp", host)
		if err != nil {
			continue
		}
		wg.Add(1)
		go func(a *net.UDPAddr) {
			defer wg.Done()
			n.findNode(a, n.id)
		}(addr)
	}
	wg.Wait()
	return nil
}

// findNode sends a find_node query and waits for the response, adding returned
// compact nodes to the routing table. Unlike the old sendFindNode, it registers a
// pending channel so the response is actually processed.
func (n *Node) findNode(addr *net.UDPAddr, target NodeID) {
	txID := randomTxID()
	msg := buildQuery(txID, "find_node", map[string]interface{}{
		"id":     string(n.id[:]),
		"target": string(target[:]),
	})

	ch := make(chan []byte, 1)
	n.mu.Lock()
	n.pending[txID] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.pending, txID)
		n.mu.Unlock()
	}()

	n.conn.WriteToUDP(msg, addr) //nolint:errcheck

	select {
	case resp := <-ch:
		n.addNodesFromResponse(resp, addr)
	case <-time.After(5 * time.Second):
	}
}

// FindPeers performs an iterative Kademlia lookup for infoHash.
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
			found, _ := n.getPeers(&nd.Addr, infoHash)
			mu.Lock()
			peers = append(peers, found...)
			mu.Unlock()
		}(node)
	}
	wg.Wait()
	return peers, nil
}

// Announce sends get_peers to the closest nodes, collects tokens, then sends
// announce_peer to every node that responded with a token.
func (n *Node) Announce(infoHash [20]byte, port int) error {
	target := NodeID(infoHash)
	closest := n.table.closestN(target, K)
	if len(closest) == 0 {
		return fmt.Errorf("dht: routing table empty")
	}

	type responder struct {
		addr  net.UDPAddr
		token []byte
	}
	var mu sync.Mutex
	var respondents []responder
	var wg sync.WaitGroup
	sem := make(chan struct{}, alpha)
	queried := make(map[NodeID]bool)

	for _, node := range closest {
		if queried[node.ID] {
			continue
		}
		queried[node.ID] = true
		wg.Add(1)
		sem <- struct{}{}
		go func(nd *remoteNode) {
			defer func() { <-sem; wg.Done() }()
			_, token := n.getPeers(&nd.Addr, infoHash)
			if len(token) > 0 {
				mu.Lock()
				respondents = append(respondents, responder{nd.Addr, token})
				mu.Unlock()
			}
		}(node)
	}
	wg.Wait()

	for _, r := range respondents {
		addr := r.addr
		n.announcePeer(&addr, infoHash, port, r.token)
	}
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
	buf := make([]byte, 65536)
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
		data := make([]byte, nr)
		copy(data, buf[:nr])
		go n.handle(data, addr)
	}
}

func (n *Node) handle(data []byte, addr *net.UDPAddr) {
	raw, err := bencode.Unmarshal(data)
	if err != nil {
		return
	}
	msg, ok := raw.(map[string]interface{})
	if !ok {
		return
	}

	// Register sender in routing table.
	n.addSenderToTable(msg, addr)

	txID, _ := msg["t"].(string)
	y, _ := msg["y"].(string)

	switch y {
	case "q":
		q, _ := msg["q"].(string)
		a, _ := msg["a"].(map[string]interface{})
		n.handleQuery(q, a, txID, addr)
	case "r":
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

func (n *Node) addSenderToTable(msg map[string]interface{}, addr *net.UDPAddr) {
	var idStr string
	if r, ok := msg["r"].(map[string]interface{}); ok {
		idStr, _ = r["id"].(string)
	} else if a, ok := msg["a"].(map[string]interface{}); ok {
		idStr, _ = a["id"].(string)
	}
	if len(idStr) == 20 {
		var id NodeID
		copy(id[:], idStr)
		n.table.add(&remoteNode{ID: id, Addr: *addr, seen: time.Now()})
	}
}

func (n *Node) handleQuery(q string, a map[string]interface{}, txID string, addr *net.UDPAddr) {
	switch q {
	case "ping":
		n.sendMsg(addr, buildResponse(txID, map[string]interface{}{
			"id": string(n.id[:]),
		}))

	case "find_node":
		targetStr, _ := a["target"].(string)
		if len(targetStr) != 20 {
			return
		}
		var target NodeID
		copy(target[:], targetStr)
		closest := n.table.closestN(target, K)
		n.sendMsg(addr, buildResponse(txID, map[string]interface{}{
			"id":    string(n.id[:]),
			"nodes": encodeCompactNodes(closest),
		}))

	case "get_peers":
		infoHashStr, _ := a["info_hash"].(string)
		if len(infoHashStr) != 20 {
			return
		}
		var ih NodeID
		copy(ih[:], infoHashStr)
		token := makeToken()
		closest := n.table.closestN(ih, K)
		n.sendMsg(addr, buildResponse(txID, map[string]interface{}{
			"id":    string(n.id[:]),
			"nodes": encodeCompactNodes(closest),
			"token": string(token),
		}))

	case "announce_peer":
		// Accept the announce; update routing table entry (already done in addSenderToTable).
		n.sendMsg(addr, buildResponse(txID, map[string]interface{}{
			"id": string(n.id[:]),
		}))
	}
}

func (n *Node) sendMsg(addr *net.UDPAddr, data []byte) {
	n.conn.WriteToUDP(data, addr) //nolint:errcheck
}


func (n *Node) getPeers(addr *net.UDPAddr, infoHash [20]byte) ([]net.UDPAddr, []byte) {
	txID := randomTxID()
	msg := buildQuery(txID, "get_peers", map[string]interface{}{
		"id":        string(n.id[:]),
		"info_hash": string(infoHash[:]),
	})

	ch := make(chan []byte, 1)
	n.mu.Lock()
	n.pending[txID] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.pending, txID)
		n.mu.Unlock()
	}()

	n.conn.WriteToUDP(msg, addr) //nolint:errcheck

	select {
	case resp := <-ch:
		peers, token := parsePeersAndToken(resp)
		// Add intermediate nodes from the response to our routing table.
		n.addNodesFromResponse(resp, addr)
		if len(token) > 0 {
			n.mu.Lock()
			n.tokens[addr.String()] = tokenEntry{token: token, expiry: time.Now().Add(tokenTTL)}
			n.mu.Unlock()
		}
		return peers, token
	case <-time.After(5 * time.Second):
		return nil, nil
	}
}

func (n *Node) addNodesFromResponse(data []byte, _ *net.UDPAddr) {
	raw, err := bencode.Unmarshal(data)
	if err != nil {
		return
	}
	top, ok := raw.(map[string]interface{})
	if !ok {
		return
	}
	r, ok := top["r"].(map[string]interface{})
	if !ok {
		return
	}
	nodesStr, _ := r["nodes"].(string)
	for _, nd := range decodeCompactNodes(nodesStr) {
		n.table.add(nd)
	}
}

func (n *Node) announcePeer(addr *net.UDPAddr, infoHash [20]byte, port int, token []byte) {
	txID := randomTxID()
	msg := buildQuery(txID, "announce_peer", map[string]interface{}{
		"id":           string(n.id[:]),
		"implied_port": 0,
		"info_hash":    string(infoHash[:]),
		"port":         port,
		"token":        string(token),
	})
	n.conn.WriteToUDP(msg, addr) //nolint:errcheck
}

// --- Message builders (keys sorted per bencode spec) ---

// buildResponse builds a bencoded DHT response: {"r": r, "t": txID, "y": "r"}.
func buildResponse(txID string, r map[string]interface{}) []byte {
	var buf bytes.Buffer
	buf.WriteByte('d')
	writeStrVal(&buf, "r")
	writeSortedDict(&buf, r)
	writeStrVal(&buf, "t")
	writeStrVal(&buf, txID)
	writeStrVal(&buf, "y")
	writeStrVal(&buf, "r")
	buf.WriteByte('e')
	return buf.Bytes()
}

// buildQuery builds a bencoded DHT query: {"a": a, "q": q, "t": txID, "y": "q"}.
func buildQuery(txID, q string, a map[string]interface{}) []byte {
	var buf bytes.Buffer
	buf.WriteByte('d')
	writeStrVal(&buf, "a")
	writeSortedDict(&buf, a)
	writeStrVal(&buf, "q")
	writeStrVal(&buf, q)
	writeStrVal(&buf, "t")
	writeStrVal(&buf, txID)
	writeStrVal(&buf, "y")
	writeStrVal(&buf, "q")
	buf.WriteByte('e')
	return buf.Bytes()
}

func writeSortedDict(buf *bytes.Buffer, m map[string]interface{}) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf.WriteByte('d')
	for _, k := range keys {
		writeStrVal(buf, k)
		writeAnyVal(buf, m[k])
	}
	buf.WriteByte('e')
}

func writeStrVal(buf *bytes.Buffer, s string) {
	fmt.Fprintf(buf, "%d:", len(s))
	buf.WriteString(s)
}

func writeAnyVal(buf *bytes.Buffer, v interface{}) {
	switch vv := v.(type) {
	case string:
		writeStrVal(buf, vv)
	case int:
		fmt.Fprintf(buf, "i%de", vv)
	}
}

// --- Compact encoding helpers ---

func encodeCompactNodes(nodes []*remoteNode) string {
	buf := make([]byte, 0, len(nodes)*26)
	for _, nd := range nodes {
		ip4 := nd.Addr.IP.To4()
		if ip4 == nil {
			continue
		}
		buf = append(buf, nd.ID[:]...)
		buf = append(buf, ip4...)
		port := make([]byte, 2)
		binary.BigEndian.PutUint16(port, uint16(nd.Addr.Port))
		buf = append(buf, port...)
	}
	return string(buf)
}

func decodeCompactNodes(s string) []*remoteNode {
	data := []byte(s)
	var nodes []*remoteNode
	for i := 0; i+26 <= len(data); i += 26 {
		var id NodeID
		copy(id[:], data[i:i+20])
		ip := make(net.IP, 4)
		copy(ip, data[i+20:i+24])
		port := binary.BigEndian.Uint16(data[i+24 : i+26])
		nodes = append(nodes, &remoteNode{
			ID:   id,
			Addr: net.UDPAddr{IP: ip, Port: int(port)},
			seen: time.Now(),
		})
	}
	return nodes
}

// parsePeersAndToken extracts peer addresses and the token from a get_peers response.
func parsePeersAndToken(data []byte) ([]net.UDPAddr, []byte) {
	raw, err := bencode.Unmarshal(data)
	if err != nil {
		return nil, nil
	}
	top, ok := raw.(map[string]interface{})
	if !ok {
		return nil, nil
	}
	r, ok := top["r"].(map[string]interface{})
	if !ok {
		return nil, nil
	}

	var token []byte
	if t, ok := r["token"].(string); ok {
		token = []byte(t)
	}

	var peers []net.UDPAddr
	if values, ok := r["values"].([]interface{}); ok {
		for _, v := range values {
			s, ok := v.(string)
			if !ok || len(s) != 6 {
				continue
			}
			b := []byte(s)
			ip := make(net.IP, 4)
			copy(ip, b[0:4])
			peers = append(peers, net.UDPAddr{
				IP:   ip,
				Port: int(binary.BigEndian.Uint16(b[4:6])),
			})
		}
	}

	return peers, token
}

func makeToken() []byte {
	b := make([]byte, 4)
	rand.Read(b)
	return b
}

func randomTxID() string {
	b := make([]byte, 2)
	rand.Read(b)
	return string(b)
}
