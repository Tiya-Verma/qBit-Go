package dht

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiyaverma/qbit-go/internal/bencode"
)

// ---- xorDistance ----

func TestXorDistance(t *testing.T) {
	tests := []struct {
		name string
		a, b NodeID
		want NodeID
	}{
		{
			name: "all zeros",
			a:    NodeID{},
			b:    NodeID{},
			want: NodeID{},
		},
		{
			name: "XOR with self is zero",
			a:    NodeID{0x01, 0x02, 0x03},
			b:    NodeID{0x01, 0x02, 0x03},
			want: NodeID{},
		},
		{
			name: "known values",
			a:    NodeID{0xff, 0x0f, 0xaa},
			b:    NodeID{0x0f, 0xf0, 0x55},
			want: NodeID{0xf0, 0xff, 0xff},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := xorDistance(tc.a, tc.b)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ---- less ----

func TestLess(t *testing.T) {
	var zero, one, big NodeID
	one[19] = 1   // smallest non-zero (least-significant byte)
	big[0] = 0xff // large value in most-significant byte

	assert.True(t, less(zero, one), "zero < one")
	assert.True(t, less(one, big), "one < big")
	assert.False(t, less(one, zero), "one is not < zero")
	assert.False(t, less(zero, zero), "equal nodes: not less")
	assert.False(t, less(big, big), "equal nodes: not less")
}

// ---- bucketIndex ----

func TestBucketIndex(t *testing.T) {
	var self NodeID // all zeros

	t.Run("self equals target returns 159", func(t *testing.T) {
		// XOR distance is 0x00…00; the loop finds no set bit → returns 159.
		assert.Equal(t, 159, bucketIndex(self, self))
	})

	t.Run("first bit differs returns 0", func(t *testing.T) {
		var target NodeID
		target[0] = 0x80 // bit 7 of byte 0 → XOR index 0
		assert.Equal(t, 0, bucketIndex(self, target))
	})

	t.Run("second bit differs returns 1", func(t *testing.T) {
		var target NodeID
		target[0] = 0x40 // bit 6 of byte 0 → XOR index 1
		assert.Equal(t, 1, bucketIndex(self, target))
	})
}

// ---- encodeCompactNodes / decodeCompactNodes ----

func makeRemoteNode(idByte byte, ip net.IP, port int) *remoteNode {
	var nodeID NodeID
	nodeID[0] = idByte
	return &remoteNode{
		ID:   nodeID,
		Addr: net.UDPAddr{IP: ip.To4(), Port: port},
		seen: time.Now(),
	}
}

func TestCompactNodesRoundTrip(t *testing.T) {
	nodes := []*remoteNode{
		makeRemoteNode(0x01, net.ParseIP("192.168.1.1"), 6881),
		makeRemoteNode(0x02, net.ParseIP("10.0.0.2"), 6882),
		makeRemoteNode(0x03, net.ParseIP("172.16.0.3"), 6883),
	}

	encoded := encodeCompactNodes(nodes)
	// 3 nodes × 26 bytes each (20 ID + 4 IP + 2 port).
	assert.Len(t, encoded, 3*26)

	decoded := decodeCompactNodes(encoded)
	require.Len(t, decoded, 3)

	for i, orig := range nodes {
		assert.Equal(t, orig.ID, decoded[i].ID, "node %d: ID mismatch", i)
		assert.Equal(t, orig.Addr.Port, decoded[i].Addr.Port, "node %d: Port mismatch", i)
		assert.True(t, orig.Addr.IP.Equal(decoded[i].Addr.IP), "node %d: IP mismatch", i)
	}
}

func TestDecodeCompactNodes_Empty(t *testing.T) {
	assert.Empty(t, decodeCompactNodes(""))
}

func TestDecodeCompactNodes_PartialData(t *testing.T) {
	// 25 bytes is not a multiple of 26 → should decode zero nodes.
	partial := make([]byte, 25)
	assert.Empty(t, decodeCompactNodes(string(partial)))
}

// ---- parsePeersAndToken ----

// buildGetPeersResponse manually constructs a bencoded get_peers response:
//
//	d 1:r d 2:id 20:<20 zeros> 5:token N:<token> 6:values l 6:<peer> e e 1:t 2:aa 1:y 1:r e
func buildGetPeersResponse(peerIP net.IP, peerPort uint16, token string) []byte {
	peer := make([]byte, 6)
	copy(peer[:4], peerIP.To4())
	binary.BigEndian.PutUint16(peer[4:], peerPort)

	idStr := string(make([]byte, 20))
	peerStr := string(peer)

	r := "d" +
		"2:id" + "20:" + idStr +
		"5:token" + lenPrefix(token) + ":" + token +
		"6:values" + "l" + "6:" + peerStr + "e" +
		"e"

	return []byte("d" + "1:r" + r + "1:t" + "2:aa" + "1:y" + "1:r" + "e")
}

// lenPrefix returns the decimal string length of s for bencode prefixes.
func lenPrefix(s string) string {
	n := len(s)
	if n == 0 {
		return "0"
	}
	result := ""
	for n > 0 {
		result = string(rune('0'+n%10)) + result
		n /= 10
	}
	return result
}

func TestParsePeersAndToken(t *testing.T) {
	wantIP := net.ParseIP("1.2.3.4")
	wantPort := uint16(6881)
	wantToken := "test"

	data := buildGetPeersResponse(wantIP, wantPort, wantToken)

	peers, token := parsePeersAndToken(data)

	require.Len(t, peers, 1)
	assert.True(t, wantIP.Equal(peers[0].IP), "peer IP mismatch: got %v", peers[0].IP)
	assert.Equal(t, int(wantPort), peers[0].Port)
	assert.Equal(t, []byte(wantToken), token)
}

func TestParsePeersAndToken_InvalidBencode(t *testing.T) {
	peers, token := parsePeersAndToken([]byte("not bencode"))
	assert.Nil(t, peers)
	assert.Nil(t, token)
}

// ---- buildQuery / buildResponse ----

func TestBuildQuery(t *testing.T) {
	txID := "aa"
	a := map[string]interface{}{
		"id":     "12345678901234567890",
		"target": "09876543210987654321",
	}
	out := buildQuery(txID, "find_node", a)

	// Must start with 'd' (dict) and end with 'e'.
	assert.Equal(t, byte('d'), out[0])
	assert.Equal(t, byte('e'), out[len(out)-1])

	raw, err := bencode.Unmarshal(out)
	require.NoError(t, err)

	msg, ok := raw.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "q", msg["y"])
	assert.Equal(t, "find_node", msg["q"])
	assert.Equal(t, txID, msg["t"])

	inner, ok := msg["a"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, a["id"], inner["id"])
	assert.Equal(t, a["target"], inner["target"])
}

func TestBuildResponse(t *testing.T) {
	txID := "bb"
	r := map[string]interface{}{
		"id": "12345678901234567890",
	}
	out := buildResponse(txID, r)

	assert.Equal(t, byte('d'), out[0])
	assert.Equal(t, byte('e'), out[len(out)-1])

	raw, err := bencode.Unmarshal(out)
	require.NoError(t, err)

	msg, ok := raw.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "r", msg["y"])
	assert.Equal(t, txID, msg["t"])

	inner, ok := msg["r"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, r["id"], inner["id"])
}

// ---- kBucket.add — LRU eviction ----

func TestKBucketAdd_LRUEviction(t *testing.T) {
	b := &kBucket{}

	// Fill the bucket to capacity.
	for i := 0; i < K; i++ {
		var id NodeID
		id[0] = byte(i + 1)
		b.add(&remoteNode{ID: id, seen: time.Now()})
	}
	assert.Len(t, b.nodes, K, "bucket should be full")

	// Adding one more should evict the oldest (head) and append the new one.
	var extra NodeID
	extra[0] = 0xff
	b.add(&remoteNode{ID: extra, seen: time.Now()})
	assert.Len(t, b.nodes, K, "bucket must not exceed K entries after eviction")
	assert.Equal(t, extra, b.nodes[K-1].ID, "new node should be at the tail")
}

func TestKBucketAdd_UpdateExistingSeen(t *testing.T) {
	b := &kBucket{}
	var id NodeID
	id[0] = 0xAA

	b.add(&remoteNode{ID: id, seen: time.Now().Add(-time.Hour)})
	require.Len(t, b.nodes, 1)

	// Adding the same ID again must not create a duplicate.
	b.add(&remoteNode{ID: id, seen: time.Now()})
	assert.Len(t, b.nodes, 1, "duplicate ID should not add a second entry")
}

// ---- RoutingTable.closestN ----

func TestRoutingTableClosestN(t *testing.T) {
	var selfID NodeID // all zeros
	rt := newRoutingTable(selfID)

	// Add 5 distinct nodes with IDs that differ from selfID only in the
	// least-significant byte so they all land in different buckets.
	for i := 1; i <= 5; i++ {
		var id NodeID
		id[19] = byte(i)
		rt.add(&remoteNode{
			ID:   id,
			Addr: net.UDPAddr{IP: net.ParseIP("127.0.0.1").To4(), Port: 6880 + i},
			seen: time.Now(),
		})
	}

	t.Run("request fewer than available returns at most n", func(t *testing.T) {
		got := rt.closestN(selfID, 3)
		assert.LessOrEqual(t, len(got), 3)
		assert.Greater(t, len(got), 0)
	})

	t.Run("request more than available returns all", func(t *testing.T) {
		got := rt.closestN(selfID, 100)
		assert.Equal(t, 5, len(got), "should return all 5 inserted nodes")
	})

	t.Run("empty table returns empty slice", func(t *testing.T) {
		emptyRT := newRoutingTable(selfID)
		got := emptyRT.closestN(selfID, 8)
		assert.Empty(t, got)
	})
}
