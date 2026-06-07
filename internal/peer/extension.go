package peer

import (
	"crypto/sha1"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/tiyaverma/qbit-go/internal/bencode"
)

const (
	extHandshakeMsgID = uint8(0)  // BEP 10: extension handshake uses ext ID 0
	ourMetaExtID      = uint8(2)  // local ID we register for ut_metadata
	metaPieceSize     = 16 * 1024 // BEP 9: metadata piece size
	maxMetadataSize   = 10 << 20  // 10 MiB sanity cap
)

// FetchMetadata dials addr, performs the BEP 3 + BEP 10 handshake, then fetches
// the torrent info dict via BEP 9 (ut_metadata). It verifies the assembled bytes
// against infoHash and returns the raw bencoded info dict on success.
func FetchMetadata(addr net.TCPAddr, infoHash, ourID [20]byte) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr.String(), 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("peer: dial %s: %w", addr.String(), err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	_, extFlags, err := handshakeFull(conn, infoHash, ourID)
	if err != nil {
		return nil, fmt.Errorf("peer: handshake: %w", err)
	}
	// BEP 10: reserved byte index 5 (0-based), bit 4 = Extension Protocol.
	if extFlags[5]&0x10 == 0 {
		return nil, fmt.Errorf("peer: Extension Protocol not supported")
	}

	// Send our extension handshake advertising ut_metadata as local ID ourMetaExtID.
	// d1:md11:ut_metadatai2eee
	extHs := []byte(fmt.Sprintf("d1:md11:ut_metadatai%deee", ourMetaExtID))
	if err := sendExtMsg(conn, extHandshakeMsgID, extHs); err != nil {
		return nil, fmt.Errorf("peer: send ext handshake: %w", err)
	}

	// Read messages until we receive the peer's extension handshake (ext ID 0).
	peerMetaExtID, metadataSize, err := recvExtHandshake(conn)
	if err != nil {
		return nil, err
	}

	// Fetch all metadata pieces and assemble them.
	conn.SetDeadline(time.Now().Add(60 * time.Second))
	assembled, err := fetchAllPieces(conn, peerMetaExtID, metadataSize)
	if err != nil {
		return nil, err
	}

	if sha1.Sum(assembled) != infoHash {
		return nil, fmt.Errorf("peer: metadata SHA1 mismatch")
	}
	return assembled, nil
}

// recvExtHandshake reads messages until it finds the peer's BEP 10 extension
// handshake (MsgExtended, ext ID 0), then extracts the peer's ut_metadata ID
// and the total metadata_size.
func recvExtHandshake(conn net.Conn) (peerMetaExtID uint8, metadataSize int, err error) {
	for {
		msg, err := Read(conn)
		if err != nil {
			return 0, 0, fmt.Errorf("peer: read ext handshake: %w", err)
		}
		if msg == nil || msg.ID != MsgExtended || len(msg.Payload) < 2 {
			continue
		}
		if msg.Payload[0] != extHandshakeMsgID {
			continue // not the handshake, skip
		}

		raw, err := bencode.Unmarshal(msg.Payload[1:])
		if err != nil {
			return 0, 0, fmt.Errorf("peer: parse ext handshake: %w", err)
		}
		hs, ok := raw.(map[string]interface{})
		if !ok {
			return 0, 0, fmt.Errorf("peer: ext handshake not a dict")
		}

		m, _ := hs["m"].(map[string]interface{})
		if m == nil {
			return 0, 0, fmt.Errorf("peer: no m dict in ext handshake")
		}
		metaIDRaw, ok := m["ut_metadata"]
		if !ok {
			return 0, 0, fmt.Errorf("peer: peer does not support ut_metadata")
		}
		switch v := metaIDRaw.(type) {
		case int64:
			peerMetaExtID = uint8(v)
		}

		if sz, ok := hs["metadata_size"].(int64); ok {
			metadataSize = int(sz)
		}
		break
	}

	if peerMetaExtID == 0 {
		return 0, 0, fmt.Errorf("peer: invalid ut_metadata extension ID 0")
	}
	if metadataSize <= 0 || metadataSize > maxMetadataSize {
		return 0, 0, fmt.Errorf("peer: invalid metadata_size %d", metadataSize)
	}
	return peerMetaExtID, metadataSize, nil
}

// fetchAllPieces requests and collects all BEP 9 metadata pieces in order.
func fetchAllPieces(conn net.Conn, peerMetaExtID uint8, metadataSize int) ([]byte, error) {
	numPieces := int(math.Ceil(float64(metadataSize) / float64(metaPieceSize)))
	assembled := make([]byte, metadataSize)

	for piece := 0; piece < numPieces; piece++ {
		// Request: d8:msg_typei0e5:piecei{N}ee  (keys sorted: m < p)
		req := []byte(fmt.Sprintf("d8:msg_typei0e5:piecei%dee", piece))
		if err := sendExtMsg(conn, peerMetaExtID, req); err != nil {
			return nil, fmt.Errorf("peer: send meta request piece %d: %w", piece, err)
		}

		pieceData, err := recvMetaPiece(conn, piece)
		if err != nil {
			return nil, err
		}

		start := piece * metaPieceSize
		end := start + len(pieceData)
		if end > len(assembled) {
			end = len(assembled)
		}
		copy(assembled[start:end], pieceData)
	}
	return assembled, nil
}

// recvMetaPiece reads messages until it finds a ut_metadata data response for
// the requested piece index (msg_type=1). Returns the raw piece bytes.
func recvMetaPiece(conn net.Conn, wantPiece int) ([]byte, error) {
	for {
		msg, err := Read(conn)
		if err != nil {
			return nil, fmt.Errorf("peer: read meta piece %d: %w", wantPiece, err)
		}
		if msg == nil || msg.ID != MsgExtended || len(msg.Payload) < 2 {
			continue
		}
		// The peer sends responses addressed to our registered extension ID.
		if msg.Payload[0] != ourMetaExtID {
			continue
		}

		// Payload layout: <ext_id byte> <bencoded dict> <raw piece bytes>
		dictEnd, err := bencode.FindValueEnd(msg.Payload[1:], 0)
		if err != nil {
			return nil, fmt.Errorf("peer: find meta dict end: %w", err)
		}
		dictBytes := msg.Payload[1 : 1+dictEnd]
		pieceBytes := msg.Payload[1+dictEnd:]

		raw, err := bencode.Unmarshal(dictBytes)
		if err != nil {
			return nil, fmt.Errorf("peer: unmarshal meta dict: %w", err)
		}
		d, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}

		msgType, _ := d["msg_type"].(int64)
		switch msgType {
		case 2: // reject
			return nil, fmt.Errorf("peer: meta piece %d rejected", wantPiece)
		case 1: // data
			return pieceBytes, nil
		}
	}
}

// sendExtMsg writes a MsgExtended frame: [ext_id][payload].
func sendExtMsg(conn net.Conn, extID uint8, payload []byte) error {
	msg := &Message{
		ID:      MsgExtended,
		Payload: append([]byte{extID}, payload...),
	}
	_, err := conn.Write(msg.Serialize())
	return err
}
