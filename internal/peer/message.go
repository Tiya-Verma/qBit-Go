package peer

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
)

// MessageID identifies a peer wire protocol message type.
type MessageID uint8

const (
	MsgChoke         MessageID = 0
	MsgUnchoke       MessageID = 1
	MsgInterested    MessageID = 2
	MsgNotInterested MessageID = 3
	MsgHave          MessageID = 4
	MsgBitfield      MessageID = 5
	MsgRequest       MessageID = 6
	MsgPiece         MessageID = 7
	MsgCancel        MessageID = 8
	MsgPort          MessageID = 9  // DHT port (BEP 5)
	MsgExtended      MessageID = 20 // Extension Protocol (BEP 10)
)

// Message is a decoded peer wire protocol message.
type Message struct {
	ID      MessageID
	Payload []byte
}

// Read reads one length-prefixed message from r.
// A zero-length message is a keepalive (ID field is undefined).
func Read(r io.Reader) (*Message, error) {
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf[:])
	if length == 0 {
		return nil, nil // keepalive
	}

	msgBuf := make([]byte, length)
	if _, err := io.ReadFull(r, msgBuf); err != nil {
		return nil, err
	}
	return &Message{ID: MessageID(msgBuf[0]), Payload: msgBuf[1:]}, nil
}

// Serialize encodes the message for writing to a TCP connection.
func (m *Message) Serialize() []byte {
	if m == nil {
		// keepalive
		return make([]byte, 4)
	}
	length := uint32(len(m.Payload) + 1)
	buf := make([]byte, length+4)
	binary.BigEndian.PutUint32(buf[:4], length)
	buf[4] = byte(m.ID)
	copy(buf[5:], m.Payload)
	return buf
}

// FormatBitfield builds a Bitfield message from our current piece possession.
func FormatBitfield(bf bitfield.Bitfield) *Message {
	payload := make([]byte, len(bf))
	copy(payload, bf)
	return &Message{ID: MsgBitfield, Payload: payload}
}

// ParseBitfield decodes a Bitfield message into a Bitfield.
func ParseBitfield(msg *Message) bitfield.Bitfield {
	bf := make(bitfield.Bitfield, len(msg.Payload))
	copy(bf, msg.Payload)
	return bf
}

// FormatPiece builds a Piece response message (used when uploading to a peer).
func FormatPiece(index, begin int, data []byte) *Message {
	payload := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	copy(payload[8:], data)
	return &Message{ID: MsgPiece, Payload: payload}
}

// FormatHave builds a Have message for pieceIndex.
func FormatHave(index int) *Message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(index))
	return &Message{ID: MsgHave, Payload: payload}
}

// FormatRequest builds a Request message.
func FormatRequest(index, begin, length int) *Message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	return &Message{ID: MsgRequest, Payload: payload}
}

// FormatCancel builds a Cancel message (used in end-game mode).
func FormatCancel(index, begin, length int) *Message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	return &Message{ID: MsgCancel, Payload: payload}
}

// ParseHave extracts the piece index from a Have message payload.
func ParseHave(msg *Message) (int, error) {
	if msg.ID != MsgHave || len(msg.Payload) != 4 {
		return 0, fmt.Errorf("peer: malformed Have message")
	}
	return int(binary.BigEndian.Uint32(msg.Payload)), nil
}

// ParseRequest extracts index, begin, length from a Request or Cancel payload.
func ParseRequest(msg *Message) (index, begin, length int, err error) {
	if len(msg.Payload) < 12 {
		return 0, 0, 0, fmt.Errorf("peer: malformed Request message")
	}
	index = int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	begin = int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	length = int(binary.BigEndian.Uint32(msg.Payload[8:12]))
	return
}

// ParsePiece extracts index, begin, and block data from a Piece message.
func ParsePiece(msg *Message) (index, begin int, data []byte, err error) {
	if msg.ID != MsgPiece || len(msg.Payload) < 8 {
		return 0, 0, nil, fmt.Errorf("peer: malformed Piece message")
	}
	index = int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	begin = int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	data = msg.Payload[8:]
	return
}
