package peer

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	protocolString = "BitTorrent protocol"
	handshakeLen   = 68 // 1 + 19 + 8 + 20 + 20
)

// ErrInfoHashMismatch is returned when the remote peer's info hash doesn't match.
var ErrInfoHashMismatch = errors.New("peer: info hash mismatch")

// Handshake performs the BitTorrent handshake over conn and returns the remote peer ID.
// Extension flags:
//   - Byte 5, bit 4 = 1: Extension Protocol (BEP 10)
//   - Byte 7, bit 0 = 1: DHT enabled (BEP 5)
func Handshake(conn net.Conn, infoHash, peerID [20]byte) ([20]byte, error) {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	if err := sendHandshake(conn, infoHash, peerID); err != nil {
		return [20]byte{}, fmt.Errorf("peer: send handshake: %w", err)
	}
	return recvHandshake(conn, infoHash)
}

func buildHandshake(infoHash, peerID [20]byte) []byte {
	buf := make([]byte, handshakeLen)
	buf[0] = byte(len(protocolString))
	copy(buf[1:20], protocolString)
	// Extension flags (bytes 20-27): enable Extension Protocol and DHT
	buf[25] = 0x10 // byte 5, bit 4
	buf[27] = 0x01 // byte 7, bit 0
	copy(buf[28:48], infoHash[:])
	copy(buf[48:68], peerID[:])
	return buf
}

// handshakeFull performs the BitTorrent handshake and also returns the remote
// peer's 8 reserved extension bytes so the caller can inspect BEP 10 support.
func handshakeFull(conn net.Conn, infoHash, peerID [20]byte) ([20]byte, [8]byte, error) {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})
	if err := sendHandshake(conn, infoHash, peerID); err != nil {
		return [20]byte{}, [8]byte{}, fmt.Errorf("peer: send handshake: %w", err)
	}
	return recvHandshakeFull(conn, infoHash)
}

func sendHandshake(w io.Writer, infoHash, peerID [20]byte) error {
	_, err := w.Write(buildHandshake(infoHash, peerID))
	return err
}

func recvHandshake(r io.Reader, expectedHash [20]byte) ([20]byte, error) {
	id, _, err := recvHandshakeFull(r, expectedHash)
	return id, err
}

func recvHandshakeFull(r io.Reader, expectedHash [20]byte) ([20]byte, [8]byte, error) {
	buf := make([]byte, handshakeLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return [20]byte{}, [8]byte{}, err
	}
	if buf[0] != byte(len(protocolString)) || string(buf[1:20]) != protocolString {
		return [20]byte{}, [8]byte{}, fmt.Errorf("peer: invalid protocol string")
	}
	var extFlags [8]byte
	copy(extFlags[:], buf[20:28])
	var remoteHash [20]byte
	copy(remoteHash[:], buf[28:48])
	if remoteHash != expectedHash {
		return [20]byte{}, [8]byte{}, ErrInfoHashMismatch
	}
	var remotePeerID [20]byte
	copy(remotePeerID[:], buf[48:68])
	return remotePeerID, extFlags, nil
}
