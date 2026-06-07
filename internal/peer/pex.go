package peer

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

const (
	ourPEXExtID = uint8(3) // local ID we register for ut_pex
	pexInterval = 60 * time.Second
)

// pexState tracks which peers we've already told a connection about,
// so we only send diffs (added/dropped since last PEX).
type pexState struct {
	peerPEXExtID uint8
	lastKnown    map[string]net.TCPAddr // addr string → addr
}

// runPEX sends periodic ut_pex messages to a connected peer.
// It runs as a goroutine and stops when the conn's quit channel closes.
// getConnected is a callback that returns all currently connected peer addresses.
func runPEX(c *Conn, state *pexState, getConnected func() []net.TCPAddr) {
	ticker := time.NewTicker(pexInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sendPEX(c, state, getConnected()); err != nil {
				return
			}
		case <-c.quit:
			return
		}
	}
}

// sendPEX computes the diff since the last PEX and sends a ut_pex message.
func sendPEX(c *Conn, state *pexState, current []net.TCPAddr) error {
	currentMap := make(map[string]net.TCPAddr, len(current))
	for _, a := range current {
		currentMap[a.String()] = a
	}

	var added, dropped []net.TCPAddr
	for k, a := range currentMap {
		if _, seen := state.lastKnown[k]; !seen {
			added = append(added, a)
		}
	}
	for k, a := range state.lastKnown {
		if _, still := currentMap[k]; !still {
			dropped = append(dropped, a)
		}
	}
	state.lastKnown = currentMap

	if len(added) == 0 && len(dropped) == 0 {
		return nil
	}

	payload := buildPEXPayload(added, dropped)
	c.Send(&Message{
		ID:      MsgExtended,
		Payload: append([]byte{state.peerPEXExtID}, payload...),
	})
	return nil
}

// ParsePEXPeers extracts peer addresses from a received ut_pex message payload
// (everything after the extension ID byte).
func ParsePEXPeers(payload []byte) ([]net.TCPAddr, error) {
	raw, err := parseBencodeDict(payload)
	if err != nil {
		return nil, fmt.Errorf("pex: decode payload: %w", err)
	}
	addedStr, _ := raw["added"].(string)
	return decodeCompact6([]byte(addedStr)), nil
}

// buildPEXPayload encodes a ut_pex message body.
// Format: d5:addedN:<compact peers>7:added.f<flags>7:droppedN:<compact peers>e
// Keys sorted: added < added.f < dropped
func buildPEXPayload(added, dropped []net.TCPAddr) []byte {
	addedBytes := encodeCompact6(added)
	droppedBytes := encodeCompact6(dropped)
	// flags: one byte per peer in added, all zero (no flags needed)
	flags := make([]byte, len(added))

	var b []byte
	b = append(b, 'd')
	b = appendBencodeStr(b, "added")
	b = appendBencodeStr(b, string(addedBytes))
	b = appendBencodeStr(b, "added.f")
	b = appendBencodeStr(b, string(flags))
	b = appendBencodeStr(b, "dropped")
	b = appendBencodeStr(b, string(droppedBytes))
	b = append(b, 'e')
	return b
}

// encodeCompact6 encodes IPv4 peer addresses as 6-byte compact format.
func encodeCompact6(addrs []net.TCPAddr) []byte {
	buf := make([]byte, 0, len(addrs)*6)
	for _, a := range addrs {
		ip4 := a.IP.To4()
		if ip4 == nil {
			continue
		}
		buf = append(buf, ip4...)
		port := make([]byte, 2)
		binary.BigEndian.PutUint16(port, uint16(a.Port))
		buf = append(buf, port...)
	}
	return buf
}

// decodeCompact6 decodes a compact IPv4 peer list (6 bytes per peer).
func decodeCompact6(data []byte) []net.TCPAddr {
	var addrs []net.TCPAddr
	for i := 0; i+6 <= len(data); i += 6 {
		ip := make(net.IP, 4)
		copy(ip, data[i:i+4])
		port := binary.BigEndian.Uint16(data[i+4 : i+6])
		addrs = append(addrs, net.TCPAddr{IP: ip, Port: int(port)})
	}
	return addrs
}

// parseBencodeDict is a minimal bencoded dict decoder for PEX payloads.
// Returns only string-valued keys.
func parseBencodeDict(data []byte) (map[string]interface{}, error) {
	if len(data) < 2 || data[0] != 'd' {
		return nil, fmt.Errorf("pex: not a bencoded dict")
	}
	i := 1
	result := make(map[string]interface{})
	for i < len(data) && data[i] != 'e' {
		key, n, err := readBencodeStr(data, i)
		if err != nil {
			return nil, err
		}
		i += n
		val, n, err := readBencodeVal(data, i)
		if err != nil {
			return nil, err
		}
		result[key] = val
		i += n
	}
	return result, nil
}

func readBencodeStr(data []byte, i int) (string, int, error) {
	if i >= len(data) || data[i] < '0' || data[i] > '9' {
		return "", 0, fmt.Errorf("pex: expected string at %d", i)
	}
	j := i
	for j < len(data) && data[j] != ':' {
		j++
	}
	if j >= len(data) {
		return "", 0, fmt.Errorf("pex: unterminated string length")
	}
	var length int
	for k := i; k < j; k++ {
		length = length*10 + int(data[k]-'0')
	}
	start := j + 1
	end := start + length
	if end > len(data) {
		return "", 0, fmt.Errorf("pex: string out of bounds")
	}
	return string(data[start:end]), end - i, nil
}

func readBencodeVal(data []byte, i int) (interface{}, int, error) {
	if i >= len(data) {
		return nil, 0, fmt.Errorf("pex: unexpected end")
	}
	switch {
	case data[i] >= '0' && data[i] <= '9':
		s, n, err := readBencodeStr(data, i)
		return s, n, err
	case data[i] == 'i':
		j := i + 1
		for j < len(data) && data[j] != 'e' {
			j++
		}
		return string(data[i+1 : j]), j + 1 - i, nil
	case data[i] == 'l' || data[i] == 'd':
		// skip nested list/dict by scanning for matching 'e'
		depth := 1
		j := i + 1
		for j < len(data) && depth > 0 {
			switch data[j] {
			case 'l', 'd':
				depth++
				j++
			case 'e':
				depth--
				j++
			case 'i':
				for j < len(data) && data[j] != 'e' {
					j++
				}
				j++
			default:
				if data[j] >= '0' && data[j] <= '9' {
					_, n, err := readBencodeStr(data, j)
					if err != nil {
						return nil, 0, err
					}
					j += n
				} else {
					j++
				}
			}
		}
		return nil, j - i, nil
	default:
		return nil, 1, nil
	}
}

func appendBencodeStr(b []byte, s string) []byte {
	b = append(b, []byte(fmt.Sprintf("%d:", len(s)))...)
	b = append(b, s...)
	return b
}
