package tracker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"time"
)

const udpMagicConnID = uint64(0x41727101980)

var ErrTrackerUnreachable = errors.New("tracker udp: unreachable after max retries")

type udpClient struct {
	addr   *net.UDPAddr
	params AnnounceParams
	conn   *net.UDPConn
}

func newUDPClient(u *url.URL, params AnnounceParams) (*udpClient, error) {
	addr, err := net.ResolveUDPAddr("udp", u.Host)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	return &udpClient{addr: addr, params: params, conn: conn}, nil
}

func (c *udpClient) Announce(event string) (*AnnounceResponse, error) {
	connID, err := c.connect()
	if err != nil {
		return nil, err
	}
	return c.announce(connID, eventCode(event))
}

func (c *udpClient) Close() error {
	return c.conn.Close()
}

func (c *udpClient) connect() (uint64, error) {
	txID := rand.Uint32()
	req := make([]byte, 16)
	binary.BigEndian.PutUint64(req[0:8], udpMagicConnID)
	binary.BigEndian.PutUint32(req[8:12], 0) // action = connect
	binary.BigEndian.PutUint32(req[12:16], txID)

	resp, err := c.sendWithRetry(req)
	if err != nil {
		return 0, err
	}
	if len(resp) < 16 {
		return 0, fmt.Errorf("tracker udp: short connect response")
	}
	if binary.BigEndian.Uint32(resp[0:4]) != 0 {
		return 0, fmt.Errorf("tracker udp: unexpected action in connect response")
	}
	if binary.BigEndian.Uint32(resp[4:8]) != txID {
		return 0, fmt.Errorf("tracker udp: transaction ID mismatch")
	}
	return binary.BigEndian.Uint64(resp[8:16]), nil
}

func (c *udpClient) announce(connID uint64, event uint32) (*AnnounceResponse, error) {
	txID := rand.Uint32()
	req := make([]byte, 98)
	binary.BigEndian.PutUint64(req[0:8], connID)
	binary.BigEndian.PutUint32(req[8:12], 1) // action = announce
	binary.BigEndian.PutUint32(req[12:16], txID)
	copy(req[16:36], c.params.InfoHash[:])
	copy(req[36:56], c.params.PeerID[:])
	binary.BigEndian.PutUint64(req[56:64], uint64(c.params.Downloaded))
	binary.BigEndian.PutUint64(req[64:72], uint64(c.params.Left))
	binary.BigEndian.PutUint64(req[72:80], uint64(c.params.Uploaded))
	binary.BigEndian.PutUint32(req[80:84], event)
	binary.BigEndian.PutUint32(req[84:88], 0)  // ip = 0 (use sender)
	binary.BigEndian.PutUint32(req[88:92], 0)  // key
	binary.BigEndian.PutUint32(req[92:96], ^uint32(0)) // num_want = -1
	binary.BigEndian.PutUint16(req[96:98], c.params.Port)

	resp, err := c.sendWithRetry(req)
	if err != nil {
		return nil, err
	}
	if len(resp) < 20 {
		return nil, fmt.Errorf("tracker udp: short announce response")
	}

	result := &AnnounceResponse{
		Interval: int(binary.BigEndian.Uint32(resp[8:12])),
		Leechers: int(binary.BigEndian.Uint32(resp[12:16])),
		Seeders:  int(binary.BigEndian.Uint32(resp[16:20])),
	}
	peers, err := parseCompactPeers(resp[20:])
	if err != nil {
		return nil, err
	}
	result.Peers = peers
	return result, nil
}

// sendWithRetry retries up to 8 times with exponentially increasing timeouts (BEP 15).
func (c *udpClient) sendWithRetry(req []byte) ([]byte, error) {
	for n := 0; n < 8; n++ {
		timeout := time.Duration(15*(1<<n)) * time.Second
		c.conn.SetDeadline(time.Now().Add(timeout))
		if _, err := c.conn.WriteToUDP(req, c.addr); err != nil {
			return nil, err
		}
		buf := make([]byte, 2048)
		nr, _, err := c.conn.ReadFromUDP(buf)
		if err == nil {
			return buf[:nr], nil
		}
	}
	return nil, ErrTrackerUnreachable
}

func eventCode(event string) uint32 {
	switch event {
	case "completed":
		return 1
	case "started":
		return 2
	case "stopped":
		return 3
	default:
		return 0
	}
}
