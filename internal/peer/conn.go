package peer

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
	"github.com/tiyaverma/qbit-go/internal/scheduler"
)

const (
	MaxPipelineDepth = 5
	BlockSize        = 16 * 1024 // 16 KiB
)

// Conn manages a single peer TCP connection with separate read/write goroutines.
type Conn struct {
	InfoHash [20]byte
	PeerID   [20]byte
	Addr     net.TCPAddr
	conn     net.Conn

	Choked     bool
	Interested bool
	Bitfield   bitfield.Bitfield

	outbound chan *Message
	quit     chan struct{}
}

// newConn wraps an established net.Conn into a Conn after handshake.
func newConn(c net.Conn, infoHash, peerID [20]byte, addr net.TCPAddr) *Conn {
	return &Conn{
		InfoHash: infoHash,
		PeerID:   peerID,
		Addr:     addr,
		conn:     c,
		Choked:   true,
		outbound: make(chan *Message, 32),
		quit:     make(chan struct{}),
	}
}

// Dial opens a connection to addr and completes the BitTorrent handshake.
func Dial(addr net.TCPAddr, infoHash, ourPeerID [20]byte) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr.String(), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("peer: dial %s: %w", addr.String(), err)
	}
	remotePeerID, err := Handshake(c, infoHash, ourPeerID)
	if err != nil {
		c.Close()
		return nil, err
	}
	return newConn(c, infoHash, remotePeerID, addr), nil
}

// Send queues a message for the write loop.
func (c *Conn) Send(msg *Message) {
	select {
	case c.outbound <- msg:
	case <-c.quit:
	}
}

// Close shuts down both goroutines and closes the TCP connection.
func (c *Conn) Close() {
	select {
	case <-c.quit:
	default:
		close(c.quit)
	}
	c.conn.Close()
}

// Download drives the download of a single piece from this peer.
// It pipelines up to MaxPipelineDepth block requests and returns the assembled data.
func (c *Conn) Download(work *scheduler.PieceWork) ([]byte, error) {
	buf := make([]byte, work.Length)
	downloaded := 0
	requested := 0
	inFlight := 0

	for downloaded < work.Length {
		// fill the pipeline
		for inFlight < MaxPipelineDepth && requested < work.Length {
			blockSize := BlockSize
			if work.Length-requested < blockSize {
				blockSize = work.Length - requested
			}
			c.Send(FormatRequest(work.Index, requested, blockSize))
			requested += blockSize
			inFlight++
		}

		msg, err := Read(c.conn)
		if err != nil {
			return nil, err
		}
		if msg == nil {
			continue // keepalive
		}

		switch msg.ID {
		case MsgChoke:
			c.Choked = true
			return nil, fmt.Errorf("peer: choked mid-download")
		case MsgUnchoke:
			c.Choked = false
		case MsgHave:
			idx, err := ParseHave(msg)
			if err == nil {
				c.Bitfield.SetPiece(idx)
			}
		case MsgPiece:
			_, begin, data, err := ParsePiece(msg)
			if err != nil {
				return nil, err
			}
			copy(buf[begin:], data)
			downloaded += len(data)
			inFlight--
		}
	}
	return buf, nil
}

// readLoop reads messages from the TCP connection and logs errors.
// This is intended to run as a goroutine for upload/seeding connections.
func (c *Conn) readLoop(handler func(*Message)) {
	for {
		c.conn.SetDeadline(time.Now().Add(2 * time.Minute))
		msg, err := Read(c.conn)
		if err != nil {
			select {
			case <-c.quit:
			default:
				log.Printf("peer: read error from %s: %v", c.Addr.String(), err)
			}
			return
		}
		if msg != nil {
			handler(msg)
		}
	}
}

// writeLoop drains the outbound channel and writes to TCP.
func (c *Conn) writeLoop() {
	for {
		select {
		case msg := <-c.outbound:
			c.conn.SetDeadline(time.Now().Add(30 * time.Second))
			if _, err := c.conn.Write(msg.Serialize()); err != nil {
				log.Printf("peer: write error to %s: %v", c.Addr.String(), err)
				return
			}
		case <-c.quit:
			return
		}
	}
}
