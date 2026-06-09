package tracker

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync"
	"time"
)

// ErrAllTrackersFailed is returned when every tracker in all tiers fails.
var ErrAllTrackersFailed = errors.New("tracker: all trackers failed")

// ErrUnsupportedTrackerScheme is returned for non-HTTP/UDP tracker URLs.
var ErrUnsupportedTrackerScheme = errors.New("tracker: unsupported scheme")

// AnnounceResponse holds the tracker's reply to an announce.
type AnnounceResponse struct {
	Interval int
	Seeders  int
	Leechers int
	Peers    []net.TCPAddr
}

// AnnounceParams are the fixed fields sent with every announce.
type AnnounceParams struct {
	InfoHash   [20]byte
	PeerID     [20]byte
	Port       uint16
	Downloaded int64
	Uploaded   int64
	Left       int64
}

// Client is the interface implemented by both HTTPClient and UDPClient.
type Client interface {
	Announce(event string) (*AnnounceResponse, error)
	Close() error
}

// New creates the appropriate client (HTTP or UDP) for announceURL.
func New(announceURL string, params AnnounceParams) (Client, error) {
	u, err := url.Parse(announceURL)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "http", "https":
		return newHTTPClient(u, params), nil
	case "udp":
		return newUDPClient(u, params)
	default:
		return nil, ErrUnsupportedTrackerScheme
	}
}

// MultiTracker implements BEP 12 tier-based tracker selection.
type MultiTracker struct {
	tiers [][]Client
}

// NewMultiTracker builds a MultiTracker from an announce-list.
func NewMultiTracker(announceList [][]string, params AnnounceParams) *MultiTracker {
	mt := &MultiTracker{}
	for _, tier := range announceList {
		var clients []Client
		for _, u := range tier {
			c, err := New(u, params)
			if err == nil {
				clients = append(clients, c)
			}
		}
		if len(clients) > 0 {
			mt.tiers = append(mt.tiers, clients)
		}
	}
	return mt
}

// Announce queries all trackers across all tiers in parallel and returns the
// first successful response. BEP 12 prescribes sequential tier walking, but in
// practice live trackers respond in <1s while dead ones eat the full retry
// budget; fanning out gives the responsive trackers a chance to win the race.
func (m *MultiTracker) Announce(event string) (*AnnounceResponse, error) {
	type result struct {
		resp *AnnounceResponse
		err  error
	}
	var clients []Client
	for _, tier := range m.tiers {
		clients = append(clients, tier...)
	}
	if len(clients) == 0 {
		return nil, ErrAllTrackersFailed
	}

	ch := make(chan result, len(clients))
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c Client) {
			defer wg.Done()
			resp, err := c.Announce(event)
			ch <- result{resp, err}
		}(c)
	}
	go func() { wg.Wait(); close(ch) }()

	for r := range ch {
		if r.err == nil && r.resp != nil {
			return r.resp, nil
		}
	}
	return nil, ErrAllTrackersFailed
}

// AnnounceLoop periodically announces to the best available tracker and feeds
// peer lists to the peers channel. It sends "started" on first call and
// "stopped" when ctx is cancelled. Uses exponential backoff on failure.
func (m *MultiTracker) AnnounceLoop(ctx context.Context, peers chan<- []net.TCPAddr) {
	bo := newBackoff(30*time.Second, 1800*time.Second)
	event := "started"
	for {
		resp, err := m.Announce(event)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(bo.Next()):
				continue
			}
		}
		bo.Reset()
		event = ""
		select {
		case peers <- resp.Peers:
		default:
		}
		select {
		case <-ctx.Done():
			m.Announce("stopped") //nolint:errcheck
			return
		case <-time.After(time.Duration(resp.Interval) * time.Second):
		}
	}
}

// Close closes all tracker clients in all tiers.
func (m *MultiTracker) Close() {
	for _, tier := range m.tiers {
		for _, c := range tier {
			c.Close()
		}
	}
}
