package tracker

import (
	"errors"
	"math/rand"
	"net"
	"net/url"
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

// Announce tries each tracker tier in order, shuffling within tiers.
// The first successful response is returned.
func (m *MultiTracker) Announce(event string) (*AnnounceResponse, error) {
	for _, tier := range m.tiers {
		rand.Shuffle(len(tier), func(i, j int) { tier[i], tier[j] = tier[j], tier[i] })
		for i, tracker := range tier {
			resp, err := tracker.Announce(event)
			if err == nil {
				if i != 0 {
					tier[0], tier[i] = tier[i], tier[0] // promote successful tracker
				}
				return resp, nil
			}
		}
	}
	return nil, ErrAllTrackersFailed
}

// Close closes all tracker clients in all tiers.
func (m *MultiTracker) Close() {
	for _, tier := range m.tiers {
		for _, c := range tier {
			c.Close()
		}
	}
}
