package tracker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/tiyaverma/qbit-go/internal/bencode"
)

type httpClient struct {
	url    *url.URL
	params AnnounceParams
	http   *http.Client
}

func newHTTPClient(u *url.URL, params AnnounceParams) *httpClient {
	return &httpClient{
		url:    u,
		params: params,
		http:   &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *httpClient) Announce(event string) (*AnnounceResponse, error) {
	q := url.Values{}
	q.Set("info_hash", string(c.params.InfoHash[:]))
	q.Set("peer_id", string(c.params.PeerID[:]))
	q.Set("port", strconv.Itoa(int(c.params.Port)))
	q.Set("uploaded", strconv.FormatInt(c.params.Uploaded, 10))
	q.Set("downloaded", strconv.FormatInt(c.params.Downloaded, 10))
	q.Set("left", strconv.FormatInt(c.params.Left, 10))
	q.Set("compact", "1")
	q.Set("numwant", "50")
	if event != "" {
		q.Set("event", event)
	}

	reqURL := *c.url
	reqURL.RawQuery = q.Encode()

	resp, err := c.http.Get(reqURL.String())
	if err != nil {
		return nil, fmt.Errorf("tracker http: get: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	raw, err := bencode.Unmarshal(body)
	if err != nil {
		return nil, fmt.Errorf("tracker http: decode: %w", err)
	}
	dict, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("tracker http: unexpected response type")
	}

	if failure, ok := dict["failure reason"].(string); ok {
		return nil, fmt.Errorf("tracker http: %s", failure)
	}

	result := &AnnounceResponse{}
	if interval, ok := dict["interval"].(int64); ok {
		result.Interval = int(interval)
	}
	if seeders, ok := dict["complete"].(int64); ok {
		result.Seeders = int(seeders)
	}
	if leechers, ok := dict["incomplete"].(int64); ok {
		result.Leechers = int(leechers)
	}
	if peers, ok := dict["peers"].(string); ok {
		result.Peers, err = parseCompactPeers([]byte(peers))
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (c *httpClient) AnnounceLoop(ctx context.Context, peers chan<- []net.TCPAddr) {
	bo := newBackoff(30*time.Second, 1800*time.Second)
	event := "started"
	for {
		resp, err := c.Announce(event)
		if err != nil {
			select {
			case <-time.After(bo.Next()):
			case <-ctx.Done():
				return
			}
			continue
		}
		bo.Reset()
		event = ""
		select {
		case peers <- resp.Peers:
		default:
		}
		select {
		case <-time.After(time.Duration(resp.Interval) * time.Second):
		case <-ctx.Done():
			c.Announce("stopped") //nolint:errcheck
			return
		}
	}
}

func (c *httpClient) Close() error { return nil }

// parseCompactPeers decodes the 6-bytes-per-peer compact format (BEP 23).
func parseCompactPeers(data []byte) ([]net.TCPAddr, error) {
	if len(data)%6 != 0 {
		return nil, fmt.Errorf("tracker: invalid compact peer data length %d", len(data))
	}
	peers := make([]net.TCPAddr, len(data)/6)
	for i := range peers {
		b := data[i*6 : i*6+6]
		peers[i] = net.TCPAddr{
			IP:   net.IP(b[0:4]),
			Port: int(binary.BigEndian.Uint16(b[4:6])),
		}
	}
	return peers, nil
}

// backoff implements exponential back-off between min and max.
type backoff struct {
	min, max, cur time.Duration
}

func newBackoff(min, max time.Duration) *backoff {
	return &backoff{min: min, max: max, cur: min}
}

func (b *backoff) Next() time.Duration {
	d := b.cur
	b.cur *= 2
	if b.cur > b.max {
		b.cur = b.max
	}
	return d
}

func (b *backoff) Reset() {
	b.cur = b.min
}
