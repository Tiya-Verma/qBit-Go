package magnet

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

// Magnet holds the parsed fields of a magnet URI.
type Magnet struct {
	InfoHash [20]byte
	Name     string
	Trackers []string
}

// Parse decodes a magnet URI into its constituent parts.
// The info hash may be either 40-char lowercase hex or 32-char base32.
func Parse(uri string) (*Magnet, error) {
	if !strings.HasPrefix(uri, "magnet:?") {
		return nil, fmt.Errorf("magnet: not a magnet URI")
	}
	q, err := url.ParseQuery(uri[len("magnet:?"):])
	if err != nil {
		return nil, fmt.Errorf("magnet: parse query: %w", err)
	}

	xt := q.Get("xt")
	if !strings.HasPrefix(xt, "urn:btih:") {
		return nil, fmt.Errorf("magnet: missing urn:btih: topic")
	}
	hashStr := xt[len("urn:btih:"):]

	var infoHash [20]byte
	switch len(hashStr) {
	case 40:
		b, err := hex.DecodeString(hashStr)
		if err != nil {
			return nil, fmt.Errorf("magnet: decode hex hash: %w", err)
		}
		copy(infoHash[:], b)
	case 32:
		b, err := base32.StdEncoding.DecodeString(strings.ToUpper(hashStr))
		if err != nil {
			return nil, fmt.Errorf("magnet: decode base32 hash: %w", err)
		}
		copy(infoHash[:], b)
	default:
		return nil, fmt.Errorf("magnet: invalid hash length %d (want 40 hex or 32 base32)", len(hashStr))
	}

	return &Magnet{
		InfoHash: infoHash,
		Name:     q.Get("dn"),
		Trackers: q["tr"],
	}, nil
}
