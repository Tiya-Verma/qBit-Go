package torrent

import (
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
)

// State represents a torrent's lifecycle stage.
type State int

const (
	StateFetching   State = iota // fetching metadata (magnet link)
	StateChecking                // verifying existing data on disk
	StateDownloading             // actively downloading
	StateSeeding                 // upload only, download complete
	StatePaused                  // user-paused
	StateStopped                 // no activity
	StateError                   // unrecoverable error
)

func (s State) String() string {
	switch s {
	case StateFetching:
		return "fetching"
	case StateChecking:
		return "checking"
	case StateDownloading:
		return "downloading"
	case StateSeeding:
		return "seeding"
	case StatePaused:
		return "paused"
	case StateStopped:
		return "stopped"
	case StateError:
		return "error"
	}
	return "unknown"
}

// File describes a single file within a (possibly multi-file) torrent.
type File struct {
	Path   []string
	Length int64
}

// TorrentFile holds all parsed metadata from a .torrent file.
type TorrentFile struct {
	Announce     string
	AnnounceList [][]string // BEP 12 multi-tracker tiers
	InfoHash     [20]byte
	PieceHashes  [][20]byte
	PieceLength  int
	Length       int64  // total bytes (single-file torrents)
	Files        []File // populated for multi-file torrents
	Name         string
}

// TotalLength returns the sum of all file lengths.
func (tf *TorrentFile) TotalLength() int64 {
	if len(tf.Files) == 0 {
		return tf.Length
	}
	var total int64
	for _, f := range tf.Files {
		total += f.Length
	}
	return total
}

// PieceCount returns the number of pieces in this torrent.
func (tf *TorrentFile) PieceCount() int {
	return len(tf.PieceHashes)
}

// PieceSize returns the byte length of piece at index.
func (tf *TorrentFile) PieceSize(index int) int {
	if index == tf.PieceCount()-1 {
		remainder := int(tf.TotalLength()) % tf.PieceLength
		if remainder != 0 {
			return remainder
		}
	}
	return tf.PieceLength
}

// Stats holds live counters for a torrent; fields are updated atomically.
type Stats struct {
	Downloaded    int64
	Uploaded      int64
	DownloadSpeed int64 // bytes/sec, rolling average
	UploadSpeed   int64 // bytes/sec, rolling average
}

// FastResume is persisted between sessions so verified pieces are not rechecked.
type FastResume struct {
	InfoHash    [20]byte
	Bitfield    []byte
	DownloadDir string
	AddedAt     time.Time
	Files       []FileState
}

// FileState stores per-file settings that persist across restarts.
type FileState struct {
	Index    int
	Priority int
	Skip     bool
}

// ParseFile decodes a .torrent file from its raw bencoded map.
func ParseFile(raw map[string]interface{}) (*TorrentFile, error) {
	tf := &TorrentFile{}

	if announce, ok := raw["announce"].(string); ok {
		tf.Announce = announce
	}

	if info, ok := raw["info"].(map[string]interface{}); ok {
		if err := parseInfo(tf, info, raw); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("torrent: missing info dict")
	}

	// BEP 12 announce-list
	if list, ok := raw["announce-list"].([]interface{}); ok {
		for _, tier := range list {
			if tierList, ok := tier.([]interface{}); ok {
				var urls []string
				for _, u := range tierList {
					if s, ok := u.(string); ok {
						urls = append(urls, s)
					}
				}
				tf.AnnounceList = append(tf.AnnounceList, urls)
			}
		}
	}

	return tf, nil
}

func parseInfo(tf *TorrentFile, info map[string]interface{}, raw map[string]interface{}) error {
	if name, ok := info["name"].(string); ok {
		tf.Name = name
	}

	pieceLen, ok := info["piece length"].(int64)
	if !ok {
		return fmt.Errorf("torrent: missing piece length")
	}
	tf.PieceLength = int(pieceLen)

	piecesStr, ok := info["pieces"].(string)
	if !ok {
		return fmt.Errorf("torrent: missing pieces")
	}
	pieces := []byte(piecesStr)
	if len(pieces)%20 != 0 {
		return fmt.Errorf("torrent: malformed pieces field")
	}
	tf.PieceHashes = make([][20]byte, len(pieces)/20)
	for i := range tf.PieceHashes {
		copy(tf.PieceHashes[i][:], pieces[i*20:(i+1)*20])
	}

	// Single-file
	if length, ok := info["length"].(int64); ok {
		tf.Length = length
	}

	// Multi-file
	if files, ok := info["files"].([]interface{}); ok {
		for _, f := range files {
			if fm, ok := f.(map[string]interface{}); ok {
				var file File
				if l, ok := fm["length"].(int64); ok {
					file.Length = l
				}
				if pathList, ok := fm["path"].([]interface{}); ok {
					for _, p := range pathList {
						if s, ok := p.(string); ok {
							file.Path = append(file.Path, s)
						}
					}
				}
				tf.Files = append(tf.Files, file)
			}
		}
	}

	// Compute info hash
	infoHash, err := hashInfo(info)
	if err != nil {
		return err
	}
	tf.InfoHash = infoHash

	return nil
}

func hashInfo(info map[string]interface{}) ([20]byte, error) {
	// Re-encode the info dict to compute its SHA1
	// In production this would re-encode the raw bytes; here we use a placeholder
	_ = info
	h := sha1.New()
	// TODO: encode info dict back to bencode and hash
	var result [20]byte
	copy(result[:], h.Sum(nil))
	return result, nil
}

// InfoHashString returns the info hash as a lowercase hex string.
func (tf *TorrentFile) InfoHashString() string {
	return fmt.Sprintf("%x", tf.InfoHash)
}

// Torrent is a live download/seed session.
type Torrent struct {
	File     TorrentFile
	State    State
	Bitfield bitfield.Bitfield
	Stats    Stats

	DownloadDir string
	AddedAt     time.Time

	quit chan struct{}
}

// New constructs a Torrent ready to run.
func New(tf TorrentFile, bf bitfield.Bitfield, dir string) *Torrent {
	return &Torrent{
		File:        tf,
		State:       StateChecking,
		Bitfield:    bf,
		DownloadDir: dir,
		AddedAt:     time.Now(),
		quit:        make(chan struct{}),
	}
}

// Stop signals the torrent's goroutine tree to shut down.
func (t *Torrent) Stop() {
	select {
	case <-t.quit:
	default:
		close(t.quit)
	}
}

// Progress returns a value in [0, 1] representing download completion.
func (t *Torrent) Progress() float64 {
	total := t.File.PieceCount()
	if total == 0 {
		return 0
	}
	return float64(t.Bitfield.CountComplete()) / float64(total)
}
