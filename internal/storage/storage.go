package storage

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
	"github.com/tiyaverma/qbit-go/internal/torrent"
)

var ErrHashMismatch = errors.New("storage: piece hash mismatch")

// Region is a contiguous byte range within a single file on disk.
type Region struct {
	File   *os.File
	Offset int64
	Length int
}

// fileInfo holds an open file handle alongside its torrent-level byte range.
type fileInfo struct {
	Handle *os.File
	Length int64
}

// writeJob serializes disk writes through a single goroutine.
type writeJob struct {
	index int
	data  []byte
	done  chan error
}

// Manager handles all disk I/O for one torrent.
type Manager struct {
	tf          *torrent.TorrentFile
	dir         string
	pieceHashes [][20]byte
	pieceLength int
	files       []fileInfo
	bf          bitfield.Bitfield

	writeJobs chan *writeJob
	quit      chan struct{}

	verifyProgress chan float64
}

// NewManager constructs a Manager. Call Init() before use.
func NewManager(tf *torrent.TorrentFile, dir string) *Manager {
	return &Manager{
		tf:             tf,
		dir:            dir,
		pieceHashes:    tf.PieceHashes,
		pieceLength:    tf.PieceLength,
		bf:             bitfield.New(tf.PieceCount()),
		writeJobs:      make(chan *writeJob, 256),
		quit:           make(chan struct{}),
		verifyProgress: make(chan float64, 1),
	}
}

// Init creates all file stubs on disk at their full size.
func (m *Manager) Init() error {
	files := m.tf.Files
	if len(files) == 0 {
		// single-file torrent
		files = []torrent.File{
			{Path: []string{m.tf.Name}, Length: m.tf.Length},
		}
	}

	for _, f := range files {
		parts := append([]string{m.dir, m.tf.Name}, f.Path...)
		path := filepath.Join(parts...)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		fh, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		if err := fh.Truncate(f.Length); err != nil {
			fh.Close()
			return err
		}
		m.files = append(m.files, fileInfo{Handle: fh, Length: f.Length})
	}

	go m.writerLoop()
	return nil
}

// PieceRegions returns the file regions that compose piece[index].
func (m *Manager) PieceRegions(index int) []Region {
	pieceStart := int64(index) * int64(m.pieceLength)
	pieceEnd := pieceStart + int64(m.tf.PieceSize(index))

	var regions []Region
	fileStart := int64(0)

	for _, f := range m.files {
		fileEnd := fileStart + f.Length
		overlapStart := maxI64(pieceStart, fileStart)
		overlapEnd := minI64(pieceEnd, fileEnd)
		if overlapStart < overlapEnd {
			regions = append(regions, Region{
				File:   f.Handle,
				Offset: overlapStart - fileStart,
				Length: int(overlapEnd - overlapStart),
			})
		}
		fileStart = fileEnd
		if fileStart >= pieceEnd {
			break
		}
	}
	return regions
}

// WritePiece queues a piece write, verifies its SHA1, then writes it to disk.
// Blocks until the write completes or an error is returned.
func (m *Manager) WritePiece(index int, data []byte) error {
	job := &writeJob{index: index, data: data, done: make(chan error, 1)}
	m.writeJobs <- job
	return <-job.done
}

func (m *Manager) writerLoop() {
	for {
		select {
		case job := <-m.writeJobs:
			job.done <- m.writePiece(job.index, job.data)
		case <-m.quit:
			return
		}
	}
}

func (m *Manager) writePiece(index int, data []byte) error {
	hash := sha1.Sum(data)
	if hash != m.pieceHashes[index] {
		return ErrHashMismatch
	}
	offset := 0
	for _, region := range m.PieceRegions(index) {
		if _, err := region.File.WriteAt(data[offset:offset+region.Length], region.Offset); err != nil {
			return fmt.Errorf("storage: write piece %d: %w", index, err)
		}
		offset += region.Length
	}
	m.bf.SetPiece(index)
	return nil
}

// ReadBlock reads length bytes starting at begin within piece[index].
func (m *Manager) ReadBlock(index, begin, length int) ([]byte, error) {
	regions := m.PieceRegions(index)
	buf := make([]byte, length)
	pieceOffset := 0
	bufOffset := 0
	remaining := length

	for _, region := range regions {
		if pieceOffset+region.Length <= begin {
			pieceOffset += region.Length
			continue
		}
		readOffset := region.Offset + int64(begin-pieceOffset)
		avail := region.Length - (begin - pieceOffset)
		n := remaining
		if avail < n {
			n = avail
		}
		if _, err := region.File.ReadAt(buf[bufOffset:bufOffset+n], readOffset); err != nil {
			return nil, err
		}
		bufOffset += n
		remaining -= n
		if remaining == 0 {
			break
		}
		begin = 0
		pieceOffset += region.Length
	}
	return buf, nil
}

// ReadPiece reads a complete piece from disk (used during verification).
func (m *Manager) ReadPiece(index int) ([]byte, error) {
	return m.ReadBlock(index, 0, m.tf.PieceSize(index))
}

// Verify SHA1-checks every piece and updates the bitfield accordingly.
// Progress values in [0,1] are sent to verifyProgress during the scan.
func (m *Manager) Verify() error {
	n := m.tf.PieceCount()
	for i := 0; i < n; i++ {
		data, err := m.ReadPiece(i)
		if err != nil {
			m.bf.ClearPiece(i)
		} else if sha1.Sum(data) == m.pieceHashes[i] {
			m.bf.SetPiece(i)
		} else {
			m.bf.ClearPiece(i)
		}
		select {
		case m.verifyProgress <- float64(i+1) / float64(n):
		default:
		}
	}
	return nil
}

// VerifyProgress returns a channel that receives progress updates during Verify.
func (m *Manager) VerifyProgress() <-chan float64 {
	return m.verifyProgress
}

// Bitfield returns the current completion bitfield.
func (m *Manager) Bitfield() bitfield.Bitfield {
	return m.bf
}

// Close flushes and closes all open file handles.
func (m *Manager) Close() error {
	close(m.quit)
	for _, f := range m.files {
		f.Handle.Close()
	}
	return nil
}

func maxI64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minI64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
