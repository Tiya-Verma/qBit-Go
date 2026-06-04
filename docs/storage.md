# Storage

`internal/storage`

The storage layer handles all disk I/O: writing piece data, verifying SHA1 hashes, assembling multi-file torrents, and reading data back for upload to peers.

---

## The Core Problem

Piece boundaries don't align with file boundaries.

```
Torrent: 3 files, 256 KiB piece size

File A: 300 KiB  ──────────────────────┐
File B: 150 KiB                        │ ← piece 2 spans File A and File B
File C: 800 KiB  ──────────────────────┘
                   ↑
              piece boundaries at 256 KiB intervals
```

Piece 1 (bytes 256-511 KiB) contains:
- bytes 256-299 KiB → tail of File A
- bytes 0-43 KiB   → head of File B

When writing piece 1, the storage manager must split the data and write to two different file handles at the correct offsets.

---

## File Layout on Disk

On torrent add, the storage manager creates all files at their full size immediately:

```go
func (m *Manager) Init(tf *torrent.TorrentFile, dir string) error {
    for _, f := range tf.Files {
        path := filepath.Join(dir, tf.Name, filepath.Join(f.Path...))
        os.MkdirAll(filepath.Dir(path), 0755)

        fh, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
        if err != nil { return err }

        // Pre-allocate using fallocate (Linux) or SetFileSize (Windows)
        // This claims disk space without zeroing — much faster than writing zeros
        syscall.Fallocate(int(fh.Fd()), 0, 0, f.Length)
        m.files = append(m.files, fh)
    }
    return nil
}
```

Pre-allocating at full size: avoids file fragmentation, fails fast if disk is full, and lets the OS optimize subsequent writes.

---

## Piece → File Mapping

```go
// Region describes a contiguous range of bytes within one file
type Region struct {
    File   *os.File
    Offset int64   // byte offset within the file
    Length int     // bytes to read/write
}

// PieceRegions returns the file regions that make up piece[index]
func (m *Manager) PieceRegions(index int) []Region {
    pieceStart := int64(index) * int64(m.pieceLength)
    pieceEnd   := pieceStart + int64(m.pieceSize(index))

    var regions []Region
    fileStart := int64(0)

    for _, f := range m.fileInfo {
        fileEnd := fileStart + f.Length

        // does this file overlap with our piece?
        overlapStart := max(pieceStart, fileStart)
        overlapEnd   := min(pieceEnd, fileEnd)

        if overlapStart < overlapEnd {
            regions = append(regions, Region{
                File:   f.Handle,
                Offset: overlapStart - fileStart,
                Length: int(overlapEnd - overlapStart),
            })
        }

        fileStart = fileEnd
        if fileStart >= pieceEnd { break }
    }
    return regions
}
```

---

## Write Pipeline

All writes go through a single writer goroutine to serialize disk access and avoid concurrent write corruption:

```go
type writeJob struct {
    index int
    data  []byte
    done  chan error
}

func (m *Manager) WritePiece(index int, data []byte) error {
    job := &writeJob{index: index, data: data, done: make(chan error, 1)}
    m.writeJobs <- job
    return <-job.done
}

func (m *Manager) writerLoop() {
    for job := range m.writeJobs {
        job.done <- m.writePiece(job.index, job.data)
    }
}

func (m *Manager) writePiece(index int, data []byte) error {
    // 1. Verify SHA1
    hash := sha1.Sum(data)
    if hash != m.pieceHashes[index] {
        return ErrHashMismatch  // discard — corrupt data from peer
    }

    // 2. Write to each region
    offset := 0
    for _, region := range m.PieceRegions(index) {
        _, err := region.File.WriteAt(data[offset:offset+region.Length], region.Offset)
        if err != nil { return err }
        offset += region.Length
    }

    // 3. Mark complete
    m.bitfield.SetPiece(index)
    return nil
}
```

---

## Read Pipeline (for Upload to Peers)

When a peer sends a `Request` message, we need to read block data from disk and send it back. Reads are concurrent (multiple peers can request simultaneously):

```go
func (m *Manager) ReadBlock(index, begin, length int) ([]byte, error) {
    regions := m.PieceRegions(index)
    buf := make([]byte, length)

    // find the region containing [begin, begin+length]
    pieceOffset := 0
    bufOffset   := 0
    remaining   := length

    for _, region := range regions {
        if pieceOffset+region.Length <= begin {
            pieceOffset += region.Length
            continue
        }
        readOffset := region.Offset + int64(begin-pieceOffset)
        n := min(remaining, region.Length)
        region.File.ReadAt(buf[bufOffset:bufOffset+n], readOffset)
        bufOffset   += n
        remaining   -= n
        if remaining == 0 { break }
        pieceOffset += region.Length
    }
    return buf, nil
}
```

---

## Verification on Startup

When resuming a torrent (no `.fastresume`), or when the user requests "Force Recheck", the storage manager verifies every piece:

```go
func (m *Manager) Verify() error {
    for i := 0; i < m.pieceCount; i++ {
        data, err := m.ReadPiece(i)
        if err != nil {
            m.bitfield.ClearPiece(i)
            continue
        }
        hash := sha1.Sum(data)
        if hash == m.pieceHashes[i] {
            m.bitfield.SetPiece(i)
        } else {
            m.bitfield.ClearPiece(i)
        }

        // report progress for the UI
        m.verifyProgress <- float64(i+1) / float64(m.pieceCount)
    }
    return nil
}
```

This is CPU-bound (SHA1 of many megabytes) and runs on a separate goroutine so the UI stays responsive.

---

## Bitfield

The bitfield tracks which pieces are complete. It's a compact `[]byte` where each bit represents one piece.

```go
type Bitfield []byte

func (bf Bitfield) HasPiece(index int) bool {
    byteIndex := index / 8
    bitIndex  := 7 - (index % 8)  // big-endian bit order (per spec)
    return bf[byteIndex]>>bitIndex&1 != 0
}

func (bf Bitfield) SetPiece(index int) {
    byteIndex := index / 8
    bitIndex  := 7 - (index % 8)
    bf[byteIndex] |= 1 << bitIndex
}

func (bf Bitfield) CountComplete() int {
    n := 0
    for _, b := range bf {
        n += bits.OnesCount8(b)
    }
    return n
}
```

The bitfield is serialized into the `.fastresume` file on shutdown.

---

## Interface

```go
type Manager struct{}

func NewManager(tf *torrent.TorrentFile, dir string) *Manager
func (m *Manager) Init() error                         // create files on disk
func (m *Manager) Verify() error                       // SHA1 check all pieces
func (m *Manager) WritePiece(index int, data []byte) error
func (m *Manager) ReadBlock(index, begin, length int) ([]byte, error)
func (m *Manager) Bitfield() bitfield.Bitfield
func (m *Manager) Close() error
```
