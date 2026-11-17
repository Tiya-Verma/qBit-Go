package peer

import (
	"crypto/rand"
	"crypto/sha1"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
	"github.com/tiyaverma/qbit-go/internal/ratelimit"
	"github.com/tiyaverma/qbit-go/internal/scheduler"
	"github.com/tiyaverma/qbit-go/internal/storage"
	"github.com/tiyaverma/qbit-go/internal/torrent"
)

// loopbackSeeder is a fully seeded torrent listening on 127.0.0.1. It serves
// blocks through the real upload path (AcceptInbound → serveUpload) and reads
// them back from disk through storage.Manager.
type loopbackSeeder struct {
	tf   *torrent.TorrentFile
	mgr  *Manager
	ln   net.Listener
	addr net.TCPAddr
}

// newLoopbackSeeder builds a single-file torrent of random data, writes every
// piece to disk through storage (which SHA1-verifies it), and starts listening.
func newLoopbackSeeder(tb testing.TB, size int64, pieceLen int) *loopbackSeeder {
	tb.Helper()

	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(tb, err)

	n := int((size + int64(pieceLen) - 1) / int64(pieceLen))
	tf := &torrent.TorrentFile{
		Name:        "bench.bin",
		Length:      size,
		PieceLength: pieceLen,
		PieceHashes: make([][20]byte, n),
	}
	_, err = rand.Read(tf.InfoHash[:])
	require.NoError(tb, err)
	for i := 0; i < n; i++ {
		tf.PieceHashes[i] = sha1.Sum(pieceOf(data, tf, i))
	}

	stor := storage.NewManager(tf, tb.TempDir())
	require.NoError(tb, stor.Init())
	tb.Cleanup(func() { stor.Close() }) //nolint:errcheck
	bf := bitfield.New(n)
	for i := 0; i < n; i++ {
		require.NoError(tb, stor.WritePiece(i, pieceOf(data, tf, i)))
		bf.SetPiece(i)
	}

	var id [20]byte
	copy(id[:], "-QB0100-seeder000000")
	mgr := NewManager(tf, id, 0, bf, scheduler.New(tf.PieceHashes, pieceLens(tf), bf), stor, ratelimit.New(0, 0), 50)
	go mgr.Run()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go mgr.AcceptInbound(c)
		}
	}()

	s := &loopbackSeeder{tf: tf, mgr: mgr, ln: ln, addr: *ln.Addr().(*net.TCPAddr)}
	tb.Cleanup(func() {
		ln.Close()
		mgr.Stop()
	})
	return s
}

// downloadAll runs a fresh leecher against the seeder over `conns` parallel
// TCP connections and blocks until every piece has been received, SHA1-verified
// and written to disk. It returns the wall-clock time of the transfer alone.
func (s *loopbackSeeder) downloadAll(tb testing.TB, conns int) time.Duration {
	tb.Helper()
	tf := s.tf
	n := tf.PieceCount()

	stor := storage.NewManager(tf, tb.TempDir())
	require.NoError(tb, stor.Init())
	defer stor.Close() //nolint:errcheck

	bf := bitfield.New(n)
	sched := scheduler.New(tf.PieceHashes, pieceLens(tf), bf)
	var id [20]byte
	copy(id[:], "-QB0100-leecher00000")
	mgr := NewManager(tf, id, 0, bf, sched, stor, ratelimit.New(0, 0), 50)
	defer mgr.Stop()

	start := time.Now()

	for w := range drain(sched.WorkQueue()) {
		mgr.work <- w
	}

	// maybeConnect refuses loopback addresses, so register each connection the
	// same way it does once a dial succeeds.
	for i := 0; i < conns; i++ {
		c, err := Dial(s.addr, tf.InfoHash, id)
		require.NoError(tb, err)
		c.BytesIn = &mgr.totalDownloaded
		entry := &connEntry{conn: c}
		mgr.mu.Lock()
		mgr.conns[c.conn.LocalAddr().String()] = entry
		mgr.mu.Unlock()
		c.Send(FormatBitfield(mgr.bf))
		go mgr.downloadFromPeer(entry)
	}

	timeout := time.After(2 * time.Minute)
	for done := 0; done < n; {
		select {
		case r := <-mgr.Results():
			// Same as the engine: storage verifies SHA1 before writing.
			require.NoError(tb, stor.WritePiece(r.Index, r.Data))
			sched.MarkComplete(r.Index)
			done++
		case <-timeout:
			tb.Fatalf("transfer stalled at %d/%d pieces", done, n)
		}
	}
	return time.Since(start)
}

func drain(ch chan *scheduler.PieceWork) chan *scheduler.PieceWork {
	close(ch)
	return ch
}

func pieceOf(data []byte, tf *torrent.TorrentFile, i int) []byte {
	start := i * tf.PieceLength
	return data[start : start+tf.PieceSize(i)]
}

func pieceLens(tf *torrent.TorrentFile) []int {
	lens := make([]int, tf.PieceCount())
	for i := range lens {
		lens[i] = tf.PieceSize(i)
	}
	return lens
}

// benchmarkLoopback measures end-to-end download throughput between two
// in-process peers on 127.0.0.1: handshake, wire protocol, 16 KiB block
// pipelining, SHA1 verification and disk writes on both sides. With no network
// in the way, this is the ceiling the client itself imposes.
func benchmarkLoopback(b *testing.B, size int64, pieceLen, conns int) {
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(nil) })

	s := newLoopbackSeeder(b, size, pieceLen)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.downloadAll(b, conns)
	}
}

func BenchmarkLoopback_256MiB_256KiBPieces_1Conn(b *testing.B) {
	benchmarkLoopback(b, 256<<20, 256<<10, 1)
}

func BenchmarkLoopback_256MiB_256KiBPieces_4Conns(b *testing.B) {
	benchmarkLoopback(b, 256<<20, 256<<10, 4)
}

func BenchmarkLoopback_256MiB_4MiBPieces_4Conns(b *testing.B) {
	benchmarkLoopback(b, 256<<20, 4<<20, 4)
}

// TestLoopbackTransfer is the correctness half of the benchmark: a small
// torrent must arrive intact between two qBit-Go peers.
func TestLoopbackTransfer(t *testing.T) {
	s := newLoopbackSeeder(t, 8<<20+12345, 256<<10)
	s.downloadAll(t, 2)
}
