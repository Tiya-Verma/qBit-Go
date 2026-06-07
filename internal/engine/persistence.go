package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/tiyaverma/qbit-go/internal/bitfield"
	"github.com/tiyaverma/qbit-go/internal/torrent"
)

var (
	bucketTorrents   = []byte("torrents")
	bucketFastResume = []byte("fastresume")
)

// torrentRecord is JSON-encoded and stored in the "torrents" bucket.
// Storing the raw .torrent bytes means we re-parse on restore rather than
// trying to serialize the full TorrentFile struct.
type torrentRecord struct {
	Data        []byte    `json:"d"`
	DownloadDir string    `json:"dir"`
	AddedAt     time.Time `json:"at"`
}

func openDB(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("engine: open db %q: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketTorrents); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(bucketFastResume)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("engine: init db buckets: %w", err)
	}
	return db, nil
}

// dbPutTorrent persists a newly added torrent so it survives restarts.
func (e *Engine) dbPutTorrent(hash [20]byte, data []byte, dir string, addedAt time.Time) {
	if e.db == nil {
		return
	}
	rec := torrentRecord{Data: data, DownloadDir: dir, AddedAt: addedAt}
	encoded, err := json.Marshal(rec)
	if err != nil {
		log.Printf("engine: marshal torrent record: %v", err)
		return
	}
	if err := e.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTorrents).Put(hash[:], encoded)
	}); err != nil {
		log.Printf("engine: db put torrent %x: %v", hash, err)
	}
}

// dbDeleteTorrent removes a torrent and its fast-resume data from the database.
func (e *Engine) dbDeleteTorrent(hash [20]byte) {
	if e.db == nil {
		return
	}
	e.db.Update(func(tx *bolt.Tx) error { //nolint:errcheck
		tx.Bucket(bucketTorrents).Delete(hash[:])
		tx.Bucket(bucketFastResume).Delete(hash[:])
		return nil
	})
}

// dbSaveFastResume writes the current piece bitfield so the next start skips re-verification.
func (e *Engine) dbSaveFastResume(sess *session) {
	if e.db == nil {
		return
	}
	bf := sess.storage.Bitfield()
	if err := e.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFastResume).Put(sess.t.File.InfoHash[:], []byte(bf))
	}); err != nil {
		log.Printf("engine: save fast resume %x: %v", sess.t.File.InfoHash, err)
	}
}

// dbLoadFastResume returns the persisted bitfield for hash, or a fresh empty one.
func (e *Engine) dbLoadFastResume(hash [20]byte, pieceCount int) bitfield.Bitfield {
	if e.db == nil {
		return bitfield.New(pieceCount)
	}
	var bf bitfield.Bitfield
	e.db.View(func(tx *bolt.Tx) error { //nolint:errcheck
		raw := tx.Bucket(bucketFastResume).Get(hash[:])
		if len(raw) > 0 {
			bf = make(bitfield.Bitfield, len(raw))
			copy(bf, raw)
		}
		return nil
	})
	if bf == nil {
		return bitfield.New(pieceCount)
	}
	return bf
}

// RestoreFromDB re-adds all torrents that were active before the last shutdown.
// Call this once after New(), before serving API requests.
func (e *Engine) RestoreFromDB() error {
	if e.db == nil {
		return nil
	}
	return e.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTorrents).ForEach(func(_, v []byte) error {
			var rec torrentRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				log.Printf("engine: skip corrupt db record: %v", err)
				return nil
			}
			tf, err := torrent.ParseFile(rec.Data)
			if err != nil {
				log.Printf("engine: skip unparseable db torrent: %v", err)
				return nil
			}
			bf := e.dbLoadFastResume(tf.InfoHash, tf.PieceCount())
			if _, err := e.startSession(tf, nil, bf, rec.DownloadDir, rec.AddedAt); err != nil {
				log.Printf("engine: restore %x: %v", tf.InfoHash, err)
			}
			return nil
		})
	})
}
