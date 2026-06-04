package ratelimit

import (
	"sync"
	"time"
)

// Limiter is a token bucket rate limiter supporting both global and
// per-torrent limits. A limit of 0 means unlimited.
type Limiter struct {
	mu sync.Mutex

	globalDown  *bucket
	globalUp    *bucket
	torrentDown map[[20]byte]*bucket
	torrentUp   map[[20]byte]*bucket
}

// New creates a Limiter with the given global byte-per-second caps (0 = unlimited).
func New(downBPS, upBPS int64) *Limiter {
	return &Limiter{
		globalDown:  newBucket(downBPS),
		globalUp:    newBucket(upBPS),
		torrentDown: make(map[[20]byte]*bucket),
		torrentUp:   make(map[[20]byte]*bucket),
	}
}

// SetGlobalDown updates the global download cap (0 = unlimited).
func (l *Limiter) SetGlobalDown(bps int64) {
	l.mu.Lock()
	l.globalDown = newBucket(bps)
	l.mu.Unlock()
}

// SetGlobalUp updates the global upload cap (0 = unlimited).
func (l *Limiter) SetGlobalUp(bps int64) {
	l.mu.Lock()
	l.globalUp = newBucket(bps)
	l.mu.Unlock()
}

// SetTorrentDown sets a per-torrent download cap.
func (l *Limiter) SetTorrentDown(hash [20]byte, bps int64) {
	l.mu.Lock()
	l.torrentDown[hash] = newBucket(bps)
	l.mu.Unlock()
}

// SetTorrentUp sets a per-torrent upload cap.
func (l *Limiter) SetTorrentUp(hash [20]byte, bps int64) {
	l.mu.Lock()
	l.torrentUp[hash] = newBucket(bps)
	l.mu.Unlock()
}

// WaitDownload blocks until n bytes of download capacity are available.
func (l *Limiter) WaitDownload(hash [20]byte, n int) {
	l.mu.Lock()
	gb := l.globalDown
	tb := l.torrentDown[hash]
	l.mu.Unlock()
	gb.wait(n)
	if tb != nil {
		tb.wait(n)
	}
}

// WaitUpload blocks until n bytes of upload capacity are available.
func (l *Limiter) WaitUpload(hash [20]byte, n int) {
	l.mu.Lock()
	gb := l.globalUp
	tb := l.torrentUp[hash]
	l.mu.Unlock()
	gb.wait(n)
	if tb != nil {
		tb.wait(n)
	}
}

// bucket is a simple token bucket.
type bucket struct {
	mu       sync.Mutex
	rate     int64 // tokens per second; 0 = unlimited
	tokens   float64
	lastFill time.Time
}

func newBucket(rate int64) *bucket {
	return &bucket{rate: rate, lastFill: time.Now()}
}

func (b *bucket) wait(n int) {
	if b.rate == 0 {
		return
	}
	for {
		b.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(b.lastFill).Seconds()
		b.tokens += elapsed * float64(b.rate)
		if b.tokens > float64(b.rate) {
			b.tokens = float64(b.rate)
		}
		b.lastFill = now

		if b.tokens >= float64(n) {
			b.tokens -= float64(n)
			b.mu.Unlock()
			return
		}
		needed := float64(n) - b.tokens
		delay := time.Duration(needed/float64(b.rate)*1000) * time.Millisecond
		b.mu.Unlock()
		time.Sleep(delay)
	}
}
