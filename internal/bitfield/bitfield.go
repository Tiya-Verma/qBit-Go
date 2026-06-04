package bitfield

import "math/bits"

// Bitfield tracks which pieces are complete.
// Each bit (big-endian within each byte) represents one piece index.
type Bitfield []byte

// New allocates a Bitfield for pieceCount pieces.
func New(pieceCount int) Bitfield {
	return make(Bitfield, (pieceCount+7)/8)
}

// HasPiece reports whether piece at index is marked complete.
func (bf Bitfield) HasPiece(index int) bool {
	byteIndex := index / 8
	bitIndex := 7 - (index % 8)
	if byteIndex >= len(bf) {
		return false
	}
	return bf[byteIndex]>>uint(bitIndex)&1 != 0
}

// SetPiece marks piece at index as complete.
func (bf Bitfield) SetPiece(index int) {
	byteIndex := index / 8
	bitIndex := 7 - (index % 8)
	if byteIndex < len(bf) {
		bf[byteIndex] |= 1 << uint(bitIndex)
	}
}

// ClearPiece marks piece at index as incomplete.
func (bf Bitfield) ClearPiece(index int) {
	byteIndex := index / 8
	bitIndex := 7 - (index % 8)
	if byteIndex < len(bf) {
		bf[byteIndex] &^= 1 << uint(bitIndex)
	}
}

// CountComplete returns the number of pieces marked complete.
func (bf Bitfield) CountComplete() int {
	n := 0
	for _, b := range bf {
		n += bits.OnesCount8(b)
	}
	return n
}

// Clone returns a copy of the bitfield.
func (bf Bitfield) Clone() Bitfield {
	out := make(Bitfield, len(bf))
	copy(out, bf)
	return out
}
