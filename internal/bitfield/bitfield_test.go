package bitfield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tiyaverma/qbit-go/internal/bitfield"
)

func TestHasAndSetPiece(t *testing.T) {
	bf := bitfield.New(16)
	for i := 0; i < 16; i++ {
		assert.False(t, bf.HasPiece(i), "expected piece %d unset initially", i)
	}

	bf.SetPiece(0)
	bf.SetPiece(7)
	bf.SetPiece(15)

	assert.True(t, bf.HasPiece(0))
	assert.True(t, bf.HasPiece(7))
	assert.True(t, bf.HasPiece(15))
	assert.False(t, bf.HasPiece(1))
	assert.Equal(t, 3, bf.CountComplete())
}

func TestClearPiece(t *testing.T) {
	bf := bitfield.New(8)
	bf.SetPiece(3)
	assert.True(t, bf.HasPiece(3))
	bf.ClearPiece(3)
	assert.False(t, bf.HasPiece(3))
}

func TestClone(t *testing.T) {
	bf := bitfield.New(8)
	bf.SetPiece(2)
	clone := bf.Clone()
	clone.SetPiece(5)
	assert.False(t, bf.HasPiece(5), "original should be unaffected")
	assert.True(t, clone.HasPiece(5))
}
