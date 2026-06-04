package bencode_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tiyaverma/qbit-go/internal/bencode"
)

func TestUnmarshalInt(t *testing.T) {
	v, err := bencode.Unmarshal([]byte("i42e"))
	require.NoError(t, err)
	assert.Equal(t, int64(42), v)
}

func TestUnmarshalString(t *testing.T) {
	v, err := bencode.Unmarshal([]byte("5:hello"))
	require.NoError(t, err)
	assert.Equal(t, "hello", v)
}

func TestUnmarshalList(t *testing.T) {
	v, err := bencode.Unmarshal([]byte("li1ei2ei3ee"))
	require.NoError(t, err)
	list, ok := v.([]interface{})
	require.True(t, ok)
	assert.Len(t, list, 3)
	assert.Equal(t, int64(1), list[0])
}

func TestUnmarshalDict(t *testing.T) {
	v, err := bencode.Unmarshal([]byte("d3:foo3:bare"))
	require.NoError(t, err)
	d, ok := v.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "bar", d["foo"])
}

func TestMarshalRoundtrip(t *testing.T) {
	cases := []interface{}{
		"hello",
		int64(99),
		[]interface{}{"a", int64(1)},
	}
	for _, c := range cases {
		data, err := bencode.Marshal(c)
		require.NoError(t, err)
		got, err := bencode.Unmarshal(data)
		require.NoError(t, err)
		assert.Equal(t, c, got)
	}
}
