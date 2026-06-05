package bencode

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// FindInfoBytes returns the raw bencoded bytes of the "info" value in a .torrent file.
// The SHA1 of these bytes is the torrent's info hash — computing it requires the
// original bencoded representation, not a re-encoded version.
func FindInfoBytes(data []byte) ([]byte, error) {
	if len(data) < 2 || data[0] != 'd' {
		return nil, fmt.Errorf("bencode: data is not a dict")
	}
	i := 1
	for i < len(data) && data[i] != 'e' {
		keyStart := i
		keyEnd, err := skipValue(data, i)
		if err != nil {
			return nil, err
		}
		key := extractRawString(data[keyStart:keyEnd])
		i = keyEnd

		valStart := i
		valEnd, err := skipValue(data, i)
		if err != nil {
			return nil, err
		}
		if key == "info" {
			return data[valStart:valEnd], nil
		}
		i = valEnd
	}
	return nil, fmt.Errorf("bencode: 'info' key not found")
}

// skipValue returns the index of the first byte past the complete bencoded value at data[i].
func skipValue(data []byte, i int) (int, error) {
	if i >= len(data) {
		return 0, fmt.Errorf("bencode: unexpected end of data at position %d", i)
	}
	switch {
	case data[i] == 'i':
		end := bytes.IndexByte(data[i:], 'e')
		if end < 0 {
			return 0, fmt.Errorf("bencode: unterminated integer at position %d", i)
		}
		return i + end + 1, nil
	case data[i] == 'l':
		i++
		for i < len(data) && data[i] != 'e' {
			var err error
			i, err = skipValue(data, i)
			if err != nil {
				return 0, err
			}
		}
		if i >= len(data) {
			return 0, fmt.Errorf("bencode: unterminated list")
		}
		return i + 1, nil
	case data[i] == 'd':
		i++
		for i < len(data) && data[i] != 'e' {
			var err error
			i, err = skipValue(data, i) // key
			if err != nil {
				return 0, err
			}
			i, err = skipValue(data, i) // value
			if err != nil {
				return 0, err
			}
		}
		if i >= len(data) {
			return 0, fmt.Errorf("bencode: unterminated dict")
		}
		return i + 1, nil
	case data[i] >= '0' && data[i] <= '9':
		colon := bytes.IndexByte(data[i:], ':')
		if colon < 0 {
			return 0, fmt.Errorf("bencode: invalid string at position %d", i)
		}
		length, err := strconv.Atoi(string(data[i : i+colon]))
		if err != nil {
			return 0, err
		}
		end := i + colon + 1 + length
		if end > len(data) {
			return 0, fmt.Errorf("bencode: string extends past end of data")
		}
		return end, nil
	default:
		return 0, fmt.Errorf("bencode: unknown type byte %q at position %d", data[i], i)
	}
}

// extractRawString returns the string content of a bencoded string like "4:spam" → "spam".
func extractRawString(data []byte) string {
	colon := bytes.IndexByte(data, ':')
	if colon < 0 {
		return ""
	}
	return string(data[colon+1:])
}

// Unmarshal decodes a bencoded byte slice into a Go value.
// Supported target types: map[string]interface{}, []interface{}, int64, string.
func Unmarshal(data []byte) (interface{}, error) {
	r := bufio.NewReader(bytes.NewReader(data))
	return decode(r)
}

// Marshal encodes a Go value into bencode.
// Supported types: string, int, int64, []interface{}, map[string]interface{}.
func Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := encode(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(r *bufio.Reader) (interface{}, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case b == 'i':
		return decodeInt(r)
	case b == 'l':
		return decodeList(r)
	case b == 'd':
		return decodeDict(r)
	case b >= '0' && b <= '9':
		r.UnreadByte()
		return decodeString(r)
	default:
		return nil, fmt.Errorf("bencode: unknown type byte %q", b)
	}
}

func decodeInt(r *bufio.Reader) (int64, error) {
	s, err := r.ReadString('e')
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s[:len(s)-1], 10, 64)
}

func decodeString(r *bufio.Reader) (string, error) {
	lenStr, err := r.ReadString(':')
	if err != nil {
		return "", err
	}
	length, err := strconv.Atoi(lenStr[:len(lenStr)-1])
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(r, buf)
	return string(buf), err
}

func decodeList(r *bufio.Reader) ([]interface{}, error) {
	var list []interface{}
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == 'e' {
			break
		}
		r.UnreadByte()
		v, err := decode(r)
		if err != nil {
			return nil, err
		}
		list = append(list, v)
	}
	return list, nil
}

func decodeDict(r *bufio.Reader) (map[string]interface{}, error) {
	d := make(map[string]interface{})
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == 'e' {
			break
		}
		r.UnreadByte()
		key, err := decodeString(r)
		if err != nil {
			return nil, err
		}
		val, err := decode(r)
		if err != nil {
			return nil, err
		}
		d[key] = val
	}
	return d, nil
}

func encode(w io.Writer, v interface{}) error {
	switch val := v.(type) {
	case string:
		_, err := fmt.Fprintf(w, "%d:%s", len(val), val)
		return err
	case int:
		_, err := fmt.Fprintf(w, "i%de", val)
		return err
	case int64:
		_, err := fmt.Fprintf(w, "i%de", val)
		return err
	case []interface{}:
		if _, err := fmt.Fprint(w, "l"); err != nil {
			return err
		}
		for _, item := range val {
			if err := encode(w, item); err != nil {
				return err
			}
		}
		_, err := fmt.Fprint(w, "e")
		return err
	case map[string]interface{}:
		if _, err := fmt.Fprint(w, "d"); err != nil {
			return err
		}
		for k, item := range val {
			if err := encode(w, k); err != nil {
				return err
			}
			if err := encode(w, item); err != nil {
				return err
			}
		}
		_, err := fmt.Fprint(w, "e")
		return err
	default:
		return fmt.Errorf("bencode: unsupported type %T", v)
	}
}
