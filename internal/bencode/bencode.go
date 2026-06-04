package bencode

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

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
