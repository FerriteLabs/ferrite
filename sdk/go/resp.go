package ferrite

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP2 type prefixes
const (
	respSimpleString = '+'
	respError        = '-'
	respInteger      = ':'
	respBulkString   = '$'
	respArray        = '*'
)

// respWriter encodes commands in RESP2 format.
type respWriter struct {
	w *bufio.Writer
}

func newRESPWriter(w io.Writer) *respWriter {
	return &respWriter{w: bufio.NewWriter(w)}
}

// WriteCommand writes a RESP array command (e.g., *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n).
func (rw *respWriter) WriteCommand(args ...string) error {
	// Array header
	if _, err := fmt.Fprintf(rw.w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(rw.w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return rw.w.Flush()
}

// respReader decodes RESP2 responses.
type respReader struct {
	r *bufio.Reader
}

func newRESPReader(r io.Reader) *respReader {
	return &respReader{r: bufio.NewReader(r)}
}

// ReadValue reads a single RESP value and returns it as an interface{}.
// Returns: string for simple strings and bulk strings, int64 for integers,
// []interface{} for arrays, error for RESP errors, nil for null bulk strings.
func (rr *respReader) ReadValue() (interface{}, error) {
	line, err := rr.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("ferrite: empty RESP line")
	}

	switch line[0] {
	case respSimpleString:
		return string(line[1:]), nil

	case respError:
		return nil, fmt.Errorf("ferrite: %s", string(line[1:]))

	case respInteger:
		n, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ferrite: invalid integer: %w", err)
		}
		return n, nil

	case respBulkString:
		length, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, fmt.Errorf("ferrite: invalid bulk length: %w", err)
		}
		if length == -1 {
			return nil, nil // null bulk string
		}
		buf := make([]byte, length+2) // +2 for trailing \r\n
		if _, err := io.ReadFull(rr.r, buf); err != nil {
			return nil, fmt.Errorf("ferrite: bulk read error: %w", err)
		}
		return string(buf[:length]), nil

	case respArray:
		count, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, fmt.Errorf("ferrite: invalid array length: %w", err)
		}
		if count == -1 {
			return nil, nil // null array
		}
		arr := make([]interface{}, count)
		for i := 0; i < count; i++ {
			arr[i], err = rr.ReadValue()
			if err != nil {
				return nil, err
			}
		}
		return arr, nil

	default:
		return nil, fmt.Errorf("ferrite: unknown RESP type: %c", line[0])
	}
}

func (rr *respReader) readLine() ([]byte, error) {
	line, err := rr.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	return line[:len(line)-1], nil
}
