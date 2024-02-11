package dockerlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/tdakkota/docker-logql/internal/iterators"
	"github.com/tdakkota/docker-logql/internal/logstorage"
	"github.com/tdakkota/docker-logql/internal/otelstorage"
)

// ParseLog parses log stream from Docker daemon.
func ParseLog(f io.ReadCloser, resource otelstorage.Attrs) iterators.Iterator[logstorage.Record] {
	return &streamIter{
		rd:       f,
		err:      nil,
		resource: resource,
	}
}

const headerLen = 8

type streamIter struct {
	rd     io.ReadCloser
	header [headerLen]byte
	buf    bytes.Buffer
	err    error

	resource otelstorage.Attrs
}

var _ logiter = (*streamIter)(nil)

// Next returns true, if there is element and fills t.
func (i *streamIter) Next(r *logstorage.Record) (ok bool) {
	// Reset record.
	*r = logstorage.Record{
		Attrs:         otelstorage.Attrs(pcommon.NewMap()),
		ResourceAttrs: i.resource,
	}

	ok, i.err = i.parseNext(r)
	return ok
}

type stdType byte

const (
	// Stdin represents standard input stream type.
	stdin stdType = iota
	// Stdout represents standard output stream type.
	stdout
	// Stderr represents standard error steam type.
	stderr
	// Systemerr represents errors originating from the system that make it
	// into the multiplexed stream.
	systemerr
)

func (i *streamIter) parseNext(r *logstorage.Record) (bool, error) {
	if _, err := io.ReadFull(i.rd, i.header[:]); err != nil {
		switch err {
		case io.EOF, io.ErrUnexpectedEOF:
			// Handle missing header gracefully, docker-cli does the same thing.
			return false, nil
		default:
			return false, errors.Wrap(err, "read header")
		}
	}

	var (
		typ       = stdType(i.header[0])
		frameSize = binary.BigEndian.Uint32(i.header[4:8])
	)
	i.buf.Reset()
	if _, err := io.CopyN(&i.buf, i.rd, int64(frameSize)); err != nil {
		return false, errors.Wrap(err, "read message")
	}
	if typ == systemerr {
		return false, errors.Errorf("daemon log stream error: %q", &i.buf)
	}

	if err := parseDockerLine(typ, i.buf.String(), r); err != nil {
		return false, errors.Wrap(err, "parse log line")
	}
	return true, nil
}

func parseDockerLine(_ stdType, input string, r *logstorage.Record) error {
	const dockerTimestampFormat = time.RFC3339Nano

	rawTimestamp, line, ok := strings.Cut(input, " ")
	if !ok {
		return errors.New("invalid line: no space between timestamp and message")
	}
	r.Body = line

	ts, err := time.Parse(dockerTimestampFormat, rawTimestamp)
	if err != nil {
		return errors.Wrap(err, "parse timestamp")
	}
	r.ObservedTimestamp = otelstorage.NewTimestampFromTime(ts)
	r.Timestamp = r.ObservedTimestamp
	return nil
}

// Err returns an error caused during iteration, if any.
func (i *streamIter) Err() error {
	return i.err
}

// Close closes iterator.
func (i *streamIter) Close() error {
	return i.rd.Close()
}
