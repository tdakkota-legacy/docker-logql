package dockerlog

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tdakkota/docker-logql/internal/iterators"
	"github.com/tdakkota/docker-logql/internal/logstorage"
	"github.com/tdakkota/docker-logql/internal/otelstorage"
)

func TestMergeIter(t *testing.T) {
	seriess := [][]otelstorage.Timestamp{
		{1, 5, 6},
		{2, 3, 7},
		{4, 8},
	}

	var (
		iters    = make([]logiter, len(seriess))
		expected []otelstorage.Timestamp
	)
	// Build iterators from given timestamp series.
	for i, series := range seriess {
		elems := make([]logstorage.Record, len(series))
		for i, ts := range series {
			elems[i] = logstorage.Record{
				Timestamp:         ts,
				ObservedTimestamp: ts,
				Body:              fmt.Sprintf("Message #%d", i),
			}
			expected = append(expected, ts)
		}
		iters[i] = iterators.Slice(elems)
	}
	// Expect a sorted list of timestamps.
	slices.Sort(expected)

	var (
		iter   = newMergeIter(iters)
		record logstorage.Record
		got    []otelstorage.Timestamp
	)
	for iter.Next(&record) {
		got = append(got, record.Timestamp)
	}
	require.NoError(t, iter.Err())
	require.Equal(t, expected, got)
}
