// Package logqlengine implements LogQL evaluation engine.
package logqlengine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/tdakkota/docker-logql/internal/iterators"
	"github.com/tdakkota/docker-logql/internal/logql"
	"github.com/tdakkota/docker-logql/internal/logstorage"
	"github.com/tdakkota/docker-logql/internal/lokiapi"
	"github.com/tdakkota/docker-logql/internal/otelstorage"
)

type inputLine struct {
	line  string
	attrs pcommon.Map
}

type resultLine struct {
	line   string
	labels map[string]string
}

type mockQuerier struct {
	lines []inputLine
	step  time.Duration
}

func (m *mockQuerier) Capabilities() (caps QuerierCapabilities) {
	return caps
}

func (m *mockQuerier) SelectLogs(_ context.Context, start, _ otelstorage.Timestamp, _ SelectLogsParams) (iterators.Iterator[logstorage.Record], error) {
	step := m.step
	if step == 0 {
		step = time.Millisecond
	}
	ts := start.AsTime()

	var (
		records    []logstorage.Record
		scopeAttrs = pcommon.NewMap()
		resAttrs   = pcommon.NewMap()
	)
	scopeAttrs.PutStr("scope", "test")
	resAttrs.PutStr("resource", "test")

	for _, l := range m.lines {
		ts = ts.Add(step)
		rec := logstorage.Record{
			Timestamp:     otelstorage.NewTimestampFromTime(ts),
			Body:          l.line,
			Attrs:         otelstorage.Attrs(l.attrs),
			ScopeAttrs:    otelstorage.Attrs(scopeAttrs),
			ResourceAttrs: otelstorage.Attrs(resAttrs),
		}
		if rec.Attrs == otelstorage.Attrs(pcommon.Map{}) {
			rec.Attrs = otelstorage.Attrs(pcommon.NewMap())
		}
		records = append(records, rec)
	}

	return iterators.Slice(records), nil
}

func justLines(lines ...string) []inputLine {
	r := make([]inputLine, len(lines))
	for i, line := range lines {
		r[i] = inputLine{
			line: line,
		}
	}
	return r
}

var (
	inputLines = justLines(
		`{"id": 1, "foo": "4m", "bar": "1s", "baz": "1kb"}`,
		`{"id": 2, "foo": "5m", "bar": "2s", "baz": "1mb"}`,
		`{"id": 3, "foo": "6m", "bar": "3s", "baz": "1gb"}`,
	)
	resultLines = []resultLine{
		{
			`{"id": 1, "foo": "4m", "bar": "1s", "baz": "1kb"}`,
			map[string]string{
				"id":  "1",
				"foo": "4m",
				"bar": "1s",
				"baz": "1kb",
			},
		},
		{
			`{"id": 2, "foo": "5m", "bar": "2s", "baz": "1mb"}`,
			map[string]string{
				"id":  "2",
				"foo": "5m",
				"bar": "2s",
				"baz": "1mb",
			},
		},
		{
			`{"id": 3, "foo": "6m", "bar": "3s", "baz": "1gb"}`,
			map[string]string{
				"id":  "3",
				"foo": "6m",
				"bar": "3s",
				"baz": "1gb",
			},
		},
	}
)

type timeRange struct {
	start uint64
	end   uint64
	step  time.Duration
}

func TestEngineEvalLiteral(t *testing.T) {
	type testCase struct {
		query   string
		tsRange timeRange

		wantData lokiapi.QueryResponseData
		wantErr  bool
	}
	test3steps := func(input, result string) testCase {
		return testCase{
			input,
			timeRange{
				start: 1700000001_000000000,
				end:   1700000003_000000000,
				step:  time.Second,
			},
			lokiapi.QueryResponseData{
				Type: lokiapi.MatrixResultQueryResponseData,
				MatrixResult: lokiapi.MatrixResult{
					Result: lokiapi.Matrix{
						{
							Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet{}),
							Values: []lokiapi.FPoint{
								{T: 1700000001, V: result},
								{T: 1700000002, V: result},
								{T: 1700000003, V: result},
							},
						},
					},
				},
			},
			false,
		}
	}

	tests := []testCase{
		// Literal eval.
		{
			`3.14`,
			timeRange{
				start: 1700000001_000000000,
				end:   1700000001_000000000,
			},
			lokiapi.QueryResponseData{
				Type: lokiapi.ScalarResultQueryResponseData,
				ScalarResult: lokiapi.ScalarResult{
					Result: lokiapi.FPoint{
						T: 1700000001,
						V: "3.14",
					},
				},
			},
			false,
		},
		{
			`3.14`,
			timeRange{
				start: 1700000001_000000000,
				end:   1700000003_000000000,
				step:  time.Second,
			},
			lokiapi.QueryResponseData{
				Type: lokiapi.MatrixResultQueryResponseData,
				MatrixResult: lokiapi.MatrixResult{
					Result: lokiapi.Matrix{
						{
							Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet{}),
							Values: []lokiapi.FPoint{
								{T: 1700000001, V: "3.14"},
								{T: 1700000002, V: "3.14"},
								{T: 1700000003, V: "3.14"},
							},
						},
					},
				},
			},
			false,
		},

		// Vector eval.
		{
			`vector(3.14)`,
			timeRange{
				start: 1700000001_000000000,
				end:   1700000001_000000000,
			},
			lokiapi.QueryResponseData{
				Type: lokiapi.VectorResultQueryResponseData,
				VectorResult: lokiapi.VectorResult{
					Result: lokiapi.Vector{
						{
							Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet{}),
							Value: lokiapi.FPoint{
								T: 1700000001,
								V: "3.14",
							},
						},
					},
				},
			},
			false,
		},
		test3steps(`vector(3.14)`, "3.14"),

		// Precedence tests.
		test3steps(`vector(2)+vector(3)*vector(4)`, "14"),
		test3steps(`vector(2)*vector(3)+vector(4)`, "10"),
		test3steps(`vector(2) + vector(3)*vector(4) + vector(5)`, "19"),
		test3steps(`vector(2)+vector(3)^vector(2)`, "11"),
		test3steps(`vector(2)*vector(3)^vector(2)`, "18"),
		test3steps(`vector(2)^vector(3)*vector(2)`, "16"),
		test3steps(`vector(2)^vector(3)^vector(2)`, `512`),
		test3steps(`(vector(2)^vector(3))^vector(2)`, `64`),
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			ctx := context.Background()

			opts := Options{
				ParseOptions: logql.ParseOptions{AllowDots: true},
			}
			e := NewEngine(&mockQuerier{}, opts)

			gotData, err := e.Eval(ctx, tt.query, EvalParams{
				Start: otelstorage.Timestamp(tt.tsRange.start),
				End:   otelstorage.Timestamp(tt.tsRange.end),
				Step:  tt.tsRange.step,
				Limit: 1000,
			})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantData, gotData)
		})
	}
}
