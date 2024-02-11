package main

import (
	"cmp"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/docker/cli/cli/command"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/tdakkota/docker-logql/internal/dockerlog"
	"github.com/tdakkota/docker-logql/internal/logql/logqlengine"
	"github.com/tdakkota/docker-logql/internal/lokiapi"
)

func queryCmd(dcli command.Cli) *cobra.Command {
	var (
		start = apiFlagFor[lokiapi.OptLokiTime]()
		end   = apiFlagFor[lokiapi.OptLokiTime]()
		since = apiFlagFor[lokiapi.OptPrometheusDuration]()
		step  = apiFlagFor[lokiapi.OptPrometheusDuration]()
		limit int

		render renderOptions
	)
	cmd := &cobra.Command{
		Use:  "query <logql>",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.Errorf("expected 1 args, got %d", len(args))
			}
			var (
				ctx   = cmd.Context()
				query = args[0]
			)

			start, end, err := parseTimeRange(
				time.Now(),
				*start.Val,
				*end.Val,
				*since.Val,
			)
			if err != nil {
				return errors.Wrap(err, "parse time range")
			}

			step, err := parseStep(*step.Val, start, end)
			if err != nil {
				return errors.Wrap(err, "parse step")
			}

			q, err := dockerlog.NewQuerier(dcli.Client())
			if err != nil {
				return errors.Wrap(err, "create querier")
			}
			eng := logqlengine.NewEngine(q, logqlengine.Options{})

			data, err := eng.Eval(ctx, query, logqlengine.EvalParams{
				Start: pcommon.NewTimestampFromTime(start),
				End:   pcommon.NewTimestampFromTime(end),
				Step:  step,
				Limit: limit,
			})
			if err != nil {
				return errors.Wrap(err, "eval")
			}
			return renderResult(cmd.OutOrStdout(), render, data)
		},
	}
	cmd.Flags().Var(&start, "start", "Start of query range")
	cmd.Flags().Var(&end, "end", "End of query range")
	cmd.Flags().Var(&since, "since", "A duration used to calculate `start` relative to `end`")
	cmd.Flags().Var(&step, "step", "Query resolution step")
	cmd.Flags().IntVar(&limit, "limit", -1, "Limit result")
	render.Register(cmd.Flags())
	return cmd
}

type renderOptions struct {
	timestamp bool
	container bool
}

func (opts *renderOptions) Register(set *pflag.FlagSet) {
	set.BoolVarP(&opts.timestamp, "timestamp", "t", true, "Show timestamps")
	set.BoolVarP(&opts.container, "container", "c", true, "Show container name")
}

type entry struct {
	lokiapi.LogEntry
	container string
}

func renderResult(stdout io.Writer, opts renderOptions, data lokiapi.QueryResponseData) error {
	switch t := data.Type; t {
	case lokiapi.StreamsResultQueryResponseData:
		var entries []entry

		for _, stream := range data.StreamsResult.Result {
			labels := stream.Stream.Value
			for _, e := range stream.Values {
				entries = append(entries, entry{
					LogEntry:  e,
					container: labels["container"],
				})
			}
		}
		slices.SortFunc(entries, func(a, b entry) int {
			return cmp.Compare(a.T, b.T)
		})

		var buf []byte
		for _, entry := range entries {
			buf = buf[:0]

			if opts.container {
				buf = append(buf, entry.container...)
				buf = append(buf, ' ')
			}
			if opts.timestamp {
				ts := time.Unix(0, int64(entry.T))
				buf = ts.AppendFormat(buf, time.RFC3339Nano)
				buf = append(buf, ' ')
			}
			msg := strings.TrimRight(entry.V, "\r\n")
			buf = append(buf, msg...)
			buf = append(buf, "\n"...)

			if _, err := stdout.Write(buf); err != nil {
				return err
			}
		}

		return nil
	default:
		return errors.Errorf("unsupported result %q", t)
	}
}