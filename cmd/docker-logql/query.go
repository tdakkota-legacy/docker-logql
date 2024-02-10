package main

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/docker/cli/cli/command"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
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
			return renderResult(cmd.OutOrStdout(), true, data)
		},
	}
	cmd.Flags().Var(&start, "start", "Start of query range")
	cmd.Flags().Var(&end, "end", "End of query range")
	cmd.Flags().Var(&since, "since", "A duration used to calculate `start` relative to `end`")
	cmd.Flags().Var(&step, "step", "Query resolution step")
	cmd.Flags().IntVar(&limit, "limit", -1, "Limit result")
	return cmd
}

func renderResult(stdout io.Writer, printTimestamp bool, data lokiapi.QueryResponseData) error {
	switch t := data.Type; t {
	case lokiapi.StreamsResultQueryResponseData:
		var entries []lokiapi.LogEntry

		for _, stream := range data.StreamsResult.Result {
			entries = append(entries, stream.Values...)
		}
		slices.SortFunc(entries, func(a, b lokiapi.LogEntry) int {
			return cmp.Compare(a.T, b.T)
		})

		for _, entry := range entries {
			msg := strings.TrimRight(entry.V, "\r\n")
			if printTimestamp {
				if _, err := fmt.Fprintf(stdout, "%s %s\n", time.Unix(0, int64(entry.T)), msg); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(stdout, "%s\n", msg); err != nil {
					return err
				}
			}
		}

		return nil
	default:
		return errors.Errorf("unsupported result %q", t)
	}
}
