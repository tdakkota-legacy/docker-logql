package dockerlog

import (
	"context"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/tdakkota/docker-logql/internal/iterators"
	"github.com/tdakkota/docker-logql/internal/logql"
	"github.com/tdakkota/docker-logql/internal/logql/logqlengine"
	"github.com/tdakkota/docker-logql/internal/logstorage"
	"github.com/tdakkota/docker-logql/internal/otelstorage"
)

// Querier implements LogQL querier.
type Querier struct {
	client client.APIClient
}

// NewQuerier creates new Querier.
func NewQuerier(c client.APIClient) (*Querier, error) {
	return &Querier{
		client: c,
	}, nil
}

// Capabilities returns Querier capabilities.
// NOTE: engine would call once and then save value.
//
// Capabilities should not change over time.
func (q *Querier) Capabilities() (caps logqlengine.QuerierCapabilities) {
	caps.Label.Add(logql.OpEq, logql.OpNotEq, logql.OpRe, logql.OpNotRe)
	return caps
}

// SelectLogs selects log records from storage.
func (q *Querier) SelectLogs(ctx context.Context, start, end otelstorage.Timestamp, params logqlengine.SelectLogsParams) (iterators.Iterator[logstorage.Record], error) {
	containers, err := q.client.ContainerList(ctx, container.ListOptions{
		All: true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "query container list")
	}

	n := 0
	for _, ctr := range containers {
		if matchContainer(ctr, params) {
			containers[n] = ctr
			n++
		}
	}
	containers = containers[:n]
	if len(containers) == 0 {
		return iterators.Empty[logstorage.Record](), nil
	}
	if len(containers) != 1 {
		return nil, errors.New("FIXME: merge logs from multiple containers")
	}
	ctr := containers[0]

	resource := otelstorage.Attrs(pcommon.NewMap())
	{
		m := resource.AsMap()
		var name string
		if len(ctr.Names) > 0 {
			name = strings.TrimPrefix(ctr.Names[0], "/")
		}
		m.PutStr("container", name)
		m.PutStr("container_name", name)
		m.PutStr("container_id", ctr.ID)
		m.PutStr("image", ctr.Image)
		m.PutStr("image_id", ctr.ImageID)
	}

	var since, until string
	if t := start.AsTime(); !t.IsZero() {
		since = strconv.FormatInt(t.Unix(), 10)
	}
	if t := end.AsTime(); !t.IsZero() {
		until = strconv.FormatInt(t.Unix(), 10)
	}

	rc, err := q.client.ContainerLogs(ctx, ctr.ID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Since:      since,
		Until:      until,
		Timestamps: true,
		Tail:       "all",
	})
	if err != nil {
		return nil, errors.Wrap(err, "query logs")
	}

	return ParseLog(rc, resource), nil
}

func matchContainer(ctr types.Container, params logqlengine.SelectLogsParams) (result bool) {
nextMatcher:
	for _, matcher := range params.Labels {
		var value string
		switch matcher.Label {
		case "container", "container_name":
			// Special case, since container may have multiple names.
			//
			// Match at least one.
			for _, value := range ctr.Names {
				value = strings.TrimPrefix(value, "/")
				if match(matcher, value) {
					continue nextMatcher
				}
			}
			return false
		case "container_id":
			value = ctr.ID
		case "image":
			value = ctr.Image
		case "image_id":
			value = ctr.ImageID
		default:
			// Unknown label.
			return false
		}
		if !match(matcher, value) {
			return false
		}
	}
	return true
}

func match(m logql.LabelMatcher, s string) bool {
	switch m.Op {
	case logql.OpEq:
		return s == m.Value
	case logql.OpNotEq:
		return s == m.Value
	case logql.OpRe:
		return m.Re.MatchString(s)
	case logql.OpNotRe:
		return !m.Re.MatchString(s)
	default:
		return false
	}
}
