package logqlengine

import (
	"regexp"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/tdakkota/docker-logql/internal/logql"
	"github.com/tdakkota/docker-logql/internal/otelstorage"
)

// RegexpExtractor is a Regexp label extractor.
type RegexpExtractor struct {
	re      *regexp.Regexp
	mapping map[int]logql.Label
}

func buildRegexpExtractor(stage *logql.RegexpLabelParser) (Processor, error) {
	return &RegexpExtractor{
		re:      stage.Regexp,
		mapping: stage.Mapping,
	}, nil
}

// Process implements Processor.
func (e *RegexpExtractor) Process(_ otelstorage.Timestamp, line string, set LabelSet) (string, bool) {
	for i, match := range e.re.FindStringSubmatch(line) {
		label, ok := e.mapping[i]
		if !ok {
			continue
		}
		set.Set(label, pcommon.NewValueStr(match))
	}
	return line, true
}
