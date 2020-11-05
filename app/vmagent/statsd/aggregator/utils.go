package aggregator

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/statsd"
)

func genRowLabels(row *parser.Row, withName bool) []prompbmarshal.Label {
	labels := make([]prompbmarshal.Label, 0, len(row.Tags)+2)
	if withName {
		labels = append(labels, prompbmarshal.Label{
			Name:  "__name__",
			Value: row.Metric,
		})
	}
	for _, tag := range row.Tags {
		labels = append(labels, prompbmarshal.Label{
			Name:  tag.Key,
			Value: tag.Value,
		})
	}
	return labels
}

func validateMetric(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		case r == '_' || r == ':':
		default:
			return false
		}
	}
	return true
}
