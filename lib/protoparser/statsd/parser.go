package statsd

import (
	"fmt"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fastjson/fastfloat"
)

type MetricType int

const (
	NoneType MetricType = iota
	CounterType
	GaugeType
	TimingType
	HistogramType
)

// Rows contains parsed statsd rows.
type Rows struct {
	Rows []Row

	tagsPool []Tag
}

// Reset resets rs.
func (rs *Rows) Reset() {
	// Reset items, so they can be GC'ed

	for i := range rs.Rows {
		rs.Rows[i].reset()
	}
	rs.Rows = rs.Rows[:0]

	for i := range rs.tagsPool {
		rs.tagsPool[i].reset()
	}
	rs.tagsPool = rs.tagsPool[:0]
}

// Unmarshal unmarshals statsd plaintext protocol rows from s.
//
// See https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/?tab=metrics
//
// s shouldn't be modified when rs is in use.
func (rs *Rows) Unmarshal(s string) {
	rs.Rows, rs.tagsPool = unmarshalRows(rs.Rows[:0], s, rs.tagsPool[:0])
}

// Row is a single graphite row.
type Row struct {
	Metric     string
	Type       MetricType
	Value      float64
	SampleRate float64
	Tags       []Tag
}

func (r *Row) reset() {
	r.Metric = ""
	r.Type = NoneType
	r.Value = 0
	r.Tags = nil
}

func (r *Row) unmarshal(s string, tagsPool []Tag) ([]Tag, error) {
	r.reset()
	n := strings.IndexByte(s, '|')
	if n < 0 {
		return tagsPool, fmt.Errorf("cannot find | between metric and type in %q", s)
	}
	metricAndValue := s[:n]
	tail := s[n+1:]

	n = strings.IndexByte(metricAndValue, ':')
	if n > 0 {
		r.Metric = metricAndValue[:n]
		v, err := fastfloat.Parse(metricAndValue[n+1:])
		if err != nil {
			return tagsPool, fmt.Errorf("cannot unmarshal value from %q: %w", tail, err)
		}
		r.Value = v
	}
	if len(r.Metric) == 0 {
		return tagsPool, fmt.Errorf("metric cannot be empty")
	}

	var metricType string
	n = strings.IndexByte(tail, '|')
	if n > 0 {
		metricType = tail[:n]
		tail = tail[n+1:]
	} else {
		metricType = tail
		tail = ""
	}
	switch metricType {
	case "c":
		r.Type = CounterType
	case "g":
		r.Type = GaugeType
	case "ms":
		r.Type = TimingType
	case "h":
		r.Type = HistogramType
	default:
		return tagsPool, fmt.Errorf("invalid metric type: %q", metricType)
	}

	r.SampleRate = 1.0
	var optionalField string
	for tail != "" {
		n = strings.IndexByte(tail, '|')
		if n > 0 {
			optionalField = tail[:n]
			tail = tail[n+1:]
		} else {
			optionalField = tail
			tail = ""
		}

		switch {
		case strings.HasPrefix(optionalField, "#"):
			tagsStart := len(tagsPool)
			tagsPool, err := unmarshalTags(tagsPool, optionalField[n+1:])
			if err != nil {
				return tagsPool, fmt.Errorf("cannot umarshal tags from %q: %w", optionalField, err)
			}
			tags := tagsPool[tagsStart:]
			r.Tags = tags[:len(tags):len(tags)]
		case strings.HasPrefix(optionalField, "@"):
			v, err := fastfloat.Parse(optionalField[n+1:])
			if err != nil {
				return tagsPool, fmt.Errorf("cannot unmarshal sample rate from %q: %w", optionalField, err)
			}
			r.SampleRate = v
		default:
			return tagsPool, fmt.Errorf("cannot unmarshal optional field from %q", optionalField)
		}
	}
	return tagsPool, nil
}

func unmarshalRows(dst []Row, s string, tagsPool []Tag) ([]Row, []Tag) {
	for len(s) > 0 {
		n := strings.IndexByte(s, '\n')
		if n < 0 {
			// The last line.
			return unmarshalRow(dst, s, tagsPool)
		}
		dst, tagsPool = unmarshalRow(dst, s[:n], tagsPool)
		s = s[n+1:]
	}
	return dst, tagsPool
}

func unmarshalRow(dst []Row, s string, tagsPool []Tag) ([]Row, []Tag) {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		s = s[:len(s)-1]
	}
	if len(s) == 0 {
		// Skip empty line
		return dst, tagsPool
	}

	if cap(dst) > len(dst) {
		dst = dst[:len(dst)+1]
	} else {
		dst = append(dst, Row{})
	}
	r := &dst[len(dst)-1]
	var err error
	tagsPool, err = r.unmarshal(s, tagsPool)
	if err != nil {
		dst = dst[:len(dst)-1]
		logger.Errorf("cannot unmarshal statsd line %q: %s", s, err)
		invalidLines.Inc()
	}
	return dst, tagsPool
}

var invalidLines = metrics.NewCounter(`vm_rows_invalid_total{type="statsd"}`)

func unmarshalTags(dst []Tag, s string) ([]Tag, error) {
	for {
		if cap(dst) > len(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, Tag{})
		}
		tag := &dst[len(dst)-1]

		n := strings.IndexByte(s, ',')
		if n < 0 {
			// The last tag found
			if err := tag.unmarshal(s); err != nil {
				return dst[:len(dst)-1], err
			}
			if len(tag.Key) == 0 || len(tag.Value) == 0 {
				// Skip empty tag
				dst = dst[:len(dst)-1]
			}
			return dst, nil
		}
		if err := tag.unmarshal(s[:n]); err != nil {
			return dst[:len(dst)-1], err
		}
		s = s[n+1:]
		if len(tag.Key) == 0 || len(tag.Value) == 0 {
			// Skip empty tag
			dst = dst[:len(dst)-1]
		}
	}
}

// Tag is a graphite tag.
type Tag struct {
	Key   string
	Value string
}

func (t *Tag) reset() {
	t.Key = ""
	t.Value = ""
}

func (t *Tag) unmarshal(s string) error {
	t.reset()
	n := strings.IndexByte(s, ':')
	if n < 0 {
		return fmt.Errorf("missing tag value for %q", s)
	}
	t.Key = s[:n]
	t.Value = s[n+1:]
	return nil
}
