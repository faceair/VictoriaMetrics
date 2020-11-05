package aggregator

import (
	"fmt"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/remotewrite"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/statsd"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted  = metrics.NewCounter(`vmagent_rows_inserted_total{type="statsd"}`)
	rowsPerInsert = metrics.NewHistogram(`vmagent_rows_per_insert{type="statsd"}`)
)

type namedMetric struct {
	key    string
	name   string
	labels []prompbmarshal.Label
	metric metric
}

type metric interface {
	marshalTo(ctx *common.PushCtx, name string, labels []prompbmarshal.Label) int
}

// Set is a set of metrics.
//
// Metrics belonging to a set are exported separately from global metrics.
//
// Set.WritePrometheus must be called for exporting metrics from the set.
type Set struct {
	mu        sync.Mutex
	metrics   map[string]*namedMetric
	window    time.Duration
	quantiles []float64
	summaries []*Summary
	buf       []*namedMetric
}

// newSet creates new set of metrics.
func newSet(window time.Duration, quantiles []float64) *Set {
	return &Set{
		metrics:   make(map[string]*namedMetric),
		window:    window,
		quantiles: quantiles,
	}
}

func (s *Set) Insert(key string, row *parser.Row) {
	switch row.Type {
	case parser.CounterType:
		if counter := s.GetOrCreateFloatCounter(key, row); counter != nil {
			counter.Add(row.Value * (1 / row.SampleRate))
		}
	case parser.GaugeType:
		if gauge := s.GetOrCreateGauge(key, row); gauge != nil {
			gauge.Set(row.Value)
		}
	case parser.TimingType:
		if summary := s.GetOrCreateSummary(key, row); summary != nil {
			summary.Update(row.Value)
		}
	case parser.HistogramType:
		if histogram := s.GetOrCreateHistogram(key, row); histogram != nil {
			histogram.Update(row.Value)
		}
	}
}

func (s *Set) Flush() {
	s.mu.Lock()
	for _, sm := range s.summaries {
		sm.updateQuantiles()
	}
	s.buf = s.buf[:0]
	for _, nm := range s.metrics {
		s.buf = append(s.buf, nm)
	}
	s.mu.Unlock()

	ctx := common.GetPushCtx()
	defer common.PutPushCtx(ctx)

	for _, nm := range s.buf {
		if staleness := nm.metric.marshalTo(ctx, nm.name, nm.labels); staleness > 6 {
			s.mu.Lock()
			delete(s.metrics, nm.key)
			s.mu.Unlock()
		}
	}
	if len(ctx.WriteRequest.Timeseries) == 0 {
		return
	}
	remotewrite.Push(&ctx.WriteRequest)
	rowsInserted.Add(len(s.buf))
	rowsPerInsert.Update(float64(len(s.buf)))
}

// GetOrCreateHistogram returns registered histogram in s with the given name
// or creates new histogram if s doesn't contain histogram with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//     * foo
//     * foo{bar="baz"}
//     * foo{bar="baz",aaa="b"}
//
// The returned histogram is safe to use from concurrent goroutines.
//
// Performance tip: prefer NewHistogram instead of GetOrCreateHistogram.
func (s *Set) GetOrCreateHistogram(key string, row *parser.Row) *Histogram {
	s.mu.Lock()
	nm := s.metrics[key]
	s.mu.Unlock()
	if nm == nil {
		// Slow path - create and register missing histogram.
		name := row.Metric
		if err := validateMetric(name); err != nil {
			panic(fmt.Errorf("BUG: invalid metric name %q: %s", name, err))
		}
		nmNew := &namedMetric{
			key:    key,
			name:   name,
			labels: genRowLabels(row, false),
			metric: &Histogram{},
		}
		s.mu.Lock()
		nm = s.metrics[key]
		if nm == nil {
			nm = nmNew
			s.metrics[key] = nm
		}
		s.mu.Unlock()
	}
	h, ok := nm.metric.(*Histogram)
	if !ok {
		return nil
	}
	return h
}

// GetOrCreateFloatCounter returns registered Counter in s with the given name
// or creates new Counter if s doesn't contain Counter with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//     * foo
//     * foo{bar="baz"}
//     * foo{bar="baz",aaa="b"}
//
// The returned Counter is safe to use from concurrent goroutines.
//
// Performance tip: prefer NewFloatCounter instead of GetOrCreateFloatCounter.
func (s *Set) GetOrCreateFloatCounter(key string, row *parser.Row) *FloatCounter {
	s.mu.Lock()
	nm := s.metrics[key]
	s.mu.Unlock()
	if nm == nil {
		// Slow path - create and register missing counter.
		name := row.Metric
		if err := validateMetric(name); err != nil {
			panic(fmt.Errorf("BUG: invalid metric name %q: %s", name, err))
		}
		nmNew := &namedMetric{
			key:    key,
			name:   name,
			labels: genRowLabels(row, true),
			metric: &FloatCounter{},
		}
		s.mu.Lock()
		nm = s.metrics[key]
		if nm == nil {
			nm = nmNew
			s.metrics[key] = nm
		}
		s.mu.Unlock()
	}
	c, ok := nm.metric.(*FloatCounter)
	if !ok {
		return nil
	}
	return c
}

// GetOrCreateGauge returns registered gauge with the given name in s
// or creates new gauge if s doesn't contain gauge with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//     * foo
//     * foo{bar="baz"}
//     * foo{bar="baz",aaa="b"}
//
// The returned gauge is safe to use from concurrent goroutines.
//
// Performance tip: prefer NewGauge instead of GetOrCreateGauge.
func (s *Set) GetOrCreateGauge(key string, row *parser.Row) *Gauge {
	s.mu.Lock()
	nm := s.metrics[key]
	s.mu.Unlock()
	if nm == nil {
		// Slow path - create and register missing gauge.
		name := row.Metric
		if err := validateMetric(name); err != nil {
			panic(fmt.Errorf("BUG: invalid metric name %q: %s", name, err))
		}
		nmNew := &namedMetric{
			key:    key,
			name:   name,
			labels: genRowLabels(row, true),
			metric: &Gauge{},
		}
		s.mu.Lock()
		nm = s.metrics[key]
		if nm == nil {
			nm = nmNew
			s.metrics[key] = nm
		}
		s.mu.Unlock()
	}
	g, ok := nm.metric.(*Gauge)
	if !ok {
		return nil
	}
	return g
}

// GetOrCreateSummary returns registered summary with the given name in s
// or creates new summary if s doesn't contain summary with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//     * foo
//     * foo{bar="baz"}
//     * foo{bar="baz",aaa="b"}
//
// The returned summary is safe to use from concurrent goroutines.
//
// Performance tip: prefer NewSummary instead of GetOrCreateSummary.
func (s *Set) GetOrCreateSummary(key string, row *parser.Row) *Summary {
	return s.GetOrCreateSummaryExt(key, row, s.window, s.quantiles)
}

// GetOrCreateSummaryExt returns registered summary with the given name,
// window and quantiles in s or creates new summary if s doesn't
// contain summary with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//     * foo
//     * foo{bar="baz"}
//     * foo{bar="baz",aaa="b"}
//
// The returned summary is safe to use from concurrent goroutines.
//
// Performance tip: prefer NewSummaryExt instead of GetOrCreateSummaryExt.
func (s *Set) GetOrCreateSummaryExt(key string, row *parser.Row, window time.Duration, quantiles []float64) *Summary {
	s.mu.Lock()
	nm := s.metrics[key]
	s.mu.Unlock()
	if nm == nil {
		// Slow path - create and register missing summary.
		name := row.Metric
		if err := validateMetric(name); err != nil {
			panic(fmt.Errorf("BUG: invalid metric name %q: %s", name, err))
		}
		sm := newSummary(window, quantiles)
		nmNew := &namedMetric{
			key:    key,
			name:   name,
			labels: genRowLabels(row, false),
			metric: sm,
		}
		s.mu.Lock()
		nm = s.metrics[key]
		if nm == nil {
			nm = nmNew
			s.metrics[key] = nm
			labels := genRowLabels(row, true)
			s.registerSummaryQuantilesLocked(key, name, labels, sm)
		}
		s.summaries = append(s.summaries, sm)
		s.mu.Unlock()
	}
	sm, ok := nm.metric.(*Summary)
	if !ok {
		return nil
	}
	if sm.window != window {
		panic(fmt.Errorf("BUG: invalid window requested for the summary %q; requested %s; need %s", key, window, sm.window))
	}
	if !isEqualQuantiles(sm.quantiles, quantiles) {
		panic(fmt.Errorf("BUG: invalid quantiles requested from the summary %q; requested %v; need %v", key, quantiles, sm.quantiles))
	}
	return sm
}

func (s *Set) registerSummaryQuantilesLocked(key, name string, labels []prompbmarshal.Label, sm *Summary) {
	for i, q := range sm.quantiles {
		qv := &quantileValue{
			sm:  sm,
			idx: i,
		}
		key := fmt.Sprintf("%s,quantile=%g", key, q)
		nm, ok := s.metrics[key]
		if !ok {
			quantileLabels := append(labels, prompbmarshal.Label{
				Name:  "quantile",
				Value: fmt.Sprintf("%g", q),
			})
			nm = &namedMetric{
				key:    key,
				name:   name,
				labels: quantileLabels,
				metric: qv,
			}
			s.metrics[key] = nm
		}
		if ok {
			panic(fmt.Errorf("BUG: metric %q is already registered", name))
		}
	}
}

func genRowLabels(row *parser.Row, withName bool) []prompbmarshal.Label {
	labels := make([]prompbmarshal.Label, 0, len(row.Tags)+1)
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
