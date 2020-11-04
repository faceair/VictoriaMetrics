package aggregator

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	"github.com/valyala/histogram"
)

// Summary implements summary.
type Summary struct {
	mu sync.Mutex

	curr *histogram.Fast
	next *histogram.Fast

	quantiles      []float64
	quantileValues []float64

	sum   float64
	count uint64

	window time.Duration

	staleness int
}

func newSummary(window time.Duration, quantiles []float64) *Summary {
	// Make a copy of quantiles in order to prevent from their modification by the caller.
	quantiles = append([]float64{}, quantiles...)
	validateQuantiles(quantiles)
	sm := &Summary{
		curr:           histogram.NewFast(),
		next:           histogram.NewFast(),
		quantiles:      quantiles,
		quantileValues: make([]float64, len(quantiles)),
		window:         window,
	}
	return sm
}

func validateQuantiles(quantiles []float64) {
	for _, q := range quantiles {
		if q < 0 || q > 1 {
			panic(fmt.Errorf("BUG: quantile must be in the range [0..1]; got %v", q))
		}
	}
}

// Reset reset the summary.
func (sm *Summary) Reset() {
	sm.mu.Lock()
	sm.curr.Reset()
	sm.next.Reset()
	sm.sum = 0
	sm.count = 0
	sm.mu.Unlock()
}

// Update updates the summary.
func (sm *Summary) Update(v float64) {
	sm.mu.Lock()
	sm.staleness = 0
	sm.curr.Update(v)
	sm.next.Update(v)
	sm.sum += v
	sm.count++
	sm.mu.Unlock()
}

// UpdateDuration updates request duration based on the given startTime.
func (sm *Summary) UpdateDuration(startTime time.Time) {
	d := time.Since(startTime).Seconds()
	sm.Update(d)
}

func (sm *Summary) isStaleness(t int) bool {
	sm.mu.Lock()
	isStaleness := sm.staleness > t
	sm.mu.Unlock()
	return isStaleness
}

func (sm *Summary) marshalTo(ctx *common.PushCtx, name string, labels []prompbmarshal.Label) {
	// Marshal only *_sum and *_count values.
	// Quantile values should be already updated by the caller via sm.updateQuantiles() call.
	// sm.quantileValues will be marshaled later via quantileValue.marshalTo.
	sm.mu.Lock()
	if sm.staleness > 0 {
		sm.mu.Unlock()
		return
	}
	sm.staleness++

	sum := sm.sum
	count := sm.count
	sm.mu.Unlock()

	if count > 0 {
		timestamp := int64(fasttime.UnixTimestamp()) * 1e3

		ctx.Samples = append(ctx.Samples, prompbmarshal.Sample{
			Value:     sum,
			Timestamp: timestamp,
		})
		sumLabels := append(labels, prompbmarshal.Label{
			Name:  "__name__",
			Value: name + "_sum",
		})
		ctx.WriteRequest.Timeseries = append(ctx.WriteRequest.Timeseries, prompbmarshal.TimeSeries{
			Labels:  sumLabels,
			Samples: ctx.Samples[len(ctx.Samples)-1:],
		})

		ctx.Samples = append(ctx.Samples, prompbmarshal.Sample{
			Value:     float64(count),
			Timestamp: timestamp,
		})
		countLabels := append(labels, prompbmarshal.Label{
			Name:  "__name__",
			Value: name + "_count",
		})
		ctx.WriteRequest.Timeseries = append(ctx.WriteRequest.Timeseries, prompbmarshal.TimeSeries{
			Labels:  countLabels,
			Samples: ctx.Samples[len(ctx.Samples)-1:],
		})
	}

	sm.Reset()
}

func (sm *Summary) updateQuantiles() {
	sm.mu.Lock()
	sm.quantileValues = sm.curr.Quantiles(sm.quantileValues[:0], sm.quantiles)
	sm.mu.Unlock()
}

func isEqualQuantiles(a, b []float64) bool {
	// Do not use relfect.DeepEqual, since it is slower than the direct comparison.
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type quantileValue struct {
	sm  *Summary
	idx int
}

func (qv *quantileValue) isStaleness(t int) bool {
	qv.sm.mu.Lock()
	isStaleness := qv.sm.staleness > t
	qv.sm.mu.Unlock()
	return isStaleness
}

func (qv *quantileValue) marshalTo(ctx *common.PushCtx, name string, labels []prompbmarshal.Label) {
	qv.sm.mu.Lock()
	v := qv.sm.quantileValues[qv.idx]
	qv.sm.mu.Unlock()

	if !math.IsNaN(v) {
		timestamp := int64(fasttime.UnixTimestamp()) * 1e3

		ctx.Samples = append(ctx.Samples, prompbmarshal.Sample{
			Value:     v,
			Timestamp: timestamp,
		})
		ctx.WriteRequest.Timeseries = append(ctx.WriteRequest.Timeseries, prompbmarshal.TimeSeries{
			Labels:  labels,
			Samples: ctx.Samples[len(ctx.Samples)-1:],
		})
	}
}

func registerSummaryLocked(sm *Summary) {
	window := sm.window
	summariesLock.Lock()
	summaries[window] = append(summaries[window], sm)
	if len(summaries[window]) == 1 {
		go summariesSwapCron(window)
	}
	summariesLock.Unlock()
}

func unregisterSummary(sm *Summary) {
	window := sm.window
	summariesLock.Lock()
	sms := summaries[window]
	found := false
	for i, xsm := range sms {
		if xsm == sm {
			sms = append(sms[:i], sms[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		panic(fmt.Errorf("BUG: cannot find registered summary %p", sm))
	}
	summaries[window] = sms
	summariesLock.Unlock()
}

func summariesSwapCron(window time.Duration) {
	for {
		time.Sleep(window / 2)
		summariesLock.Lock()
		for _, sm := range summaries[window] {
			sm.mu.Lock()
			tmp := sm.curr
			sm.curr = sm.next
			sm.next = tmp
			sm.next.Reset()
			sm.mu.Unlock()
		}
		summariesLock.Unlock()
	}
}

var (
	summaries     = map[time.Duration][]*Summary{}
	summariesLock sync.Mutex
)
