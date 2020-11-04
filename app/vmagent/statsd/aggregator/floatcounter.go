package aggregator

import (
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
)

// FloatCounter is a float64 counter guarded by RWmutex.
//
// It may be used as a gauge if Add and Sub are called.
type FloatCounter struct {
	mu        sync.Mutex
	n         float64
	staleness int
}

// Add adds n to fc.
func (fc *FloatCounter) Add(n float64) {
	fc.mu.Lock()
	fc.staleness = 0
	fc.n += n
	fc.mu.Unlock()
}

// Get returns the current value for fc.
func (fc *FloatCounter) Get() float64 {
	fc.mu.Lock()
	n := fc.n
	fc.mu.Unlock()
	return n
}

// Reset sets fc value to n.
func (fc *FloatCounter) Reset() {
	fc.mu.Lock()
	fc.n = 0
	fc.mu.Unlock()
}

func (fc *FloatCounter) isStaleness(t int) bool {
	fc.mu.Lock()
	isStaleness := fc.staleness > t
	fc.mu.Unlock()
	return isStaleness
}

// marshalTo marshals fc with the given prefix to w.
func (fc *FloatCounter) marshalTo(ctx *common.PushCtx, _ string, labels []prompbmarshal.Label) {
	fc.mu.Lock()
	if fc.staleness > 0 {
		fc.mu.Unlock()
		return
	}
	fc.staleness++
	fc.mu.Unlock()

	ctx.Samples = append(ctx.Samples, prompbmarshal.Sample{
		Value:     fc.Get(),
		Timestamp: int64(fasttime.UnixTimestamp()) * 1e3,
	})
	ctx.WriteRequest.Timeseries = append(ctx.WriteRequest.Timeseries, prompbmarshal.TimeSeries{
		Labels:  labels,
		Samples: ctx.Samples[len(ctx.Samples)-1:],
	})
	fc.Reset()
}
