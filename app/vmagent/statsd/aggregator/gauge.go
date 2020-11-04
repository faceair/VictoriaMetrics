package aggregator

import (
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
)

// Gauge is a float64 gauge.
//
// See also Counter, which could be used as a gauge with Set and Dec calls.
type Gauge struct {
	mu        sync.Mutex
	n         float64
	staleness int
}

// Set the current value to g.
func (g *Gauge) Set(n float64) {
	g.mu.Lock()
	g.staleness = 0
	g.n = n
	g.mu.Unlock()
}

// Get returns the current value for g.
func (g *Gauge) Get() float64 {
	g.mu.Lock()
	n := g.n
	g.mu.Unlock()
	return n
}

// Reset the current value to g.
func (g *Gauge) Reset() {
	g.mu.Lock()
	g.n = 0
	g.mu.Unlock()
}

// marshalTo marshals fc with the given prefix to w.
func (g *Gauge) marshalTo(ctx *common.PushCtx, _ string, labels []prompbmarshal.Label) (staleness int) {
	g.mu.Lock()
	g.staleness++
	staleness = g.staleness
	if staleness > 1 {
		g.mu.Unlock()
		return
	}
	g.mu.Unlock()

	ctx.Samples = append(ctx.Samples, prompbmarshal.Sample{
		Value:     g.Get(),
		Timestamp: int64(fasttime.UnixTimestamp()) * 1e3,
	})
	ctx.WriteRequest.Timeseries = append(ctx.WriteRequest.Timeseries, prompbmarshal.TimeSeries{
		Labels:  labels,
		Samples: ctx.Samples[len(ctx.Samples)-1:],
	})
	g.Reset()

	return
}
