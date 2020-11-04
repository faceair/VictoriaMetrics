package statsd

import (
	"io"
	"runtime"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/statsd/aggregator"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/statsd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
)

// InsertHandler processes remote write for statsd plaintext protocol.
//
// See https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/?tab=metrics
func InsertHandler(r io.Reader) error {
	return writeconcurrencylimiter.Do(func() error {
		return parser.ParseStream(r, insertRows)
	})
}

func insertRows(rows []parser.Row) error {
	ctx := getPushCtx()
	defer putPushCtx(ctx)

	for i := range rows {
		r := &rows[i]
		ctx.addLabel("__name__", r.Metric)
		for j := range r.Tags {
			tag := &r.Tags[j]
			ctx.addLabel(tag.Key, tag.Value)
		}
		ctx.metricNameBuf = storage.MarshalMetricNameRaw(ctx.metricNameBuf[:0], ctx.labels)

		aggregator.Insert(string(ctx.metricNameBuf), *r)
	}
	return nil
}

type pushCtx struct {
	labels        []prompb.Label
	metricNameBuf []byte
}

// addLabel adds (name, value) label to ctx.labels.
//
// name and value must exist until ctx.Labels is used.
func (ctx *pushCtx) addLabel(name, value string) {
	if len(value) == 0 {
		// Skip labels without values, since they have no sense.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/600
		// Do not skip labels with empty name, since they are equal to __name__.
		return
	}
	ctx.labels = append(ctx.labels, prompb.Label{
		// Do not copy name and value contents for performance reasons.
		// This reduces GC overhead on the number of objects and allocations.
		Name:  bytesutil.ToUnsafeBytes(name),
		Value: bytesutil.ToUnsafeBytes(value),
	})
}

func (ctx *pushCtx) reset() {
	for i := range ctx.labels {
		label := &ctx.labels[i]
		label.Name = nil
		label.Value = nil
	}
	ctx.labels = ctx.labels[:0]
	ctx.metricNameBuf = ctx.metricNameBuf[:0]
}

func getPushCtx() *pushCtx {
	select {
	case ctx := <-pushCtxPoolCh:
		return ctx
	default:
		if v := pushCtxPool.Get(); v != nil {
			return v.(*pushCtx)
		}
		return &pushCtx{}
	}
}

func putPushCtx(ctx *pushCtx) {
	ctx.reset()
	select {
	case pushCtxPoolCh <- ctx:
	default:
		pushCtxPool.Put(ctx)
	}
}

var pushCtxPool sync.Pool
var pushCtxPoolCh = make(chan *pushCtx, runtime.GOMAXPROCS(-1))
