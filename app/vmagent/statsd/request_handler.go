package statsd

import (
	"bytes"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/statsd/aggregator"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/statsd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
)

var defaultSet = aggregator.New(64, time.Second*10, []float64{0.5, 0.9, 0.97, 0.99, 1})

// InsertHandler processes remote write for statsd plaintext protocol.
//
// See https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/?tab=metrics
func InsertHandler(r io.Reader) error {
	return writeconcurrencylimiter.Do(func() error {
		return parser.ParseStream(r, insertRows)
	})
}

func insertRows(rows []parser.Row) error {
	buffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buffer)

	for i := range rows {
		r := &rows[i]
		buffer.Reset()

		buffer.WriteString("__name__:")
		buffer.WriteString(r.Metric)

		sort.Slice(r.Tags, func(i, j int) bool {
			return r.Tags[i].Key < r.Tags[j].Key
		})

		for j := range r.Tags {
			tag := &r.Tags[j]
			buffer.WriteByte(',')
			buffer.WriteString(tag.Key)
			buffer.WriteByte(':')
			buffer.WriteString(tag.Value)
		}

		defaultSet.Insert(buffer.String(), *r)
	}
	return nil
}

var (
	bufferPool = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)
