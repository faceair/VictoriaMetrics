package statsd

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/statsd/aggregator"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/statsd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
)

var (
	shardCount    = flag.Int("aggregator.shardCount", 64, "")
	flushInterval = flag.Duration("aggregator.flushInterval", time.Second*10, "")
	quantilesStr  = flag.String("aggregator.quantiles", "0.5,0.75,0.95,0.99,0.999", "")
)

var (
	defaultSet     *aggregator.Aggregator
	defaultSetOnce sync.Once
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
	defaultSetOnce.Do(func() {
		quantilesStrs := strings.Split(*quantilesStr, ",")
		quantiles := make([]float64, 0)
		for _, quantileStr := range quantilesStrs {
			quantile, err := strconv.ParseFloat(quantileStr, 64)
			if err != nil {
				panic(fmt.Errorf("invalid quantile %s, %w", quantileStr, err))
			}
			quantiles = append(quantiles, quantile)
		}
		defaultSet = aggregator.New(*shardCount, *flushInterval, quantiles)
	})

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
