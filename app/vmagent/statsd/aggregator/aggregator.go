package aggregator

import (
	"time"

	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/statsd"
	"github.com/cespare/xxhash/v2"
)

func New(shardCount int, flushInterval time.Duration, quantiles []float64) *Aggregator {
	shards := make([]*Set, shardCount)
	for i := 0; i < len(shards); i++ {
		shards[i] = newSet(flushInterval, quantiles)
	}
	a := &Aggregator{shards}
	go a.flushLoop(flushInterval)
	return a
}

type Aggregator struct {
	shards []*Set
}

func (a Aggregator) getShard(key string) *Set {
	return a.shards[xxhash.Sum64String(key)%uint64(len(a.shards))]
}

func (a Aggregator) Insert(key string, row parser.Row) {
	a.getShard(key).Insert(key, &row)
}

func (a Aggregator) flushLoop(flushInterval time.Duration) {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		for _, shard := range a.shards {
			shard.Flush()
		}
	}
}
